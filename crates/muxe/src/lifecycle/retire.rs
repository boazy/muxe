//! Broker retirement without replacement.
//!
//! `muxe broker retire` drains active menus and foreground work through the
//! same broker drain state machine as activation, then removes the active
//! endpoint without starting a replacement. Detached generic children remain
//! under supervisor-only processes until reaped. Retirement is idempotent only
//! when the control endpoint is positively absent: a missing broker is
//! reported as already gone, never as a failure. This is the pre-uninstall
//! path.

use std::path::Path;

use thiserror::Error;

use crate::{cli::HostScope, logging::Logger};

use super::{
    activate::{
        ActivateError, ControlPort, ControlSession, DetectedHost, PlannedUnit, select_units,
        unit_label,
    },
    control::ControlError,
    registry::{BrokerEntry, Registry, RegistryError},
};

#[derive(Debug, Error)]
pub enum RetireError {
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Control(#[from] ControlError),
    #[error(transparent)]
    Activate(#[from] ActivateError),
    #[error("--host current requires invocation from a managed host")]
    CurrentHostRequired,
    #[error("auditable operation cannot proceed without its log record")]
    Audit(#[from] crate::logging::LogError),
}

/// Outcome of retiring one unit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetireOutcome {
    Retired { unit: String },
    AlreadyGone { unit: String },
}

/// Final retirement report naming every unit outcome.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetireReport {
    pub units: Vec<RetireOutcome>,
}

/// Inputs for `muxe broker retire`.
pub struct RetireInputs<'a, C> {
    pub cache_dir: &'a Path,
    pub scope: HostScope,
    pub current: Option<DetectedHost>,
    pub control: &'a C,
    pub logger: Option<&'a Logger>,
}

/// Retires every selected broker without starting a replacement.
///
/// # Errors
///
/// Fails when the registry or audit log is unusable, `--host current` has no
/// detectable invoking host, or a unit retirement fails; per-unit outcomes
/// otherwise report through the returned report.
pub async fn retire<C>(inputs: RetireInputs<'_, C>) -> Result<RetireReport, RetireError>
where
    C: ControlPort,
{
    let registry = Registry::open(inputs.cache_dir)?;
    // Selection runs over recorded entries, not just live probes: a stale
    // socket still needs its record retired idempotently.
    let entries = registry.entries()?;
    let units = select_units(&entries, inputs.scope, inputs.current.as_ref())?;
    let mut report = RetireReport::default();
    for unit in units {
        report
            .units
            .push(retire_unit(inputs.control, &registry, &unit, inputs.logger).await?);
    }
    Ok(report)
}

async fn retire_unit<C>(
    control: &C,
    registry: &Registry,
    unit: &PlannedUnit,
    logger: Option<&Logger>,
) -> Result<RetireOutcome, RetireError>
where
    C: ControlPort,
{
    let label = unit_label(unit);
    let entries: Vec<&BrokerEntry> = match unit {
        PlannedUnit::Herdr { entry } => vec![entry],
        PlannedUnit::Zellij { entries, .. } => entries.iter().collect(),
    };
    let mut retired_any = false;
    for entry in entries {
        // One retained session per member: retire, then drop the record.
        // Removal is scoped to the exact observed entry so a replacement
        // that rebound the same socket is never deleted.
        let outcome = match control.connect(&entry.socket).await {
            Ok(mut session) => session.retire().await.map(|_| ()),
            Err(error) if error.is_absent_endpoint() => {
                registry.unregister_entry(entry)?;
                continue;
            }
            Err(error) => Err(error),
        };
        match outcome {
            Ok(()) => {
                retired_any = true;
                registry.unregister_entry(entry)?;
            }
            Err(error) => {
                log(
                    logger,
                    &label,
                    &format!("retire of {} failed: {error}", entry.socket.display()),
                )?;
                return Err(error.into());
            }
        }
    }
    if retired_any {
        log(logger, &label, "broker retired")?;
        Ok(RetireOutcome::Retired { unit: label })
    } else {
        Ok(RetireOutcome::AlreadyGone { unit: label })
    }
}

fn log(logger: Option<&Logger>, unit: &str, message: &str) -> Result<(), RetireError> {
    if let Some(logger) = logger {
        // Messages carry identifiers and state only, never payloads.
        let event =
            crate::logging::LogEvent::new(logger.version().to_owned(), unit, "retire", message)?;
        logger.append(&event)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::activate::ControlSession;
    use crate::lifecycle::registry::{BrokerEntry, REGISTRY_DIR_NAME, REGISTRY_FILE_NAME};
    use muxe_protocol::control::{ActivationStatus, CompatibilityRecord};
    use muxe_protocol::wire::{HostKind, LiveServerIdentity, ServerId};
    use std::{io, path::PathBuf};

    fn status() -> ActivationStatus {
        ActivationStatus {
            lifecycle: muxe_protocol::control::LifecycleState::Retired,
            live_server: LiveServerIdentity {
                host: HostKind::Herdr,
                discovery_key: "server".to_owned(),
                server_id: ServerId::new("id"),
            },
            current: CompatibilityRecord {
                muxe_version: "0.1.0".to_owned(),
                target_triple: "test".to_owned(),
                application_schema_fingerprint: muxe_protocol::SchemaFingerprint([1; 32]),
                zellij: None,
                herdr: None,
            },
            target: None,
            handoff_id: None,
            ready: None,
        }
    }

    #[derive(Clone, Copy)]
    struct GoneSession;

    impl ControlSession for GoneSession {
        async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn prepare(
            &mut self,
            _target: &CompatibilityRecord,
        ) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn commit(
            &mut self,
            _handoff: &muxe_protocol::control::HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn abort(
            &mut self,
            _handoff: &muxe_protocol::control::HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
    }

    #[derive(Clone, Copy)]
    struct GoneControl;

    impl ControlPort for GoneControl {
        type Session = GoneSession;
        async fn connect(&self, socket: &Path) -> Result<GoneSession, ControlError> {
            Err(ControlError::Connect {
                socket: socket.to_owned(),
                source: io::Error::from(io::ErrorKind::NotFound),
            })
        }
    }

    #[derive(Clone, Copy)]
    enum Failure {
        Closed,
        Timeout,
        Malformed,
        Permission,
    }

    impl Failure {
        fn error(self) -> ControlError {
            match self {
                Self::Closed => ControlError::Closed,
                Self::Timeout => ControlError::Timeout,
                Self::Malformed => ControlError::UnexpectedResult("non-retired"),
                Self::Permission => {
                    ControlError::Io(io::Error::from(io::ErrorKind::PermissionDenied))
                }
            }
        }
    }

    #[derive(Clone, Copy)]
    struct FailingSession {
        failure: Failure,
    }

    impl ControlSession for FailingSession {
        async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
            Err(self.failure.error())
        }
        async fn prepare(
            &mut self,
            _target: &CompatibilityRecord,
        ) -> Result<ActivationStatus, ControlError> {
            Err(self.failure.error())
        }
        async fn commit(
            &mut self,
            _handoff: &muxe_protocol::control::HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Err(self.failure.error())
        }
        async fn abort(
            &mut self,
            _handoff: &muxe_protocol::control::HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Err(self.failure.error())
        }
        async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
            Err(self.failure.error())
        }
    }

    #[derive(Clone, Copy)]
    struct FailingControl {
        failure: Failure,
    }

    impl ControlPort for FailingControl {
        type Session = FailingSession;
        async fn connect(&self, _socket: &Path) -> Result<FailingSession, ControlError> {
            Ok(FailingSession {
                failure: self.failure,
            })
        }
    }

    #[derive(Clone, Copy)]
    struct RefusedControl;

    impl ControlPort for RefusedControl {
        type Session = GoneSession;
        async fn connect(&self, socket: &Path) -> Result<GoneSession, ControlError> {
            Err(ControlError::Connect {
                socket: socket.to_owned(),
                source: io::Error::from(io::ErrorKind::ConnectionRefused),
            })
        }
    }

    struct CleanupFailureSession {
        registry_file: PathBuf,
        backup_file: PathBuf,
    }

    impl ControlSession for CleanupFailureSession {
        async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn prepare(
            &mut self,
            _target: &CompatibilityRecord,
        ) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn commit(
            &mut self,
            _handoff: &muxe_protocol::control::HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn abort(
            &mut self,
            _handoff: &muxe_protocol::control::HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
            std::fs::rename(&self.registry_file, &self.backup_file).map_err(ControlError::Io)?;
            std::fs::create_dir(&self.registry_file).map_err(ControlError::Io)?;
            Ok(status())
        }
    }

    struct CleanupFailureControl {
        cache: PathBuf,
    }

    impl ControlPort for CleanupFailureControl {
        type Session = CleanupFailureSession;
        async fn connect(&self, _socket: &Path) -> Result<CleanupFailureSession, ControlError> {
            let directory = self.cache.join(REGISTRY_DIR_NAME);
            Ok(CleanupFailureSession {
                registry_file: directory.join(REGISTRY_FILE_NAME),
                backup_file: directory.join("registry.json.retire-test-backup"),
            })
        }
    }

    #[derive(Clone, Copy)]
    struct LiveSession;

    impl ControlSession for LiveSession {
        async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
            Ok(status())
        }
        async fn prepare(
            &mut self,
            _target: &CompatibilityRecord,
        ) -> Result<ActivationStatus, ControlError> {
            Ok(status())
        }
        async fn commit(
            &mut self,
            _handoff: &muxe_protocol::control::HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Ok(status())
        }
        async fn abort(
            &mut self,
            _handoff: &muxe_protocol::control::HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Ok(status())
        }
        async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
            Ok(status())
        }
    }

    #[derive(Clone, Copy)]
    struct LiveRetireControl;

    impl ControlPort for LiveRetireControl {
        type Session = LiveSession;
        async fn connect(&self, _socket: &Path) -> Result<LiveSession, ControlError> {
            Ok(LiveSession)
        }
    }

    fn register(cache: &Path, socket: PathBuf) {
        Registry::open(cache)
            .unwrap()
            .register(BrokerEntry::now("herdr", "server", socket, 1))
            .unwrap();
    }

    #[tokio::test]
    async fn retire_is_idempotent_when_endpoint_is_absent() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let cache = temp.path().join("cache");
        register(&cache, cache.join("gone.sock"));
        let control = GoneControl;
        let report = retire(RetireInputs {
            cache_dir: &cache,
            scope: HostScope::Herdr,
            current: None,
            control: &control,
            logger: None,
        })
        .await
        .unwrap();
        assert_eq!(
            report.units,
            vec![RetireOutcome::AlreadyGone {
                unit: "herdr:server".to_owned()
            }]
        );
        assert!(
            Registry::open(&cache)
                .unwrap()
                .entries()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn retire_drains_and_unregisters() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let cache = temp.path().join("cache");
        register(&cache, cache.join("live.sock"));
        let control = LiveRetireControl;
        let report = retire(RetireInputs {
            cache_dir: &cache,
            scope: HostScope::Herdr,
            current: None,
            control: &control,
            logger: None,
        })
        .await
        .unwrap();
        assert_eq!(
            report.units,
            vec![RetireOutcome::Retired {
                unit: "herdr:server".to_owned()
            }]
        );
        assert!(
            Registry::open(&cache)
                .unwrap()
                .entries()
                .unwrap()
                .is_empty()
        );
    }
    #[tokio::test]
    async fn refused_connection_is_error_and_retains_record() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let cache = temp.path().join("cache");
        let socket = cache.join("refused.sock");
        register(&cache, socket);
        let error = retire(RetireInputs {
            cache_dir: &cache,
            scope: HostScope::Herdr,
            current: None,
            control: &RefusedControl,
            logger: None,
        })
        .await
        .expect_err("connection refusal is not proof of absence");
        assert!(matches!(
            error,
            RetireError::Control(ControlError::Connect { source, .. })
                if source.kind() == io::ErrorKind::ConnectionRefused
        ));
        assert_eq!(Registry::open(&cache).unwrap().entries().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn retire_failures_are_errors_and_retain_record() {
        for (failure, diagnostic) in [
            (Failure::Closed, "closed"),
            (Failure::Timeout, "timed out"),
            (Failure::Malformed, "unexpected"),
            (Failure::Permission, "permission denied"),
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            std::fs::set_permissions(
                temp.path(),
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
            let cache = temp.path().join("cache");
            register(&cache, cache.join("failed.sock"));
            let error = retire(RetireInputs {
                cache_dir: &cache,
                scope: HostScope::Herdr,
                current: None,
                control: &FailingControl { failure },
                logger: None,
            })
            .await
            .expect_err("a failed retire must not report absence");
            assert!(
                error.to_string().contains(diagnostic),
                "missing diagnostic in {error}"
            );
            assert_eq!(Registry::open(&cache).unwrap().entries().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn registry_cleanup_failure_is_error_and_retains_record() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let cache = temp.path().join("cache");
        register(&cache, cache.join("cleanup-failure.sock"));
        let registry_dir = cache.join(REGISTRY_DIR_NAME);
        let registry_file = registry_dir.join(REGISTRY_FILE_NAME);
        let backup_file = registry_dir.join("registry.json.retire-test-backup");
        let error = retire(RetireInputs {
            cache_dir: &cache,
            scope: HostScope::Herdr,
            current: None,
            control: &CleanupFailureControl {
                cache: cache.clone(),
            },
            logger: None,
        })
        .await
        .expect_err("registry cleanup failure must be reported");
        assert!(matches!(error, RetireError::Registry(_)));
        std::fs::remove_dir(&registry_file).unwrap();
        std::fs::rename(&backup_file, &registry_file).unwrap();
        assert_eq!(Registry::open(&cache).unwrap().entries().unwrap().len(), 1);
    }
}
