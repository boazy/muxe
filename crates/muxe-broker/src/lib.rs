//! Host-independent broker runtime.
//!
//! Concrete host adapters are constructor-injected through `muxe-adapter-api`; this crate never
//! imports a concrete adapter.

#![forbid(unsafe_code)]

#[cfg(test)]
#[path = "../../muxe/tests/support/generated_executable.rs"]
mod generated_executable;

pub mod broker;
pub mod client;
pub mod config;
pub mod gate;
pub mod runtime;
pub mod service;
pub mod spawn;
mod wire;

#[cfg(test)]
#[expect(
    dead_code,
    reason = "broker reconnect proofs share the adapter's complete production-connect fixture"
)]
#[path = "../../muxe-adapter-herdr/tests/support/production_connect.rs"]
pub(crate) mod production_connect;
#[cfg(test)]
#[expect(
    dead_code,
    reason = "broker reconnect proofs share the adapter's complete recorded socket fixture"
)]
#[path = "../../muxe-adapter-herdr/tests/support/recorded_socket.rs"]
pub(crate) mod recorded_socket;

pub use broker::{Broker, BrokerDiagnostic, BrokerError, PendingAttachment, RequestResult};
pub use client::{BrokerClient, ClientError};
pub use config::{ConfigError, ConfigSnapshot, ConfigStore, load_effective_config};
pub use gate::{
    AttachDisposition, GateError, LaunchGate, OsTokenSource, PendingLaunch, PreparedLaunch,
    RegisteredPane, ScopeOwner, TokenSource,
};
pub use runtime::{
    RuntimeEndpoint, RuntimeError, StartupLock, validate_owner_directory, validate_owner_file,
};
pub use service::{
    ActivationBootstrap, BrokerServer, RecoveryAck, RecoveryDecision, RecoveryJournal,
    RecoveryPermit, RetiredRegistrationProof, ServerError,
};
pub use spawn::{ServeHerdrSpawn, ServeZellijSpawn, SpawnArgvError};

/// Test-only, supervisor-owned gates parked between cleanup entry removal and
/// task-slot removal. Each gate pauses one terminal pass in the B2 race window;
/// its guard disarms and releases only that supervisor's gate on drop.
#[cfg(test)]
pub(crate) mod cleanup_task_hooks {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex as StdMutex},
    };

    use tokio::sync::Notify;

    struct Gate {
        entered: Notify,
        release: Notify,
    }

    #[derive(Default)]
    pub(crate) struct ExitGates {
        gates: StdMutex<HashMap<crate::broker::CleanupTaskKey, Arc<Gate>>>,
    }

    pub(crate) struct ExitGateGuard<'a> {
        owner: &'a ExitGates,
        key: crate::broker::CleanupTaskKey,
        gate: Arc<Gate>,
    }

    impl ExitGates {
        /// Arms one terminal pass for this supervisor's cleanup key.
        pub(crate) fn arm(&self, key: &crate::broker::CleanupTaskKey) -> ExitGateGuard<'_> {
            let gate = Arc::new(Gate {
                entered: Notify::new(),
                release: Notify::new(),
            });
            let mut gates = self
                .gates
                .lock()
                .expect("cleanup task gates are not poisoned");
            assert!(
                !gates.contains_key(key),
                "cleanup exit gate is already armed"
            );
            gates.insert(key.clone(), Arc::clone(&gate));
            ExitGateGuard {
                owner: self,
                key: key.clone(),
                gate,
            }
        }

        pub(crate) async fn exit_gate(&self, key: &crate::broker::CleanupTaskKey) {
            let gate = self
                .gates
                .lock()
                .expect("cleanup task gates are not poisoned")
                .remove(key);
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }
    }

    impl ExitGateGuard<'_> {
        pub(crate) async fn entered(&self) {
            self.gate.entered.notified().await;
        }
    }

    impl Drop for ExitGateGuard<'_> {
        fn drop(&mut self) {
            let mut gates = self
                .owner
                .gates
                .lock()
                .expect("cleanup task gates are not poisoned");
            if gates
                .get(&self.key)
                .is_some_and(|gate| Arc::ptr_eq(gate, &self.gate))
            {
                gates.remove(&self.key);
            }
            drop(gates);
            self.gate.release.notify_one();
        }
    }
}
