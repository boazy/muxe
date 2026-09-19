//! Host-independent broker runtime.
//!
//! Concrete host adapters are constructor-injected through `muxe-adapter-api`; this crate never
//! imports a concrete adapter.

#![forbid(unsafe_code)]

pub mod broker;
pub mod client;
pub mod config;
pub mod gate;
pub mod runtime;
pub mod service;
pub mod spawn;
mod wire;

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
    RecoveryPermit, ServerError,
};
pub use spawn::{ServeHerdrSpawn, ServeZellijSpawn, SpawnArgvError};

/// Test-only gates parked in the terminal step of each cleanup task, between
/// the success entry-removal and the task-slot removal. Arming a gate for a
/// key pauses that task exactly in the B2 race window: the entry is gone but
/// the slot is still live, so a re-enqueue in that window observes
/// `has_live_task` and wakes instead of spawning. The terminal
/// `release_slot_unless_requeued` handshake then keeps the slot and loops to
/// process the re-enqueued entry.
#[cfg(test)]
pub(crate) mod cleanup_task_hooks {
    use std::{
        collections::HashMap,
        sync::{Arc, LazyLock, Mutex as StdMutex},
    };

    use tokio::sync::Notify;

    struct Gate {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    static GATES: LazyLock<StdMutex<HashMap<String, Arc<Gate>>>> =
        LazyLock::new(|| StdMutex::new(HashMap::new()));

    fn key_string(key: &crate::broker::CleanupTaskKey) -> String {
        match key {
            crate::broker::CleanupTaskKey::PendingPane(lease) => {
                format!("pane:{}", lease.as_str())
            }
            crate::broker::CleanupTaskKey::Capture(lease) => {
                format!("capture:{}", lease.as_str())
            }
        }
    }

    /// Arms the exit gate for one cleanup key; returns the (entered, release)
    /// notifies. The next task pass for that key notifies `entered` and waits
    /// for `release` before removing its task slot.
    #[must_use]
    pub(crate) fn arm(key: &crate::broker::CleanupTaskKey) -> (Arc<Notify>, Arc<Notify>) {
        let gate = Arc::new(Gate {
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        });
        GATES
            .lock()
            .expect("cleanup task gates are not poisoned")
            .insert(key_string(key), Arc::clone(&gate));
        (Arc::clone(&gate.entered), Arc::clone(&gate.release))
    }

    /// Clears every armed gate without releasing waiters.
    pub(crate) fn clear() {
        GATES
            .lock()
            .expect("cleanup task gates are not poisoned")
            .clear();
    }

    pub(crate) async fn exit_gate(key: &crate::broker::CleanupTaskKey) {
        let gate = GATES
            .lock()
            .expect("cleanup task gates are not poisoned")
            .get(&key_string(key))
            .cloned();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
    }
}
