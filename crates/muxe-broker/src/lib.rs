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

pub use broker::{Broker, BrokerError, PendingAttachment, RequestResult};
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
