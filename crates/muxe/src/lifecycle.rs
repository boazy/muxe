//! Transactional activation, retirement, and the broker registry.
//!
//! Changing the selected Muxe version and activating it are separate
//! operations. The coordinator (this module) preflights every selected
//! activation unit before mutating any of them, writes an owner-only journal
//! before the first external mutation, and converges each unit to either the
//! complete old stack or the complete target stack. Units commit independently
//! after global preflight; a later unit failure never rolls back an already
//! healthy unit.
//!
//! The broker crate retains server-side drain, supervision, and
//! control-protocol serving. This module owns the coordinator client, the
//! journals, the group transactions, and the owner-only broker registry used
//! for host selection.

pub mod activate;
pub mod control;
pub mod journal;
pub mod registry;
pub mod retire;

pub use activate::{
    ActivateError, ActivateHooks, ActivateInputs, ActivateReport, ActivateStep, BrokerSpawner,
    ControlPort, ControlSession, DetectedHost, HostReloader, LiveControl, Preflight,
    ProcessSpawner, RecoveryOutcome, SpawnMember, SpawnRequest, StagedBridge, TargetHandle,
    UnitOutcome, ZellijCliReloader, activate, recover,
};
pub use journal::{JournalState, MemberState, MemberTransition, UnitKind};
pub use registry::{BrokerEntry, Liveness, Registry};
pub use retire::{RetireError, RetireInputs, RetireOutcome, RetireReport, retire};
