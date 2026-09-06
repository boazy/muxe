#![forbid(unsafe_code)]

//! Coordinator-side lifecycle implementation for the native `muxe` executable.
//!
//! The broker crate retains server-side activation controls, the control-protocol
//! framing, and the runtime registry. This crate coordinates: it verifies native
//! assets, owns the Zellij integration receipt and its transaction journal,
//! drives activation journals and group transactions through broker-provided
//! control clients, keeps persistent logs, and purges retained data.
//!
//! `cli` is re-exported byte-identically from the binary's `cli.rs` so the
//! release-only completion generator can build from the same `clap` definition
//! without a duplicate command module. `main.rs` must use `muxe::cli` rather
//! than declaring its own `mod cli`.

/// The public Muxe command tree (re-exported from `cli.rs`).
#[path = "cli.rs"]
pub mod cli;

pub mod compatibility;
pub mod integration;
pub mod lifecycle;
pub mod logging;
pub mod paths;
pub mod purge;

/// Crate-internal file primitives shared by the lifecycle modules.
///
/// Owner-only modes, descriptor-safe opens, atomic sibling staging files, and
/// directory synchronization live in one module so transactions, receipts,
/// logs, and KDL edits share the same durability boundary.
pub(crate) mod fsutil;
