//! Client-scoped bridge state machine with injected host effects.
//!
//! All Zellij host calls flow through [`HostEffects`], so transition tests
//! drive a fake and assert exact emitted commands, capture state, and pipe
//! targeting without any host. Production wires [`ShimEffects`] to the real
//! plugin shims.
//!
//! ## Pipe identity contract (pinned evidence)
//!
//! A CLI child gets a fresh UUID `pipe_id` per invocation
//! (`zellij-utils/src/input/actions.rs`, `CliAction::Pipe`: `pipe_id =
//! Uuid::new_v4()`), distinct from the semantic `--name`. The plugin receives
//! `PipeSource::Cli(pipe_id)` with `message.name` set to the semantic name.
//! The server routes `unblock`/`output` by that ID (`get_pipe`), falling back
//! to broadcast only when the ID is unknown — and pipe clients drop output
//! whose name does not equal their own UUID. Therefore:
//!
//! - The bridge retains the CLI source UUID per pipe role and addresses
//!   `unblock` (request UUID) and `output` (event UUID) with those UUIDs.
//!   Addressing either with the semantic name black-holes output and
//!   broadcast-unblocks the event child, which then stalls the event stream
//!   while waiting on stdin (pinned `pipe_client` state machine).
//! - The semantic name only routes: `muxe-request-*` frames versus
//!   `muxe-event-*` subscriptions.
//!
//! ## Capture contract
//!
//! Capture uses Zellij's existing Locked mode without touching bindings. The
//! prior mode is retained as a typed [`InputMode`]: nothing is ever restored
//! from a guessed name, and an unobserved prior degrades to Locked (safe in
//! both branches — restoring Locked while still Locked is identity, and a
//! user-driven mode change skips restoration). Begin waits for an actually
//! observed mode before requesting Locked; the broker's capture timeout bounds
//! the wait. A mode change away from Locked while capture is active is
//! user-owned newer state: the menu is dismissed and the snapshot is never
//! restored over it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::str::FromStr;

use muxe_zellij_protocol::{
    BRIDGE_PERMISSIONS, BRIDGE_PROTOCOL_VERSION, BridgeIdentity, BridgeRequest, CaptureEndReason,
    CaptureLostReason, CommandOutcome, PipeEvent, PipeEventKind, ZellijOrigin, bridge_build_id,
    bridge_protocol_fingerprint, decode_request_line, encode_event_line,
    generated::RawNativeCommand, generated_action_fingerprint, pinned_source_revision,
};
use zellij_tile::prelude::*;

use crate::{
    dispatcher::{ReadyDispatch, completion_execution, execute_sync, prepare},
    focus::{PaneGeometry, PaneInventory},
};

/// Pipe-name prefixes separating the two channels by role.
const REQUEST_PREFIX: &str = "muxe-request-";
const EVENT_PREFIX: &str = "muxe-event-";

/// Heartbeat period in seconds, well under the broker's 15-second lease.
const HEARTBEAT_SECS: f64 = 5.0;

/// Narrow host-effects boundary: every nondeterministic host interaction the
/// bridge needs. Production implements it with plugin shims; tests inject a
/// recording fake.
pub trait HostEffects {
    /// Requests plugin permissions.
    fn request_permissions(&mut self, permissions: &[PermissionType]);
    /// Subscribes to host events.
    fn subscribe(&mut self, events: &[EventType]);
    /// Asks the host for the client list (answered with `ListClients`).
    fn list_clients(&mut self);
    /// Returns this plugin instance's IDs.
    fn plugin_ids(&mut self) -> PluginIds;
    /// Returns a pane's working directory, if the host exposes it.
    fn pane_cwd(&mut self, pane: PaneId) -> Option<PathBuf>;
    /// Writes one event line to the CLI child with this source UUID.
    fn pipe_output(&mut self, cli_id: &str, line: &str);
    /// Blocks the CLI child with this source UUID until an explicit release.
    fn block_pipe(&mut self, cli_id: &str);
    /// Unblocks the CLI child with this source UUID.
    fn unblock_pipe(&mut self, cli_id: &str);
    /// Requests a host input-mode change.
    fn switch_mode(&mut self, mode: InputMode);
    /// Focuses one host pane by ID.
    fn focus_pane(&mut self, pane: PaneId);
    /// Fills bytes from OS randomness; false when unavailable.
    fn fill_random(&mut self, bytes: &mut [u8]) -> bool;
    /// Arms a one-shot host timer delivering `Timer` after secs.
    fn arm_timer(&mut self, secs: f64);
}

/// Production effects delegating to the pinned plugin shims.
#[derive(Default)]
pub struct ShimEffects;

impl HostEffects for ShimEffects {
    fn request_permissions(&mut self, permissions: &[PermissionType]) {
        request_permission(permissions);
    }

    fn subscribe(&mut self, events: &[EventType]) {
        subscribe(events);
    }

    fn list_clients(&mut self) {
        list_clients();
    }

    fn plugin_ids(&mut self) -> PluginIds {
        get_plugin_ids()
    }

    fn pane_cwd(&mut self, pane: PaneId) -> Option<PathBuf> {
        get_pane_cwd(pane).ok()
    }

    fn pipe_output(&mut self, cli_id: &str, line: &str) {
        cli_pipe_output(cli_id, line);
    }
    fn block_pipe(&mut self, cli_id: &str) {
        block_cli_pipe_input(cli_id);
    }

    fn unblock_pipe(&mut self, cli_id: &str) {
        unblock_cli_pipe_input(cli_id);
    }

    fn switch_mode(&mut self, mode: InputMode) {
        switch_to_input_mode(&mode);
    }

    fn focus_pane(&mut self, pane: PaneId) {
        focus_pane_with_id(pane, false, false);
    }

    fn fill_random(&mut self, bytes: &mut [u8]) -> bool {
        getrandom::getrandom(bytes).is_ok()
    }

    fn arm_timer(&mut self, secs: f64) {
        set_timeout(secs);
    }
}

struct PendingCapture {
    lease: [u8; 16],
    prior: Option<InputMode>,
    requested: bool,
}

struct ActiveCapture {
    lease: [u8; 16],
    prior: InputMode,
}

/// Permission gate for privileged host queries: the bridge requests
/// permissions at load but issues no privileged query before the host's
/// explicit grant event. The pinned host delivers
/// `PermissionRequestResult` regardless of subscription
/// (`wasm_bridge.rs`, event fan-out exempts it), so no subscription entry
/// is needed for the grant to arrive.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PermissionGate {
    /// `request_permissions` sent, no grant event observed yet.
    Pending,
    /// Host delivered `PermissionRequestResult(Granted)`.
    Granted,
    /// Host delivered `PermissionRequestResult(Denied)`.
    Denied,
}

/// Bridge state for one Zellij client. Identity fields stay empty until
/// verified host data arrives; no registration emits before that.
pub struct Bridge {
    client_id: Option<String>,
    plugin_id: Option<u32>,
    focused_pane: Option<String>,
    permission_gate: PermissionGate,
    event_cli_id: Option<String>,
    pending_subscribe: bool,
    registration: [u8; 16],
    sequence: u64,
    current_mode: Option<InputMode>,
    pending: Option<PendingCapture>,
    active: Option<ActiveCapture>,
    muxe_panes: BTreeSet<String>,
    last_non_muxe_pane: Option<String>,
    origin_pane: Option<String>,
    inventory: PaneInventory,
    pending_actions: BTreeMap<String, [u8; 16]>,
}

impl Default for Bridge {
    fn default() -> Self {
        Self {
            client_id: None,
            plugin_id: None,
            focused_pane: None,
            permission_gate: PermissionGate::Pending,
            event_cli_id: None,
            pending_subscribe: false,
            registration: [0; 16],
            sequence: 0,
            current_mode: None,
            pending: None,
            active: None,
            muxe_panes: BTreeSet::new(),
            last_non_muxe_pane: None,
            origin_pane: None,
            inventory: PaneInventory::new(),
            pending_actions: BTreeMap::new(),
        }
    }
}

impl Bridge {
    /// Initial plugin load: permission request, control subscriptions,
    /// plugin IDs, and the first heartbeat timer. The privileged identity
    /// query stays deferred until the host's explicit grant event: the
    /// grant arrives asynchronously after the request, and a pre-grant
    /// query is denied by the host.
    pub fn load(&mut self, effects: &mut dyn HostEffects) {
        effects.request_permissions(&BRIDGE_PERMISSIONS);
        effects.subscribe(&[
            EventType::ListClients,
            EventType::ModeUpdate,
            EventType::PaneUpdate,
            EventType::TabUpdate,
            EventType::ActionComplete,
            EventType::Timer,
        ]);
        self.plugin_id = Some(effects.plugin_ids().plugin_id);
        effects.arm_timer(HEARTBEAT_SECS);
    }

    /// Host event pump.
    pub fn update(&mut self, event: Event, effects: &mut dyn HostEffects) {
        match event {
            Event::ListClients(clients) => self.on_list_clients(&clients, effects),
            Event::ModeUpdate(mode) => self.on_mode_update(&mode, effects),
            Event::PaneUpdate(manifest) => self.on_pane_update(&manifest),
            Event::TabUpdate(tabs) => self.on_tab_update(&tabs),
            Event::ActionComplete(_, _, context) => self.on_action_complete(&context, effects),
            Event::Timer(_) => self.on_timer(effects),
            Event::PermissionRequestResult(status) => {
                match status {
                    // Idempotent: only the transition into Granted issues
                    // the deferred identity query, so repeats never
                    // duplicate it. A later grant after denial (user
                    // answers the host prompt) legitimately queries once.
                    PermissionStatus::Granted => {
                        if self.permission_gate != PermissionGate::Granted {
                            self.permission_gate = PermissionGate::Granted;
                            effects.list_clients();
                        }
                    }
                    PermissionStatus::Denied => {
                        self.permission_gate = PermissionGate::Denied;
                    }
                }
            }
            Event::BeforeClose => self.on_before_close(effects),
            _ => {}
        }
    }

    /// Pipe message pump. The CLI source UUID addresses the sending child;
    /// the semantic name only selects the channel role.
    pub fn pipe(&mut self, message: PipeMessage, effects: &mut dyn HostEffects) {
        let PipeMessage {
            source,
            name,
            payload,
            ..
        } = message;
        match source {
            PipeSource::Cli(cli_id) => {
                if name.starts_with(EVENT_PREFIX) {
                    self.subscribe(cli_id, effects);
                } else if name.starts_with(REQUEST_PREFIX) {
                    self.on_request(&cli_id, payload, effects);
                }
            }
            PipeSource::Plugin(_) | PipeSource::Keybind => {}
        }
    }
    /// Test-visible client identity.
    #[cfg(test)]
    pub fn client_identity(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Test-visible active registration.
    #[cfg(test)]
    pub fn active_registration(&self) -> Option<[u8; 16]> {
        (self.registration != [0; 16]).then_some(self.registration)
    }

    /// Test-visible capture state: pending lease, active lease, or none.
    #[cfg(test)]
    pub fn capture_state(&self) -> (Option<[u8; 16]>, Option<[u8; 16]>) {
        (
            self.pending.as_ref().map(|p| p.lease),
            self.active.as_ref().map(|a| a.lease),
        )
    }

    fn on_list_clients(&mut self, clients: &[ClientInfo], effects: &mut dyn HostEffects) {
        for client in clients {
            if client.is_current_client {
                self.client_id = Some(client.client_id.to_string());
                let focused = format!("{}", client.pane_id);
                // Focus history advances only while no menu owns capture and
                // only for panes outside the known MUXE set: a focused pane
                // during capture, or a known menu pane, is never origin.
                if self.active.is_none()
                    && self.pending.is_none()
                    && !self.muxe_panes.contains(&focused)
                    && self.focused_pane.as_deref() != Some(focused.as_str())
                {
                    self.last_non_muxe_pane = Some(focused.clone());
                }
                self.focused_pane = Some(focused);
                break;
            }
        }
        self.try_register(effects);
    }

    fn on_mode_update(&mut self, mode: &ModeInfo, effects: &mut dyn HostEffects) {
        let locked = mode.mode == InputMode::Locked;
        self.current_mode = Some(mode.mode);
        if locked {
            if let Some(pending) = self.pending.take() {
                // An unobserved prior degrades to Locked: restoring Locked
                // while still Locked is identity, and a later user-driven mode
                // change skips restoration entirely.
                let prior = pending.prior.unwrap_or(InputMode::Locked);
                let lease = pending.lease;
                self.active = Some(ActiveCapture { lease, prior });
                self.emit(
                    PipeEventKind::CaptureReady {
                        lease,
                        prior_mode: format!("{prior:?}"),
                    },
                    effects,
                );
            }
            return;
        }
        // A first observation while capture is pending snapshots the true
        // prior; the Locked request goes out only now, never on assumed state.
        if let Some(pending) = &mut self.pending {
            pending.prior = Some(mode.mode);
            if !pending.requested {
                pending.requested = true;
                effects.switch_mode(InputMode::Locked);
            }
        }
        // Away from Locked with an active capture is user-owned newer state:
        // dismiss without restoring the older snapshot.
        if let Some(active) = self.active.take() {
            self.emit(
                PipeEventKind::CaptureLost {
                    lease: active.lease,
                    reason: CaptureLostReason::UserModeChanged,
                },
                effects,
            );
        }
    }

    fn on_pane_update(&mut self, manifest: &PaneManifest) {
        let mut panes = BTreeMap::new();
        for (tab, infos) in &manifest.panes {
            panes.insert(
                *tab,
                infos
                    .iter()
                    .map(|info| PaneGeometry {
                        id: info.id,
                        is_plugin: info.is_plugin,
                        x: info.pane_x,
                        y: info.pane_y,
                        columns: info.pane_columns,
                        rows: info.pane_rows,
                    })
                    .collect(),
            );
        }
        self.inventory.set_manifest(panes);
    }

    fn on_tab_update(&mut self, tabs: &[TabInfo]) {
        let active = tabs.iter().find(|tab| tab.active).map(|tab| tab.position);
        self.inventory.set_active_tab(active);
    }

    fn on_action_complete(
        &mut self,
        context: &BTreeMap<String, String>,
        effects: &mut dyn HostEffects,
    ) {
        let Some(execution) = completion_execution(context) else {
            return;
        };
        if let Some(request_id) = self.pending_actions.remove(execution) {
            self.emit(
                PipeEventKind::DispatchCompleted {
                    request_id,
                    execution: execution.to_owned(),
                    outcome: CommandOutcome::succeeded(),
                },
                effects,
            );
        }
    }

    fn on_timer(&mut self, effects: &mut dyn HostEffects) {
        // Heartbeats renew the broker-side heartbeat lease; without them an
        // idle healthy bridge would be expired by the registry.
        if self.registration != [0; 16]
            && let Some(client_id) = self.client_id.clone()
        {
            self.emit(
                PipeEventKind::Heartbeat {
                    registration: self.registration,
                    client_id,
                },
                effects,
            );
        }
        effects.arm_timer(HEARTBEAT_SECS);
    }

    fn on_before_close(&mut self, effects: &mut dyn HostEffects) {
        // Unloading with owned Locked capture restores the guarded prior;
        // pending async completions fail instead of dangling.
        if let Some(active) = self.active.take()
            && self.current_mode == Some(InputMode::Locked)
        {
            effects.switch_mode(active.prior);
            self.emit(
                PipeEventKind::CaptureLost {
                    lease: active.lease,
                    reason: CaptureLostReason::BridgeUnloading,
                },
                effects,
            );
        }
        self.pending = None;
        let pending: Vec<(String, [u8; 16])> = std::mem::take(&mut self.pending_actions)
            .into_iter()
            .collect();
        for (execution, request_id) in pending {
            self.emit(
                PipeEventKind::DispatchCompleted {
                    request_id,
                    execution,
                    outcome: CommandOutcome::failed("bridge unloading".to_owned()),
                },
                effects,
            );
        }
    }

    /// New event channel: guarded-restore any still-owned Locked capture
    /// first, then reset epoch state so old completions can never reattach
    /// under the fresh registration below.
    fn subscribe(&mut self, cli_id: String, effects: &mut dyn HostEffects) {
        if let Some(active) = self.active.take()
            && self.current_mode == Some(InputMode::Locked)
        {
            effects.switch_mode(active.prior);
        }
        self.pending = None;
        self.pending_actions.clear();
        effects.block_pipe(&cli_id);
        self.event_cli_id = Some(cli_id);
        self.pending_subscribe = true;
        self.try_register(effects);
    }

    /// Emits a registration once the permission grant, verified identity,
    /// and randomness all hold. Pending or denied permissions, missing
    /// identity, or missing randomness emit nothing — never an
    /// empty-identity fallback.
    fn try_register(&mut self, effects: &mut dyn HostEffects) {
        if !self.pending_subscribe || self.permission_gate != PermissionGate::Granted {
            return;
        }
        let Some(client_id) = self.client_id.clone() else {
            return;
        };
        let mut registration = [0u8; 16];
        if !effects.fill_random(&mut registration) || registration == [0; 16] {
            return;
        }
        self.registration = registration;
        self.pending_subscribe = false;
        self.emit(
            PipeEventKind::Register {
                client_id,
                current_pane: self.focused_pane.clone(),
                registration: self.registration,
                plugin_id: self.plugin_id,
                identity: BridgeIdentity {
                    muxe_version: env!("CARGO_PKG_VERSION").to_owned(),
                    source_revision: pinned_source_revision().to_owned(),
                    action_fingerprint: generated_action_fingerprint().0,
                    protocol_fingerprint: bridge_protocol_fingerprint().0,
                    bridge_build_id: Some(bridge_build_id()),
                },
            },
            effects,
        );
    }

    fn on_request(&mut self, cli_id: &str, payload: Option<String>, effects: &mut dyn HostEffects) {
        let Some(payload) = payload else {
            return;
        };
        // An undecodable broadcast names no target: ignore silently without
        // unblocking anything. The broker's release timeout and single-child
        // replacement own that recovery; inventing a target would unblock the
        // wrong child or acknowledge work nobody did.
        let Ok(request) = decode_request_line(&payload) else {
            return;
        };
        let Some(client_id) = &self.client_id else {
            return;
        };
        // Only the named active registration acts; every other instance drops
        // the frame silently so exactly one bridge unblocks the request child.
        if request.target.client_id != *client_id
            || request.target.registration != self.registration
        {
            return;
        }
        if request.protocol != BRIDGE_PROTOCOL_VERSION {
            let execution = match &request.payload {
                BridgeRequest::Dispatch { execution, .. }
                | BridgeRequest::FocusPaneByIndex { execution, .. }
                | BridgeRequest::FocusPaneNeighbor { execution, .. } => Some(execution.clone()),
                _ => None,
            };
            self.fail(
                cli_id,
                request.request_id,
                request.channel_generation,
                execution,
                "unsupported bridge protocol",
                effects,
            );
            return;
        }
        let request_id = request.request_id;
        let generation = request.channel_generation;
        match request.payload {
            BridgeRequest::Dispatch { execution, command } => {
                self.dispatch_command(cli_id, request_id, generation, execution, command, effects);
            }
            BridgeRequest::BeginCapture { lease, ui_session } => {
                self.begin_capture(cli_id, request_id, generation, lease, ui_session, effects);
            }
            BridgeRequest::EndCapture { lease, reason } => {
                self.end_capture(cli_id, request_id, generation, lease, reason, effects);
            }
            BridgeRequest::RequestOrigin {
                ui_session,
                ui_pane,
            } => {
                self.request_origin(cli_id, request_id, generation, ui_session, ui_pane, effects);
            }
            BridgeRequest::FocusPaneByIndex { execution, index } => {
                self.focus_by_index(cli_id, request_id, generation, execution, index, effects);
            }
            BridgeRequest::FocusPaneNeighbor {
                execution,
                direction,
            } => {
                self.focus_neighbor(
                    cli_id, request_id, generation, execution, direction, effects,
                );
            }
            BridgeRequest::RetireBridge => {
                self.retire(cli_id, request_id, generation, effects);
            }
        }
    }

    fn dispatch_command(
        &mut self,
        cli_id: &str,
        request_id: [u8; 16],
        generation: u64,
        execution: String,
        command: RawNativeCommand,
        effects: &mut dyn HostEffects,
    ) {
        let ready = match prepare(command, &execution) {
            Ok(ready) => ready,
            Err(message) => {
                self.release(cli_id, request_id, generation, effects);
                self.emit(
                    PipeEventKind::DispatchAccepted {
                        request_id,
                        execution: execution.clone(),
                    },
                    effects,
                );
                self.emit(
                    PipeEventKind::DispatchCompleted {
                        request_id,
                        execution,
                        outcome: CommandOutcome::failed(format!("invalid command: {message}")),
                    },
                    effects,
                );
                return;
            }
        };
        match ready {
            ReadyDispatch::Sync(dispatch) => {
                let outcome = execute_sync(dispatch);
                self.release(cli_id, request_id, generation, effects);
                self.emit(
                    PipeEventKind::DispatchAccepted {
                        request_id,
                        execution: execution.clone(),
                    },
                    effects,
                );
                self.emit(
                    PipeEventKind::DispatchCompleted {
                        request_id,
                        execution,
                        outcome,
                    },
                    effects,
                );
            }
            ReadyDispatch::Async {
                dispatch,
                execution: correlated,
            } => {
                self.pending_actions.insert(correlated, request_id);
                // run_action queues host dispatch on another thread and
                // returns immediately; the ActionComplete echo completes it.
                let _ = execute_sync(dispatch);
                self.release(cli_id, request_id, generation, effects);
                self.emit(
                    PipeEventKind::DispatchAccepted {
                        request_id,
                        execution,
                    },
                    effects,
                );
            }
        }
    }

    fn begin_capture(
        &mut self,
        cli_id: &str,
        request_id: [u8; 16],
        generation: u64,
        lease: [u8; 16],
        _ui_session: String,
        effects: &mut dyn HostEffects,
    ) {
        // Snapshot from actually observed mode only; with no observation yet,
        // the first ModeUpdate snapshots before any Locked request goes out.
        // The pipe releases immediately either way so the broker can queue
        // further work while capture establishes.
        if self.current_mode == Some(InputMode::Locked) {
            self.active = Some(ActiveCapture {
                lease,
                prior: InputMode::Locked,
            });
            self.release(cli_id, request_id, generation, effects);
            self.emit(
                PipeEventKind::CaptureReady {
                    lease,
                    prior_mode: format!("{:?}", InputMode::Locked),
                },
                effects,
            );
            return;
        }
        let requested = self.current_mode.is_some();
        self.pending = Some(PendingCapture {
            lease,
            prior: self.current_mode,
            requested,
        });
        if requested {
            effects.switch_mode(InputMode::Locked);
        }
        self.release(cli_id, request_id, generation, effects);
    }

    fn end_capture(
        &mut self,
        cli_id: &str,
        request_id: [u8; 16],
        generation: u64,
        lease: [u8; 16],
        _reason: CaptureEndReason,
        effects: &mut dyn HostEffects,
    ) {
        self.pending = None;
        // Guarded restore: only the owning lease, and only while the client
        // is still in Muxe-owned Locked mode. A stale lease leaves the
        // current owner untouched.
        if let Some(active) = self.active.take() {
            if active.lease == lease {
                if self.current_mode == Some(InputMode::Locked) {
                    effects.switch_mode(active.prior);
                }
            } else {
                self.active = Some(active);
            }
        }
        self.release(cli_id, request_id, generation, effects);
    }

    fn request_origin(
        &mut self,
        cli_id: &str,
        request_id: [u8; 16],
        generation: u64,
        ui_session: String,
        ui_pane: String,
        effects: &mut dyn HostEffects,
    ) {
        // The owning bridge is the one whose focused pane is the attaching UI
        // pane; every other bridge declines so the adapter moves on. The UI
        // pane joins the MUXE set so later focus history never mistakes a menu
        // pane for origin.
        if self.focused_pane.as_deref() != Some(ui_pane.as_str()) {
            self.release(cli_id, request_id, generation, effects);
            self.emit(
                PipeEventKind::OriginDeclined {
                    ui_session,
                    request_id,
                    registration: self.registration,
                },
                effects,
            );
            return;
        }
        self.muxe_panes.insert(ui_pane.clone());
        let prior = self.last_non_muxe_pane.clone();
        let cwd = prior
            .as_deref()
            .and_then(|pane| PaneId::from_str(pane).ok())
            .and_then(|pane| effects.pane_cwd(pane))
            .map(|path| path.to_string_lossy().into_owned());
        if let Some(prior) = &prior {
            self.origin_pane = Some(prior.clone());
        }
        self.release(cli_id, request_id, generation, effects);
        self.emit(
            PipeEventKind::OriginSnapshot {
                ui_session,
                origin: ZellijOrigin {
                    client_id: self.client_id.clone().unwrap_or_default(),
                    session_name: None,
                    prior_pane_id: prior,
                    ui_pane_id: ui_pane,
                    prior_pane_cwd: cwd,
                },
            },
            effects,
        );
    }

    fn focus_by_index(
        &mut self,
        cli_id: &str,
        request_id: [u8; 16],
        generation: u64,
        execution: String,
        index: u32,
        effects: &mut dyn HostEffects,
    ) {
        let outcome = match self.inventory.pane_at(index) {
            Some(pane) => {
                effects.focus_pane(host_pane_id(pane));
                CommandOutcome::succeeded()
            }
            None => CommandOutcome::failed(format!("no pane at manifest index {index}")),
        };
        self.release(cli_id, request_id, generation, effects);
        self.emit(
            PipeEventKind::DispatchAccepted {
                request_id,
                execution: execution.clone(),
            },
            effects,
        );
        self.emit(
            PipeEventKind::DispatchCompleted {
                request_id,
                execution,
                outcome,
            },
            effects,
        );
    }

    fn focus_neighbor(
        &mut self,
        cli_id: &str,
        request_id: [u8; 16],
        generation: u64,
        execution: String,
        direction: muxe_zellij_protocol::NeighborDirection,
        effects: &mut dyn HostEffects,
    ) {
        let base = self
            .origin_pane
            .as_deref()
            .and_then(|pane| PaneId::from_str(pane).ok())
            .and_then(|pane| to_geometry(pane, &self.inventory));
        let outcome = match base.and_then(|base| self.inventory.neighbor(base, direction)) {
            Some(neighbor) => {
                effects.focus_pane(host_pane_id(neighbor));
                CommandOutcome::succeeded()
            }
            None => CommandOutcome::failed("no tracked neighbor in that direction".to_owned()),
        };
        self.release(cli_id, request_id, generation, effects);
        self.emit(
            PipeEventKind::DispatchAccepted {
                request_id,
                execution: execution.clone(),
            },
            effects,
        );
        self.emit(
            PipeEventKind::DispatchCompleted {
                request_id,
                execution,
                outcome,
            },
            effects,
        );
    }

    fn retire(
        &mut self,
        cli_id: &str,
        request_id: [u8; 16],
        generation: u64,
        effects: &mut dyn HostEffects,
    ) {
        // Retirement releases capture through the same guarded restore, then
        // fails every pending async completion instead of dangling it.
        if let Some(active) = self.active.take()
            && self.current_mode == Some(InputMode::Locked)
        {
            effects.switch_mode(active.prior);
        }
        self.pending = None;
        let pending: Vec<(String, [u8; 16])> = std::mem::take(&mut self.pending_actions)
            .into_iter()
            .collect();
        self.release(cli_id, request_id, generation, effects);
        for (execution, pending_id) in pending {
            self.emit(
                PipeEventKind::DispatchCompleted {
                    request_id: pending_id,
                    execution,
                    outcome: CommandOutcome::failed("bridge retiring".to_owned()),
                },
                effects,
            );
        }
    }

    /// Validates the request, asks Zellij to unblock the request child by its
    /// CLI source UUID, then emits the transport acknowledgement on the event
    /// child. Dispatch acceptance and completion are separate later events.
    fn release(
        &mut self,
        cli_id: &str,
        request_id: [u8; 16],
        generation: u64,
        effects: &mut dyn HostEffects,
    ) {
        effects.unblock_pipe(cli_id);
        self.emit(
            PipeEventKind::RequestReleased {
                request_id,
                channel_generation: generation,
                registration: self.registration,
            },
            effects,
        );
    }

    fn fail(
        &mut self,
        cli_id: &str,
        request_id: [u8; 16],
        generation: u64,
        execution: Option<String>,
        message: &str,
        effects: &mut dyn HostEffects,
    ) {
        let Some(execution) = execution else {
            self.release(cli_id, request_id, generation, effects);
            return;
        };
        self.release(cli_id, request_id, generation, effects);
        self.emit(
            PipeEventKind::DispatchAccepted {
                request_id,
                execution: execution.clone(),
            },
            effects,
        );
        self.emit(
            PipeEventKind::DispatchCompleted {
                request_id,
                execution,
                outcome: CommandOutcome::failed(message.to_owned()),
            },
            effects,
        );
    }

    fn emit(&mut self, event: PipeEventKind, effects: &mut dyn HostEffects) {
        let Some(event_cli_id) = self.event_cli_id.clone() else {
            return;
        };
        self.sequence += 1;
        let frame = PipeEvent {
            sequence: self.sequence,
            event,
        };
        if let Ok(line) = encode_event_line(&frame) {
            // The encoded line carries its `\n` terminator: the pinned host
            // relays `CliPipeOutput` bytes verbatim to the CLI child and the
            // native reader frames on `\n`, so stripping it would leave every
            // event buffered unreadably. Pass the wire bytes through intact.
            effects.pipe_output(&event_cli_id, &line);
        }
    }
}

/// Host pane ID for tracked geometry.
fn host_pane_id(pane: PaneGeometry) -> PaneId {
    if pane.is_plugin {
        PaneId::Plugin(pane.id)
    } else {
        PaneId::Terminal(pane.id)
    }
}

/// Geometry lookup for a host pane ID against the tracked inventory.
fn to_geometry(pane: PaneId, inventory: &PaneInventory) -> Option<PaneGeometry> {
    match pane {
        PaneId::Terminal(id) => inventory.find(id, false),
        PaneId::Plugin(id) => inventory.find(id, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_zellij_protocol::{decode_event_line, encode_request_line};
    use std::collections::VecDeque;

    /// Recording fake host: scripted query answers plus exact effect capture.
    /// Pipe fixtures deliberately use DIFFERENT semantic names and CLI source
    /// UUIDs so tests prove effects target the unique source ID, never the
    /// routing name.
    struct FakeHost {
        outputs: Vec<(String, String)>,
        unblocks: Vec<String>,
        output_states: Vec<(String, Option<FakePipeState>)>,
        pipe_states: BTreeMap<String, FakePipeState>,
        modes: Vec<InputMode>,
        focused: Vec<PaneId>,
        timers: u32,
        lists: u32,
        plugin_id: u32,
        cwd: BTreeMap<String, String>,
        random: VecDeque<[u8; 16]>,
    }

    impl FakeHost {
        fn new() -> Self {
            Self {
                outputs: Vec::new(),
                unblocks: Vec::new(),
                output_states: Vec::new(),
                pipe_states: BTreeMap::new(),
                modes: Vec::new(),
                focused: Vec::new(),
                timers: 0,
                lists: 0,
                plugin_id: 41,
                cwd: BTreeMap::new(),
                random: VecDeque::from([[7u8; 16], [8u8; 16], [9u8; 16]]),
            }
        }

        fn events(&self) -> Vec<(u64, PipeEventKind)> {
            self.outputs
                .iter()
                .map(|(_, line)| {
                    let frame = decode_event_line(line).expect("typed event frame");
                    (frame.sequence, frame.event)
                })
                .collect()
        }

        fn last_event(&self) -> (u64, PipeEventKind) {
            self.events().pop().expect("at least one event")
        }
        fn pipe_state(&self, cli_id: &str) -> Option<FakePipeState> {
            self.pipe_states.get(cli_id).copied()
        }
    }

    impl HostEffects for FakeHost {
        fn request_permissions(&mut self, _permissions: &[PermissionType]) {}
        fn subscribe(&mut self, _events: &[EventType]) {}
        fn list_clients(&mut self) {
            self.lists += 1;
        }
        fn plugin_ids(&mut self) -> PluginIds {
            PluginIds {
                plugin_id: self.plugin_id,
                zellij_pid: 1000,
                initial_cwd: PathBuf::from("/tmp"),
                client_id: 3,
            }
        }
        fn pane_cwd(&mut self, pane: PaneId) -> Option<PathBuf> {
            self.cwd.get(&format!("{pane}")).map(PathBuf::from)
        }
        fn pipe_output(&mut self, cli_id: &str, line: &str) {
            self.outputs.push((cli_id.to_owned(), line.to_owned()));
            self.output_states
                .push((cli_id.to_owned(), self.pipe_state(cli_id)));
        }
        fn block_pipe(&mut self, cli_id: &str) {
            self.pipe_states
                .insert(cli_id.to_owned(), FakePipeState::Blocked);
        }
        fn unblock_pipe(&mut self, cli_id: &str) {
            self.unblocks.push(cli_id.to_owned());
            self.pipe_states
                .insert(cli_id.to_owned(), FakePipeState::Released);
        }
        fn switch_mode(&mut self, mode: InputMode) {
            self.modes.push(mode);
        }
        fn focus_pane(&mut self, pane: PaneId) {
            self.focused.push(pane);
        }
        fn fill_random(&mut self, bytes: &mut [u8]) -> bool {
            match self.random.pop_front() {
                Some(id) if bytes.len() == id.len() => {
                    bytes.copy_from_slice(&id);
                    true
                }
                _ => false,
            }
        }
        fn arm_timer(&mut self, _secs: f64) {
            self.timers += 1;
        }
    }

    const EVENT_NAME: &str = "muxe-event-alpha";
    const EVENT_CLI: &str = "event-cli-uuid-1";
    const REQUEST_NAME: &str = "muxe-request-alpha";
    const REQUEST_CLI: &str = "request-cli-uuid-9";

    fn clients_current(pane: PaneId) -> Vec<ClientInfo> {
        vec![ClientInfo {
            client_id: 5,
            pane_id: pane,
            running_command: String::new(),
            is_current_client: true,
        }]
    }

    fn mode_info(mode: InputMode) -> ModeInfo {
        ModeInfo {
            mode,
            ..Default::default()
        }
    }

    fn request_line(target_reg: [u8; 16], payload: BridgeRequest) -> String {
        use muxe_zellij_protocol::{BridgeTarget, PipeRequest};
        let frame = PipeRequest {
            protocol: BRIDGE_PROTOCOL_VERSION,
            request_id: [1; 16],
            channel_generation: 1,
            target: BridgeTarget {
                client_id: "5".to_owned(),
                registration: target_reg,
            },
            payload,
        };
        let mut line = encode_request_line(&frame).expect("encodes");
        line.push('\n');
        line
    }

    fn request_msg(payload: BridgeRequest) -> PipeMessage {
        PipeMessage {
            source: PipeSource::Cli(REQUEST_CLI.to_owned()),
            name: REQUEST_NAME.to_owned(),
            payload: Some(request_line([7; 16], payload)),
            args: BTreeMap::new(),
            is_private: false,
        }
    }

    fn subscribe_msg() -> PipeMessage {
        PipeMessage {
            source: PipeSource::Cli(EVENT_CLI.to_owned()),
            name: EVENT_NAME.to_owned(),
            payload: Some("{\"muxe\":\"subscribe\"}".to_owned()),
            args: BTreeMap::new(),
            is_private: false,
        }
    }

    /// Full startup through the real host sequence: load (no privileged
    /// query), explicit grant (exactly one identity query), identity
    /// event, then event-channel subscription driving registration.
    fn boot() -> (Bridge, FakeHost) {
        let mut bridge = Bridge::default();
        let mut host = FakeHost::new();
        bridge.load(&mut host);
        assert_eq!(host.lists, 0);
        bridge.update(
            Event::PermissionRequestResult(PermissionStatus::Granted),
            &mut host,
        );
        assert_eq!(host.lists, 1);
        bridge.update(
            Event::ListClients(clients_current(PaneId::Terminal(2))),
            &mut host,
        );
        bridge.pipe(subscribe_msg(), &mut host);
        assert_eq!(bridge.client_identity(), Some("5"));
        assert_eq!(bridge.active_registration(), Some([7; 16]));
        (bridge, host)
    }

    /// The emitted Register must survive the real wire path: the pinned host
    /// relays `CliPipeOutput` bytes verbatim (no added newline) and the
    /// native reader only yields a line at `\n`. Stream the exact emitted
    /// bytes through an incremental newline framer with no EOF, in odd-sized
    /// chunks, and require the Register frame to emerge decodable. Decoding
    /// the recorded strings whole would hide a missing terminator.
    #[test]
    fn emitted_register_frames_without_eof() {
        let (_bridge, host) = boot();
        assert!(!host.outputs.is_empty());
        for (target, _) in &host.outputs {
            assert_eq!(target, EVENT_CLI);
        }
        let stream: Vec<u8> = host
            .outputs
            .iter()
            .flat_map(|(_, line)| line.bytes())
            .collect();
        let mut pending: Vec<u8> = Vec::new();
        let mut frames: Vec<String> = Vec::new();
        for chunk in stream.chunks(7) {
            pending.extend_from_slice(chunk);
            while let Some(pos) = pending.iter().position(|byte| *byte == b'\n') {
                let raw: Vec<u8> = pending.drain(..=pos).collect();
                frames.push(
                    String::from_utf8(raw[..raw.len() - 1].to_vec()).expect("frame is UTF-8"),
                );
            }
        }
        assert!(
            !frames.is_empty(),
            "emitted event bytes never framed a line without EOF"
        );
        let first = decode_event_line(&frames[0]).expect("framed line decodes");
        match first.event {
            PipeEventKind::Register {
                client_id,
                registration,
                ..
            } => {
                assert_eq!(client_id, "5");
                assert_eq!(registration, [7; 16]);
            }
            other => panic!("first framed event is not Register: {other:?}"),
        }
    }

    #[test]
    fn no_privileged_query_before_grant() {
        let mut bridge = Bridge::default();
        let mut host = FakeHost::new();
        bridge.load(&mut host);
        assert_eq!(host.lists, 0);
        // Identity data and subscription arrive, but without a grant no
        // query issues and nothing registers.
        bridge.update(
            Event::ListClients(clients_current(PaneId::Terminal(2))),
            &mut host,
        );
        bridge.pipe(subscribe_msg(), &mut host);
        assert_eq!(host.lists, 0);
        assert_eq!(bridge.active_registration(), None);
        assert!(host.outputs.is_empty());
    }

    #[test]
    fn denied_grant_never_queries_or_registers() {
        let mut bridge = Bridge::default();
        let mut host = FakeHost::new();
        bridge.load(&mut host);
        bridge.update(
            Event::PermissionRequestResult(PermissionStatus::Denied),
            &mut host,
        );
        bridge.update(
            Event::ListClients(clients_current(PaneId::Terminal(2))),
            &mut host,
        );
        bridge.pipe(subscribe_msg(), &mut host);
        assert_eq!(host.lists, 0);
        assert_eq!(bridge.active_registration(), None);
        assert!(host.outputs.is_empty());
    }

    #[test]
    fn repeated_grant_queries_exactly_once() {
        let mut bridge = Bridge::default();
        let mut host = FakeHost::new();
        bridge.load(&mut host);
        bridge.update(
            Event::PermissionRequestResult(PermissionStatus::Granted),
            &mut host,
        );
        bridge.update(
            Event::PermissionRequestResult(PermissionStatus::Granted),
            &mut host,
        );
        assert_eq!(host.lists, 1);
    }

    #[test]
    fn registration_needs_grant_identity_and_randomness() {
        let mut bridge = Bridge::default();
        let mut host = FakeHost::new();
        bridge.load(&mut host);
        // Subscribe before grant and identity: nothing emits.
        bridge.pipe(subscribe_msg(), &mut host);
        assert_eq!(bridge.active_registration(), None);
        assert!(host.outputs.is_empty());
        // Grant issues the identity query but randomness is gone: still
        // nothing emits.
        host.random.clear();
        bridge.update(
            Event::PermissionRequestResult(PermissionStatus::Granted),
            &mut host,
        );
        assert_eq!(host.lists, 1);
        bridge.update(
            Event::ListClients(clients_current(PaneId::Terminal(2))),
            &mut host,
        );
        assert_eq!(bridge.active_registration(), None);
        assert!(host.outputs.is_empty());
    }

    #[test]
    fn unblock_and_output_target_source_uuids_not_names() {
        let (mut bridge, mut host) = boot();
        let before = host.outputs.len();
        bridge.pipe(
            request_msg(BridgeRequest::Dispatch {
                execution: "e1".to_owned(),
                command: RawNativeCommand::CloseFocus,
            }),
            &mut host,
        );
        // Unblock addresses the request child UUID, never the routing name.
        assert_eq!(host.unblocks.as_slice(), [REQUEST_CLI]);
        // All event lines address the event child UUID, never the name.
        for (target, _) in host.outputs.iter().skip(before) {
            assert_eq!(target, EVENT_CLI);
        }
        // Release, acceptance, and completion all emitted in order.
        let kinds: Vec<&str> = host
            .events()
            .into_iter()
            .map(|(_, event)| match event {
                PipeEventKind::Register { .. } => "register",
                PipeEventKind::RequestReleased { .. } => "released",
                PipeEventKind::DispatchAccepted { .. } => "accepted",
                PipeEventKind::DispatchCompleted { .. } => "completed",
                _ => "other",
            })
            .collect();
        assert_eq!(kinds, ["register", "released", "accepted", "completed"]);
    }

    #[test]
    fn non_target_frames_are_dropped_silently() {
        let (mut bridge, mut host) = boot();
        let before_unblocks = host.unblocks.len();
        let before_outputs = host.outputs.len();
        // Wrong registration: silent drop, no unblock, no output.
        let mut line = request_line(
            [9; 16],
            BridgeRequest::Dispatch {
                execution: "e2".to_owned(),
                command: RawNativeCommand::CloseFocus,
            },
        );
        line.push('\n');
        bridge.pipe(
            PipeMessage {
                source: PipeSource::Cli("other-cli".to_owned()),
                name: REQUEST_NAME.to_owned(),
                payload: Some(line),
                args: BTreeMap::new(),
                is_private: false,
            },
            &mut host,
        );
        assert_eq!(host.unblocks.len(), before_unblocks);
        assert_eq!(host.outputs.len(), before_outputs);
        // Undecodable broadcasts never unblock anything either.
        bridge.pipe(
            PipeMessage {
                source: PipeSource::Cli("other-cli".to_owned()),
                name: REQUEST_NAME.to_owned(),
                payload: Some("not-json\n".to_owned()),
                args: BTreeMap::new(),
                is_private: false,
            },
            &mut host,
        );
        assert_eq!(host.unblocks.len(), before_unblocks);
        assert_eq!(host.outputs.len(), before_outputs);
    }

    #[test]
    fn capture_waits_for_observed_mode_then_confirms() {
        let (mut bridge, mut host) = boot();
        // Begin with no observed mode: no Locked request yet.
        bridge.pipe(
            request_msg(BridgeRequest::BeginCapture {
                lease: [11; 16],
                ui_session: "ui-1".to_owned(),
            }),
            &mut host,
        );
        assert!(host.modes.is_empty());
        assert_eq!(bridge.capture_state(), (Some([11; 16]), None));
        // First observation snapshots the true prior, then requests Locked.
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        assert_eq!(host.modes.as_slice(), [InputMode::Locked]);
        // Confirmation carries the exact observed prior, not a guess.
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (None, Some([11; 16])));
        let (_, event) = host.last_event();
        assert!(matches!(
            event,
            PipeEventKind::CaptureReady { lease, .. } if lease == [11; 16]
        ));
    }

    #[test]
    fn user_mode_change_dismisses_without_restore() {
        let (mut bridge, mut host) = boot();
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        bridge.pipe(
            request_msg(BridgeRequest::BeginCapture {
                lease: [12; 16],
                ui_session: "ui-1".to_owned(),
            }),
            &mut host,
        );
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (None, Some([12; 16])));
        host.modes.clear();
        // User leaves Locked: dismissal, and no mode request restores Normal.
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Tab)), &mut host);
        assert_eq!(bridge.capture_state(), (None, None));
        assert!(host.modes.is_empty());
        let (_, event) = host.last_event();
        assert!(matches!(
            event,
            PipeEventKind::CaptureLost {
                reason: CaptureLostReason::UserModeChanged,
                ..
            }
        ));
    }

    #[test]
    fn origin_snapshots_prior_pane_not_the_menu() {
        let (mut bridge, mut host) = boot();
        // Menu pane takes focus while no capture is active... it must NOT
        // become origin: the claim below still snapshots terminal_2.
        bridge.update(
            Event::ListClients(clients_current(PaneId::Terminal(2))),
            &mut host,
        );
        host.cwd.insert("terminal_2".to_owned(), "/work".to_owned());
        bridge.pipe(
            PipeMessage {
                source: PipeSource::Cli(REQUEST_CLI.to_owned()),
                name: REQUEST_NAME.to_owned(),
                payload: Some(request_line(
                    [7; 16],
                    BridgeRequest::RequestOrigin {
                        ui_session: "ui-9".to_owned(),
                        ui_pane: "terminal_2".to_owned(),
                    },
                )),
                args: BTreeMap::new(),
                is_private: false,
            },
            &mut host,
        );
        let (_, event) = host.last_event();
        match event {
            PipeEventKind::OriginSnapshot { origin, .. } => {
                assert_eq!(origin.prior_pane_id.as_deref(), Some("terminal_2"));
            }
            _ => panic!("expected snapshot, got decline"),
        }
    }

    #[test]
    fn retire_restores_and_fails_pending() {
        let (mut bridge, mut host) = boot();
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        bridge.pipe(
            request_msg(BridgeRequest::BeginCapture {
                lease: [13; 16],
                ui_session: "ui-1".to_owned(),
            }),
            &mut host,
        );
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        // Queue an async run_action without completing it.
        bridge.pipe(
            request_msg(BridgeRequest::Dispatch {
                execution: "e9".to_owned(),
                command: RawNativeCommand::RunAction {
                    action: muxe_zellij_protocol::generated::raw::Action::CloseFocus,
                    context: Vec::new(),
                },
            }),
            &mut host,
        );
        host.modes.clear();
        bridge.pipe(request_msg(BridgeRequest::RetireBridge), &mut host);
        // Guarded restore ran (still Locked, owned lease) and the pending
        // async completion failed instead of dangling.
        assert_eq!(host.modes.as_slice(), [InputMode::Normal]);
        let failed = host.events().into_iter().any(|(_, event)| {
            matches!(
                event,
                PipeEventKind::DispatchCompleted { execution, .. } if execution == "e9"
            )
        });
        assert!(failed);
        assert_eq!(bridge.capture_state(), (None, None));
    }

    #[test]
    fn heartbeat_fires_on_timer_while_idle() {
        let (mut bridge, mut host) = boot();
        let before = host.outputs.len();
        bridge.update(Event::Timer(5.0), &mut host);
        assert!(host.outputs.len() > before);
        let (_, event) = host.last_event();
        assert!(matches!(event, PipeEventKind::Heartbeat { .. }));
        assert!(host.timers >= 2);
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum FakePipeState {
        Blocked,
        Released,
    }

    #[test]
    fn event_pipe_stays_blocked_across_later_output() {
        let (mut bridge, mut host) = boot();
        assert_eq!(host.pipe_state(EVENT_CLI), Some(FakePipeState::Blocked));
        host.outputs.clear();

        bridge.update(Event::Timer(5.0), &mut host);

        assert!(
            host.outputs.iter().any(|(target, line)| {
                target == EVENT_CLI
                    && matches!(
                        decode_event_line(line)
                            .expect("typed heartbeat frame")
                            .event,
                        PipeEventKind::Heartbeat { .. }
                    )
            }),
            "later heartbeat output must target the held event source"
        );
        assert!(
            host.output_states
                .iter()
                .filter(|(target, _)| target == EVENT_CLI)
                .all(|(_, state)| *state == Some(FakePipeState::Blocked)),
            "event output must be emitted only after its source is blocked"
        );
        assert_eq!(host.pipe_state(EVENT_CLI), Some(FakePipeState::Blocked));
        assert!(
            host.unblocks.is_empty(),
            "later event output must not release the event source"
        );
    }

    #[test]
    fn request_release_does_not_release_event_pipe() {
        let (mut bridge, mut host) = boot();

        bridge.pipe(
            request_msg(BridgeRequest::Dispatch {
                execution: "request-release".to_owned(),
                command: RawNativeCommand::CloseFocus,
            }),
            &mut host,
        );

        assert_eq!(host.pipe_state(REQUEST_CLI), Some(FakePipeState::Released));
        assert_eq!(host.pipe_state(EVENT_CLI), Some(FakePipeState::Blocked));
        assert_eq!(host.unblocks.as_slice(), [REQUEST_CLI]);
    }
}
