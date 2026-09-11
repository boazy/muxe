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

use muxe_protocol::{CaptureLeaseId, ExecutionId, UiSessionId};
use muxe_zellij_protocol::{
    BRIDGE_PERMISSIONS, BRIDGE_PROTOCOL_VERSION, BridgeEvent, BridgeIdentity, BridgeRequest,
    BridgeResponse, CaptureEndReason, CaptureLostReason, ChannelGeneration, CommandOutcome,
    PipeEvent, PipeEventKind, RegistrationId, RequestId, ZellijCaptureState, ZellijDispatchRequest,
    ZellijOrigin, ZellijOriginRequest, ZellijRegistration, bridge_build_id,
    bridge_protocol_fingerprint, decode_event_subscription, decode_request_line, encode_event_line,
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
    lease: CaptureLeaseId,
    request_id: RequestId,
    channel_generation: ChannelGeneration,
    prior: Option<InputMode>,
    requested: bool,
}

struct ActiveCapture {
    lease: CaptureLeaseId,
    prior: InputMode,
}

struct RestoreBarrier {
    locked_observed: bool,
}

/// Permission gate for privileged host queries: the bridge requests
/// permissions at load but issues no privileged query before the host's
/// explicit grant event. The grant is subscribed explicitly because the
/// pinned host replays cached events only for subscribed event types.
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
    plugin_client_id: Option<u16>,
    focused_pane: Option<String>,
    permission_gate: PermissionGate,
    event_cli_id: Option<String>,
    pending_subscribe: bool,
    registration: Option<RegistrationId>,
    channel_generation: Option<ChannelGeneration>,
    last_request_id: Option<RequestId>,
    current_mode: Option<InputMode>,
    pending: Option<PendingCapture>,
    active: Option<ActiveCapture>,
    restoring_mode: Option<RestoreBarrier>,
    muxe_panes: BTreeSet<String>,
    last_non_muxe_pane: Option<String>,
    origin_pane: Option<String>,
    inventory: PaneInventory,
    pending_actions: BTreeMap<String, PendingAction>,
    pending_post_dismissals: Vec<PendingPostDismissal>,
}

struct PendingAction {
    request_id: RequestId,
    execution: ExecutionId,
    channel_generation: ChannelGeneration,
}

struct PendingPostDismissal {
    request_id: RequestId,
    channel_generation: ChannelGeneration,
    execution: ExecutionId,
    ui_pane: String,
    origin_pane: String,
    command: RawNativeCommand,
}

impl Default for Bridge {
    fn default() -> Self {
        Self {
            client_id: None,
            plugin_id: None,
            plugin_client_id: None,
            focused_pane: None,
            permission_gate: PermissionGate::Pending,
            event_cli_id: None,
            pending_subscribe: false,
            registration: None,
            channel_generation: None,
            last_request_id: None,
            current_mode: None,
            pending: None,
            active: None,
            restoring_mode: None,
            muxe_panes: BTreeSet::new(),
            last_non_muxe_pane: None,
            origin_pane: None,
            inventory: PaneInventory::new(),
            pending_actions: BTreeMap::new(),
            pending_post_dismissals: Vec::new(),
        }
    }
}

impl Bridge {
    /// Initial plugin load: control subscriptions, permission request,
    /// plugin IDs, and the first heartbeat timer. Subscribe before requesting
    /// permissions because the host can replay a cached grant synchronously;
    /// the privileged identity query still waits for that explicit grant
    /// event, so a pre-grant query is never attempted.
    pub fn load(&mut self, effects: &mut dyn HostEffects) {
        effects.subscribe(&[
            EventType::PermissionRequestResult,
            EventType::ListClients,
            EventType::ModeUpdate,
            EventType::PaneUpdate,
            EventType::TabUpdate,
            EventType::ActionComplete,
            EventType::Timer,
        ]);
        effects.request_permissions(&BRIDGE_PERMISSIONS);
        let ids = effects.plugin_ids();
        self.plugin_id = Some(ids.plugin_id);
        self.plugin_client_id = Some(ids.client_id);
        effects.arm_timer(HEARTBEAT_SECS);
    }

    /// Host event pump.
    pub fn update(&mut self, event: Event, effects: &mut dyn HostEffects) {
        match event {
            Event::ListClients(clients) => self.on_list_clients(&clients, effects),
            Event::ModeUpdate(mode) => self.on_mode_update(&mode, effects),
            Event::PaneUpdate(manifest) => self.on_pane_update(&manifest, effects),
            Event::TabUpdate(tabs) => self.on_tab_update(&tabs),
            Event::ActionComplete(_, _, context) => self.on_action_complete(&context, effects),
            Event::Timer(_) => self.on_timer(effects),
            Event::PermissionRequestResult(status) => match status {
                PermissionStatus::Granted => {
                    self.permission_gate = PermissionGate::Granted;
                    if self.client_id.is_none() {
                        effects.list_clients();
                    }
                }
                PermissionStatus::Denied => {
                    if self.client_id.is_none() {
                        self.permission_gate = PermissionGate::Denied;
                    }
                }
            },
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
                    self.subscribe(cli_id, payload.as_deref(), effects);
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
    pub fn active_registration(&self) -> Option<RegistrationId> {
        self.registration
    }

    /// Test-visible capture state: pending lease, active lease, or none.
    #[cfg(test)]
    pub fn capture_state(&self) -> (Option<[u8; 16]>, Option<[u8; 16]>) {
        (
            self.pending.as_ref().map(|p| p.lease.0),
            self.active.as_ref().map(|a| a.lease.0),
        )
    }

    fn on_list_clients(&mut self, clients: &[ClientInfo], effects: &mut dyn HostEffects) {
        let mut current_client_present = false;
        if let Some(anchor_client_id) = self.plugin_client_id {
            for client in clients {
                if client.is_current_client && client.client_id == anchor_client_id {
                    current_client_present = true;
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
        }
        if current_client_present {
            // An anchor-matching census is proof that this instance's
            // privileged query was authorized. Keep that proof across later
            // broadcast PermissionRequestResult events.
            self.permission_gate = PermissionGate::Granted;
            self.try_register(effects);
        }
        self.try_post_dismissals(effects);
    }

    fn on_mode_update(&mut self, mode: &ModeInfo, effects: &mut dyn HostEffects) {
        let locked = mode.mode == InputMode::Locked;
        self.current_mode = Some(mode.mode);
        // Active restoration has already observed Locked. Canceling a pending
        // requested capture must first observe the queued Locked transition,
        // then a later non-Locked restoration, before a replacement can start.
        if let Some(restoring) = &mut self.restoring_mode {
            if locked {
                restoring.locked_observed = true;
                return;
            }
            if !restoring.locked_observed {
                return;
            }
            self.restoring_mode = None;
        }
        if locked {
            if let Some(pending) = self.pending.take() {
                // An unobserved prior degrades to Locked: restoring Locked
                // while still Locked is identity, and a later user-driven mode
                // change skips restoration entirely.
                let prior = pending.prior.unwrap_or(InputMode::Locked);
                let lease = pending.lease;
                self.active = Some(ActiveCapture { lease, prior });
                self.emit_for_request(
                    pending.request_id,
                    pending.channel_generation,
                    BridgeResponse::CaptureReady {
                        lease,
                        state: ZellijCaptureState {
                            prior_mode: format!("{prior:?}"),
                        },
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
            self.emit_unsolicited(
                BridgeEvent::CaptureLost {
                    lease: active.lease,
                    reason: CaptureLostReason::UserModeChanged,
                },
                effects,
            );
        }
    }

    fn on_pane_update(&mut self, manifest: &PaneManifest, effects: &mut dyn HostEffects) {
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
        self.try_post_dismissals(effects);
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
        let Some(correlation) = completion_execution(context) else {
            return;
        };
        if let Some(pending) = self.pending_actions.remove(correlation) {
            self.emit_for_request(
                pending.request_id,
                pending.channel_generation,
                BridgeResponse::DispatchCompleted {
                    execution: pending.execution,
                    outcome: CommandOutcome::succeeded(),
                },
                effects,
            );
        }
    }

    fn on_timer(&mut self, effects: &mut dyn HostEffects) {
        // Heartbeats renew the broker-side heartbeat lease; without them an
        // idle healthy bridge would be expired by the registry.
        self.emit_unsolicited(BridgeEvent::Heartbeat, effects);
        effects.arm_timer(HEARTBEAT_SECS);
    }

    /// Cancels matching capture state and compensates every queued Locked
    /// transition before allowing another capture to inherit observed mode.
    ///
    /// `lease = None` cancels all capture state for channel reset/retirement.
    /// A specific lease preserves newer, unrelated pending or active owners.
    fn cancel_capture_for_restore(
        &mut self,
        lease: Option<CaptureLeaseId>,
        effects: &mut dyn HostEffects,
    ) -> Option<ActiveCapture> {
        let pending_matches = self
            .pending
            .as_ref()
            .is_some_and(|pending| lease.is_none_or(|lease| pending.lease == lease));
        if pending_matches {
            let pending = self.pending.take().expect("matching pending capture");
            if pending.requested
                && let Some(prior) = pending.prior
            {
                self.restoring_mode = Some(RestoreBarrier {
                    locked_observed: false,
                });
                // Queued after the earlier Locked request, so the host applies
                // Locked and then restores the observed prior mode.
                effects.switch_mode(prior);
            }
        }

        let active_matches = self
            .active
            .as_ref()
            .is_some_and(|active| lease.is_none_or(|lease| active.lease == lease));
        if !active_matches {
            return None;
        }
        let active = self.active.take().expect("matching active capture");
        if self.current_mode == Some(InputMode::Locked) {
            if active.prior != InputMode::Locked {
                self.restoring_mode = Some(RestoreBarrier {
                    locked_observed: true,
                });
            }
            effects.switch_mode(active.prior);
        }
        Some(active)
    }

    fn on_before_close(&mut self, effects: &mut dyn HostEffects) {
        // Unloading with owned Locked capture restores the guarded prior;
        // pending async completions fail instead of dangling. Only unload
        // discards the restoring barrier.
        let restore = self.current_mode == Some(InputMode::Locked);
        if let Some(active) = self.cancel_capture_for_restore(None, effects)
            && restore
        {
            self.emit_unsolicited(
                BridgeEvent::CaptureLost {
                    lease: active.lease,
                    reason: CaptureLostReason::BridgeUnloading,
                },
                effects,
            );
        }
        self.pending = None;
        self.restoring_mode = None;
        let pending: Vec<PendingAction> = std::mem::take(&mut self.pending_actions)
            .into_values()
            .collect();
        for pending in pending {
            self.emit_for_request(
                pending.request_id,
                pending.channel_generation,
                BridgeResponse::DispatchCompleted {
                    execution: pending.execution,
                    outcome: CommandOutcome::failed("bridge unloading".to_owned()),
                },
                effects,
            );
        }
        self.fail_post_dismissals("bridge unloading", effects);
    }

    fn fail_post_dismissals(&mut self, reason: &str, effects: &mut dyn HostEffects) {
        for pending in std::mem::take(&mut self.pending_post_dismissals) {
            self.emit_for_request(
                pending.request_id,
                pending.channel_generation,
                BridgeResponse::DispatchCompleted {
                    execution: pending.execution,
                    outcome: CommandOutcome::failed(reason.to_owned()),
                },
                effects,
            );
        }
    }

    /// New event channel: guarded-restore any still-owned Locked capture
    /// first, then reset epoch state so old completions can never reattach
    /// under the fresh registration below.
    fn subscribe(&mut self, cli_id: String, payload: Option<&str>, effects: &mut dyn HostEffects) {
        let Some(subscription) = payload.and_then(|value| decode_event_subscription(value).ok())
        else {
            return;
        };
        // A replacement channel restores any still-owned capture through the
        // shared guard, but preserves the restoring barrier until a
        // non-Locked ModeUpdate is observed.
        self.cancel_capture_for_restore(None, effects);
        self.pending_actions.clear();
        self.registration = None;
        self.channel_generation = Some(subscription.channel_generation());
        self.last_request_id = None;
        effects.block_pipe(&cli_id);
        self.event_cli_id = Some(cli_id);
        self.pending_subscribe = true;
        if self.permission_gate == PermissionGate::Granted {
            effects.list_clients();
        }
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
        let mut random_bytes = [0_u8; 16];
        if !effects.fill_random(&mut random_bytes) {
            return;
        }
        let Ok(registration) = RegistrationId::from_random_bytes(random_bytes) else {
            return;
        };
        self.registration = Some(registration);
        self.pending_subscribe = false;
        self.emit_unsolicited(
            BridgeEvent::Register {
                registration: ZellijRegistration {
                    client_id,
                    current_pane: self.focused_pane.clone(),
                    plugin_id: self.plugin_id,
                    identity: BridgeIdentity {
                        muxe_version: env!("CARGO_PKG_VERSION").to_owned(),
                        source_revision: pinned_source_revision().to_owned(),
                        action_fingerprint: generated_action_fingerprint().0,
                        protocol_fingerprint: bridge_protocol_fingerprint().0,
                        bridge_build_id: Some(bridge_build_id()),
                    },
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
        // Only the named active registration and pipe generation act; every
        // other instance drops the frame silently so exactly one bridge
        // unblocks the request child.
        if request.target.client_id != *client_id
            || Some(request.registration) != self.registration
            || Some(request.channel_generation) != self.channel_generation
        {
            return;
        }
        if let Some(previous) = self.last_request_id
            && previous.next() != Ok(request.request_id)
        {
            eprintln!(
                "muxe bridge: request ID {} followed by {} for registration {}",
                previous, request.request_id, request.registration
            );
        }
        self.last_request_id = Some(request.request_id);
        let request_id = request.request_id;
        let generation = request.channel_generation;
        match request.payload {
            BridgeRequest::Dispatch {
                execution,
                request: ZellijDispatchRequest::Command(command),
            } => {
                self.dispatch_command(cli_id, request_id, generation, execution, command, effects);
            }
            BridgeRequest::Dispatch {
                execution,
                request:
                    ZellijDispatchRequest::PostDismissalCreation {
                        ui_pane,
                        origin_pane,
                        command,
                    },
            } => {
                self.defer_post_dismissal_creation(
                    cli_id,
                    request_id,
                    generation,
                    execution,
                    ui_pane,
                    origin_pane,
                    command,
                    effects,
                );
            }
            BridgeRequest::Dispatch {
                execution,
                request: ZellijDispatchRequest::FocusPaneByIndex { index },
            } => {
                self.focus_by_index(cli_id, request_id, generation, execution, index, effects);
            }
            BridgeRequest::Dispatch {
                execution,
                request: ZellijDispatchRequest::FocusPaneNeighbor { direction },
            } => {
                self.focus_neighbor(
                    cli_id, request_id, generation, execution, direction, effects,
                );
            }
            BridgeRequest::BeginCapture { lease, ui_session } => {
                self.begin_capture(cli_id, request_id, generation, lease, ui_session, effects);
            }
            BridgeRequest::EndCapture { lease, reason } => {
                self.end_capture(cli_id, request_id, generation, lease, reason, effects);
            }
            BridgeRequest::RequestOrigin {
                ui_session,
                request: ZellijOriginRequest { ui_pane },
            } => {
                self.request_origin(cli_id, request_id, generation, ui_session, ui_pane, effects);
            }
            BridgeRequest::Retire | BridgeRequest::Shutdown => {
                self.retire(cli_id, request_id, generation, effects);
            }
            BridgeRequest::Host(_) => {}
        }
    }

    fn dispatch_command(
        &mut self,
        cli_id: &str,
        request_id: RequestId,
        generation: ChannelGeneration,
        execution: ExecutionId,
        command: RawNativeCommand,
        effects: &mut dyn HostEffects,
    ) {
        let Some(registration) = self.registration else {
            return;
        };
        let correlation = format!("{registration}:{request_id}");
        let ready = match prepare(command, &correlation) {
            Ok(ready) => ready,
            Err(message) => {
                self.release(cli_id, request_id, generation, effects);
                self.emit_for_request(
                    request_id,
                    generation,
                    BridgeResponse::DispatchAccepted { execution },
                    effects,
                );
                self.emit_for_request(
                    request_id,
                    generation,
                    BridgeResponse::DispatchCompleted {
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
                self.emit_for_request(
                    request_id,
                    generation,
                    BridgeResponse::DispatchAccepted { execution },
                    effects,
                );
                self.emit_for_request(
                    request_id,
                    generation,
                    BridgeResponse::DispatchCompleted { execution, outcome },
                    effects,
                );
            }
            ReadyDispatch::Async {
                dispatch,
                execution: correlated,
            } => {
                self.pending_actions.insert(
                    correlated,
                    PendingAction {
                        request_id,
                        execution,
                        channel_generation: generation,
                    },
                );
                // run_action queues host dispatch on another thread and
                // returns immediately; the ActionComplete echo completes it.
                let _ = execute_sync(dispatch);
                self.release(cli_id, request_id, generation, effects);
                self.emit_for_request(
                    request_id,
                    generation,
                    BridgeResponse::DispatchAccepted { execution },
                    effects,
                );
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the request fields are explicit protocol provenance, not an untyped payload"
    )]
    fn defer_post_dismissal_creation(
        &mut self,
        cli_id: &str,
        request_id: RequestId,
        generation: ChannelGeneration,
        execution: ExecutionId,
        ui_pane: String,
        origin_pane: String,
        command: RawNativeCommand,
        effects: &mut dyn HostEffects,
    ) {
        self.release(cli_id, request_id, generation, effects);
        self.emit_for_request(
            request_id,
            generation,
            BridgeResponse::DispatchAccepted { execution },
            effects,
        );
        self.pending_post_dismissals.push(PendingPostDismissal {
            request_id,
            channel_generation: generation,
            execution,
            ui_pane,
            origin_pane,
            command,
        });
        self.try_post_dismissals(effects);
    }

    fn try_post_dismissals(&mut self, effects: &mut dyn HostEffects) {
        let pending = std::mem::take(&mut self.pending_post_dismissals);
        let mut remaining = Vec::with_capacity(pending.len());
        let mut refresh_client_focus = false;
        for pending in pending {
            if !self.inventory.has_manifest() {
                remaining.push(pending);
                continue;
            }
            let ui_pane = PaneId::from_str(&pending.ui_pane).ok();
            let ui_is_live = ui_pane.is_some_and(|pane| match pane {
                PaneId::Terminal(id) => self.inventory.contains(id, false),
                PaneId::Plugin(id) => self.inventory.contains(id, true),
            });
            if ui_is_live {
                remaining.push(pending);
                continue;
            }
            if self.focused_pane.as_deref() != Some(pending.origin_pane.as_str()) {
                refresh_client_focus = true;
                remaining.push(pending);
                continue;
            }
            self.dispatch_post_dismissal_creation(pending, effects);
        }
        self.pending_post_dismissals = remaining;
        if refresh_client_focus {
            effects.list_clients();
        }
    }

    fn dispatch_post_dismissal_creation(
        &mut self,
        pending: PendingPostDismissal,
        effects: &mut dyn HostEffects,
    ) {
        let Some(registration) = self.registration else {
            self.emit_for_request(
                pending.request_id,
                pending.channel_generation,
                BridgeResponse::DispatchCompleted {
                    execution: pending.execution,
                    outcome: CommandOutcome::failed("bridge registration disappeared".to_owned()),
                },
                effects,
            );
            return;
        };
        let correlation = format!("{registration}:{}", pending.request_id);
        let ready = match prepare(pending.command, &correlation) {
            Ok(ready) => ready,
            Err(message) => {
                self.emit_for_request(
                    pending.request_id,
                    pending.channel_generation,
                    BridgeResponse::DispatchCompleted {
                        execution: pending.execution,
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
                self.emit_for_request(
                    pending.request_id,
                    pending.channel_generation,
                    BridgeResponse::DispatchCompleted {
                        execution: pending.execution,
                        outcome,
                    },
                    effects,
                );
            }
            ReadyDispatch::Async {
                dispatch,
                execution: correlated,
            } => {
                self.pending_actions.insert(
                    correlated,
                    PendingAction {
                        request_id: pending.request_id,
                        execution: pending.execution,
                        channel_generation: pending.channel_generation,
                    },
                );
                let _ = execute_sync(dispatch);
            }
        }
    }

    fn begin_capture(
        &mut self,
        cli_id: &str,
        request_id: RequestId,
        generation: ChannelGeneration,
        lease: CaptureLeaseId,
        _ui_session: UiSessionId,
        effects: &mut dyn HostEffects,
    ) {
        if let Some(active) = &self.active
            && active.lease == lease
        {
            let prior = active.prior;
            self.release(cli_id, request_id, generation, effects);
            self.emit_for_request(
                request_id,
                generation,
                BridgeResponse::CaptureReady {
                    lease,
                    state: ZellijCaptureState {
                        prior_mode: format!("{prior:?}"),
                    },
                },
                effects,
            );
            return;
        }
        if let Some(pending) = &mut self.pending
            && pending.lease == lease
        {
            pending.request_id = request_id;
            pending.channel_generation = generation;
            self.release(cli_id, request_id, generation, effects);
            return;
        }
        // Snapshot from actually observed mode only; with no observation yet,
        // the first ModeUpdate snapshots before any Locked request goes out.
        // The pipe releases immediately either way so the broker can queue
        // further work while capture establishes.
        if self.restoring_mode.is_none() && self.current_mode == Some(InputMode::Locked) {
            self.active = Some(ActiveCapture {
                lease,
                prior: InputMode::Locked,
            });
            self.release(cli_id, request_id, generation, effects);
            self.emit_for_request(
                request_id,
                generation,
                BridgeResponse::CaptureReady {
                    lease,
                    state: ZellijCaptureState {
                        prior_mode: format!("{:?}", InputMode::Locked),
                    },
                },
                effects,
            );
            return;
        }
        let requested = self.restoring_mode.is_none() && self.current_mode.is_some();
        self.pending = Some(PendingCapture {
            lease,
            request_id,
            channel_generation: generation,
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
        request_id: RequestId,
        generation: ChannelGeneration,
        lease: CaptureLeaseId,
        _reason: CaptureEndReason,
        effects: &mut dyn HostEffects,
    ) {
        // Guarded cancellation restores only matching pending or active state.
        // A stale lease leaves every newer owner untouched.
        self.cancel_capture_for_restore(Some(lease), effects);
        self.release(cli_id, request_id, generation, effects);
    }

    fn request_origin(
        &mut self,
        cli_id: &str,
        request_id: RequestId,
        generation: ChannelGeneration,
        ui_session: UiSessionId,
        ui_pane: String,
        effects: &mut dyn HostEffects,
    ) {
        // The owning bridge is the one whose focused pane is the attaching UI
        // pane; every other bridge declines so the adapter moves on. The UI
        // pane joins the MUXE set so later focus history never mistakes a menu
        // pane for origin.
        if self.focused_pane.as_deref() != Some(ui_pane.as_str()) {
            self.release(cli_id, request_id, generation, effects);
            self.emit_for_request(
                request_id,
                generation,
                BridgeResponse::OriginDeclined { ui_session },
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
        self.emit_for_request(
            request_id,
            generation,
            BridgeResponse::OriginSnapshot {
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
        request_id: RequestId,
        generation: ChannelGeneration,
        execution: ExecutionId,
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
        self.emit_for_request(
            request_id,
            generation,
            BridgeResponse::DispatchAccepted { execution },
            effects,
        );
        self.emit_for_request(
            request_id,
            generation,
            BridgeResponse::DispatchCompleted { execution, outcome },
            effects,
        );
    }

    fn focus_neighbor(
        &mut self,
        cli_id: &str,
        request_id: RequestId,
        generation: ChannelGeneration,
        execution: ExecutionId,
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
        self.emit_for_request(
            request_id,
            generation,
            BridgeResponse::DispatchAccepted { execution },
            effects,
        );
        self.emit_for_request(
            request_id,
            generation,
            BridgeResponse::DispatchCompleted { execution, outcome },
            effects,
        );
    }

    fn retire(
        &mut self,
        cli_id: &str,
        request_id: RequestId,
        generation: ChannelGeneration,
        effects: &mut dyn HostEffects,
    ) {
        // Retirement releases capture through the same guarded restore, then
        // fails every pending async completion instead of dangling it. The
        // restoring barrier is preserved until a non-Locked ModeUpdate is
        // observed.
        self.cancel_capture_for_restore(None, effects);
        let pending: Vec<PendingAction> = std::mem::take(&mut self.pending_actions)
            .into_values()
            .collect();
        self.release(cli_id, request_id, generation, effects);
        for pending in pending {
            self.emit_for_request(
                pending.request_id,
                pending.channel_generation,
                BridgeResponse::DispatchCompleted {
                    execution: pending.execution,
                    outcome: CommandOutcome::failed("bridge retiring".to_owned()),
                },
                effects,
            );
        }
        self.fail_post_dismissals("bridge retiring", effects);
    }

    /// Validates the request, asks Zellij to unblock the request child by its
    /// CLI source UUID, then emits the transport acknowledgement on the event
    /// child. Dispatch acceptance and completion are separate later events.
    fn release(
        &mut self,
        cli_id: &str,
        request_id: RequestId,
        generation: ChannelGeneration,
        effects: &mut dyn HostEffects,
    ) {
        effects.unblock_pipe(cli_id);
        self.emit_for_request(
            request_id,
            generation,
            BridgeResponse::RequestReleased,
            effects,
        );
    }

    fn emit_unsolicited(&self, event: BridgeEvent, effects: &mut dyn HostEffects) {
        self.emit(
            None,
            self.channel_generation,
            PipeEventKind::Event(event),
            effects,
        );
    }

    fn emit_for_request(
        &self,
        request_id: RequestId,
        generation: ChannelGeneration,
        response: BridgeResponse,
        effects: &mut dyn HostEffects,
    ) {
        self.emit(
            Some(request_id),
            Some(generation),
            PipeEventKind::Response(response),
            effects,
        );
    }

    fn emit(
        &self,
        request_id: Option<RequestId>,
        channel_generation: Option<ChannelGeneration>,
        event: PipeEventKind,
        effects: &mut dyn HostEffects,
    ) {
        let (Some(event_cli_id), Some(channel_generation), Some(registration)) = (
            self.event_cli_id.as_deref(),
            channel_generation,
            self.registration,
        ) else {
            return;
        };
        let frame = PipeEvent {
            protocol: BRIDGE_PROTOCOL_VERSION,
            request_id,
            channel_generation,
            registration,
            event,
        };
        if let Ok(line) = encode_event_line(&frame) {
            // The encoded line carries its `\n` terminator: the pinned host
            // relays `CliPipeOutput` bytes verbatim to the CLI child and the
            // native reader frames on `\n`, so stripping it would leave every
            // event buffered unreadably. Pass the wire bytes through intact.
            effects.pipe_output(event_cli_id, &line);
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
        effect_order: Vec<&'static str>,
        list_clients_ready: bool,
        list_response_pending: bool,
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
                effect_order: Vec::new(),
                list_clients_ready: false,
                list_response_pending: false,
                plugin_id: 41,
                cwd: BTreeMap::new(),
                random: VecDeque::from([[7_u8; 16], [8_u8; 16], [9_u8; 16]]),
            }
        }

        fn events(&self) -> Vec<(Option<RequestId>, PipeEventKind)> {
            self.outputs
                .iter()
                .map(|(_, line)| {
                    let frame = decode_event_line(line).expect("typed event frame");
                    (frame.request_id, frame.event)
                })
                .collect()
        }

        fn register_client_ids(&self, cli_id: &str) -> Vec<String> {
            self.outputs
                .iter()
                .filter(|(target, _)| target == cli_id)
                .filter_map(|(_, line)| decode_event_line(line).ok())
                .filter_map(|frame| match frame.event {
                    PipeEventKind::Event(BridgeEvent::Register { registration }) => {
                        Some(registration.client_id)
                    }
                    _ => None,
                })
                .collect()
        }

        fn last_event(&self) -> (Option<RequestId>, PipeEventKind) {
            self.events().pop().expect("at least one event")
        }

        fn pipe_state(&self, cli_id: &str) -> Option<FakePipeState> {
            self.pipe_states.get(cli_id).copied()
        }

        fn take_list_response(&mut self) -> bool {
            std::mem::take(&mut self.list_response_pending)
        }
    }

    impl HostEffects for FakeHost {
        fn request_permissions(&mut self, _permissions: &[PermissionType]) {
            self.effect_order.push("request_permissions");
        }
        fn subscribe(&mut self, _events: &[EventType]) {
            self.effect_order.push("subscribe");
        }
        fn list_clients(&mut self) {
            self.lists += 1;
            if self.list_clients_ready {
                self.list_response_pending = true;
            }
        }
        fn plugin_ids(&mut self) -> PluginIds {
            PluginIds {
                plugin_id: self.plugin_id,
                zellij_pid: 1000,
                initial_cwd: PathBuf::from("/tmp"),
                client_id: 5,
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
    const EVENT_CLI_TWO: &str = "event-cli-uuid-2";
    const REQUEST_NAME: &str = "muxe-request-alpha";
    const REQUEST_CLI: &str = "request-cli-uuid-9";
    fn registration(seed: u8) -> RegistrationId {
        RegistrationId::from_random_bytes([seed; 16]).expect("test registration")
    }
    fn lease(seed: u8) -> CaptureLeaseId {
        CaptureLeaseId([seed; 16])
    }
    fn session(name: &str) -> UiSessionId {
        UiSessionId::new(name)
    }
    fn exec(seed: u8) -> ExecutionId {
        ExecutionId([seed; 16])
    }

    fn clients_for(client_id: u16, pane: PaneId) -> Vec<ClientInfo> {
        vec![ClientInfo {
            client_id,
            pane_id: pane,
            running_command: String::new(),
            is_current_client: true,
        }]
    }

    fn clients_current(pane: PaneId) -> Vec<ClientInfo> {
        clients_for(5, pane)
    }

    fn mode_info(mode: InputMode) -> ModeInfo {
        ModeInfo {
            mode,
            ..Default::default()
        }
    }

    fn request_line_with_id(
        target_reg: RegistrationId,
        request_id: RequestId,
        payload: BridgeRequest,
    ) -> String {
        use muxe_zellij_protocol::{BridgeTarget, PipeRequest};
        let frame = PipeRequest {
            protocol: BRIDGE_PROTOCOL_VERSION,
            request_id,
            registration: target_reg,
            channel_generation: ChannelGeneration::INITIAL,
            target: BridgeTarget {
                client_id: "5".to_owned(),
            },
            payload,
        };
        let mut line = encode_request_line(&frame).expect("encodes");
        line.push('\n');
        line
    }

    fn request_line(target_reg: RegistrationId, payload: BridgeRequest) -> String {
        request_line_with_id(target_reg, RequestId::INITIAL, payload)
    }

    fn request_msg_for(
        registration: RegistrationId,
        request_id: RequestId,
        payload: BridgeRequest,
    ) -> PipeMessage {
        PipeMessage {
            source: PipeSource::Cli(REQUEST_CLI.to_owned()),
            name: REQUEST_NAME.to_owned(),
            payload: Some(request_line_with_id(registration, request_id, payload)),
            args: BTreeMap::new(),
            is_private: false,
        }
    }

    fn request_msg(payload: BridgeRequest) -> PipeMessage {
        request_msg_for(registration(7), RequestId::INITIAL, payload)
    }

    fn request_msg_with_id(request_id: RequestId, payload: BridgeRequest) -> PipeMessage {
        request_msg_for(registration(7), request_id, payload)
    }

    fn subscribe_msg_for(cli_id: &str) -> PipeMessage {
        let subscription = muxe_zellij_protocol::encode_event_subscription(
            muxe_zellij_protocol::EventSubscription::new(ChannelGeneration::INITIAL),
        )
        .expect("subscription encodes");
        PipeMessage {
            source: PipeSource::Cli(cli_id.to_owned()),
            name: EVENT_NAME.to_owned(),
            payload: Some(subscription),
            args: BTreeMap::new(),
            is_private: false,
        }
    }

    fn subscribe_msg() -> PipeMessage {
        subscribe_msg_for(EVENT_CLI)
    }

    /// Full startup through the real host sequence: load (no privileged
    /// query), explicit grant (one identity query), initial identity event,
    /// then event-channel subscription and a fresh identity event driving
    /// registration.
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
        assert_eq!(host.lists, 2);
        assert_eq!(bridge.active_registration(), None);
        bridge.update(
            Event::ListClients(clients_current(PaneId::Terminal(2))),
            &mut host,
        );
        assert_eq!(bridge.client_identity(), Some("5"));
        assert_eq!(bridge.active_registration(), Some(registration(7)));
        (bridge, host)
    }

    #[test]
    fn post_dismissal_creation_waits_for_missing_ui_and_restored_origin_focus() {
        let (mut bridge, mut host) = boot();
        bridge.inventory.set_manifest(BTreeMap::from([(
            0,
            vec![PaneGeometry {
                id: 7,
                is_plugin: false,
                x: 0,
                y: 0,
                columns: 80,
                rows: 24,
            }],
        )]));
        bridge.focused_pane = Some("terminal_7".to_owned());
        let before = host.events().len();

        bridge.defer_post_dismissal_creation(
            REQUEST_CLI,
            RequestId::INITIAL,
            ChannelGeneration::INITIAL,
            exec(42),
            "terminal_7".to_owned(),
            "terminal_2".to_owned(),
            RawNativeCommand::CloseFocus,
            &mut host,
        );
        assert!(
            host.events()[before..].iter().all(|(_, event)| !matches!(
                event,
                PipeEventKind::Response(BridgeResponse::DispatchCompleted { .. })
            )),
            "request is accepted but must not dispatch while the UI pane is present"
        );

        bridge.inventory.set_manifest(BTreeMap::from([(
            0,
            vec![PaneGeometry {
                id: 2,
                is_plugin: false,
                x: 0,
                y: 0,
                columns: 80,
                rows: 24,
            }],
        )]));
        bridge.on_list_clients(&clients_current(PaneId::Terminal(2)), &mut host);
        assert!(matches!(
            host.last_event(),
            (
                Some(RequestId::INITIAL),
                PipeEventKind::Response(BridgeResponse::DispatchCompleted {
                    execution,
                    outcome,
                }),
            ) if execution == exec(42) && outcome.status == muxe_zellij_protocol::CommandStatus::Succeeded
        ));
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
        assert_eq!(first.registration, registration(7));
        match first.event {
            PipeEventKind::Event(BridgeEvent::Register { registration }) => {
                assert_eq!(registration.client_id, "5");
            }
            other => panic!("first framed event is not Register: {other:?}"),
        }
    }

    #[test]
    fn no_privileged_query_before_grant() {
        let mut bridge = Bridge::default();
        let mut host = FakeHost::new();
        bridge.load(&mut host);
        assert_eq!(
            host.effect_order,
            ["subscribe", "request_permissions"],
            "load must subscribe before requesting a replayable permission result"
        );
        assert_eq!(host.lists, 0);
        // A subscription alone cannot register without an authorized
        // anchor-matching census.
        bridge.pipe(subscribe_msg(), &mut host);
        assert_eq!(host.lists, 0);
        assert_eq!(bridge.active_registration(), None);
        assert!(host.outputs.is_empty());
    }

    #[test]
    fn denied_grant_without_identity_never_registers() {
        let mut bridge = Bridge::default();
        let mut host = FakeHost::new();
        bridge.load(&mut host);
        bridge.update(
            Event::PermissionRequestResult(PermissionStatus::Denied),
            &mut host,
        );
        bridge.pipe(subscribe_msg(), &mut host);
        assert_eq!(host.lists, 0);
        assert_eq!(bridge.active_registration(), None);
        assert!(host.outputs.is_empty());
    }

    #[test]
    fn foreign_grant_and_census_cannot_register_before_anchor_match() {
        let mut bridge = Bridge::default();
        let mut host = FakeHost::new();
        bridge.load(&mut host);
        bridge.pipe(subscribe_msg(), &mut host);

        // Two consecutive grants arrive while this plugin's identity is
        // unverified. The first query is unavailable; the second must still
        // query after the host makes the actual permission usable.
        bridge.update(
            Event::PermissionRequestResult(PermissionStatus::Granted),
            &mut host,
        );
        host.list_clients_ready = true;
        bridge.update(
            Event::PermissionRequestResult(PermissionStatus::Granted),
            &mut host,
        );

        // A later foreign denial cannot erase the actual query's pending
        // authorization; the anchor-matching census below confirms it.
        bridge.update(
            Event::PermissionRequestResult(PermissionStatus::Denied),
            &mut host,
        );

        // A replayed census for another client is ignored, even though it
        // marks that client current in its own plugin instance.
        bridge.update(
            Event::ListClients(clients_for(6, PaneId::Terminal(2))),
            &mut host,
        );
        assert_eq!(bridge.client_identity(), None);
        assert_eq!(bridge.active_registration(), None);
        assert!(host.outputs.is_empty());

        // Only this plugin's anchored client census authorizes registration.
        if host.take_list_response() {
            bridge.update(
                Event::ListClients(clients_current(PaneId::Terminal(2))),
                &mut host,
            );
        }
        assert_eq!(host.register_client_ids(EVENT_CLI), vec!["5".to_owned()]);
    }

    #[test]
    fn new_subscription_revalidates_identity_after_foreign_denial() {
        let (mut bridge, mut host) = boot();
        bridge.update(
            Event::PermissionRequestResult(PermissionStatus::Denied),
            &mut host,
        );
        let before = host.outputs.len();
        bridge.pipe(subscribe_msg_for(EVENT_CLI_TWO), &mut host);
        assert_eq!(host.lists, 3);
        assert_eq!(bridge.client_identity(), Some("5"));
        assert_eq!(bridge.active_registration(), None);
        assert!(host.outputs[before..].is_empty());
        bridge.update(Event::Timer(HEARTBEAT_SECS), &mut host);
        bridge.pipe(
            request_msg(BridgeRequest::RequestOrigin {
                ui_session: session("stale-ui"),
                request: ZellijOriginRequest {
                    ui_pane: "terminal_2".to_owned(),
                },
            }),
            &mut host,
        );
        assert!(
            host.outputs[before..].is_empty(),
            "replacement channel must not heartbeat or accept the displaced registration"
        );

        bridge.update(
            Event::ListClients(clients_for(6, PaneId::Terminal(2))),
            &mut host,
        );
        assert_eq!(bridge.client_identity(), Some("5"));
        assert_eq!(bridge.active_registration(), None);
        assert!(host.register_client_ids(EVENT_CLI_TWO).is_empty());

        bridge.update(
            Event::ListClients(clients_current(PaneId::Terminal(2))),
            &mut host,
        );
        assert_eq!(bridge.active_registration(), Some(registration(8)));
        assert_eq!(
            host.register_client_ids(EVENT_CLI_TWO),
            vec!["5".to_owned()]
        );
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
                execution: exec(1),
                request: ZellijDispatchRequest::Command(RawNativeCommand::CloseFocus),
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
                PipeEventKind::Event(BridgeEvent::Register { .. }) => "register",
                PipeEventKind::Response(BridgeResponse::RequestReleased) => "released",
                PipeEventKind::Response(BridgeResponse::DispatchAccepted { .. }) => "accepted",
                PipeEventKind::Response(BridgeResponse::DispatchCompleted { .. }) => "completed",
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
            registration(9),
            BridgeRequest::Dispatch {
                execution: exec(2),
                request: ZellijDispatchRequest::Command(RawNativeCommand::CloseFocus),
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
                lease: lease(11),
                ui_session: session("ui-1"),
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
            PipeEventKind::Response(BridgeResponse::CaptureReady {
                lease: actual_lease,
                ..
            }) if actual_lease == lease(11)
        ));
    }

    #[test]
    fn ending_requested_pending_capture_compensates_delayed_locked() {
        let (mut bridge, mut host) = boot();
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        bridge.pipe(
            request_msg(BridgeRequest::BeginCapture {
                lease: lease(61),
                ui_session: session("ui-1"),
            }),
            &mut host,
        );
        assert_eq!(host.modes.as_slice(), [InputMode::Locked]);
        bridge.pipe(
            request_msg_with_id(
                RequestId::try_from(2).expect("second request"),
                BridgeRequest::EndCapture {
                    lease: lease(61),
                    reason: CaptureEndReason::UiDismissed,
                },
            ),
            &mut host,
        );
        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal]
        );
        assert_eq!(bridge.capture_state(), (None, None));

        // A repeated pre-transition Normal observation cannot clear the
        // barrier before the already queued Locked transition is observed.
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        bridge.pipe(
            request_msg_with_id(
                RequestId::try_from(3).expect("third request"),
                BridgeRequest::BeginCapture {
                    lease: lease(62),
                    ui_session: session("ui-2"),
                },
            ),
            &mut host,
        );
        assert_eq!(bridge.capture_state(), (Some([62; 16]), None));
        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal]
        );

        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (Some([62; 16]), None));
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal, InputMode::Locked]
        );
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (None, Some([62; 16])));
    }

    #[test]
    fn resubscribing_requested_pending_capture_compensates_delayed_locked() {
        let (mut bridge, mut host) = boot();
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        bridge.pipe(
            request_msg(BridgeRequest::BeginCapture {
                lease: lease(71),
                ui_session: session("ui-1"),
            }),
            &mut host,
        );
        bridge.pipe(subscribe_msg_for(EVENT_CLI_TWO), &mut host);
        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal]
        );
        assert_eq!(bridge.capture_state(), (None, None));

        bridge.update(
            Event::ListClients(clients_current(PaneId::Terminal(2))),
            &mut host,
        );
        assert_eq!(bridge.active_registration(), Some(registration(8)));
        bridge.pipe(
            request_msg_for(
                registration(8),
                RequestId::INITIAL,
                BridgeRequest::BeginCapture {
                    lease: lease(72),
                    ui_session: session("ui-2"),
                },
            ),
            &mut host,
        );
        assert_eq!(bridge.capture_state(), (Some([72; 16]), None));

        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (Some([72; 16]), None));
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal, InputMode::Locked]
        );
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (None, Some([72; 16])));
    }

    #[test]
    fn stale_end_does_not_cancel_a_new_pending_capture() {
        let (mut bridge, mut host) = boot();
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        bridge.pipe(
            request_msg(BridgeRequest::BeginCapture {
                lease: lease(31),
                ui_session: session("ui-1"),
            }),
            &mut host,
        );
        bridge.pipe(
            request_msg_with_id(
                RequestId::try_from(2).expect("second request"),
                BridgeRequest::EndCapture {
                    lease: lease(30),
                    reason: CaptureEndReason::LeaseExpired,
                },
            ),
            &mut host,
        );

        assert_eq!(bridge.capture_state(), (Some([31; 16]), None));
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (None, Some([31; 16])));
        let (_, event) = host.last_event();
        assert!(matches!(
            event,
            PipeEventKind::Response(BridgeResponse::CaptureReady {
                lease: actual_lease,
                state
            }) if actual_lease == lease(31) && state.prior_mode == "Normal"
        ));
    }

    #[test]
    fn replacement_capture_waits_for_restore_observation() {
        let (mut bridge, mut host) = boot();
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        bridge.pipe(
            request_msg(BridgeRequest::BeginCapture {
                lease: lease(41),
                ui_session: session("ui-1"),
            }),
            &mut host,
        );
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        bridge.pipe(
            request_msg_with_id(
                RequestId::try_from(2).expect("second request"),
                BridgeRequest::EndCapture {
                    lease: lease(41),
                    reason: CaptureEndReason::Replaced,
                },
            ),
            &mut host,
        );
        bridge.pipe(
            request_msg_with_id(
                RequestId::try_from(3).expect("third request"),
                BridgeRequest::BeginCapture {
                    lease: lease(42),
                    ui_session: session("ui-2"),
                },
            ),
            &mut host,
        );

        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal]
        );
        assert_eq!(bridge.capture_state(), (Some([42; 16]), None));
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (Some([42; 16]), None));
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal, InputMode::Locked]
        );
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (None, Some([42; 16])));
        let (_, event) = host.last_event();
        assert!(matches!(
            event,
            PipeEventKind::Response(BridgeResponse::CaptureReady {
                lease: actual_lease,
                state
            }) if actual_lease == lease(42) && state.prior_mode == "Normal"
        ));
    }
    #[test]
    fn resubscription_restores_before_replacement_completes() {
        let (mut bridge, mut host) = boot();
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        bridge.pipe(
            request_msg(BridgeRequest::BeginCapture {
                lease: lease(51),
                ui_session: session("ui-1"),
            }),
            &mut host,
        );
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (None, Some([51; 16])));
        // A new event subscription restores the active capture through the
        // shared guard, arming the barrier with the observed Normal prior.
        bridge.pipe(subscribe_msg_for(EVENT_CLI_TWO), &mut host);
        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal]
        );
        assert_eq!(bridge.capture_state(), (None, None));
        // Re-registration lands before the restore ModeUpdate is observed.
        bridge.update(
            Event::ListClients(clients_current(PaneId::Terminal(2))),
            &mut host,
        );
        assert_eq!(bridge.active_registration(), Some(registration(8)));
        // Begin B while the barrier holds stays pending without a Locked
        // request: the host still reports the stale Locked mode.
        bridge.pipe(
            request_msg_for(
                registration(8),
                RequestId::INITIAL,
                BridgeRequest::BeginCapture {
                    lease: lease(52),
                    ui_session: session("ui-2"),
                },
            ),
            &mut host,
        );
        assert_eq!(bridge.capture_state(), (Some([52; 16]), None));
        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal]
        );
        // Stale Locked is ignored until the restore observation arrives.
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (Some([52; 16]), None));
        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal]
        );
        // The restore observation clears the barrier and requests Locked;
        // its confirmation completes the replacement with the true prior.
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        assert_eq!(
            host.modes.as_slice(),
            [InputMode::Locked, InputMode::Normal, InputMode::Locked]
        );
        assert_eq!(bridge.capture_state(), (Some([52; 16]), None));
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        assert_eq!(bridge.capture_state(), (None, Some([52; 16])));
        let (_, event) = host.last_event();
        assert!(matches!(
            event,
            PipeEventKind::Response(BridgeResponse::CaptureReady {
                lease: actual_lease,
                state
            }) if actual_lease == lease(52) && state.prior_mode == "Normal"
        ));
    }

    #[test]
    fn badly_ordered_request_ids_are_observed_but_not_rejected() {
        let (mut bridge, mut host) = boot();
        let before = host.unblocks.len();
        for id in [3_u64, 2] {
            bridge.pipe(
                request_msg_with_id(
                    RequestId::try_from(id).expect("nonzero request"),
                    BridgeRequest::Dispatch {
                        execution: exec(u8::try_from(id).expect("small request ID")),
                        request: ZellijDispatchRequest::Command(RawNativeCommand::CloseFocus),
                    },
                ),
                &mut host,
            );
        }
        assert_eq!(host.unblocks.len(), before + 2);
        let completed: Vec<_> = host
            .outputs
            .iter()
            .filter_map(|(_, line)| decode_event_line(line).ok())
            .filter_map(|frame| {
                matches!(
                    frame.event,
                    PipeEventKind::Response(BridgeResponse::DispatchCompleted { .. })
                )
                .then_some(frame.request_id)
            })
            .collect();
        assert!(completed.contains(&Some(RequestId::try_from(3).expect("three"))));
        assert!(completed.contains(&Some(RequestId::try_from(2).expect("two"))));
    }

    #[test]
    fn retrying_active_capture_preserves_original_mode() {
        let (mut bridge, mut host) = boot();
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        bridge.pipe(
            request_msg(BridgeRequest::BeginCapture {
                lease: lease(21),
                ui_session: session("ui-1"),
            }),
            &mut host,
        );
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        host.modes.clear();
        bridge.pipe(
            request_msg_with_id(
                RequestId::try_from(3).expect("third request"),
                BridgeRequest::BeginCapture {
                    lease: lease(21),
                    ui_session: session("ui-1"),
                },
            ),
            &mut host,
        );
        assert!(host.modes.is_empty());
        let (request_id, event) = host.last_event();
        assert_eq!(
            request_id,
            Some(RequestId::try_from(3).expect("third request"))
        );
        assert!(matches!(
            event,
            PipeEventKind::Response(BridgeResponse::CaptureReady {
                lease: actual_lease,
                state
            }) if actual_lease == lease(21) && state.prior_mode == "Normal"
        ));
        bridge.pipe(
            request_msg_with_id(
                RequestId::try_from(4).expect("fourth request"),
                BridgeRequest::EndCapture {
                    lease: lease(21),
                    reason: CaptureEndReason::LeaseExpired,
                },
            ),
            &mut host,
        );
        assert_eq!(host.modes.as_slice(), [InputMode::Normal]);
    }

    #[test]
    fn late_async_completion_cannot_cross_registration_reset() {
        let (mut bridge, mut host) = boot();
        bridge.pipe(
            request_msg(BridgeRequest::Dispatch {
                execution: exec(60),
                request: ZellijDispatchRequest::Command(RawNativeCommand::RunAction {
                    action: muxe_zellij_protocol::generated::raw::Action::CloseFocus,
                    context: Vec::new(),
                }),
            }),
            &mut host,
        );
        bridge.pipe(subscribe_msg_for(EVENT_CLI_TWO), &mut host);
        bridge.update(
            Event::ListClients(clients_current(PaneId::Terminal(2))),
            &mut host,
        );
        assert_eq!(bridge.active_registration(), Some(registration(8)));
        bridge.pipe(
            request_msg_for(
                registration(8),
                RequestId::INITIAL,
                BridgeRequest::Dispatch {
                    execution: exec(61),
                    request: ZellijDispatchRequest::Command(RawNativeCommand::RunAction {
                        action: muxe_zellij_protocol::generated::raw::Action::CloseFocus,
                        context: Vec::new(),
                    }),
                },
            ),
            &mut host,
        );

        let before = host.outputs.len();
        bridge.update(
            Event::ActionComplete(
                zellij_utils::input::actions::Action::CloseFocus,
                None,
                BTreeMap::from([(
                    crate::dispatcher::EXECUTION_CONTEXT_KEY.to_owned(),
                    format!("{}:{}", registration(7), RequestId::INITIAL),
                )]),
            ),
            &mut host,
        );
        assert_eq!(host.outputs.len(), before);

        bridge.update(
            Event::ActionComplete(
                zellij_utils::input::actions::Action::CloseFocus,
                None,
                BTreeMap::from([(
                    crate::dispatcher::EXECUTION_CONTEXT_KEY.to_owned(),
                    format!("{}:{}", registration(8), RequestId::INITIAL),
                )]),
            ),
            &mut host,
        );
        let (request_id, event) = host.last_event();
        assert_eq!(request_id, Some(RequestId::INITIAL));
        assert!(matches!(
            event,
            PipeEventKind::Response(BridgeResponse::DispatchCompleted { execution, .. })
                if execution == exec(61)
        ));
    }

    #[test]
    fn user_mode_change_dismisses_without_restore() {
        let (mut bridge, mut host) = boot();
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Normal)), &mut host);
        bridge.pipe(
            request_msg(BridgeRequest::BeginCapture {
                lease: lease(12),
                ui_session: session("ui-1"),
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
            PipeEventKind::Event(BridgeEvent::CaptureLost {
                reason: CaptureLostReason::UserModeChanged,
                ..
            })
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
                    registration(7),
                    BridgeRequest::RequestOrigin {
                        ui_session: session("ui-9"),
                        request: ZellijOriginRequest {
                            ui_pane: "terminal_2".to_owned(),
                        },
                    },
                )),
                args: BTreeMap::new(),
                is_private: false,
            },
            &mut host,
        );
        let (_, event) = host.last_event();
        match event {
            PipeEventKind::Response(BridgeResponse::OriginSnapshot { origin, .. }) => {
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
                lease: lease(13),
                ui_session: session("ui-1"),
            }),
            &mut host,
        );
        bridge.update(Event::ModeUpdate(mode_info(InputMode::Locked)), &mut host);
        // Queue an async run_action without completing it.
        bridge.pipe(
            request_msg(BridgeRequest::Dispatch {
                execution: exec(9),
                request: ZellijDispatchRequest::Command(RawNativeCommand::RunAction {
                    action: muxe_zellij_protocol::generated::raw::Action::CloseFocus,
                    context: Vec::new(),
                }),
            }),
            &mut host,
        );
        host.modes.clear();
        bridge.pipe(request_msg(BridgeRequest::Retire), &mut host);
        // Guarded restore ran (still Locked, owned lease) and the pending
        // async completion failed instead of dangling.
        assert_eq!(host.modes.as_slice(), [InputMode::Normal]);
        let failed = host.events().into_iter().any(|(_, event)| {
            matches!(
                event,
                PipeEventKind::Response(BridgeResponse::DispatchCompleted { execution, .. })
                    if execution == exec(9)
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
        assert!(matches!(
            event,
            PipeEventKind::Event(BridgeEvent::Heartbeat)
        ));
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
                        PipeEventKind::Event(BridgeEvent::Heartbeat)
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
                execution: exec(70),
                request: ZellijDispatchRequest::Command(RawNativeCommand::CloseFocus),
            }),
            &mut host,
        );

        assert_eq!(host.pipe_state(REQUEST_CLI), Some(FakePipeState::Released));
        assert_eq!(host.pipe_state(EVENT_CLI), Some(FakePipeState::Blocked));
        assert_eq!(host.unblocks.as_slice(), [REQUEST_CLI]);
    }
}
