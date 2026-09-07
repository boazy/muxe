//! Transport classifications audited against Herdr 0.8.2's pinned request schema.
//!
//! `events.subscribe` is the only request whose successful initial response changes the
//! connection into a long-lived subscription-event stream. Every other listed request is
//! explicitly classified as one request followed by one response. New pinned methods must be
//! classified here rather than receiving an unsafe default.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Unary,
    EventStream,
}

const UNARY_METHODS: &[&str] = &[
    "agent.explain",
    "agent.focus",
    "agent.get",
    "agent.list",
    "agent.prompt",
    "agent.read",
    "agent.rename",
    "agent.send_keys",
    "agent.start",
    "agent.view.clear",
    "agent.view.set",
    "agent.wait",
    "client.window_title.clear",
    "client.window_title.set",
    "events.wait",
    "integration.install",
    "integration.uninstall",
    "layout.apply",
    "layout.export",
    "layout.set_split_ratio",
    "notification.show",
    "pane.clear_agent_authority",
    "pane.close",
    "pane.current",
    "pane.edges",
    "pane.focus",
    "pane.focus_direction",
    "pane.get",
    "pane.graphics.clear",
    "pane.graphics.info",
    "pane.graphics.set",
    "pane.input.set",
    "pane.layout",
    "pane.list",
    "pane.move",
    "pane.neighbor",
    "pane.process_info",
    "pane.read",
    "pane.release_agent",
    "pane.rename",
    "pane.report_agent",
    "pane.report_agent_session",
    "pane.report_metadata",
    "pane.resize",
    "pane.send_input",
    "pane.send_keys",
    "pane.send_text",
    "pane.split",
    "pane.swap",
    "pane.wait_for_output",
    "pane.zoom",
    "ping",
    "plugin.action.invoke",
    "plugin.action.list",
    "plugin.disable",
    "plugin.enable",
    "plugin.link",
    "plugin.list",
    "plugin.log.list",
    "plugin.pane.close",
    "plugin.pane.focus",
    "plugin.pane.open",
    "plugin.unlink",
    "popup.close",
    "server.agent_manifests",
    "server.live_handoff",
    "server.reload_agent_manifests",
    "server.reload_config",
    "server.stop",
    "session.snapshot",
    "tab.close",
    "tab.create",
    "tab.focus",
    "tab.get",
    "tab.list",
    "tab.move",
    "tab.rename",
    "workspace.close",
    "workspace.create",
    "workspace.focus",
    "workspace.get",
    "workspace.list",
    "workspace.move",
    "workspace.move_block",
    "workspace.rename",
    "workspace.report_metadata",
    "worktree.create",
    "worktree.list",
    "worktree.open",
    "worktree.remove",
];

const EVENT_STREAM_METHODS: &[&str] = &["events.subscribe"];

pub fn transport_for(method: &str) -> Option<Transport> {
    if UNARY_METHODS.binary_search(&method).is_ok() {
        Some(Transport::Unary)
    } else if EVENT_STREAM_METHODS.binary_search(&method).is_ok() {
        Some(Transport::EventStream)
    } else {
        None
    }
}
