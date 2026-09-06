//! Origin-context capture from bridge snapshots.
//!
//! The target bridge snapshots the last focused non-Muxe pane when the Muxe UI
//! attaches through its own pane ID. This module converts the typed
//! [`ZellijOrigin`] snapshot into an immutable [`OriginContext`], treating every
//! host value as untrusted: malformed IDs fail capture instead of poisoning
//! dispatch targets, and missing values stay missing so the broker can report
//! `context_unavailable` before dispatch.

use std::path::PathBuf;

use muxe_core::{
    ClientId, OriginContext, OriginHostKind, OriginInvocationSource, PaneId, ServerId, SessionId,
    TabId,
};
use muxe_zellij_protocol::ZellijOrigin;
use thiserror::Error;

/// Origin capture failure.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum OriginError {
    /// A host-supplied identifier is empty or overlong.
    #[error("invalid origin {field}: {reason}")]
    InvalidId {
        /// Which identifier failed.
        field: &'static str,
        /// Why it failed.
        reason: &'static str,
    },
    /// A supplied working directory is not absolute.
    #[error("origin working directory must be absolute")]
    RelativeCwd,
}

/// Builds the immutable attach-time origin from a validated bridge snapshot.
///
/// `server_name` is the live session name the broker was configured with; a
/// snapshot for a different session fails rather than cross-wiring dispatch.
/// `ui_pane` is the attaching Muxe UI's own pane, kept as the pane identity so
/// cleanup targets the right pane while action targets resolve from the prior
/// (pre-menu) pane snapshot.
pub fn build_origin_context(
    snapshot: &ZellijOrigin,
    server_name: &str,
    ui_pane: &str,
    session_id: Option<&str>,
    tab_id: Option<&str>,
    tab_index: Option<u64>,
) -> Result<OriginContext, OriginError> {
    if let Some(session) = &snapshot.session_name
        && session != server_name
    {
        return Err(OriginError::InvalidId {
            field: "session",
            reason: "snapshot names a different live session",
        });
    }
    let prior_cwd: Option<PathBuf> = snapshot
        .prior_pane_cwd
        .as_deref()
        .map(|cwd| {
            let path = PathBuf::from(cwd);
            if path.is_absolute() {
                Ok(path)
            } else {
                Err(OriginError::RelativeCwd)
            }
        })
        .transpose()?;
    Ok(OriginContext {
        host_kind: OriginHostKind::Zellij,
        server_id: ServerId::new(server_name),
        client_id: Some(ClientId::new(snapshot.client_id.clone())),
        session_id: session_id
            .or(snapshot.session_name.as_deref())
            .map(SessionId::new),
        workspace_id: None,
        tab_id: tab_id.map(TabId::new),
        tab_index,
        pane_id: Some(PaneId::new(ui_pane)),
        pane_type: None,
        pane_cwd: prior_cwd,
        selection_text: None,
        invocation_source: OriginInvocationSource::RootBinding,
        worktree_id: None,
        worktree_path: None,
        agent_id: None,
        link_url: None,
        link_handler_id: None,
    })
}

/// The prior (pre-menu) pane is the action target for pane-scoped behavior.
/// Returns its snapshot ID when the bridge tracked one.
pub fn prior_pane_id(snapshot: &ZellijOrigin) -> Option<&str> {
    snapshot.prior_pane_id.as_deref()
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_zellij_protocol::ZellijOrigin;

    fn snapshot() -> ZellijOrigin {
        ZellijOrigin {
            client_id: "client-1".to_owned(),
            session_name: Some("alpha".to_owned()),
            prior_pane_id: Some("terminal-2".to_owned()),
            ui_pane_id: "plugin-9".to_owned(),
            prior_pane_cwd: Some("/home/user/work".to_owned()),
        }
    }

    #[test]
    fn snapshot_builds_immutable_origin() {
        let origin = build_origin_context(
            &snapshot(),
            "alpha",
            "plugin-9",
            None,
            Some("tab-0"),
            Some(0),
        )
        .expect("valid snapshot builds");
        assert_eq!(origin.host_kind, OriginHostKind::Zellij);
        assert_eq!(origin.server_id.as_str(), "alpha");
        assert_eq!(origin.pane_cwd, Some(PathBuf::from("/home/user/work")));
        assert_eq!(origin.tab_index, Some(0));
        assert_eq!(prior_pane_id(&snapshot()), Some("terminal-2"));
    }

    #[test]
    fn cross_session_snapshot_fails() {
        let error = build_origin_context(&snapshot(), "beta", "plugin-9", None, None, None)
            .expect_err("different session fails");
        assert!(matches!(
            error,
            OriginError::InvalidId {
                field: "session",
                ..
            }
        ));
    }

    #[test]
    fn relative_cwd_fails() {
        let mut snapshot = snapshot();
        snapshot.prior_pane_cwd = Some("relative/path".to_owned());
        assert!(matches!(
            build_origin_context(&snapshot, "alpha", "plugin-9", None, None, None),
            Err(OriginError::RelativeCwd)
        ));
    }

    #[test]
    fn missing_values_stay_missing() {
        let mut snapshot = snapshot();
        snapshot.prior_pane_cwd = None;
        snapshot.session_name = None;
        let origin =
            build_origin_context(&snapshot, "alpha", "plugin-9", None, None, None).expect("builds");
        assert_eq!(origin.pane_cwd, None);
        assert_eq!(origin.session_id, None);
        assert_eq!(prior_pane_id(&snapshot), Some("terminal-2"));
    }
}
