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
/// `ui_pane` is the attaching Muxe UI's own pane. It is verified against the
/// snapshot but never stored as the action origin: pane-scoped actions resolve
/// from the verified bridge PRIOR (pre-menu) pane, so the UI pane can never
/// become a dispatch target. The UI pane identity remains the key for
/// cleanup/session/capture correlation in the adapter's snapshot tables.
///
/// # Errors
///
/// Returns [`OriginError::InvalidId`] when the snapshot names a different UI
/// pane or session, or when the prior pane is empty. Returns
/// [`OriginError::RelativeCwd`] when the prior pane working directory is not
/// absolute.
pub fn build_origin_context(
    snapshot: &ZellijOrigin,
    server_name: &str,
    ui_pane: &str,
    session_id: Option<&str>,
    tab_id: Option<&str>,
    tab_index: Option<u64>,
) -> Result<OriginContext, OriginError> {
    // The bridge frame is the sole source of tab/session metadata: the
    // snapshot's typed active-tab and session fields flow straight into the
    // immutable origin, with explicit caller overrides winning only when the
    // caller proves a value. There is no post-capture field injection path.
    let tab_index = tab_index.or(snapshot.active_tab_index);
    let tab_id = tab_id
        .map(str::to_owned)
        .or_else(|| snapshot.active_tab_id.map(|id| id.to_string()));
    if snapshot.ui_pane_id != ui_pane {
        return Err(OriginError::InvalidId {
            field: "ui-pane",
            reason: "snapshot is for a different UI pane",
        });
    }
    let prior = match snapshot.prior_pane_id.as_deref() {
        None => None,
        Some("") => {
            return Err(OriginError::InvalidId {
                field: "prior-pane",
                reason: "bridge reported an empty prior pane",
            });
        }
        Some(pane) => Some(PaneId::new(pane)),
    };
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
        pane_id: prior,
        pane_type: snapshot.prior_pane_is_plugin.map(|is_plugin| {
            if is_plugin {
                muxe_core::OriginPaneType::Plugin
            } else {
                muxe_core::OriginPaneType::Terminal
            }
        }),
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
#[must_use]
pub fn prior_pane_id(snapshot: &ZellijOrigin) -> Option<&str> {
    snapshot.prior_pane_id.as_deref()
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_zellij_protocol::ZellijOrigin;

    /// Production-shaped snapshot: every tab/session field comes from the
    /// bridge frame itself, never hand-filled after capture.
    fn snapshot() -> ZellijOrigin {
        ZellijOrigin {
            client_id: "client-1".to_owned(),
            session_name: Some("alpha".to_owned()),
            active_tab_index: Some(3),
            active_tab_id: Some(11),
            prior_pane_id: Some("terminal-2".to_owned()),
            ui_pane_id: "plugin-9".to_owned(),
            prior_pane_cwd: Some("/home/user/work".to_owned()),
            prior_pane_is_plugin: Some(false),
        }
    }

    #[test]
    fn snapshot_builds_immutable_origin() {
        // Production-shaped capture: no caller-supplied tab/session values.
        // Every asserted identity comes from the bridge frame itself.
        let origin = build_origin_context(&snapshot(), "alpha", "plugin-9", None, None, None)
            .expect("valid snapshot builds");
        assert_eq!(origin.host_kind, OriginHostKind::Zellij);
        assert_eq!(origin.server_id.as_str(), "alpha");
        // The action origin is the bridge PRIOR pane, never the Muxe UI pane:
        // pane-scoped dispatch must not close or write into the menu itself.
        assert_eq!(
            origin.pane_id.as_ref().map(muxe_core::PaneId::as_str),
            Some("terminal-2")
        );
        assert_eq!(origin.pane_cwd, Some(PathBuf::from("/home/user/work")));
        assert_eq!(
            origin.session_id.as_ref().map(muxe_core::SessionId::as_str),
            Some("alpha")
        );
        assert_eq!(
            origin.tab_id.as_ref().map(muxe_core::TabId::as_str),
            Some("11")
        );
        assert_eq!(origin.tab_index, Some(3));
        assert_eq!(origin.pane_type, Some(muxe_core::OriginPaneType::Terminal));
        assert_eq!(prior_pane_id(&snapshot()), Some("terminal-2"));
    }

    #[test]
    fn caller_tab_override_wins_over_snapshot() {
        let origin = build_origin_context(
            &snapshot(),
            "alpha",
            "plugin-9",
            None,
            Some("tab-9"),
            Some(9),
        )
        .expect("caller override builds");
        assert_eq!(
            origin.tab_id.as_ref().map(muxe_core::TabId::as_str),
            Some("tab-9")
        );
        assert_eq!(origin.tab_index, Some(9));
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
        snapshot.active_tab_index = None;
        snapshot.active_tab_id = None;
        snapshot.prior_pane_is_plugin = None;
        let origin =
            build_origin_context(&snapshot, "alpha", "plugin-9", None, None, None).expect("builds");
        assert_eq!(origin.pane_cwd, None);
        assert_eq!(origin.session_id, None);
        assert_eq!(origin.tab_index, None);
        assert_eq!(origin.tab_id, None);
        assert_eq!(origin.pane_type, None);
        assert_eq!(
            origin.pane_id.as_ref().map(muxe_core::PaneId::as_str),
            Some("terminal-2")
        );
        assert_eq!(prior_pane_id(&snapshot), Some("terminal-2"));
    }

    #[test]
    fn ui_pane_mismatch_fails() {
        let error = build_origin_context(&snapshot(), "alpha", "plugin-8", None, None, None)
            .expect_err("foreign UI pane fails");
        assert!(matches!(
            error,
            OriginError::InvalidId {
                field: "ui-pane",
                ..
            }
        ));
    }

    #[test]
    fn empty_prior_pane_fails() {
        let mut snapshot = snapshot();
        snapshot.prior_pane_id = Some(String::new());
        let error = build_origin_context(&snapshot, "alpha", "plugin-9", None, None, None)
            .expect_err("empty prior fails");
        assert!(matches!(
            error,
            OriginError::InvalidId {
                field: "prior-pane",
                ..
            }
        ));
    }

    #[test]
    fn missing_prior_pane_stays_missing() {
        let mut snapshot = snapshot();
        snapshot.prior_pane_id = None;
        let origin =
            build_origin_context(&snapshot, "alpha", "plugin-9", None, None, None).expect("builds");
        assert_eq!(origin.pane_id, None);
    }
}
