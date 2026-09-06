//! Per-client modal capture state machine for Zellij Locked mode.
//!
//! The broker models modal capture as serialized single-owner state, not a
//! reference count: one record per client holds the current UI session, the
//! capture lease, and the snapshotted original input mode. Replacement always
//! completes the old capture transaction first; a lease transfer without a
//! release-then-recapture transition is deferred.
//!
//! Restoration is guarded twice: the adapter releases only while the recorded
//! lease still owns capture, and only while the client remains in Muxe-owned
//! Locked mode. A user-driven mode change is authoritative newer state and is
//! never restored over. This is a pure state machine; the adapter performs the
//! host round-trips the transitions require.

use std::collections::BTreeMap;
use thiserror::Error;

/// One client's capture record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureRecord {
    /// Broker UI session the capture serves.
    pub ui_session: String,
    /// Lease minted for this capture generation.
    pub lease: [u8; 16],
    /// Input mode snapshotted before Locked mode, as a Zellij mode name.
    pub prior_mode: String,
    /// Whether the bridge confirmed Locked mode.
    pub ready: bool,
}

/// Capture lifecycle of one client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaptureState {
    /// No menu owns capture; a begin snapshots the current mode.
    Idle,
    /// Capture requested; waiting for the bridge to confirm Locked mode.
    Beginning {
        /// UI session the pending capture serves.
        ui_session: String,
        /// Lease minted for the pending capture.
        lease: [u8; 16],
    },
    /// One UI session owns Locked-mode capture.
    Captured(CaptureRecord),
}

impl CaptureState {
    /// Whether a new capture may begin immediately.
    pub fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

/// Capture transition errors.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum CaptureError {
    /// Another UI already owns or is acquiring capture.
    #[error("client capture is busy")]
    Busy,
    /// No capture is active for this client.
    #[error("no active capture for client")]
    NotCaptured,
    /// The supplied lease no longer owns capture.
    #[error("stale capture lease")]
    StaleLease,
}

/// Per-client capture table. Replacement ordering (dismiss the old UI and wait
/// for its detach acknowledgement) is broker-driven; this table owns the
/// single-owner invariant and guarded restoration decisions.
#[derive(Clone, Debug, Default)]
pub struct CaptureTable {
    states: BTreeMap<String, CaptureState>,
}

impl CaptureTable {
    /// Empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Current state for a client, defaulting to idle.
    pub fn state(&self, client_id: &str) -> &CaptureState {
        self.states.get(client_id).unwrap_or(&CaptureState::Idle)
    }

    /// Begins capture for one UI session. Fails while another capture owns or
    /// acquires the client so replacements serialize through release first.
    pub fn begin(
        &mut self,
        client_id: &str,
        ui_session: &str,
        lease: [u8; 16],
    ) -> Result<(), CaptureError> {
        if !self.state(client_id).is_idle() {
            return Err(CaptureError::Busy);
        }
        self.states.insert(
            client_id.to_owned(),
            CaptureState::Beginning {
                ui_session: ui_session.to_owned(),
                lease,
            },
        );
        Ok(())
    }

    /// Confirms Locked mode with the snapshotted prior mode.
    pub fn confirm(
        &mut self,
        client_id: &str,
        lease: [u8; 16],
        prior_mode: String,
    ) -> Result<(), CaptureError> {
        match self.states.get(client_id) {
            Some(CaptureState::Beginning {
                ui_session,
                lease: pending,
            }) if *pending == lease => {
                let record = CaptureRecord {
                    ui_session: ui_session.clone(),
                    lease,
                    prior_mode,
                    ready: true,
                };
                self.states
                    .insert(client_id.to_owned(), CaptureState::Captured(record));
                Ok(())
            }
            // A lease that no longer owns the client is stale, whether the
            // client is mid-begin under another lease or already captured.
            Some(CaptureState::Beginning { .. }) | Some(CaptureState::Captured(_)) => {
                Err(CaptureError::StaleLease)
            }
            _ => Err(CaptureError::NotCaptured),
        }
    }

    /// Decides guarded restoration for a broker-driven release.
    ///
    /// Returns the prior mode to restore when the lease still owns capture and
    /// the client remains in Muxe-owned Locked mode. Returns `None` (no restore)
    /// when the mode already changed externally: the newer mode is user-owned
    /// state and must be preserved.
    pub fn release(
        &mut self,
        client_id: &str,
        lease: [u8; 16],
        still_locked: bool,
    ) -> Result<Option<String>, CaptureError> {
        match self.states.get(client_id) {
            Some(CaptureState::Captured(record)) if record.lease == lease => {
                let restore = still_locked.then(|| record.prior_mode.clone());
                self.states.insert(client_id.to_owned(), CaptureState::Idle);
                Ok(restore)
            }
            Some(CaptureState::Captured(_)) => Err(CaptureError::StaleLease),
            Some(CaptureState::Beginning { lease: pending, .. }) if *pending == lease => {
                self.states.insert(client_id.to_owned(), CaptureState::Idle);
                Ok(None)
            }
            Some(CaptureState::Beginning { .. }) => Err(CaptureError::StaleLease),
            _ => Err(CaptureError::NotCaptured),
        }
    }
    #[cfg(test)]
    /// Releases a test capture after a simulated user-owned mode change.
    fn user_mode_changed(&mut self, client_id: &str) -> Option<String> {
        let session = match self.states.get(client_id) {
            Some(CaptureState::Captured(record)) => Some(record.ui_session.clone()),
            Some(CaptureState::Beginning { ui_session, .. }) => Some(ui_session.clone()),
            _ => None,
        };
        if session.is_some() {
            self.states.insert(client_id.to_owned(), CaptureState::Idle);
        }
        session
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_serializes_through_release() {
        let mut table = CaptureTable::new();
        table.begin("a", "ui-1", [1; 16]).expect("begins");
        // A second root cannot begin while the first owns the client.
        assert!(matches!(
            table.begin("a", "ui-2", [2; 16]),
            Err(CaptureError::Busy)
        ));
        table
            .confirm("a", [1; 16], "Normal".to_owned())
            .expect("confirms");
        // Release restores the captured mode while still Locked.
        assert_eq!(
            table.release("a", [1; 16], true).expect("releases"),
            Some("Normal".to_owned())
        );
        // After release the replacement begins cleanly.
        table
            .begin("a", "ui-2", [2; 16])
            .expect("replacement begins");
    }

    #[test]
    fn externally_changed_mode_is_preserved() {
        let mut table = CaptureTable::new();
        table.begin("a", "ui-1", [1; 16]).expect("begins");
        table
            .confirm("a", [1; 16], "Normal".to_owned())
            .expect("confirms");
        // The mode already changed: no restoration, capture still released.
        assert_eq!(table.release("a", [1; 16], false).expect("releases"), None);
        assert!(table.state("a").is_idle());
    }

    #[test]
    fn user_mode_change_dismisses_without_restore() {
        let mut table = CaptureTable::new();
        table.begin("a", "ui-1", [1; 16]).expect("begins");
        table
            .confirm("a", [1; 16], "Normal".to_owned())
            .expect("confirms");
        assert_eq!(table.user_mode_changed("a"), Some("ui-1".to_owned()));
        assert!(table.state("a").is_idle());
        assert_eq!(table.user_mode_changed("a"), None);
    }

    #[test]
    fn stale_lease_never_restores() {
        let mut table = CaptureTable::new();
        table.begin("a", "ui-1", [1; 16]).expect("begins");
        table
            .confirm("a", [1; 16], "Normal".to_owned())
            .expect("confirms");
        assert!(matches!(
            table.release("a", [9; 16], true),
            Err(CaptureError::StaleLease)
        ));
        assert!(matches!(
            table.confirm("a", [9; 16], "Normal".to_owned()),
            Err(CaptureError::StaleLease)
        ));
        // The rightful lease still owns capture.
        assert_eq!(
            table.release("a", [1; 16], true).expect("releases"),
            Some("Normal".to_owned())
        );
    }

    #[test]
    fn pending_begin_releases_without_restore() {
        let mut table = CaptureTable::new();
        table.begin("a", "ui-1", [1; 16]).expect("begins");
        assert_eq!(table.release("a", [1; 16], true).expect("aborts"), None);
        assert!(table.state("a").is_idle());
    }
}
