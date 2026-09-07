//! Per-client bridge registration table with heartbeat leases.
//!
//! The broker maintains exactly one active registration per Zellij client and
//! targets `(client_id, bridge_registration_id)` on the broadcast pipe. A new
//! registration atomically supersedes the previous one for its client; late
//! events from a displaced registration are rejected, never reactivated.
//!
//! Each active registration owns a heartbeat lease renewed by that
//! registration's events. When one lease expires, only that registration is
//! invalidated and only that client's queue pauses; healthy clients on the same
//! event pipe continue. Whole-pipe restarts are a transport decision elsewhere.
//!
//! This is a pure state machine with an injected clock so the adapter-contract
//! suite can simulate registration churn deterministically.

use std::collections::BTreeMap;
use std::time::Duration;

use thiserror::Error;

/// Heartbeat lease duration: a registration that goes quiet this long is stale.
pub const HEARTBEAT_LEASE: Duration = Duration::from_secs(15);

/// One active client registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BridgeRecord {
    /// Zellij client ID.
    pub client_id: String,
    /// Active registration ID; displaced IDs are rejected.
    pub registration: [u8; 16],
    /// Last focused pane reported by the bridge, if any.
    pub current_pane: Option<String>,
    /// Muxe version from the handshake.
    pub muxe_version: String,
    /// Last event time in milliseconds; renews the heartbeat lease.
    pub last_event_millis: u64,
    /// Whether the handshake fingerprints matched the compiled record.
    pub compatible: bool,
}

/// Registration table errors.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum RegistryError {
    /// Event arrived for an unknown or displaced registration.
    #[error("stale registration for client '{client_id}'")]
    Stale {
        /// Client that sent the event.
        client_id: String,
    },
    /// Registration attempted with a zero ID.
    #[error("registration ID must not be zero")]
    ZeroRegistration,
}

/// Per-client active registration table.
#[derive(Clone, Debug, Default)]
pub struct ZellijRegistry {
    records: BTreeMap<String, BridgeRecord>,
}

impl ZellijRegistry {
    /// Empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a fresh bridge, atomically superseding any previous
    /// registration for the client. Returns the displaced registration ID, if any,
    /// so the caller can send a best-effort retirement notice.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::ZeroRegistration`] when `registration` is all zeros.
    pub fn register(
        &mut self,
        client_id: &str,
        registration: [u8; 16],
        current_pane: Option<String>,
        muxe_version: String,
        compatible: bool,
        now_millis: u64,
    ) -> Result<Option<[u8; 16]>, RegistryError> {
        if registration == [0; 16] {
            return Err(RegistryError::ZeroRegistration);
        }
        let displaced = self
            .records
            .get(client_id)
            .map(|record| record.registration);
        // A re-registration of the already-active ID only renews the lease.
        if displaced == Some(registration) {
            if let Some(record) = self.records.get_mut(client_id) {
                record.last_event_millis = now_millis;
                record.current_pane = current_pane;
            }
            return Ok(None);
        }
        self.records.insert(
            client_id.to_owned(),
            BridgeRecord {
                client_id: client_id.to_owned(),
                registration,
                current_pane,
                muxe_version,
                last_event_millis: now_millis,
                compatible,
            },
        );
        Ok(displaced)
    }
    /// Deterministically ordered active client IDs, for origin fan-out.
    #[must_use]
    pub fn client_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.records.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Renews the heartbeat lease for the active registration; rejects late
    /// events from displaced registrations without mutating table state.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Stale`] when no active registration matches
    /// (`client_id`, `registration`).
    pub fn heartbeat(
        &mut self,
        client_id: &str,
        registration: [u8; 16],
        now_millis: u64,
    ) -> Result<(), RegistryError> {
        match self.records.get_mut(client_id) {
            Some(record) if record.registration == registration => {
                record.last_event_millis = now_millis;
                Ok(())
            }
            _ => Err(RegistryError::Stale {
                client_id: client_id.to_owned(),
            }),
        }
    }

    /// Checks that an event's registration is still active.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Stale`] when no active registration matches
    /// (`client_id`, `registration`).
    pub fn check(
        &self,
        client_id: &str,
        registration: [u8; 16],
    ) -> Result<&BridgeRecord, RegistryError> {
        match self.records.get(client_id) {
            Some(record) if record.registration == registration => Ok(record),
            _ => Err(RegistryError::Stale {
                client_id: client_id.to_owned(),
            }),
        }
    }

    /// Returns the active record for a client, if any.
    #[must_use]
    pub fn get(&self, client_id: &str) -> Option<&BridgeRecord> {
        self.records.get(client_id)
    }

    /// Resolves the unique client owning a pane through active registrations.
    /// Fails rather than guessing when no unique client owns the pane.
    #[must_use]
    pub fn client_for_pane(&self, pane_id: &str) -> Option<&BridgeRecord> {
        let mut matches = self
            .records
            .values()
            .filter(|record| record.current_pane.as_deref() == Some(pane_id));
        let first = matches.next()?;
        matches.next().is_none().then_some(first)
    }

    /// Invalidates registrations whose heartbeat lease expired as of `now`.
    /// Returns the invalidated client IDs; their queues pause until fresh
    /// registrations arrive.
    pub fn expire_leases(&mut self, now_millis: u64) -> Vec<String> {
        let lease_millis: u64 = HEARTBEAT_LEASE.as_millis().try_into().unwrap_or(u64::MAX);
        let expired: Vec<String> = self
            .records
            .iter()
            .filter(|(_, record)| {
                now_millis.saturating_sub(record.last_event_millis) > lease_millis
            })
            .map(|(client, _)| client.clone())
            .collect();
        for client in &expired {
            self.records.remove(client);
        }
        expired
    }

    /// Invalidates every registration, for whole-pipe replacement. Queues pause
    /// until fresh registrations arrive on the new channel.
    pub fn invalidate_all(&mut self) {
        self.records.clear();
    }

    /// Removes one client's registration after an orderly bridge shutdown.
    pub fn remove(&mut self, client_id: &str, registration: [u8; 16]) -> bool {
        match self.records.get(client_id) {
            Some(record) if record.registration == registration => {
                self.records.remove(client_id);
                true
            }
            _ => false,
        }
    }

    /// Number of active registrations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the table holds no registrations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_000_000;

    #[test]
    fn registration_supersedes_atomically() {
        let mut table = ZellijRegistry::new();
        let displaced = table
            .register("a", [1; 16], None, "0.1.0".to_owned(), true, NOW)
            .expect("registers");
        assert_eq!(displaced, None);
        let displaced = table
            .register("a", [2; 16], None, "0.1.0".to_owned(), true, NOW)
            .expect("re-registers");
        assert_eq!(displaced, Some([1; 16]));
        // Late events from the displaced registration are rejected.
        assert!(matches!(
            table.heartbeat("a", [1; 16], NOW),
            Err(RegistryError::Stale { .. })
        ));
        assert!(table.heartbeat("a", [2; 16], NOW).is_ok());
    }

    #[test]
    fn same_id_reregistration_renews_without_displacement() {
        let mut table = ZellijRegistry::new();
        table
            .register("a", [1; 16], None, "0.1.0".to_owned(), true, NOW)
            .expect("registers");
        let displaced = table
            .register(
                "a",
                [1; 16],
                Some("pane-1".to_owned()),
                "0.1.0".to_owned(),
                true,
                NOW + 1,
            )
            .expect("renews");
        assert_eq!(displaced, None);
        assert_eq!(
            table.get("a").expect("present").current_pane.as_deref(),
            Some("pane-1")
        );
    }

    #[test]
    fn multi_client_routing_resolves_unique_pane_owner() {
        let mut table = ZellijRegistry::new();
        table
            .register(
                "a",
                [1; 16],
                Some("pane-1".to_owned()),
                "0.1.0".to_owned(),
                true,
                NOW,
            )
            .expect("a");
        table
            .register(
                "b",
                [2; 16],
                Some("pane-2".to_owned()),
                "0.1.0".to_owned(),
                true,
                NOW,
            )
            .expect("b");
        assert_eq!(
            table.client_for_pane("pane-1").expect("owner").client_id,
            "a"
        );
        assert_eq!(
            table.client_for_pane("pane-2").expect("owner").client_id,
            "b"
        );
        assert!(table.client_for_pane("pane-9").is_none());
    }

    #[test]
    fn expired_lease_invalidates_only_that_client() {
        let mut table = ZellijRegistry::new();
        table
            .register("a", [1; 16], None, "0.1.0".to_owned(), true, NOW)
            .expect("a");
        table
            .register("b", [2; 16], None, "0.1.0".to_owned(), true, NOW)
            .expect("b");
        table
            .heartbeat("b", [2; 16], NOW + 5_000)
            .expect("b renews");
        let expired = table.expire_leases(NOW + 15_001);
        assert_eq!(expired, vec!["a".to_owned()]);
        assert!(table.get("a").is_none());
        assert!(table.get("b").is_some());
    }

    #[test]
    fn zero_registration_is_rejected() {
        let mut table = ZellijRegistry::new();
        assert!(matches!(
            table.register("a", [0; 16], None, "0.1.0".to_owned(), true, NOW),
            Err(RegistryError::ZeroRegistration)
        ));
    }

    #[test]
    fn orderly_removal_needs_active_id() {
        let mut table = ZellijRegistry::new();
        table
            .register("a", [1; 16], None, "0.1.0".to_owned(), true, NOW)
            .expect("a");
        assert!(!table.remove("a", [9; 16]));
        assert!(table.remove("a", [1; 16]));
        assert!(table.is_empty());
    }
}
