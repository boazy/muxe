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

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use thiserror::Error;
use muxe_zellij_protocol::{RegistrationId, RequestId};


/// Heartbeat lease duration: a registration that goes quiet this long is stale.
pub const HEARTBEAT_LEASE: Duration = Duration::from_secs(15);

/// One active client registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BridgeRecord {
    /// Zellij client ID.
    pub client_id: String,
    /// Active registration ID; displaced IDs are rejected.
    pub registration: RegistrationId,
    /// Last focused pane reported by the bridge, if any.
    pub current_pane: Option<String>,
    /// Muxe version from the handshake.
    pub muxe_version: String,
    /// Last event time in milliseconds; renews the heartbeat lease.
    pub last_event_millis: u64,
    /// Whether the handshake fingerprints matched the compiled record.
    pub compatible: bool,
    /// Next request identity allocated within this registration.
    next_request_id: RequestId,
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
    /// The registration exhausted its sequential request ID space.
    #[error("request ID space exhausted for client '{client_id}'")]
    RequestIdExhausted {
        /// Client whose active registration exhausted its request IDs.
        client_id: String,
    },
    /// The active registration failed its compatibility handshake.
    #[error("incompatible registration for client '{client_id}'")]
    Incompatible {
        /// Client whose registration cannot accept requests.
        client_id: String,
    },
    /// A bridge reused an identity retired by an earlier registration epoch.
    #[error("retired registration reused for client '{client_id}'")]
    RetiredRegistration {
        /// Client attempting to reuse the retired registration.
        client_id: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct ZellijRegistry {
    records: BTreeMap<String, BridgeRecord>,
    retired: BTreeSet<RegistrationId>,
}

impl ZellijRegistry {
    /// Empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a fresh bridge, atomically superseding any previous
    /// registration for the client. Returns the displaced registration ID, if any.
    pub fn register(
        &mut self,
        client_id: &str,
        registration: RegistrationId,
        current_pane: Option<String>,
        muxe_version: String,
        compatible: bool,
        now_millis: u64,
    ) -> Result<Option<RegistrationId>, RegistryError> {
        let displaced = self
            .records
            .get(client_id)
            .map(|record| record.registration);
        // A duplicate Register on the same live channel renews the lease
        // without resetting request correlation.
        if displaced == Some(registration) {
            if let Some(record) = self.records.get_mut(client_id) {
                record.last_event_millis = now_millis;
                record.current_pane = current_pane;
            }
            return Ok(None);
        }
        if self.retired.contains(&registration) {
            return Err(RegistryError::RetiredRegistration {
                client_id: client_id.to_owned(),
            });
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
                next_request_id: RequestId::INITIAL,
            },
        );
        if let Some(displaced) = displaced {
            self.retired.insert(displaced);
        }
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
        registration: RegistrationId,
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
        registration: RegistrationId,
    ) -> Result<&BridgeRecord, RegistryError> {
        match self.records.get(client_id) {
            Some(record) if record.registration == registration => Ok(record),
            _ => Err(RegistryError::Stale {
                client_id: client_id.to_owned(),
            }),
        }
    }

    /// Allocates the next request identity from the active registration.
    ///
    /// A new registration starts at [`RequestId::INITIAL`]. Re-registering the
    /// same ID preserves its counter, so a duplicate Register cannot alias an
    /// in-flight request.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Stale`] when the client has no active
    /// registration, [`RegistryError::Incompatible`] when its handshake failed,
    /// and [`RegistryError::RequestIdExhausted`] before the counter could wrap.
    pub fn allocate_request(
        &mut self,
        client_id: &str,
    ) -> Result<(RegistrationId, RequestId), RegistryError> {
        let record = self
            .records
            .get_mut(client_id)
            .ok_or_else(|| RegistryError::Stale {
                client_id: client_id.to_owned(),
            })?;
        if !record.compatible {
            return Err(RegistryError::Incompatible {
                client_id: client_id.to_owned(),
            });
        }
        let allocated = record.next_request_id;
        record.next_request_id =
            allocated
                .next()
                .map_err(|_| RegistryError::RequestIdExhausted {
                    client_id: client_id.to_owned(),
                })?;
        Ok((record.registration, allocated))
    }

    /// Returns the active record for a client, if any.
    #[must_use]
    pub fn get(&self, client_id: &str) -> Option<&BridgeRecord> {
        self.records.get(client_id)
    }
    /// Resolves an active client by registration identity.
    #[must_use]
    pub fn client_for_registration(&self, registration: RegistrationId) -> Option<&str> {
        self.records
            .values()
            .find(|record| record.registration == registration)
            .map(|record| record.client_id.as_str())
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
    /// Returns each invalidated client and registration; their queues pause
    /// until fresh registrations arrive.
    pub fn expire_leases(&mut self, now_millis: u64) -> Vec<(String, RegistrationId)> {
        let lease_millis: u64 = HEARTBEAT_LEASE.as_millis().try_into().unwrap_or(u64::MAX);
        let expired_clients: Vec<String> = self
            .records
            .iter()
            .filter(|(_, record)| {
                now_millis.saturating_sub(record.last_event_millis) > lease_millis
            })
            .map(|(client, _)| client.clone())
            .collect();
        let mut expired = Vec::with_capacity(expired_clients.len());
        for client in expired_clients {
            if let Some(record) = self.records.remove(&client) {
                self.retired.insert(record.registration);
                expired.push((client, record.registration));
            }
        }
        expired
    }
    /// Invalidates every registration, retires each identity, and returns
    /// the displaced client/registration pairs for pending-work cleanup.
    pub fn invalidate_all(&mut self) -> Vec<(String, RegistrationId)> {
        let records = std::mem::take(&mut self.records);
        let displaced: Vec<_> = records
            .into_values()
            .map(|record| (record.client_id, record.registration))
            .collect();
        self.retired
            .extend(displaced.iter().map(|(_, registration)| *registration));
        displaced
    }

    /// Removes one client's registration after an orderly bridge shutdown.
    pub fn remove(&mut self, client_id: &str, registration: RegistrationId) -> bool {
        match self.records.get(client_id) {
            Some(record) if record.registration == registration => {
                let record = self.records.remove(client_id).expect("record existed");
                self.retired.insert(record.registration);
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

    fn registration(seed: u8) -> RegistrationId {
        RegistrationId::from_random_bytes([seed; 16]).expect("test registration")
    }

    fn register(table: &mut ZellijRegistry, client: &str, seed: u8) -> Option<RegistrationId> {
        table
            .register(
                client,
                registration(seed),
                None,
                "0.1.0".to_owned(),
                true,
                NOW,
            )
            .expect("registration accepted")
    }

    #[test]
    fn registration_supersedes_atomically() {
        let mut table = ZellijRegistry::new();
        assert_eq!(register(&mut table, "a", 1), None);
        assert_eq!(register(&mut table, "a", 2), Some(registration(1)));
        assert!(matches!(
            table.heartbeat("a", registration(1), NOW),
            Err(RegistryError::Stale { .. })
        ));
        assert!(table.heartbeat("a", registration(2), NOW).is_ok());
    }

    #[test]
    fn event_channel_reset_rejects_retired_registration() {
        let mut table = ZellijRegistry::new();
        register(&mut table, "a", 1);
        assert_eq!(
            table.invalidate_all(),
            vec![("a".to_owned(), registration(1))]
        );
        assert!(matches!(
            table.register(
                "a",
                registration(1),
                None,
                "0.1.0".to_owned(),
                true,
                NOW + 1,
            ),
            Err(RegistryError::RetiredRegistration { .. })
        ));
        assert!(table.is_empty());
    }

    #[test]
    fn request_counter_resets_only_for_new_registration() {
        let mut table = ZellijRegistry::new();
        register(&mut table, "a", 1);
        assert_eq!(
            table.allocate_request("a"),
            Ok((registration(1), RequestId::INITIAL))
        );
        table
            .register(
                "a",
                registration(1),
                Some("pane-1".to_owned()),
                "0.1.0".to_owned(),
                true,
                NOW + 1,
            )
            .expect("same registration renews");
        assert_eq!(
            table.allocate_request("a"),
            Ok((
                registration(1),
                RequestId::INITIAL.next().expect("second request")
            ))
        );
        register(&mut table, "a", 2);
        assert_eq!(
            table.allocate_request("a"),
            Ok((registration(2), RequestId::INITIAL))
        );
    }

    #[test]
    fn multi_client_routing_resolves_unique_pane_owner() {
        let mut table = ZellijRegistry::new();
        table
            .register(
                "a",
                registration(1),
                Some("pane-1".to_owned()),
                "0.1.0".to_owned(),
                true,
                NOW,
            )
            .expect("a");
        table
            .register(
                "b",
                registration(2),
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
        register(&mut table, "a", 1);
        register(&mut table, "b", 2);
        table
            .heartbeat("b", registration(2), NOW + 5_000)
            .expect("b renews");
        let expired = table.expire_leases(NOW + 15_001);
        assert_eq!(expired, vec![("a".to_owned(), registration(1))]);
        assert!(table.get("a").is_none());
        assert!(table.get("b").is_some());
    }

    #[test]
    fn orderly_removal_needs_active_id() {
        let mut table = ZellijRegistry::new();
        register(&mut table, "a", 1);
        assert!(!table.remove("a", registration(9)));
        assert!(table.remove("a", registration(1)));
        assert!(table.is_empty());
    }
}
