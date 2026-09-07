use std::{collections::HashMap, time::Instant};

use muxe_adapter_api::PendingPaneLease;
use muxe_protocol::{HostPaneId, HostTabId, MenuId, ModalScopeId, PendingLaunchToken, UiSessionId};
use rand::{TryRngCore, rngs::OsRng};
use thiserror::Error;

pub trait TokenSource {
    /// Mints one nonzero pending-launch token.
    ///
    /// # Errors
    ///
    /// Returns `GateError::EntropyUnavailable` when OS randomness fails.
    fn pending_launch_token(&mut self) -> Result<PendingLaunchToken, GateError>;
}

#[derive(Default)]
pub struct OsTokenSource;

impl TokenSource for OsTokenSource {
    fn pending_launch_token(&mut self) -> Result<PendingLaunchToken, GateError> {
        let mut bytes = [0; 16];
        OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(|_| GateError::EntropyUnavailable)?;
        Ok(PendingLaunchToken(bytes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredPane {
    pub pane: HostPaneId,
    pub temporary_tab: Option<HostTabId>,
    pub lease: Option<PendingPaneLease>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingLaunch {
    pub token: PendingLaunchToken,
    pub scope: ModalScopeId,
    pub root: MenuId,
    pub expires_at: Instant,
    pub registered_pane: Option<RegisteredPane>,
    pub attached_ui: Option<UiSessionId>,
    pub attached_pane: Option<HostPaneId>,
    /// Placement is committed independently from UI attachment. The token
    /// remains pending until both facts are recorded and complete.
    pub placement_committed: bool,
    pub session_published: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScopeOwner {
    Pending(PendingLaunchToken),
    Ready(UiSessionId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedLaunch {
    pub pending: PendingLaunch,
    /// The caller must apply the normal detach/abort cleanup to this owner before host pane
    /// creation. The new token is not made ready until that cleanup has completed.
    pub replaced: Option<ScopeOwner>,
    /// A replaced pending launch carries the exact registered pane identity so the broker can
    /// revalidate and clean it without ever inferring an origin pane.
    pub replaced_pending: Option<PendingLaunch>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttachDisposition {
    Ready,
    WaitingForCommit { token: PendingLaunchToken },
}

#[derive(Default)]
pub struct LaunchGate {
    pending: HashMap<PendingLaunchToken, PendingLaunch>,
    scopes: HashMap<ModalScopeId, ScopeOwner>,
    sessions: HashMap<UiSessionId, ModalScopeId>,
}

impl LaunchGate {
    /// Mints a pending launch, replacing any prior launch in the same scope.
    ///
    /// # Errors
    ///
    /// Returns `GateError` on zero leases, token-source failure, or duplicate tokens.
    pub fn prepare(
        &mut self,
        source: &mut dyn TokenSource,
        scope: ModalScopeId,
        root: MenuId,
        now: Instant,
        lease: std::time::Duration,
    ) -> Result<PreparedLaunch, GateError> {
        if lease.is_zero() {
            return Err(GateError::ZeroLease);
        }
        let token = source.pending_launch_token()?;
        if token.is_zero() || self.pending.contains_key(&token) {
            return Err(GateError::DuplicateToken);
        }
        let replaced = self.scopes.remove(&scope);
        let replaced_pending = match replaced.as_ref() {
            Some(ScopeOwner::Pending(previous)) => self.pending.remove(previous),
            Some(ScopeOwner::Ready(_)) | None => None,
        };
        let pending = PendingLaunch {
            token,
            scope: scope.clone(),
            root,
            expires_at: now.checked_add(lease).ok_or(GateError::LeaseOverflow)?,
            registered_pane: None,
            attached_ui: None,
            attached_pane: None,
            placement_committed: false,
            session_published: false,
        };
        self.scopes.insert(scope, ScopeOwner::Pending(token));
        self.pending.insert(token, pending.clone());
        Ok(PreparedLaunch {
            pending,
            replaced,
            replaced_pending,
        })
    }

    /// Records the host-created pane for a pending launch.
    ///
    /// # Errors
    ///
    /// Returns `GateError` for unknown tokens or mismatched panes and tabs.
    pub fn register_pending_pane(
        &mut self,
        token: PendingLaunchToken,
        pane: RegisteredPane,
    ) -> Result<(), GateError> {
        let pending = self
            .pending
            .get_mut(&token)
            .ok_or(GateError::UnknownToken)?;
        if pending
            .attached_pane
            .as_ref()
            .is_some_and(|attached| attached != &pane.pane)
        {
            return Err(GateError::PaneMismatch);
        }
        match &pending.registered_pane {
            Some(existing) if existing != &pane => Err(GateError::PaneMismatch),
            Some(_) => Ok(()),
            None => {
                pending.registered_pane = Some(pane);
                Ok(())
            }
        }
    }
    /// Binds an adapter lease to a pending launch's registered pane.
    ///
    /// # Errors
    ///
    /// Returns `GateError::UnknownToken` when the launch is gone or
    /// `GateError::PaneMismatch` when no matching pane is registered.
    pub fn bind_pending_lease(
        &mut self,
        token: PendingLaunchToken,
        lease: PendingPaneLease,
    ) -> Result<(), GateError> {
        let pending = self
            .pending
            .get_mut(&token)
            .ok_or(GateError::UnknownToken)?;
        let registered = pending
            .registered_pane
            .as_mut()
            .ok_or(GateError::PaneMismatch)?;
        if registered.lease.is_some() && registered.lease.as_ref() != Some(&lease) {
            return Err(GateError::PaneMismatch);
        }
        registered.lease = Some(lease);
        Ok(())
    }
    /// Returns the registered pane for a pending launch.
    ///
    /// # Errors
    ///
    /// Returns `GateError::UnknownToken` when the launch is gone.
    pub fn registered_pane(
        &self,
        token: PendingLaunchToken,
    ) -> Result<Option<RegisteredPane>, GateError> {
        Ok(self
            .pending
            .get(&token)
            .ok_or(GateError::UnknownToken)?
            .registered_pane
            .clone())
    }

    /// Attaches a session to a scope or a pending launch.
    ///
    /// # Errors
    ///
    /// Returns `GateError` on duplicate sessions, unknown tokens, or scope/pane mismatches.
    pub fn attach(
        &mut self,
        session: UiSessionId,
        scope: ModalScopeId,
        pane: HostPaneId,
        token: Option<PendingLaunchToken>,
    ) -> Result<AttachDisposition, GateError> {
        if self.sessions.contains_key(&session) {
            return Err(GateError::DuplicateSession);
        }
        if let Some(token) = token {
            let scope = {
                let pending = self
                    .pending
                    .get_mut(&token)
                    .ok_or(GateError::UnknownToken)?;
                if pending.scope != scope {
                    return Err(GateError::ScopeMismatch);
                }
                if pending
                    .registered_pane
                    .as_ref()
                    .is_some_and(|registered| registered.pane != pane)
                {
                    return Err(GateError::PaneMismatch);
                }
                if pending
                    .attached_ui
                    .as_ref()
                    .is_some_and(|ui| ui != &session)
                {
                    return Err(GateError::TokenAlreadyAttached);
                }
                pending.attached_ui = Some(session.clone());
                pending.attached_pane = Some(pane);
                pending.scope.clone()
            };
            self.sessions.insert(session, scope);
            Ok(AttachDisposition::WaitingForCommit { token })
        } else {
            if self.scopes.contains_key(&scope) {
                return Err(GateError::ScopeOccupied);
            }
            self.scopes
                .insert(scope.clone(), ScopeOwner::Ready(session.clone()));
            self.sessions.insert(session, scope);
            Ok(AttachDisposition::Ready)
        }
    }

    /// Commits a pending launch to its registered pane. Commit may precede UI
    /// attachment; the token remains pending until both facts are recorded and complete.
    ///
    /// # Errors
    ///
    /// Returns `GateError` for unknown tokens or pane mismatches.
    pub fn commit(
        &mut self,
        token: PendingLaunchToken,
        final_pane: &HostPaneId,
    ) -> Result<Option<UiSessionId>, GateError> {
        let (scope, session, published) = {
            let pending = self.pending.get(&token).ok_or(GateError::UnknownToken)?;
            let registered = pending
                .registered_pane
                .as_ref()
                .ok_or(GateError::PaneMismatch)?;
            if registered.pane != *final_pane
                || pending
                    .attached_pane
                    .as_ref()
                    .is_some_and(|attached| attached != final_pane)
            {
                return Err(GateError::PaneMismatch);
            }
            (
                pending.scope.clone(),
                pending.attached_ui.clone(),
                pending.session_published,
            )
        };
        if !matches!(
            self.scopes.get(&scope),
            Some(ScopeOwner::Pending(active)) if *active == token
        ) {
            return Err(GateError::ScopeMismatch);
        }
        self.pending
            .get_mut(&token)
            .ok_or(GateError::UnknownToken)?
            .placement_committed = true;
        if !published {
            return Ok(None);
        }
        let Some(session) = session else {
            return Err(GateError::UiNotAttached);
        };
        self.pending.remove(&token);
        self.scopes
            .insert(scope, ScopeOwner::Ready(session.clone()));
        Ok(Some(session))
    }

    /// Publishes the fully-created broker session after awaited origin capture.
    ///
    /// # Errors
    ///
    /// Returns `GateError::UnknownToken`, `GateError::TokenAlreadyAttached`, or
    /// `GateError::ScopeMismatch` when the reservation no longer matches.
    pub fn publish_attached(
        &mut self,
        token: PendingLaunchToken,
        session: &UiSessionId,
    ) -> Result<bool, GateError> {
        let (scope, placement_committed) = {
            let pending = self
                .pending
                .get_mut(&token)
                .ok_or(GateError::UnknownToken)?;
            if pending.attached_ui.as_ref() != Some(session) {
                return Err(GateError::TokenAlreadyAttached);
            }
            pending.session_published = true;
            (pending.scope.clone(), pending.placement_committed)
        };
        if !placement_committed {
            return Ok(false);
        }
        if !matches!(
            self.scopes.get(&scope),
            Some(ScopeOwner::Pending(active)) if *active == token
        ) {
            return Err(GateError::ScopeMismatch);
        }
        self.pending.remove(&token);
        self.scopes
            .insert(scope, ScopeOwner::Ready(session.clone()));
        Ok(true)
    }
    /// Aborts an uncommitted pending launch and returns its pane for cleanup.
    ///
    /// # Errors
    ///
    /// Returns `GateError::UnknownToken` when the launch is gone.
    pub fn abort_if_uncommitted(
        &mut self,
        token: PendingLaunchToken,
    ) -> Result<Option<RegisteredPane>, GateError> {
        if self.placement_committed(token) {
            return Ok(None);
        }
        self.abort(token)
    }
    ///
    /// # Errors
    ///
    /// Returns `GateError::UnknownToken` for unknown tokens.
    pub fn abort(
        &mut self,
        token: PendingLaunchToken,
    ) -> Result<Option<RegisteredPane>, GateError> {
        let pending = self.pending.remove(&token).ok_or(GateError::UnknownToken)?;
        if matches!(self.scopes.get(&pending.scope), Some(ScopeOwner::Pending(active)) if *active == token)
        {
            self.scopes.remove(&pending.scope);
        }
        if let Some(session) = pending.attached_ui {
            self.sessions.remove(&session);
        }
        Ok(pending.registered_pane)
    }
    /// Returns whether a pending launch has committed placement but is not yet
    /// fully published. Launcher disconnect must not abort such a launch.
    #[must_use]
    pub fn placement_committed(&self, token: PendingLaunchToken) -> bool {
        self.pending
            .get(&token)
            .is_some_and(|pending| pending.placement_committed)
    }

    /// Tokens of pending launches bound to `scope`, for scoped health
    /// invalidation. The broker aborts each token and releases the scope owner.
    #[must_use]
    pub fn pending_tokens_in_scope(&self, scope: &ModalScopeId) -> Vec<PendingLaunchToken> {
        self.pending
            .values()
            .filter(|launch| launch.scope == *scope)
            .map(|launch| launch.token)
            .collect()
    }

    /// Releases a scope owner left dangling by scoped invalidation: a pending
    /// owner whose token is gone, or a ready owner with no session. Owners
    /// minted after the expiry always carry a live token or session, so fresh
    /// registrations survive this cleanup.
    pub fn release_dangling_owner(&mut self, scope: &ModalScopeId) {
        let dangling = match self.scopes.get(scope) {
            Some(ScopeOwner::Pending(token)) => !self.pending.contains_key(token),
            Some(ScopeOwner::Ready(session)) => !self.sessions.contains_key(session),
            None => false,
        };
        if dangling {
            self.scopes.remove(scope);
        }
    }

    pub fn expire(&mut self, now: Instant) -> Vec<PendingLaunch> {
        let expired: Vec<_> = self
            .pending
            .values()
            .filter(|pending| pending.expires_at <= now)
            .cloned()
            .collect();
        for pending in &expired {
            self.pending.remove(&pending.token);
            if matches!(self.scopes.get(&pending.scope), Some(ScopeOwner::Pending(active)) if *active == pending.token)
            {
                self.scopes.remove(&pending.scope);
            }
            if let Some(session) = &pending.attached_ui {
                self.sessions.remove(session);
            }
        }
        expired
    }

    /// Removes every pending launch and scope owner while activation drains the broker.
    ///
    /// The broker performs capture and host-pane cleanup from the returned records before its
    /// listener is released to a target. No subsequent attachment can inherit a pre-drain scope.
    pub fn drain(&mut self) -> Vec<PendingLaunch> {
        self.scopes.clear();
        self.sessions.clear();
        self.pending.drain().map(|(_, launch)| launch).collect()
    }

    pub fn detach(&mut self, session: &UiSessionId) -> Option<ModalScopeId> {
        let scope = self.sessions.remove(session)?;
        if matches!(self.scopes.get(&scope), Some(ScopeOwner::Ready(active)) if active == session) {
            self.scopes.remove(&scope);
        }
        Some(scope)
    }

    #[must_use]
    pub fn owner(&self, scope: &ModalScopeId) -> Option<&ScopeOwner> {
        self.scopes.get(scope)
    }

    #[must_use]
    pub fn pending(&self, token: PendingLaunchToken) -> Option<&PendingLaunch> {
        self.pending.get(&token)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GateError {
    #[error("operating system randomness is unavailable")]
    EntropyUnavailable,
    #[error("pending launch lease must be nonzero")]
    ZeroLease,
    #[error("pending launch lease overflowed its deadline")]
    LeaseOverflow,
    #[error("token source produced a duplicate or zero token")]
    DuplicateToken,
    #[error("unknown pending launch token")]
    UnknownToken,
    #[error("pending launch belongs to another modal scope")]
    ScopeMismatch,
    #[error("modal scope is already owned")]
    ScopeOccupied,
    #[error("pending pane registration is already in progress")]
    RegistrationInProgress,
    #[error("a different pane was registered or committed for this launch")]
    PaneMismatch,
    #[error("the pending launch already has a different attached UI")]
    TokenAlreadyAttached,
    #[error("UI session is already attached")]
    DuplicateSession,
    #[error("pending launch cannot commit before its UI has attached")]
    UiNotAttached,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedTokens(Vec<PendingLaunchToken>);

    impl TokenSource for FixedTokens {
        fn pending_launch_token(&mut self) -> Result<PendingLaunchToken, GateError> {
            self.0.pop().ok_or(GateError::EntropyUnavailable)
        }
    }

    fn id(value: &str) -> String {
        value.into()
    }

    #[test]
    fn pending_launch_waits_for_exact_pane_commit_and_releases_on_abort() {
        let now = Instant::now();
        let mut tokens = FixedTokens(vec![PendingLaunchToken([1; 16])]);
        let mut gate = LaunchGate::default();
        let prepared = gate
            .prepare(
                &mut tokens,
                ModalScopeId::new(id("scope")),
                MenuId::new(id("root")),
                now,
                std::time::Duration::from_secs(1),
            )
            .unwrap();
        let pane = RegisteredPane {
            pane: HostPaneId::new(id("pending-pane")),
            temporary_tab: Some(HostTabId::new(id("temporary-tab"))),
            lease: None,
        };
        gate.register_pending_pane(prepared.pending.token, pane.clone())
            .unwrap();
        assert_eq!(
            gate.attach(
                UiSessionId::new(id("ui")),
                ModalScopeId::new(id("scope")),
                pane.pane.clone(),
                Some(prepared.pending.token),
            )
            .unwrap(),
            AttachDisposition::WaitingForCommit {
                token: prepared.pending.token
            }
        );
        assert!(
            !gate
                .publish_attached(prepared.pending.token, &UiSessionId::new(id("ui")))
                .unwrap()
        );
        assert_eq!(
            gate.commit(prepared.pending.token, &pane.pane).unwrap(),
            Some(UiSessionId::new(id("ui")))
        );
        assert!(matches!(
            gate.owner(&ModalScopeId::new(id("scope"))),
            Some(ScopeOwner::Ready(_))
        ));
        gate.detach(&UiSessionId::new(id("ui")));
        assert!(gate.owner(&ModalScopeId::new(id("scope"))).is_none());
    }

    #[test]
    fn commit_before_attach_remains_pending_until_attachment() {
        let now = Instant::now();
        let mut tokens = FixedTokens(vec![PendingLaunchToken([3; 16])]);
        let mut gate = LaunchGate::default();
        let prepared = gate
            .prepare(
                &mut tokens,
                ModalScopeId::new(id("scope")),
                MenuId::new(id("root")),
                now,
                std::time::Duration::from_secs(1),
            )
            .unwrap();
        let pane = RegisteredPane {
            pane: HostPaneId::new(id("pane")),
            temporary_tab: None,
            lease: None,
        };
        gate.register_pending_pane(prepared.pending.token, pane.clone())
            .unwrap();
        assert_eq!(
            gate.commit(prepared.pending.token, &pane.pane).unwrap(),
            None
        );
        assert!(matches!(
            gate.owner(&ModalScopeId::new(id("scope"))),
            Some(ScopeOwner::Pending(token)) if *token == prepared.pending.token
        ));
        assert_eq!(
            gate.attach(
                UiSessionId::new(id("ui")),
                ModalScopeId::new(id("scope")),
                pane.pane.clone(),
                Some(prepared.pending.token),
            )
            .unwrap(),
            AttachDisposition::WaitingForCommit {
                token: prepared.pending.token
            }
        );
        assert!(
            gate.publish_attached(prepared.pending.token, &UiSessionId::new(id("ui")))
                .unwrap()
        );
        assert!(matches!(
            gate.owner(&ModalScopeId::new(id("scope"))),
            Some(ScopeOwner::Ready(session)) if *session == UiSessionId::new(id("ui"))
        ));
    }
    #[test]
    fn disconnect_does_not_win_after_placement_commit() {
        let now = Instant::now();
        let mut tokens = FixedTokens(vec![PendingLaunchToken([9; 16])]);
        let mut gate = LaunchGate::default();
        let prepared = gate
            .prepare(
                &mut tokens,
                ModalScopeId::new(id("scope")),
                MenuId::new(id("root")),
                now,
                std::time::Duration::from_secs(1),
            )
            .unwrap();
        let pane = RegisteredPane {
            pane: HostPaneId::new(id("pane")),
            temporary_tab: None,
            lease: None,
        };
        gate.register_pending_pane(prepared.pending.token, pane.clone())
            .unwrap();
        assert_eq!(
            gate.commit(prepared.pending.token, &pane.pane).unwrap(),
            None
        );
        assert_eq!(
            gate.abort_if_uncommitted(prepared.pending.token).unwrap(),
            None
        );
        assert!(matches!(
            gate.owner(&ModalScopeId::new(id("scope"))),
            Some(ScopeOwner::Pending(token)) if *token == prepared.pending.token
        ));
    }

    #[test]
    fn expiry_releases_only_the_expired_scope() {
        let now = Instant::now();
        let mut tokens = FixedTokens(vec![
            PendingLaunchToken([2; 16]),
            PendingLaunchToken([1; 16]),
        ]);
        let mut gate = LaunchGate::default();
        let first = gate
            .prepare(
                &mut tokens,
                ModalScopeId::new(id("one")),
                MenuId::new(id("root")),
                now,
                std::time::Duration::from_millis(1),
            )
            .unwrap();
        gate.prepare(
            &mut tokens,
            ModalScopeId::new(id("two")),
            MenuId::new(id("root")),
            now,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        let expired = gate.expire(now + std::time::Duration::from_millis(2));
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].token, first.pending.token);
        assert!(gate.owner(&ModalScopeId::new(id("one"))).is_none());
        assert!(gate.owner(&ModalScopeId::new(id("two"))).is_some());
    }
}
