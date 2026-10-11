//! Actual authenticated Retire response followed by a test-owned inode change.
use muxe::lifecycle::{
    ControlPort, ControlSession, LiveControl,
    control::{ControlAuthority, ControlClient, ControlError, ForcedPeer},
    registry::NativeBrokerAuthority,
};
use muxe_protocol::control::{ActivationStatus, CompatibilityRecord, HandoffId};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::net::UnixListener;

pub struct ReplacingControl {
    pub endpoint: PathBuf,
    pub listener: Arc<tokio::sync::Mutex<Option<UnixListener>>>,
}
impl ControlPort for ReplacingControl {
    type Session = ReplacingSession;
    async fn connect(&self, socket: &Path) -> Result<Self::Session, ControlError> {
        Ok(ReplacingSession {
            inner: ControlClient::connect(socket).await?,
            endpoint: self.endpoint.clone(),
            listener: Arc::clone(&self.listener),
        })
    }
    async fn verified_status(
        &self,
        socket: &Path,
    ) -> Result<muxe::lifecycle::control::VerifiedControlStatus, ControlError> {
        LiveControl.verified_status(socket).await
    }
}
pub struct ReplacingSession {
    inner: ControlClient,
    endpoint: PathBuf,
    listener: Arc<tokio::sync::Mutex<Option<UnixListener>>>,
}
impl ControlSession for ReplacingSession {
    fn authority(&self) -> Option<&ControlAuthority> {
        Some(self.inner.authority())
    }
    async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
        self.inner.status().await
    }
    async fn prepare(
        &mut self,
        target: &CompatibilityRecord,
        handoff: &HandoffId,
    ) -> Result<ActivationStatus, ControlError> {
        self.inner.prepare(target.clone(), *handoff).await
    }
    async fn commit(&mut self, handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
        self.inner.commit(*handoff).await
    }
    async fn abort(&mut self, handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
        self.inner.abort(*handoff).await
    }
    async fn forced_peer(
        &mut self,
        entry: &NativeBrokerAuthority<'_>,
    ) -> Result<ForcedPeer, ControlError> {
        self.inner.forced_peer(entry).await
    }
    async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
        use std::os::unix::fs::PermissionsExt as _;

        // This is the real broker's framed ACK after host/listener retirement,
        // never a synthetic Status. Only then bind a new test-owned listener.
        let status = self.inner.retire().await?;
        let replacement =
            UnixListener::bind(&self.endpoint).map_err(|source| ControlError::Connect {
                socket: self.endpoint.clone(),
                source,
            })?;
        std::fs::set_permissions(&self.endpoint, std::fs::Permissions::from_mode(0o600)).map_err(
            |source| ControlError::Connect {
                socket: self.endpoint.clone(),
                source,
            },
        )?;
        *self.listener.lock().await = Some(replacement);
        Ok(status)
    }
}
