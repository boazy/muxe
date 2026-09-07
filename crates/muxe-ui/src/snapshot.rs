use muxe_protocol::{
    ArchivedBrokerResponse, ArchivedFrame, ArchivedUiAttachmentWire, ArchivedWireMessage,
    DecodeError,
};
use thiserror::Error;

/// A checked broker attachment frame retained without deserializing its menu graph.
pub struct ArchivedUiSnapshot {
    frame: ArchivedFrame,
}

/// Failure to retain an attachment snapshot.
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error("broker frame is not a UI attachment response")]
    NotUiAttached,
}

impl ArchivedUiSnapshot {
    /// Retains a checked `UiAttached` archive for borrowed menu traversal.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Decode`] when the frame bytes fail to decode, or
    /// [`SnapshotError::NotUiAttached`] when the frame is not a UI attachment response.
    pub fn new(frame: ArchivedFrame) -> Result<Self, SnapshotError> {
        let archived = frame.archived()?;
        match archived {
            ArchivedWireMessage::Response {
                response: ArchivedBrokerResponse::UiAttached { .. },
                ..
            } => Ok(Self { frame }),
            _ => Err(SnapshotError::NotUiAttached),
        }
    }
    /// Calls `visit` with the checked attachment archive. The menu graph remains borrowed from
    /// the retained frame for its entire use.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Decode`] when the retained frame fails to decode, or
    /// [`SnapshotError::NotUiAttached`] when the retained frame is not a UI attachment response.
    pub fn with_attachment<T>(
        &self,
        visit: impl FnOnce(&ArchivedUiAttachmentWire) -> T,
    ) -> Result<T, SnapshotError> {
        let archived = self.frame.archived()?;
        match archived {
            ArchivedWireMessage::Response {
                response: ArchivedBrokerResponse::UiAttached { snapshot, .. },
                ..
            } => Ok(visit(snapshot)),
            _ => Err(SnapshotError::NotUiAttached),
        }
    }

    /// Returns the checked attachment archive for one borrowed traversal.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Decode`] when the retained frame fails to decode, or
    /// [`SnapshotError::NotUiAttached`] when the retained frame is not a UI attachment response.
    pub fn attachment(&self) -> Result<&ArchivedUiAttachmentWire, SnapshotError> {
        let archived = self.frame.archived()?;
        match archived {
            ArchivedWireMessage::Response {
                response: ArchivedBrokerResponse::UiAttached { snapshot, .. },
                ..
            } => Ok(snapshot),
            _ => Err(SnapshotError::NotUiAttached),
        }
    }

    /// Returns the attached UI session ID without deserializing the response.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Decode`] when the retained frame fails to decode, or
    /// [`SnapshotError::NotUiAttached`] when the retained frame is not a UI attachment response.
    pub fn session_id(&self) -> Result<&str, SnapshotError> {
        let archived = self.frame.archived()?;
        match archived {
            ArchivedWireMessage::Response {
                response: ArchivedBrokerResponse::UiAttached { session, .. },
                ..
            } => Ok(session.0.as_str()),
            _ => Err(SnapshotError::NotUiAttached),
        }
    }

    /// Returns the retained transport frame for callers that need its bytes for diagnostics.
    #[must_use]
    pub fn frame(&self) -> &ArchivedFrame {
        &self.frame
    }
}
