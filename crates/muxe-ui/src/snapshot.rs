use muxe_protocol::{ArchivedBrokerResponse, ArchivedFrame, ArchivedWireMessage, DecodeError};
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

    /// Returns the checked archive. Callers must traverse it through `ArchivedFrame::archived`.
    pub fn frame(&self) -> &ArchivedFrame {
        &self.frame
    }
}
