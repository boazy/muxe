#[cfg(test)]
use std::cell::Cell;

use muxe_protocol::{
    ArchivedBrokerResponse, ArchivedFrame, ArchivedUiAttachmentWire, ArchivedWireMessage,
    DecodeError,
};
use thiserror::Error;

/// A checked broker attachment frame retained without deserializing its menu graph.
pub struct ArchivedUiSnapshot {
    frame: ArchivedFrame,
    #[cfg(test)]
    validation_count: Cell<usize>,
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
    /// Checks a new frame and lends its attachment to one initialization operation.
    ///
    /// Keeping the validation and initialization borrow in one operation avoids validating the
    /// immutable archive once for each field copied into the runtime.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Decode`] when the frame bytes fail to decode, or
    /// [`SnapshotError::NotUiAttached`] when the frame is not a UI attachment response.
    pub fn with_new_attachment<T>(
        frame: ArchivedFrame,
        visit: impl FnOnce(&str, &ArchivedUiAttachmentWire) -> T,
    ) -> Result<(Self, T), SnapshotError> {
        let value = match frame.archived()? {
            ArchivedWireMessage::Response {
                response: ArchivedBrokerResponse::UiAttached { session, snapshot },
                ..
            } => visit(session.0.as_str(), snapshot),
            _ => return Err(SnapshotError::NotUiAttached),
        };
        Ok((
            Self {
                frame,
                #[cfg(test)]
                validation_count: Cell::new(1),
            },
            value,
        ))
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
        #[cfg(test)]
        self.validation_count
            .set(self.validation_count.get().saturating_add(1));
        let archived = self.frame.archived()?;
        match archived {
            ArchivedWireMessage::Response {
                response: ArchivedBrokerResponse::UiAttached { snapshot, .. },
                ..
            } => Ok(visit(snapshot)),
            _ => Err(SnapshotError::NotUiAttached),
        }
    }

    #[cfg(test)]
    pub(crate) fn validation_count(&self) -> usize {
        self.validation_count.get()
    }
}
