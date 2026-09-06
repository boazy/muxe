//! Typed outcomes for synchronous shim dispatch returns.

use muxe_zellij_protocol::{CommandOutcome, generated::NativeCommandReturn};

/// Maps a synchronous dispatch return to its typed outcome.
///
/// Success means Zellij finished dispatching the action, not that an
/// arbitrary resulting operation succeeded. Fallible returns surface as
/// `Failed` with their bounded message; there is no general failure value.
pub fn outcome_of(value: &NativeCommandReturn) -> CommandOutcome {
    match value {
        NativeCommandReturn::BreakPanesToNewTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::BreakPanesToTabWithId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::BreakPanesToTabWithIndex(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ChangeFloatingPanesCoordinates(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ChangeHostFolder(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ClearPaneHighlights(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ClearScreen(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ClearScreenForPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::CloseFocus(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::CloseFocusedTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::CloseMultiplePanes(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ClosePaneWithId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ClosePluginPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::CloseSelf(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::CloseTabWithId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::CloseTabWithIndex(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::CloseTerminalPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::CopyToClipboard(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::DeleteAllDeadSessions(result) => match result {
            Ok(()) => CommandOutcome::succeeded(),
            Err(message) => CommandOutcome::failed(message),
        },
        NativeCommandReturn::DeleteDeadSession(result) => match result {
            Ok(()) => CommandOutcome::succeeded(),
            Err(message) => CommandOutcome::failed(message),
        },
        NativeCommandReturn::DeleteLayout(result) => match result {
            Ok(()) => CommandOutcome::succeeded(),
            Err(message) => CommandOutcome::failed(message),
        },
        NativeCommandReturn::Detach(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::DisconnectOtherClients(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::EditLayout(result) => match result {
            Ok(()) => CommandOutcome::succeeded(),
            Err(message) => CommandOutcome::failed(message),
        },
        NativeCommandReturn::EditScrollback(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::EditScrollbackForPaneWithId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::EmbedMultiplePanes(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::FloatMultiplePanes(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::FocusHostSession(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::FocusLastPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::FocusNextPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::FocusOrCreateTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::FocusPaneWithId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::FocusPluginPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::FocusPreviousPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::FocusTerminalPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::GoToNextTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::GoToPreviousTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::GoToTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::GoToTabName(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::GroupAndUngroupPanes(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::HideFloatingPanes(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::HidePaneWithId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::HideSelf(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::HighlightAndUnhighlightPanes(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::KillSessions(result) => match result {
            Ok(()) => CommandOutcome::succeeded(),
            Err(message) => CommandOutcome::failed(message),
        },
        NativeCommandReturn::MoveFocus(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::MoveFocusOrTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::MovePane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::MovePaneWithDirection(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::MovePaneWithPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::MovePaneWithPaneIdInDirection(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::NewPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::NewTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::NewTabUnfocused(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::NewTabsWithLayout(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::NewTabsWithLayoutInfo(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::NewTiledPaneInTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::NextSwapLayout(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenCommandPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenCommandPaneBackground(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenCommandPaneFloating(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenCommandPaneFloatingNearPlugin(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenCommandPaneInNewTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenCommandPaneInPlace(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenCommandPaneInPlaceOfPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenCommandPaneInPlaceOfPlugin(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenCommandPaneNearPlugin(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenEditPaneInPlaceOfPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenEditorPaneInNewTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenFile(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenFileFloating(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenFileFloatingNearPlugin(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenFileInPlace(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenFileInPlaceOfPlugin(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenFileNearPlugin(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenPluginPaneFloating(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenPluginPaneInNewTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenTerminal(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenTerminalFloating(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenTerminalFloatingNearPlugin(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenTerminalInPlace(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenTerminalInPlaceOfPlugin(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenTerminalNearPlugin(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OpenTerminalPaneInPlaceOfPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::OverrideLayout(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::PageScrollDown(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::PageScrollDownInPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::PageScrollUp(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::PageScrollUpInPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::PreviousSwapLayout(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::QuitZellij(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::RebindKeys(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::Reconfigure(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::RenameLayout(result) => match result {
            Ok(()) => CommandOutcome::succeeded(),
            Err(message) => CommandOutcome::failed(message),
        },
        NativeCommandReturn::RenamePaneWithId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::RenamePluginPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::RenameSession(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::RenameTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::RenameTabWithId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::RenameTerminalPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ReplacePaneWithExistingPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::RerunCommandPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ResizeFocusedPane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ResizeFocusedPaneWithDirection(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ResizePaneWithId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::RunAction(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SaveLayout(result) => match result {
            Ok(()) => CommandOutcome::succeeded(),
            Err(message) => CommandOutcome::failed(message),
        },
        NativeCommandReturn::SaveSession(result) => match result {
            Ok(()) => CommandOutcome::succeeded(),
            Err(message) => CommandOutcome::failed(message),
        },
        NativeCommandReturn::ScrollDown(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ScrollDownInPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ScrollToBottom(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ScrollToBottomInPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ScrollToTop(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ScrollToTopInPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ScrollUp(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ScrollUpInPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SendSigintToPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SendSigkillToPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SetFloatingPanePinned(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SetPaneBorderless(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SetPaneColor(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SetPaneFrameStyle(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SetPaneRegexHighlights(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SetSelectable(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SetSelfMouseSelectionSupport(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SetSoftKeyboard(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ShowCursor(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ShowFloatingPanes(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ShowPaneWithId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ShowSelf(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::StackPanes(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SwitchSession(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SwitchSessionWithCwd(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SwitchSessionWithFocus(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SwitchSessionWithLayout(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::SwitchTabTo(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ToggleActiveTabSync(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ToggleFloatingPanes(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ToggleFocusFullscreen(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ToggleFocusNoUiFullscreen(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::TogglePaneBorderless(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::TogglePaneEmbedOrEject(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::TogglePaneEmbedOrEjectForPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::TogglePaneFrames(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::TogglePaneIdFullscreen(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::ToggleTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::UndoRenamePane(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::UndoRenameTab(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::Write(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::WriteChars(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::WriteCharsToPaneId(_) => CommandOutcome::succeeded(),
        NativeCommandReturn::WriteToPaneId(_) => CommandOutcome::succeeded(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_zellij_protocol::CommandStatus;

    #[test]
    fn unit_returns_succeed() {
        assert_eq!(
            outcome_of(&NativeCommandReturn::CloseFocus(())).status,
            CommandStatus::Succeeded
        );
        assert_eq!(
            outcome_of(&NativeCommandReturn::GoToTab(())).status,
            CommandStatus::Succeeded
        );
    }

    #[test]
    fn optional_returns_succeed() {
        assert_eq!(
            outcome_of(&NativeCommandReturn::BreakPanesToNewTab(Some(3))).status,
            CommandStatus::Succeeded
        );
        assert_eq!(
            outcome_of(&NativeCommandReturn::BreakPanesToNewTab(None)).status,
            CommandStatus::Succeeded
        );
    }

    #[test]
    fn fallible_returns_split() {
        assert_eq!(
            outcome_of(&NativeCommandReturn::DeleteDeadSession(Ok(()))).status,
            CommandStatus::Succeeded
        );
        let outcome = outcome_of(&NativeCommandReturn::DeleteDeadSession(Err(
            "gone".to_owned(),
        )));
        assert_eq!(outcome.status, CommandStatus::Failed);
        assert_eq!(outcome.detail, "gone");
    }
}
