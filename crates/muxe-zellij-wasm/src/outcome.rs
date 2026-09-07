//! Typed outcomes for synchronous shim dispatch returns.

use muxe_zellij_protocol::{CommandOutcome, generated::NativeCommandReturn};

/// Maps a synchronous dispatch return to its typed outcome.
///
/// Success means Zellij finished dispatching the action, not that an
/// arbitrary resulting operation succeeded. Fallible returns surface as
/// `Failed` with their bounded message; there is no general failure value.
#[expect(
    clippy::too_many_lines,
    reason = "exhaustive dispatch over the pinned 153-variant return inventory: every variant is classified infallible/fallible exactly once, and a wildcard would silently misclassify future fallible variants as success"
)]
pub fn outcome_of(value: &NativeCommandReturn) -> CommandOutcome {
    match value {
        NativeCommandReturn::BreakPanesToNewTab(_)
        | NativeCommandReturn::BreakPanesToTabWithId(_)
        | NativeCommandReturn::BreakPanesToTabWithIndex(_)
        | NativeCommandReturn::ChangeFloatingPanesCoordinates(())
        | NativeCommandReturn::ChangeHostFolder(())
        | NativeCommandReturn::ClearPaneHighlights(())
        | NativeCommandReturn::ClearScreen(())
        | NativeCommandReturn::ClearScreenForPaneId(())
        | NativeCommandReturn::CloseFocus(())
        | NativeCommandReturn::CloseFocusedTab(())
        | NativeCommandReturn::CloseMultiplePanes(())
        | NativeCommandReturn::ClosePaneWithId(())
        | NativeCommandReturn::ClosePluginPane(())
        | NativeCommandReturn::CloseSelf(())
        | NativeCommandReturn::CloseTabWithId(())
        | NativeCommandReturn::CloseTabWithIndex(())
        | NativeCommandReturn::CloseTerminalPane(())
        | NativeCommandReturn::CopyToClipboard(())
        | NativeCommandReturn::Detach(())
        | NativeCommandReturn::DisconnectOtherClients(())
        | NativeCommandReturn::EditScrollback(())
        | NativeCommandReturn::EditScrollbackForPaneWithId(())
        | NativeCommandReturn::EmbedMultiplePanes(())
        | NativeCommandReturn::FloatMultiplePanes(())
        | NativeCommandReturn::FocusHostSession(())
        | NativeCommandReturn::FocusLastPane(())
        | NativeCommandReturn::FocusNextPane(())
        | NativeCommandReturn::FocusOrCreateTab(_)
        | NativeCommandReturn::FocusPaneWithId(())
        | NativeCommandReturn::FocusPluginPane(())
        | NativeCommandReturn::FocusPreviousPane(())
        | NativeCommandReturn::FocusTerminalPane(())
        | NativeCommandReturn::GoToNextTab(())
        | NativeCommandReturn::GoToPreviousTab(())
        | NativeCommandReturn::GoToTab(())
        | NativeCommandReturn::GoToTabName(())
        | NativeCommandReturn::GroupAndUngroupPanes(())
        | NativeCommandReturn::HideFloatingPanes(_)
        | NativeCommandReturn::HidePaneWithId(())
        | NativeCommandReturn::HideSelf(())
        | NativeCommandReturn::HighlightAndUnhighlightPanes(())
        | NativeCommandReturn::MoveFocus(())
        | NativeCommandReturn::MoveFocusOrTab(())
        | NativeCommandReturn::MovePane(())
        | NativeCommandReturn::MovePaneWithDirection(())
        | NativeCommandReturn::MovePaneWithPaneId(())
        | NativeCommandReturn::MovePaneWithPaneIdInDirection(())
        | NativeCommandReturn::NewPane(())
        | NativeCommandReturn::NewTab(_)
        | NativeCommandReturn::NewTabUnfocused(_)
        | NativeCommandReturn::NewTabsWithLayout(_)
        | NativeCommandReturn::NewTabsWithLayoutInfo(_)
        | NativeCommandReturn::NewTiledPaneInTab(_)
        | NativeCommandReturn::NextSwapLayout(())
        | NativeCommandReturn::OpenCommandPane(_)
        | NativeCommandReturn::OpenCommandPaneBackground(_)
        | NativeCommandReturn::OpenCommandPaneFloating(_)
        | NativeCommandReturn::OpenCommandPaneFloatingNearPlugin(_)
        | NativeCommandReturn::OpenCommandPaneInNewTab(_)
        | NativeCommandReturn::OpenCommandPaneInPlace(_)
        | NativeCommandReturn::OpenCommandPaneInPlaceOfPaneId(_)
        | NativeCommandReturn::OpenCommandPaneInPlaceOfPlugin(_)
        | NativeCommandReturn::OpenCommandPaneNearPlugin(_)
        | NativeCommandReturn::OpenEditPaneInPlaceOfPaneId(_)
        | NativeCommandReturn::OpenEditorPaneInNewTab(_)
        | NativeCommandReturn::OpenFile(_)
        | NativeCommandReturn::OpenFileFloating(_)
        | NativeCommandReturn::OpenFileFloatingNearPlugin(_)
        | NativeCommandReturn::OpenFileInPlace(_)
        | NativeCommandReturn::OpenFileInPlaceOfPlugin(_)
        | NativeCommandReturn::OpenFileNearPlugin(_)
        | NativeCommandReturn::OpenPluginPaneFloating(_)
        | NativeCommandReturn::OpenPluginPaneInNewTab(_)
        | NativeCommandReturn::OpenTerminal(_)
        | NativeCommandReturn::OpenTerminalFloating(_)
        | NativeCommandReturn::OpenTerminalFloatingNearPlugin(_)
        | NativeCommandReturn::OpenTerminalInPlace(_)
        | NativeCommandReturn::OpenTerminalInPlaceOfPlugin(_)
        | NativeCommandReturn::OpenTerminalNearPlugin(_)
        | NativeCommandReturn::OpenTerminalPaneInPlaceOfPaneId(_)
        | NativeCommandReturn::OverrideLayout(())
        | NativeCommandReturn::PageScrollDown(())
        | NativeCommandReturn::PageScrollDownInPaneId(())
        | NativeCommandReturn::PageScrollUp(())
        | NativeCommandReturn::PageScrollUpInPaneId(())
        | NativeCommandReturn::PreviousSwapLayout(())
        | NativeCommandReturn::QuitZellij(())
        | NativeCommandReturn::RebindKeys(())
        | NativeCommandReturn::Reconfigure(())
        | NativeCommandReturn::RenamePaneWithId(())
        | NativeCommandReturn::RenamePluginPane(())
        | NativeCommandReturn::RenameSession(())
        | NativeCommandReturn::RenameTab(())
        | NativeCommandReturn::RenameTabWithId(())
        | NativeCommandReturn::RenameTerminalPane(())
        | NativeCommandReturn::ReplacePaneWithExistingPane(())
        | NativeCommandReturn::RerunCommandPane(())
        | NativeCommandReturn::ResizeFocusedPane(())
        | NativeCommandReturn::ResizeFocusedPaneWithDirection(())
        | NativeCommandReturn::ResizePaneWithId(())
        | NativeCommandReturn::RunAction(())
        | NativeCommandReturn::ScrollDown(())
        | NativeCommandReturn::ScrollDownInPaneId(())
        | NativeCommandReturn::ScrollToBottom(())
        | NativeCommandReturn::ScrollToBottomInPaneId(())
        | NativeCommandReturn::ScrollToTop(())
        | NativeCommandReturn::ScrollToTopInPaneId(())
        | NativeCommandReturn::ScrollUp(())
        | NativeCommandReturn::ScrollUpInPaneId(())
        | NativeCommandReturn::SendSigintToPaneId(())
        | NativeCommandReturn::SendSigkillToPaneId(())
        | NativeCommandReturn::SetFloatingPanePinned(())
        | NativeCommandReturn::SetPaneBorderless(())
        | NativeCommandReturn::SetPaneColor(())
        | NativeCommandReturn::SetPaneFrameStyle(())
        | NativeCommandReturn::SetPaneRegexHighlights(())
        | NativeCommandReturn::SetSelectable(())
        | NativeCommandReturn::SetSelfMouseSelectionSupport(())
        | NativeCommandReturn::SetSoftKeyboard(())
        | NativeCommandReturn::ShowCursor(())
        | NativeCommandReturn::ShowFloatingPanes(_)
        | NativeCommandReturn::ShowPaneWithId(())
        | NativeCommandReturn::ShowSelf(())
        | NativeCommandReturn::StackPanes(())
        | NativeCommandReturn::SwitchSession(())
        | NativeCommandReturn::SwitchSessionWithCwd(())
        | NativeCommandReturn::SwitchSessionWithFocus(())
        | NativeCommandReturn::SwitchSessionWithLayout(())
        | NativeCommandReturn::SwitchTabTo(())
        | NativeCommandReturn::ToggleActiveTabSync(())
        | NativeCommandReturn::ToggleFloatingPanes(())
        | NativeCommandReturn::ToggleFocusFullscreen(())
        | NativeCommandReturn::ToggleFocusNoUiFullscreen(())
        | NativeCommandReturn::TogglePaneBorderless(())
        | NativeCommandReturn::TogglePaneEmbedOrEject(())
        | NativeCommandReturn::TogglePaneEmbedOrEjectForPaneId(())
        | NativeCommandReturn::TogglePaneFrames(())
        | NativeCommandReturn::TogglePaneIdFullscreen(())
        | NativeCommandReturn::ToggleTab(())
        | NativeCommandReturn::UndoRenamePane(())
        | NativeCommandReturn::UndoRenameTab(())
        | NativeCommandReturn::Write(())
        | NativeCommandReturn::WriteChars(())
        | NativeCommandReturn::WriteCharsToPaneId(())
        | NativeCommandReturn::WriteToPaneId(()) => CommandOutcome::succeeded(),
        NativeCommandReturn::DeleteAllDeadSessions(result)
        | NativeCommandReturn::DeleteDeadSession(result)
        | NativeCommandReturn::DeleteLayout(result)
        | NativeCommandReturn::EditLayout(result)
        | NativeCommandReturn::KillSessions(result)
        | NativeCommandReturn::RenameLayout(result)
        | NativeCommandReturn::SaveLayout(result)
        | NativeCommandReturn::SaveSession(result) => match result {
            Ok(()) => CommandOutcome::succeeded(),
            Err(message) => CommandOutcome::failed(message),
        },
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
            "gone".to_owned()
        )));
        assert_eq!(outcome.status, CommandStatus::Failed);
        assert_eq!(outcome.detail, "gone");
    }
}
