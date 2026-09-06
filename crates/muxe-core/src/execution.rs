use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AfterAction {
    Quit,
    Return,
    Stay,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExecutionMode {
    Await,
    Detach,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TimeoutAction {
    Detach,
    Cancel,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MenuControlAction {
    Detach,
    Cancel,
}

/// Inherited execution policy, resolved before a binding becomes compiled.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionPolicy {
    pub mode: ExecutionMode,
    pub timeout: Option<Duration>,
    pub on_timeout: TimeoutAction,
    pub on_menu_control: MenuControlAction,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::Await,
            timeout: None,
            on_timeout: TimeoutAction::Detach,
            on_menu_control: MenuControlAction::Detach,
        }
    }
}

/// Execution properties declared by the action registry or returned by adapter validation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExecutionCapabilities {
    pub awaitable: bool,
    pub detachable: bool,
    pub cancellable: bool,
}

impl ExecutionCapabilities {
    pub const SYNCHRONOUS: Self = Self {
        awaitable: false,
        detachable: false,
        cancellable: false,
    };

    pub const ASYNCHRONOUS: Self = Self {
        awaitable: true,
        detachable: true,
        cancellable: false,
    };
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MenuControl {
    Quit,
    Return,
}
