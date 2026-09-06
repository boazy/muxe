// Minimized from the exact pinned source: zellij-utils/src/input/mouse.rs

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
/// A mouse event can have any number of buttons (including no
/// buttons) pressed or released.
pub struct MouseEvent {
    /// A mouse event can current be a Press, Release, or Motion.
    /// Future events could consider double-click and triple-click.
    pub event_type: MouseEventType,
    pub left: bool,
    pub right: bool,
    pub middle: bool,
    pub wheel_up: bool,
    pub wheel_down: bool,
    #[serde(default)]
    pub wheel_left: bool,
    #[serde(default)]
    pub wheel_right: bool,
    pub shift: bool,
    pub alt: bool,
    pub ctrl: bool,
    /// The coordinates are zero-based.
    pub position: Position,
}
/// A mouse related event
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum MouseEventType {
    /// A mouse button was pressed.
    Press,
    /// A mouse button was released.
    Release,
    /// A mouse button is held over the given coordinates.
    Motion,
}
