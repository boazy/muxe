use crate::diagnostic::{ConfigDiagnostic, DiagnosticCode, SourceSpan};
use std::fmt;
use std::fmt::Write as _;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EventKind {
    Press,
    Repeat,
    Release,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Modifiers(u16);

impl Modifiers {
    pub const SHIFT: u16 = 1 << 0;
    pub const ALT: u16 = 1 << 1;
    pub const CTRL: u16 = 1 << 2;
    pub const SUPER: u16 = 1 << 3;
    pub const HYPER: u16 = 1 << 4;
    pub const META: u16 = 1 << 5;
    pub const CAPS_LOCK: u16 = 1 << 6;
    pub const NUM_LOCK: u16 = 1 << 7;

    #[must_use]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn bits(self) -> u16 {
        self.0
    }

    #[must_use]
    pub const fn contains(self, flag: u16) -> bool {
        self.0 & flag != 0
    }

    pub fn insert(&mut self, flag: u16) {
        self.0 |= flag;
    }

    #[must_use]
    pub const fn has_lock_modifier(self) -> bool {
        self.contains(Self::CAPS_LOCK) || self.contains(Self::NUM_LOCK)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct LockModifiers {
    pub caps_lock: bool,
    pub num_lock: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum KeyIdentitySource {
    Primary,
    Alternate,
    BaseLayout,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum KeyIdentity {
    Text(char),
    Named(NamedKey),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum NamedKey {
    Escape,
    Enter,
    Tab,
    Backspace,
    Insert,
    Delete,
    Left,
    Right,
    Up,
    Down,
    Home,
    End,
    PageUp,
    PageDown,
    PrintScreen,
    Pause,
    Menu,
    CapsLock,
    ScrollLock,
    NumLock,
    Function(u8),
    Keypad(u8),
    KeypadDecimal,
    KeypadDivide,
    KeypadMultiply,
    KeypadSubtract,
    KeypadAdd,
    KeypadEnter,
    KeypadEqual,
    KeypadSeparator,
    KeypadLeft,
    KeypadRight,
    KeypadUp,
    KeypadDown,
    KeypadPageUp,
    KeypadPageDown,
    KeypadHome,
    KeypadEnd,
    KeypadInsert,
    KeypadDelete,
    KeypadBegin,
    MediaPlay,
    MediaPause,
    MediaPlayPause,
    MediaReverse,
    MediaStop,
    MediaFastForward,
    MediaRewind,
    MediaTrackNext,
    MediaTrackPrevious,
    MediaRecord,
    LowerVolume,
    RaiseVolume,
    MuteVolume,
    LeftShift,
    LeftControl,
    LeftAlt,
    LeftSuper,
    LeftHyper,
    LeftMeta,
    RightShift,
    RightControl,
    RightAlt,
    RightSuper,
    RightHyper,
    RightMeta,
    IsoLevel3Shift,
    IsoLevel5Shift,
}

impl NamedKey {
    pub const FINITE: &'static [Self] = &[
        Self::Escape,
        Self::Enter,
        Self::Tab,
        Self::Backspace,
        Self::Insert,
        Self::Delete,
        Self::Left,
        Self::Right,
        Self::Up,
        Self::Down,
        Self::Home,
        Self::End,
        Self::PageUp,
        Self::PageDown,
        Self::PrintScreen,
        Self::Pause,
        Self::Menu,
        Self::CapsLock,
        Self::ScrollLock,
        Self::NumLock,
        Self::Function(1),
        Self::Function(2),
        Self::Function(3),
        Self::Function(4),
        Self::Function(5),
        Self::Function(6),
        Self::Function(7),
        Self::Function(8),
        Self::Function(9),
        Self::Function(10),
        Self::Function(11),
        Self::Function(12),
        Self::Function(13),
        Self::Function(14),
        Self::Function(15),
        Self::Function(16),
        Self::Function(17),
        Self::Function(18),
        Self::Function(19),
        Self::Function(20),
        Self::Function(21),
        Self::Function(22),
        Self::Function(23),
        Self::Function(24),
        Self::Function(25),
        Self::Function(26),
        Self::Function(27),
        Self::Function(28),
        Self::Function(29),
        Self::Function(30),
        Self::Function(31),
        Self::Function(32),
        Self::Function(33),
        Self::Function(34),
        Self::Function(35),
        Self::Keypad(0),
        Self::Keypad(1),
        Self::Keypad(2),
        Self::Keypad(3),
        Self::Keypad(4),
        Self::Keypad(5),
        Self::Keypad(6),
        Self::Keypad(7),
        Self::Keypad(8),
        Self::Keypad(9),
        Self::KeypadDecimal,
        Self::KeypadDivide,
        Self::KeypadMultiply,
        Self::KeypadSubtract,
        Self::KeypadAdd,
        Self::KeypadEnter,
        Self::KeypadEqual,
        Self::KeypadSeparator,
        Self::KeypadLeft,
        Self::KeypadRight,
        Self::KeypadUp,
        Self::KeypadDown,
        Self::KeypadPageUp,
        Self::KeypadPageDown,
        Self::KeypadHome,
        Self::KeypadEnd,
        Self::KeypadInsert,
        Self::KeypadDelete,
        Self::KeypadBegin,
        Self::MediaPlay,
        Self::MediaPause,
        Self::MediaPlayPause,
        Self::MediaReverse,
        Self::MediaStop,
        Self::MediaFastForward,
        Self::MediaRewind,
        Self::MediaTrackNext,
        Self::MediaTrackPrevious,
        Self::MediaRecord,
        Self::LowerVolume,
        Self::RaiseVolume,
        Self::MuteVolume,
        Self::LeftShift,
        Self::LeftControl,
        Self::LeftAlt,
        Self::LeftSuper,
        Self::LeftHyper,
        Self::LeftMeta,
        Self::RightShift,
        Self::RightControl,
        Self::RightAlt,
        Self::RightSuper,
        Self::RightHyper,
        Self::RightMeta,
        Self::IsoLevel3Shift,
        Self::IsoLevel5Shift,
    ];
    #[must_use]
    pub const fn is_text_producing_keypad(self) -> bool {
        matches!(
            self,
            Self::Keypad(_)
                | Self::KeypadDecimal
                | Self::KeypadDivide
                | Self::KeypadMultiply
                | Self::KeypadSubtract
                | Self::KeypadAdd
        )
    }

    #[must_use]
    pub const fn is_modifier_key(self) -> bool {
        matches!(
            self,
            Self::LeftShift
                | Self::LeftControl
                | Self::LeftAlt
                | Self::LeftSuper
                | Self::LeftHyper
                | Self::LeftMeta
                | Self::RightShift
                | Self::RightControl
                | Self::RightAlt
                | Self::RightSuper
                | Self::RightHyper
                | Self::RightMeta
                | Self::IsoLevel3Shift
                | Self::IsoLevel5Shift
        )
    }

    #[must_use]
    pub const fn requires_all_keys_for_event_type(self) -> bool {
        matches!(self, Self::Enter | Self::Tab | Self::Backspace)
    }

    /// Converts every assigned Kitty `CSI ... u` functional key code accepted by v1. The published
    /// map is contiguous through `57454`; only values outside it return `None` and stay
    /// non-matching.
    #[must_use]
    pub fn from_kitty_functional_code(code: u32) -> Option<Self> {
        Some(match code {
            9 | 57346 => Self::Tab,
            13 | 57345 => Self::Enter,
            27 | 57344 => Self::Escape,
            127 | 57347 => Self::Backspace,
            57348 => Self::Insert,
            57349 => Self::Delete,
            57350 => Self::Left,
            57351 => Self::Right,
            57352 => Self::Up,
            57353 => Self::Down,
            57354 => Self::PageUp,
            57355 => Self::PageDown,
            57356 => Self::Home,
            57357 => Self::End,
            57358 => Self::CapsLock,
            57359 => Self::ScrollLock,
            57360 => Self::NumLock,
            57361 => Self::PrintScreen,
            57362 => Self::Pause,
            57363 => Self::Menu,
            57364..=57398 => Self::Function(u8::try_from(code - 57363).ok()?),
            57399..=57408 => Self::Keypad(u8::try_from(code - 57399).ok()?),
            57409 => Self::KeypadDecimal,
            57410 => Self::KeypadDivide,
            57411 => Self::KeypadMultiply,
            57412 => Self::KeypadSubtract,
            57413 => Self::KeypadAdd,
            57414 => Self::KeypadEnter,
            57415 => Self::KeypadEqual,
            57416 => Self::KeypadSeparator,
            57417 => Self::KeypadLeft,
            57418 => Self::KeypadRight,
            57419 => Self::KeypadUp,
            57420 => Self::KeypadDown,
            57421 => Self::KeypadPageUp,
            57422 => Self::KeypadPageDown,
            57423 => Self::KeypadHome,
            57424 => Self::KeypadEnd,
            57425 => Self::KeypadInsert,
            57426 => Self::KeypadDelete,
            57427 => Self::KeypadBegin,
            57428 => Self::MediaPlay,
            57429 => Self::MediaPause,
            57430 => Self::MediaPlayPause,
            57431 => Self::MediaReverse,
            57432 => Self::MediaStop,
            57433 => Self::MediaFastForward,
            57434 => Self::MediaRewind,
            57435 => Self::MediaTrackNext,
            57436 => Self::MediaTrackPrevious,
            57437 => Self::MediaRecord,
            57438 => Self::LowerVolume,
            57439 => Self::RaiseVolume,
            57440 => Self::MuteVolume,
            57441 => Self::LeftShift,
            57442 => Self::LeftControl,
            57443 => Self::LeftAlt,
            57444 => Self::LeftSuper,
            57445 => Self::LeftHyper,
            57446 => Self::LeftMeta,
            57447 => Self::RightShift,
            57448 => Self::RightControl,
            57449 => Self::RightAlt,
            57450 => Self::RightSuper,
            57451 => Self::RightHyper,
            57452 => Self::RightMeta,
            57453 => Self::IsoLevel3Shift,
            57454 => Self::IsoLevel5Shift,
            _ => return None,
        })
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "esc" => Self::Escape,
            "enter" => Self::Enter,
            "tab" => Self::Tab,
            "backspace" => Self::Backspace,
            "insert" => Self::Insert,
            "delete" => Self::Delete,
            "left" => Self::Left,
            "right" => Self::Right,
            "up" => Self::Up,
            "down" => Self::Down,
            "home" => Self::Home,
            "end" => Self::End,
            "pgup" => Self::PageUp,
            "pgdn" => Self::PageDown,
            "print-screen" => Self::PrintScreen,
            "pause" => Self::Pause,
            "menu" => Self::Menu,
            "caps-lock" => Self::CapsLock,
            "scroll-lock" => Self::ScrollLock,
            "num-lock" => Self::NumLock,
            "keypad+decimal" => Self::KeypadDecimal,
            "keypad+divide" => Self::KeypadDivide,
            "keypad+multiply" => Self::KeypadMultiply,
            "keypad+subtract" => Self::KeypadSubtract,
            "keypad+add" => Self::KeypadAdd,
            "keypad+enter" => Self::KeypadEnter,
            "keypad+equal" => Self::KeypadEqual,
            "keypad+separator" => Self::KeypadSeparator,
            "keypad+left" => Self::KeypadLeft,
            "keypad+right" => Self::KeypadRight,
            "keypad+up" => Self::KeypadUp,
            "keypad+down" => Self::KeypadDown,
            "keypad+pgup" => Self::KeypadPageUp,
            "keypad+pgdn" => Self::KeypadPageDown,
            "keypad+home" => Self::KeypadHome,
            "keypad+end" => Self::KeypadEnd,
            "keypad+insert" => Self::KeypadInsert,
            "keypad+delete" => Self::KeypadDelete,
            "keypad+begin" => Self::KeypadBegin,
            "media-play" => Self::MediaPlay,
            "media-pause" => Self::MediaPause,
            "media-play-pause" => Self::MediaPlayPause,
            "media-reverse" => Self::MediaReverse,
            "media-stop" => Self::MediaStop,
            "media-fast-forward" => Self::MediaFastForward,
            "media-rewind" => Self::MediaRewind,
            "media-track-next" => Self::MediaTrackNext,
            "media-track-previous" => Self::MediaTrackPrevious,
            "media-record" => Self::MediaRecord,
            "lower-volume" => Self::LowerVolume,
            "raise-volume" => Self::RaiseVolume,
            "mute-volume" => Self::MuteVolume,
            "left-shift" => Self::LeftShift,
            "left-ctrl" => Self::LeftControl,
            "left-alt" => Self::LeftAlt,
            "left-super" => Self::LeftSuper,
            "left-hyper" => Self::LeftHyper,
            "left-meta" => Self::LeftMeta,
            "right-shift" => Self::RightShift,
            "right-ctrl" => Self::RightControl,
            "right-alt" => Self::RightAlt,
            "right-super" => Self::RightSuper,
            "right-hyper" => Self::RightHyper,
            "right-meta" => Self::RightMeta,
            "iso-level3-shift" => Self::IsoLevel3Shift,
            "iso-level5-shift" => Self::IsoLevel5Shift,
            _ => return Self::parse_numbered(value),
        })
    }

    fn parse_numbered(value: &str) -> Option<Self> {
        if let Some(function) = value.strip_prefix('f') {
            let function = function.parse::<u8>().ok()?;
            return (1..=35)
                .contains(&function)
                .then_some(Self::Function(function));
        }
        if let Some(keypad) = value.strip_prefix("keypad+") {
            let keypad = keypad.parse::<u8>().ok()?;
            return (0..=9).contains(&keypad).then_some(Self::Keypad(keypad));
        }
        None
    }

    #[must_use]
    pub fn as_str(self) -> String {
        match self {
            Self::Escape => "esc".to_owned(),
            Self::Enter => "enter".to_owned(),
            Self::Tab => "tab".to_owned(),
            Self::Backspace => "backspace".to_owned(),
            Self::Insert => "insert".to_owned(),
            Self::Delete => "delete".to_owned(),
            Self::Left => "left".to_owned(),
            Self::Right => "right".to_owned(),
            Self::Up => "up".to_owned(),
            Self::Down => "down".to_owned(),
            Self::Home => "home".to_owned(),
            Self::End => "end".to_owned(),
            Self::PageUp => "pgup".to_owned(),
            Self::PageDown => "pgdn".to_owned(),
            Self::PrintScreen => "print-screen".to_owned(),
            Self::Pause => "pause".to_owned(),
            Self::Menu => "menu".to_owned(),
            Self::CapsLock => "caps-lock".to_owned(),
            Self::ScrollLock => "scroll-lock".to_owned(),
            Self::NumLock => "num-lock".to_owned(),
            Self::Function(number) => format!("f{number}"),
            Self::Keypad(number) => format!("keypad+{number}"),
            Self::KeypadDecimal => "keypad+decimal".to_owned(),
            Self::KeypadDivide => "keypad+divide".to_owned(),
            Self::KeypadMultiply => "keypad+multiply".to_owned(),
            Self::KeypadSubtract => "keypad+subtract".to_owned(),
            Self::KeypadAdd => "keypad+add".to_owned(),
            Self::KeypadEnter => "keypad+enter".to_owned(),
            Self::KeypadEqual => "keypad+equal".to_owned(),
            Self::KeypadSeparator => "keypad+separator".to_owned(),
            Self::KeypadLeft => "keypad+left".to_owned(),
            Self::KeypadRight => "keypad+right".to_owned(),
            Self::KeypadUp => "keypad+up".to_owned(),
            Self::KeypadDown => "keypad+down".to_owned(),
            Self::KeypadPageUp => "keypad+pgup".to_owned(),
            Self::KeypadPageDown => "keypad+pgdn".to_owned(),
            Self::KeypadHome => "keypad+home".to_owned(),
            Self::KeypadEnd => "keypad+end".to_owned(),
            Self::KeypadInsert => "keypad+insert".to_owned(),
            Self::KeypadDelete => "keypad+delete".to_owned(),
            Self::KeypadBegin => "keypad+begin".to_owned(),
            Self::MediaPlay => "media-play".to_owned(),
            Self::MediaPause => "media-pause".to_owned(),
            Self::MediaPlayPause => "media-play-pause".to_owned(),
            Self::MediaReverse => "media-reverse".to_owned(),
            Self::MediaStop => "media-stop".to_owned(),
            Self::MediaFastForward => "media-fast-forward".to_owned(),
            Self::MediaRewind => "media-rewind".to_owned(),
            Self::MediaTrackNext => "media-track-next".to_owned(),
            Self::MediaTrackPrevious => "media-track-previous".to_owned(),
            Self::MediaRecord => "media-record".to_owned(),
            Self::LowerVolume => "lower-volume".to_owned(),
            Self::RaiseVolume => "raise-volume".to_owned(),
            Self::MuteVolume => "mute-volume".to_owned(),
            Self::LeftShift => "left-shift".to_owned(),
            Self::LeftControl => "left-ctrl".to_owned(),
            Self::LeftAlt => "left-alt".to_owned(),
            Self::LeftSuper => "left-super".to_owned(),
            Self::LeftHyper => "left-hyper".to_owned(),
            Self::LeftMeta => "left-meta".to_owned(),
            Self::RightShift => "right-shift".to_owned(),
            Self::RightControl => "right-ctrl".to_owned(),
            Self::RightAlt => "right-alt".to_owned(),
            Self::RightSuper => "right-super".to_owned(),
            Self::RightHyper => "right-hyper".to_owned(),
            Self::RightMeta => "right-meta".to_owned(),
            Self::IsoLevel3Shift => "iso-level3-shift".to_owned(),
            Self::IsoLevel5Shift => "iso-level5-shift".to_owned(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct KeyCapabilities {
    pub event_types: bool,
    pub alternate_keys: bool,
    pub all_keys_as_escape_codes: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyEvent {
    pub primary: Option<KeyIdentity>,
    pub alternate: Option<KeyIdentity>,
    pub base_layout: Option<KeyIdentity>,
    pub modifiers: Modifiers,
    pub kind: EventKind,
    pub locks: LockModifiers,

    pub keypad: bool,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CanonicalKey {
    pub source: KeyIdentitySource,
    pub modifiers: Modifiers,
    pub identity: KeyIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyParseError {
    Empty,
    UnknownModifier(String),
    DuplicateModifier(String),
    NonCanonicalModifierOrder,
    MissingIdentity,
    InvalidUnicode(String),
    UnknownNamedKey(String),
    InvalidSelector(String),
}

impl fmt::Display for KeyParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("key is empty"),
            Self::UnknownModifier(value) => write!(formatter, "unknown modifier `{value}`"),
            Self::DuplicateModifier(value) => write!(formatter, "duplicate modifier `{value}`"),
            Self::NonCanonicalModifierOrder => {
                formatter.write_str("modifiers are not in canonical order")
            }
            Self::MissingIdentity => formatter.write_str("key has no identity"),
            Self::InvalidUnicode(value) => write!(formatter, "invalid Unicode scalar `{value}`"),
            Self::UnknownNamedKey(value) => write!(formatter, "unknown named key `{value}`"),
            Self::InvalidSelector(value) => {
                write!(formatter, "unknown identity selector `{value}`")
            }
        }
    }
}

impl std::error::Error for KeyParseError {}

impl CanonicalKey {
    /// Parses canonical key syntax.
    ///
    /// # Errors
    ///
    /// Returns [`KeyParseError`] when the selector, modifiers, or identity are invalid.
    pub fn parse(value: &str) -> Result<Self, KeyParseError> {
        let (source, key) = if let Some((selector, remainder)) = value.split_once(':') {
            let source = match selector {
                "alternate" => KeyIdentitySource::Alternate,
                "base" => KeyIdentitySource::BaseLayout,
                "primary" => KeyIdentitySource::Primary,
                _ => return Err(KeyParseError::InvalidSelector(selector.to_owned())),
            };
            (source, remainder)
        } else {
            (KeyIdentitySource::Primary, value)
        };
        if key.is_empty() {
            return Err(KeyParseError::Empty);
        }
        let (modifiers_text, identity) = split_modifiers_and_identity(key)?;
        let mut modifiers = Modifiers::empty();
        let mut previous_order = None;
        if !modifiers_text.is_empty() {
            for modifier in modifiers_text.split('+') {
                let (order, flag) = modifier_flag(modifier)
                    .ok_or_else(|| KeyParseError::UnknownModifier(modifier.to_owned()))?;
                if previous_order.is_some_and(|previous| previous > order) {
                    return Err(KeyParseError::NonCanonicalModifierOrder);
                }
                if modifiers.contains(flag) {
                    return Err(KeyParseError::DuplicateModifier(modifier.to_owned()));
                }
                modifiers.insert(flag);
                previous_order = Some(order);
            }
        }
        Ok(Self {
            source,
            modifiers,
            identity: parse_identity(identity)?,
        })
    }

    /// Parses canonical key syntax and labels any failure at the supplied source span.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigDiagnostic`] when canonical key parsing fails.
    pub fn parse_diagnostic(value: &str, span: SourceSpan) -> Result<Self, ConfigDiagnostic> {
        Self::parse(value).map_err(|error| {
            ConfigDiagnostic::error(DiagnosticCode::InvalidKey, error.to_string(), span)
        })
    }

    #[must_use]
    pub fn required_capabilities(&self, explicit_repeat: Option<bool>) -> KeyCapabilities {
        let mut capabilities = KeyCapabilities::default();
        if self.source != KeyIdentitySource::Primary {
            capabilities.alternate_keys = true;
        }
        let all_keys_identity = match self.identity {
            KeyIdentity::Text(_) => self.modifiers.has_lock_modifier(),
            KeyIdentity::Named(named) => {
                named.is_modifier_key() || named.is_text_producing_keypad()
            }
        };
        capabilities.all_keys_as_escape_codes = all_keys_identity;
        if explicit_repeat.is_some() {
            capabilities.event_types = true;
            let event_type_all_keys = match self.identity {
                KeyIdentity::Text(_) => text_identity_generates_text(self.modifiers),
                KeyIdentity::Named(named) => named.requires_all_keys_for_event_type(),
            };
            capabilities.all_keys_as_escape_codes |= event_type_all_keys;
        }
        capabilities
    }

    #[must_use]
    pub fn matches(&self, event: &KeyEvent) -> bool {
        let identity = match self.source {
            KeyIdentitySource::Primary => event.primary.as_ref(),
            KeyIdentitySource::Alternate => event.alternate.as_ref(),
            KeyIdentitySource::BaseLayout => event.base_layout.as_ref(),
        };
        identity == Some(&self.identity) && event.modifiers == self.modifiers
    }

    #[must_use]
    pub fn canonical_string(&self) -> String {
        let mut output = String::new();
        match self.source {
            KeyIdentitySource::Primary => {}
            KeyIdentitySource::Alternate => output.push_str("alternate:"),
            KeyIdentitySource::BaseLayout => output.push_str("base:"),
        }
        for (name, flag) in CANONICAL_MODIFIERS {
            if self.modifiers.contains(flag) {
                output.push_str(name);
                output.push('+');
            }
        }
        match &self.identity {
            KeyIdentity::Text(character)
                if character.is_ascii_graphic() && *character != '+' && *character != ':' =>
            {
                output.push(*character);
            }
            KeyIdentity::Text(character) => {
                write!(&mut output, "unicode+{:x}", u32::from(*character))
                    .expect("writing to String cannot fail");
            }
            KeyIdentity::Named(named) => output.push_str(&named.as_str()),
        }
        output
    }
}

/// Profile-dependent VT100 representation of one configured binding key.
///
/// The legacy terminal stream cannot distinguish several canonical keys. The compiler uses this
/// projection to reject ambiguous bindings before they reach the UI.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum Vt100BindingKey {
    ControlByte(u8),
    Exact(CanonicalKey),
}

impl CanonicalKey {
    /// Returns the legacy input projection used for VT100 collision checks.
    pub(crate) fn vt100_binding_key(&self) -> Vt100BindingKey {
        legacy_control_code(self).map_or_else(
            || Vt100BindingKey::Exact(self.clone()),
            Vt100BindingKey::ControlByte,
        )
    }

    /// Matches a legacy VT100 event without inventing modifier or identity fields that the
    /// terminal did not supply.
    pub(crate) fn matches_vt100(&self, event: &KeyEvent) -> bool {
        match legacy_control_code(self) {
            Some(expected) => legacy_event_control_code(event) == Some(expected),
            None => self.matches(event),
        }
    }
}

fn legacy_control_code(key: &CanonicalKey) -> Option<u8> {
    if key.source != KeyIdentitySource::Primary {
        return None;
    }
    match &key.identity {
        KeyIdentity::Named(NamedKey::Escape) if key.modifiers == Modifiers::empty() => Some(0x1b),
        KeyIdentity::Named(NamedKey::Enter) if key.modifiers == Modifiers::empty() => Some(0x0d),
        KeyIdentity::Named(NamedKey::Tab) if key.modifiers == Modifiers::empty() => Some(0x09),
        KeyIdentity::Named(NamedKey::Backspace) if key.modifiers == Modifiers::empty() => {
            Some(0x08)
        }
        KeyIdentity::Text(character)
            if key.modifiers.contains(Modifiers::CTRL)
                && key.modifiers.bits() & !(Modifiers::CTRL | Modifiers::SHIFT) == 0 =>
        {
            legacy_control_text_code(*character)
        }
        _ => None,
    }
}

fn legacy_control_text_code(character: char) -> Option<u8> {
    match character.to_ascii_uppercase() {
        'A'..='Z' => Some(character.to_ascii_uppercase() as u8 - b'@'),
        '[' => Some(0x1b),
        _ => None,
    }
}

fn legacy_event_control_code(event: &KeyEvent) -> Option<u8> {
    if event.modifiers != Modifiers::empty()
        || event.locks != LockModifiers::default()
        || event.keypad
    {
        return None;
    }
    match event.primary.as_ref()? {
        KeyIdentity::Named(NamedKey::Escape) => Some(0x1b),
        KeyIdentity::Named(NamedKey::Enter) => Some(0x0d),
        KeyIdentity::Named(NamedKey::Tab) => Some(0x09),
        KeyIdentity::Named(NamedKey::Backspace) => Some(0x08),
        KeyIdentity::Text(character) if character.is_control() => {
            u8::try_from(*character as u32).ok()
        }
        _ => None,
    }
}

impl fmt::Display for CanonicalKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.canonical_string())
    }
}

const CANONICAL_MODIFIERS: [(&str, u16); 8] = [
    ("ctrl", Modifiers::CTRL),
    ("alt", Modifiers::ALT),
    ("shift", Modifiers::SHIFT),
    ("super", Modifiers::SUPER),
    ("hyper", Modifiers::HYPER),
    ("meta", Modifiers::META),
    ("caps-lock", Modifiers::CAPS_LOCK),
    ("num-lock", Modifiers::NUM_LOCK),
];

fn modifier_flag(value: &str) -> Option<(u8, u16)> {
    CANONICAL_MODIFIERS
        .iter()
        .zip(0_u8..)
        .find_map(|((name, flag), index)| (*name == value).then_some((index, *flag)))
}

fn parse_identity(value: &str) -> Result<KeyIdentity, KeyParseError> {
    if let Some(codepoint) = value.strip_prefix("unicode+") {
        let scalar = u32::from_str_radix(codepoint, 16)
            .map_err(|_| KeyParseError::InvalidUnicode(codepoint.to_owned()))?;
        let character = char::from_u32(scalar)
            .ok_or_else(|| KeyParseError::InvalidUnicode(codepoint.to_owned()))?;
        return Ok(KeyIdentity::Text(character));
    }
    if value.chars().count() == 1 {
        let character = value.chars().next().expect("count checked");
        if !character.is_control() {
            return Ok(KeyIdentity::Text(character));
        }
    }
    NamedKey::parse(value)
        .map(KeyIdentity::Named)
        .ok_or_else(|| KeyParseError::UnknownNamedKey(value.to_owned()))
}

fn split_modifiers_and_identity(key: &str) -> Result<(&str, &str), KeyParseError> {
    let mut structured_identity: Option<usize> = None;
    for marker in ["unicode+", "keypad+"] {
        if let Some(start) = key.rfind(marker)
            && (start == 0 || key.as_bytes().get(start - 1) == Some(&b'+'))
        {
            structured_identity =
                Some(structured_identity.map_or(start, |current| current.max(start)));
        }
    }
    if let Some(start) = structured_identity {
        return Ok((&key[..start.saturating_sub(1)], &key[start..]));
    }
    match key.rsplit_once('+') {
        Some((modifiers, identity)) if !identity.is_empty() => Ok((modifiers, identity)),
        Some(_) => Err(KeyParseError::MissingIdentity),
        None => Ok(("", key)),
    }
}

fn text_identity_generates_text(modifiers: Modifiers) -> bool {
    !modifiers.contains(Modifiers::CTRL)
        && !modifiers.contains(Modifiers::ALT)
        && !modifiers.contains(Modifiers::SUPER)
        && !modifiers.contains(Modifiers::HYPER)
        && !modifiers.contains(Modifiers::META)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_key_round_trips_examples() {
        for key in [
            "super+g",
            "ctrl+shift+a",
            "f13",
            "keypad+1",
            "unicode+1f642",
            "esc",
            "backspace",
        ] {
            let key = CanonicalKey::parse(key).unwrap();
            assert_eq!(CanonicalKey::parse(&key.canonical_string()).unwrap(), key);
        }
    }

    #[test]
    fn non_canonical_modifiers_are_rejected() {
        assert_eq!(
            CanonicalKey::parse("shift+ctrl+a"),
            Err(KeyParseError::NonCanonicalModifierOrder)
        );
    }

    #[test]
    fn key_capabilities_are_derived_without_inventing_events() {
        let alternate = CanonicalKey::parse("alternate:a").unwrap();
        assert!(alternate.required_capabilities(None).alternate_keys);
        let repeat_text = CanonicalKey::parse("a").unwrap();
        assert!(repeat_text.required_capabilities(Some(true)).event_types);
        assert!(
            repeat_text
                .required_capabilities(Some(true))
                .all_keys_as_escape_codes
        );
    }

    #[test]
    fn herdr_ctrl_l_repeat_does_not_require_all_keys() {
        let key = CanonicalKey::parse("ctrl+l").unwrap();
        let required = key.required_capabilities(Some(true));
        assert!(required.event_types);
        assert!(!required.all_keys_as_escape_codes);
    }

    #[test]
    fn every_assigned_kitty_functional_code_has_a_core_identity() {
        for code in 57_344..=57_454 {
            assert!(
                NamedKey::from_kitty_functional_code(code).is_some(),
                "missing {code}"
            );
        }
        assert_eq!(NamedKey::from_kitty_functional_code(57_455), None);
    }
}
