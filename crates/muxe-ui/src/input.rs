use muxe_core::{EventKind, KeyEvent, KeyIdentity, LockModifiers, Modifiers, NamedKey};
use muxe_terminal_input::{
    EventKind as RawEventKind, FunctionalKey, InputEvent, KeyIdentity as RawKeyIdentity, KeypadKey,
    MediaKey, ModifierKey, Modifiers as RawModifiers, ProtocolResponse, RawKeyEvent,
};

/// A parsed input result after its representable identities have been converted for core matching.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConvertedInput {
    Key(ConvertedKeyEvent),
    ProtocolResponse(ProtocolResponse),
    Unknown,
}

/// A raw event and its core matching projection.
///
/// `raw` retains every supplied identity. `event` contains the representable primary, alternate,
/// and base-layout identities in their original fields. Unknown and unidentified identities remain
/// absent from `event`, so they never match a configured binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConvertedKeyEvent {
    pub raw: RawKeyEvent,
    pub event: KeyEvent,
}

impl ConvertedKeyEvent {
    /// Returns whether at least one reported identity can participate in core matching.
    #[must_use]
    pub fn is_matchable(&self) -> bool {
        self.event.primary.is_some()
            || self.event.alternate.is_some()
            || self.event.base_layout.is_some()
    }
}

/// Converts one parser result without inventing aliases or discarding raw data.
pub fn convert_input(input: InputEvent) -> ConvertedInput {
    match input {
        InputEvent::Key(raw) => ConvertedInput::Key(ConvertedKeyEvent {
            event: KeyEvent {
                primary: convert_identity(raw.primary),
                alternate: raw.shifted.and_then(convert_identity),
                base_layout: raw.base.and_then(convert_identity),
                modifiers: convert_modifiers(raw.modifiers),
                kind: convert_event_kind(raw.kind),
                locks: LockModifiers {
                    caps_lock: raw.locks.caps_lock,
                    num_lock: raw.locks.num_lock,
                },
                keypad: raw.keypad.is_some(),
            },
            raw,
        }),
        InputEvent::ProtocolResponse(response) => ConvertedInput::ProtocolResponse(response),
        InputEvent::Unknown(_) | InputEvent::Malformed(_) => ConvertedInput::Unknown,
    }
}

fn convert_identity(identity: RawKeyIdentity) -> Option<KeyIdentity> {
    match identity {
        RawKeyIdentity::Unicode(character) => Some(KeyIdentity::Text(character)),
        RawKeyIdentity::Functional(key) => convert_functional(key).map(KeyIdentity::Named),
        RawKeyIdentity::UnknownFunctional(_) | RawKeyIdentity::Unidentified => None,
    }
}

fn convert_functional(key: FunctionalKey) -> Option<NamedKey> {
    NamedKey::from_kitty_functional_code(functional_code(key)?)
}

fn functional_code(key: FunctionalKey) -> Option<u32> {
    Some(match key {
        FunctionalKey::Escape => 57344,
        FunctionalKey::Enter => 57345,
        FunctionalKey::Tab => 57346,
        FunctionalKey::Backspace => 57347,
        FunctionalKey::Insert => 57348,
        FunctionalKey::Delete => 57349,
        FunctionalKey::Left => 57350,
        FunctionalKey::Right => 57351,
        FunctionalKey::Up => 57352,
        FunctionalKey::Down => 57353,
        FunctionalKey::PageUp => 57354,
        FunctionalKey::PageDown => 57355,
        FunctionalKey::Home => 57356,
        FunctionalKey::End => 57357,
        FunctionalKey::Begin => 57427,
        FunctionalKey::CapsLock => 57358,
        FunctionalKey::ScrollLock => 57359,
        FunctionalKey::NumLock => 57360,
        FunctionalKey::PrintScreen => 57361,
        FunctionalKey::Pause => 57362,
        FunctionalKey::Menu => 57363,
        FunctionalKey::Function(number @ 1..=35) => 57363 + u32::from(number),
        FunctionalKey::Function(_) => return None,
        FunctionalKey::Keypad(key) => keypad_code(key)?,
        FunctionalKey::Media(key) => media_code(key),
        FunctionalKey::Modifier(key) => modifier_key_code(key),
    })
}

fn keypad_code(key: KeypadKey) -> Option<u32> {
    Some(match key {
        KeypadKey::Digit(number @ 0..=9) => 57399 + u32::from(number),
        KeypadKey::Digit(_) => return None,
        KeypadKey::Decimal => 57409,
        KeypadKey::Divide => 57410,
        KeypadKey::Multiply => 57411,
        KeypadKey::Subtract => 57412,
        KeypadKey::Add => 57413,
        KeypadKey::Enter => 57414,
        KeypadKey::Equal => 57415,
        KeypadKey::Separator => 57416,
        KeypadKey::Left => 57417,
        KeypadKey::Right => 57418,
        KeypadKey::Up => 57419,
        KeypadKey::Down => 57420,
        KeypadKey::PageUp => 57421,
        KeypadKey::PageDown => 57422,
        KeypadKey::Home => 57423,
        KeypadKey::End => 57424,
        KeypadKey::Insert => 57425,
        KeypadKey::Delete => 57426,
        KeypadKey::Begin => 57427,
    })
}

fn media_code(key: MediaKey) -> u32 {
    match key {
        MediaKey::Play => 57428,
        MediaKey::Pause => 57429,
        MediaKey::PlayPause => 57430,
        MediaKey::Reverse => 57431,
        MediaKey::Stop => 57432,
        MediaKey::FastForward => 57433,
        MediaKey::Rewind => 57434,
        MediaKey::TrackNext => 57435,
        MediaKey::TrackPrevious => 57436,
        MediaKey::Record => 57437,
        MediaKey::LowerVolume => 57438,
        MediaKey::RaiseVolume => 57439,
        MediaKey::MuteVolume => 57440,
    }
}

fn modifier_key_code(key: ModifierKey) -> u32 {
    match key {
        ModifierKey::LeftShift => 57441,
        ModifierKey::LeftControl => 57442,
        ModifierKey::LeftAlt => 57443,
        ModifierKey::LeftSuper => 57444,
        ModifierKey::LeftHyper => 57445,
        ModifierKey::LeftMeta => 57446,
        ModifierKey::RightShift => 57447,
        ModifierKey::RightControl => 57448,
        ModifierKey::RightAlt => 57449,
        ModifierKey::RightSuper => 57450,
        ModifierKey::RightHyper => 57451,
        ModifierKey::RightMeta => 57452,
        ModifierKey::IsoLevel3Shift => 57453,
        ModifierKey::IsoLevel5Shift => 57454,
    }
}

fn convert_modifiers(raw: RawModifiers) -> Modifiers {
    let mut converted = Modifiers::empty();
    for (raw_flag, core_flag) in [
        (RawModifiers::SHIFT, Modifiers::SHIFT),
        (RawModifiers::ALT, Modifiers::ALT),
        (RawModifiers::CONTROL, Modifiers::CTRL),
        (RawModifiers::SUPER, Modifiers::SUPER),
        (RawModifiers::HYPER, Modifiers::HYPER),
        (RawModifiers::META, Modifiers::META),
    ] {
        if raw.contains(raw_flag) {
            converted.insert(core_flag);
        }
    }
    converted
}

fn convert_event_kind(kind: RawEventKind) -> EventKind {
    match kind {
        RawEventKind::Press => EventKind::Press,
        RawEventKind::Repeat => EventKind::Repeat,
        RawEventKind::Release => EventKind::Release,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_terminal_input::{
        EventKind as RawEventKind, KeyIdentity as RawKeyIdentity, LockState, Parser,
    };

    #[test]
    fn conversion_preserves_identity_sources_and_key_metadata() {
        let converted = convert_input(InputEvent::Key(RawKeyEvent {
            primary: RawKeyIdentity::Unicode('a'),
            shifted: Some(RawKeyIdentity::Unicode('A')),
            base: Some(RawKeyIdentity::Unicode('q')),
            modifiers: RawModifiers::SHIFT | RawModifiers::CONTROL,
            kind: RawEventKind::Repeat,
            locks: LockState {
                caps_lock: true,
                num_lock: true,
            },
            keypad: None,
        }));
        let ConvertedInput::Key(converted) = converted else {
            panic!("key input must remain a key event");
        };
        assert_eq!(converted.event.primary, Some(KeyIdentity::Text('a')));
        assert_eq!(converted.event.alternate, Some(KeyIdentity::Text('A')));
        assert_eq!(converted.event.base_layout, Some(KeyIdentity::Text('q')));
        assert!(converted.event.modifiers.contains(Modifiers::SHIFT));
        assert!(converted.event.modifiers.contains(Modifiers::CTRL));
        assert_eq!(converted.event.kind, EventKind::Repeat);
        assert_eq!(
            converted.event.locks,
            LockModifiers {
                caps_lock: true,
                num_lock: true,
            }
        );
    }

    #[test]
    fn unknown_identity_remains_raw_and_cannot_match() {
        let raw = RawKeyEvent {
            primary: RawKeyIdentity::UnknownFunctional(57455),
            shifted: Some(RawKeyIdentity::UnknownFunctional(57456)),
            base: Some(RawKeyIdentity::Unidentified),
            modifiers: RawModifiers::ALT,
            kind: RawEventKind::Press,
            locks: LockState::NONE,
            keypad: None,
        };
        let ConvertedInput::Key(converted) = convert_input(InputEvent::Key(raw)) else {
            panic!("raw key must remain observable");
        };
        assert_eq!(converted.raw, raw);
        assert!(!converted.is_matchable());
        assert_eq!(converted.event.primary, None);
        assert_eq!(converted.event.alternate, None);
        assert_eq!(converted.event.base_layout, None);
    }

    #[test]
    fn keypad_identity_and_state_reach_core_together() {
        let ConvertedInput::Key(converted) = convert_input(InputEvent::Key(RawKeyEvent {
            primary: RawKeyIdentity::Functional(FunctionalKey::Keypad(KeypadKey::Digit(7))),
            shifted: None,
            base: None,
            modifiers: RawModifiers::NONE,
            kind: RawEventKind::Release,
            locks: LockState::NONE,
            keypad: Some(KeypadKey::Digit(7)),
        })) else {
            panic!("keypad input must remain observable");
        };
        assert_eq!(
            converted.event.primary,
            Some(KeyIdentity::Named(NamedKey::Keypad(7)))
        );
        assert!(converted.event.keypad);
        assert_eq!(converted.event.kind, EventKind::Release);
    }

    #[test]
    fn every_canonical_kitty_functional_identity_reaches_the_core_registry() {
        for code in [9_u32, 13, 27, 127].into_iter().chain(57344..=57454) {
            let mut parser = Parser::new();
            let mut parsed = None;
            let stream = format!("\x1b[{code};198:3u");
            parser.push(stream.as_bytes(), |event| parsed = Some(event));
            parser.finish(|event| parsed = Some(event));
            let Some(InputEvent::Key(raw)) = parsed else {
                panic!("canonical Kitty key did not parse: {code}");
            };
            let ConvertedInput::Key(converted) = convert_input(InputEvent::Key(raw)) else {
                panic!("canonical Kitty key did not convert: {code}");
            };
            assert_eq!(
                converted.event.primary,
                NamedKey::from_kitty_functional_code(code).map(KeyIdentity::Named),
                "core registry mismatch for {code}"
            );
            assert_eq!(converted.event.kind, EventKind::Release);
            assert!(converted.event.modifiers.contains(Modifiers::SHIFT));
            assert!(converted.event.modifiers.contains(Modifiers::CTRL));
            assert!(converted.event.locks.caps_lock);
            assert!(converted.event.locks.num_lock);
        }
    }
}
