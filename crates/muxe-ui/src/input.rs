use muxe_core::{EventKind, KeyEvent, KeyIdentity, LockModifiers, Modifiers, NamedKey};
use muxe_terminal_input::{
    EventKind as RawEventKind, FunctionalKey, InputEvent, KeyIdentity as RawKeyIdentity, KeypadKey,
    MalformedInput, MediaKey, ModifierKey, Modifiers as RawModifiers, ProtocolResponse,
    RawKeyEvent, UnknownSequence,
};

/// A parsed input result after its representable identities have been converted for core matching.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConvertedInput {
    Key(ConvertedKeyEvent),
    ProtocolResponse(ProtocolResponse),
    /// A syntactically complete terminal sequence that is not a supported key.
    Unknown(UnknownSequence),
    /// Malformed or undecodable input, which is ignored for UI activity.
    Malformed(MalformedInput),
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
        InputEvent::Unknown(sequence) => ConvertedInput::Unknown(sequence),
        InputEvent::Malformed(input) => ConvertedInput::Malformed(input),
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
    Some(match key {
        FunctionalKey::Escape => NamedKey::Escape,
        FunctionalKey::Enter => NamedKey::Enter,
        FunctionalKey::Tab => NamedKey::Tab,
        FunctionalKey::Backspace => NamedKey::Backspace,
        FunctionalKey::Insert => NamedKey::Insert,
        FunctionalKey::Delete => NamedKey::Delete,
        FunctionalKey::Left => NamedKey::Left,
        FunctionalKey::Right => NamedKey::Right,
        FunctionalKey::Up => NamedKey::Up,
        FunctionalKey::Down => NamedKey::Down,
        FunctionalKey::PageUp => NamedKey::PageUp,
        FunctionalKey::PageDown => NamedKey::PageDown,
        FunctionalKey::Home => NamedKey::Home,
        FunctionalKey::End => NamedKey::End,
        FunctionalKey::Begin => NamedKey::KeypadBegin,
        FunctionalKey::CapsLock => NamedKey::CapsLock,
        FunctionalKey::ScrollLock => NamedKey::ScrollLock,
        FunctionalKey::NumLock => NamedKey::NumLock,
        FunctionalKey::PrintScreen => NamedKey::PrintScreen,
        FunctionalKey::Pause => NamedKey::Pause,
        FunctionalKey::Menu => NamedKey::Menu,
        FunctionalKey::Function(number @ 1..=35) => NamedKey::Function(number),
        FunctionalKey::Function(_) => return None,
        FunctionalKey::Keypad(key) => match key {
            KeypadKey::Digit(number @ 0..=9) => NamedKey::Keypad(number),
            KeypadKey::Digit(_) => return None,
            KeypadKey::Decimal => NamedKey::KeypadDecimal,
            KeypadKey::Divide => NamedKey::KeypadDivide,
            KeypadKey::Multiply => NamedKey::KeypadMultiply,
            KeypadKey::Subtract => NamedKey::KeypadSubtract,
            KeypadKey::Add => NamedKey::KeypadAdd,
            KeypadKey::Enter => NamedKey::KeypadEnter,
            KeypadKey::Equal => NamedKey::KeypadEqual,
            KeypadKey::Separator => NamedKey::KeypadSeparator,
            KeypadKey::Left => NamedKey::KeypadLeft,
            KeypadKey::Right => NamedKey::KeypadRight,
            KeypadKey::Up => NamedKey::KeypadUp,
            KeypadKey::Down => NamedKey::KeypadDown,
            KeypadKey::PageUp => NamedKey::KeypadPageUp,
            KeypadKey::PageDown => NamedKey::KeypadPageDown,
            KeypadKey::Home => NamedKey::KeypadHome,
            KeypadKey::End => NamedKey::KeypadEnd,
            KeypadKey::Insert => NamedKey::KeypadInsert,
            KeypadKey::Delete => NamedKey::KeypadDelete,
            KeypadKey::Begin => NamedKey::KeypadBegin,
        },
        FunctionalKey::Media(key) => match key {
            MediaKey::Play => NamedKey::MediaPlay,
            MediaKey::Pause => NamedKey::MediaPause,
            MediaKey::PlayPause => NamedKey::MediaPlayPause,
            MediaKey::Reverse => NamedKey::MediaReverse,
            MediaKey::Stop => NamedKey::MediaStop,
            MediaKey::FastForward => NamedKey::MediaFastForward,
            MediaKey::Rewind => NamedKey::MediaRewind,
            MediaKey::TrackNext => NamedKey::MediaTrackNext,
            MediaKey::TrackPrevious => NamedKey::MediaTrackPrevious,
            MediaKey::Record => NamedKey::MediaRecord,
            MediaKey::LowerVolume => NamedKey::LowerVolume,
            MediaKey::RaiseVolume => NamedKey::RaiseVolume,
            MediaKey::MuteVolume => NamedKey::MuteVolume,
        },
        FunctionalKey::Modifier(key) => match key {
            ModifierKey::LeftShift => NamedKey::LeftShift,
            ModifierKey::LeftControl => NamedKey::LeftControl,
            ModifierKey::LeftAlt => NamedKey::LeftAlt,
            ModifierKey::LeftSuper => NamedKey::LeftSuper,
            ModifierKey::LeftHyper => NamedKey::LeftHyper,
            ModifierKey::LeftMeta => NamedKey::LeftMeta,
            ModifierKey::RightShift => NamedKey::RightShift,
            ModifierKey::RightControl => NamedKey::RightControl,
            ModifierKey::RightAlt => NamedKey::RightAlt,
            ModifierKey::RightSuper => NamedKey::RightSuper,
            ModifierKey::RightHyper => NamedKey::RightHyper,
            ModifierKey::RightMeta => NamedKey::RightMeta,
            ModifierKey::IsoLevel3Shift => NamedKey::IsoLevel3Shift,
            ModifierKey::IsoLevel5Shift => NamedKey::IsoLevel5Shift,
        },
    })
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
        EventKind as RawEventKind, KeyIdentity as RawKeyIdentity, LockState, MalformedReason,
        Parser, SequenceClass,
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
    fn conversion_distinguishes_unsupported_and_malformed_input() {
        let unknown = UnknownSequence {
            class: SequenceClass::Csi,
            final_byte: b'u',
            parameter_bytes: 1,
        };
        assert_eq!(
            convert_input(InputEvent::Unknown(unknown)),
            ConvertedInput::Unknown(unknown)
        );

        let malformed = MalformedInput {
            class: SequenceClass::Csi,
            reason: MalformedReason::InvalidScalar,
            observed_bytes: 5,
        };
        assert_eq!(
            convert_input(InputEvent::Malformed(malformed)),
            ConvertedInput::Malformed(malformed)
        );
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
    fn functional_key_families_match_only_their_configured_bindings() {
        use muxe_core::CanonicalKey;

        for (bytes, binding, other) in [
            (b"\x1b[57380;1u".as_slice(), "f17", "f16"),
            (b"\x1b[57404;1u".as_slice(), "keypad+5", "5"),
            (b"\x1b[57427;1u".as_slice(), "keypad+begin", "home"),
            (b"\x1b[E".as_slice(), "keypad+begin", "home"),
            (
                b"\x1b[57430;1u".as_slice(),
                "media-play-pause",
                "media-play",
            ),
            (b"\x1b[57448;1u".as_slice(), "right-ctrl", "left-ctrl"),
            (
                b"\x1b[57454;1u".as_slice(),
                "iso-level5-shift",
                "iso-level3-shift",
            ),
        ] {
            let converted = parsed_key_event(bytes);
            assert!(
                CanonicalKey::parse(binding)
                    .unwrap()
                    .matches(&converted.event)
            );
            assert!(
                !CanonicalKey::parse(other)
                    .unwrap()
                    .matches(&converted.event)
            );
        }
    }

    #[test]
    fn numbered_functional_key_bounds_preserve_raw_nonmatching_identities() {
        use muxe_core::CanonicalKey;

        for key in [
            FunctionalKey::Function(0),
            FunctionalKey::Function(36),
            FunctionalKey::Function(u8::MAX),
            FunctionalKey::Keypad(KeypadKey::Digit(10)),
            FunctionalKey::Keypad(KeypadKey::Digit(u8::MAX)),
        ] {
            let raw = RawKeyEvent {
                primary: RawKeyIdentity::Functional(key),
                shifted: Some(RawKeyIdentity::Functional(key)),
                base: Some(RawKeyIdentity::Functional(key)),
                modifiers: RawModifiers::NONE,
                kind: RawEventKind::Press,
                locks: LockState::NONE,
                keypad: None,
            };
            let ConvertedInput::Key(converted) = convert_input(InputEvent::Key(raw)) else {
                panic!("out-of-range identities must remain observable");
            };
            assert_eq!(converted.raw, raw);
            assert!(!converted.is_matchable(), "{key:?}");
        }
        for (bytes, binding) in [
            (b"\x1b[57364;1u".as_slice(), "f1"),
            (b"\x1b[57398;1u".as_slice(), "f35"),
            (b"\x1b[57399;1u".as_slice(), "keypad+0"),
            (b"\x1b[57408;1u".as_slice(), "keypad+9"),
        ] {
            let converted = parsed_key_event(bytes);
            assert!(
                CanonicalKey::parse(binding)
                    .unwrap()
                    .matches(&converted.event)
            );
        }
    }

    fn parsed_key_event(bytes: &[u8]) -> ConvertedKeyEvent {
        let mut parser = Parser::new();
        let mut parsed = None;
        parser.push(bytes, |event| {
            assert!(
                parsed.replace(event).is_none(),
                "one sequence emits one event"
            );
        });
        parser.finish(|event| {
            assert!(
                parsed.replace(event).is_none(),
                "one sequence emits one event"
            );
        });
        let ConvertedInput::Key(converted) = convert_input(parsed.expect("sequence emits")) else {
            panic!("CSI-u key sequence must convert to a key event");
        };
        converted
    }

    #[test]
    fn lock_bearing_csi_u_events_match_their_configured_lock_bindings() {
        use muxe_core::{CanonicalKey, KeyboardProfile};

        let profile = KeyboardProfile::Kitty(muxe_core::KeyCapabilities::default());
        for (bytes, lock_binding, other_lock_binding) in [
            (
                b"\x1b[57350;65u".as_slice(),
                "caps-lock+left",
                "num-lock+left",
            ),
            (
                b"\x1b[57350;129u".as_slice(),
                "num-lock+left",
                "caps-lock+left",
            ),
        ] {
            let converted = parsed_key_event(bytes);
            assert_eq!(
                converted.event.primary,
                Some(KeyIdentity::Named(NamedKey::Left))
            );
            let configured = CanonicalKey::parse(lock_binding).unwrap();
            let other = CanonicalKey::parse(other_lock_binding).unwrap();
            let plain = CanonicalKey::parse("left").unwrap();
            assert!(configured.matches(&converted.event));
            assert!(profile.matches_binding(&configured, &converted.event));
            assert!(!other.matches(&converted.event));
            assert!(!profile.matches_binding(&other, &converted.event));
            assert!(plain.matches(&converted.event));
            assert!(profile.matches_binding(&plain, &converted.event));
        }
    }

    #[test]
    fn lock_selectors_reject_lock_free_csi_u_events() {
        use muxe_core::{CanonicalKey, KeyboardProfile};

        let profile = KeyboardProfile::Kitty(muxe_core::KeyCapabilities::default());
        let converted = parsed_key_event(b"\x1b[57350;1u");
        assert!(!converted.event.locks.caps_lock);
        assert!(!converted.event.locks.num_lock);
        for binding in ["caps-lock+left", "num-lock+left"] {
            let configured = CanonicalKey::parse(binding).unwrap();
            assert!(!configured.matches(&converted.event));
            assert!(!profile.matches_binding(&configured, &converted.event));
        }
        let plain = CanonicalKey::parse("left").unwrap();
        assert!(plain.matches(&converted.event));
        assert!(profile.matches_binding(&plain, &converted.event));
    }
}
