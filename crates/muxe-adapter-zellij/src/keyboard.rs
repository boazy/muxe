//! Finite canonical-key mapping for `keyboard:send`.
//!
//! Canonical keys arrive already parsed by core into identity, source, and
//! modifiers. This module maps the finite subset with exact pinned
//! representations into `(BareKey, modifiers, bytes)`:
//!
//! - Text identities map to their UTF-8 bytes with `BareKey::Char`.
//! - `ctrl` + ASCII letter (with optional `shift`, per core's
//!   `legacy_control_code`) maps to the C0 control byte, exactly as core
//!   projects legacy bindings (`legacy_control_text_code`: uppercased letter
//!   minus `@`; `[` maps to `0x1b`).
//! - Bare `esc`/`enter`/`tab`/`backspace` map to `0x1b`/`0x0d`/`0x09`/`0x08`,
//!   again matching core's legacy projection.
//! - `alt` + text maps to ESC-prefixed bytes (the de-facto meta convention).
//! - Bare named specials map to standard xterm sequences (arrows `ESC [ A-D`,
//!   home/end `ESC [ H/F`, insert/delete `ESC [ 2~/3~`, pgup/pgdn `ESC [ 5~/6~`,
//!   `F1`-`F4` `ESC O P-S`, `F5`-`F12` `ESC [ 15~…24~`), which Zellij's own
//!   terminal parser recognizes (pinned `stdin_ansi_parser_tests.rs` exercises
//!   the legacy CSI arrow shape through the same parser family).
//! - Modified specials use CSI `1;<modifier>` codes (`2` shift, `3` alt,
//!   `5` ctrl, `4`/`6`/`7` shift+alt/shift+ctrl/alt+ctrl).
//!
//! Everything else fails precisely: `super`/`hyper`/`meta`/`caps-lock`/
//! `num-lock` have no `KeyModifier` in the pinned `KeyModifier` enum
//! (`Ctrl, Alt, Shift, Super` only, and `Super` has no terminal byte encoding);
//! keypad/media/modifier identities have no pinned `BareKey` text or sequence;
//! `F13`-`F35` have no legacy sequence.

use std::collections::BTreeSet;

use muxe_core::{CanonicalKey, KeyIdentity, KeyIdentitySource, Modifiers, NamedKey};
use muxe_zellij_protocol::generated::raw::{BareKey, KeyModifier};
use thiserror::Error;

/// Keyboard mapping failure.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum KeyboardError {
    /// The canonical key has no pinned representation.
    #[error("unsupported key '{key}': {reason}")]
    Unsupported {
        /// Canonical key string.
        key: String,
        /// Why it cannot map.
        reason: &'static str,
    },
}

/// A mapped key: pinned identity plus the exact bytes written to the pane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappedKey {
    /// Pinned key identity for `key_with_modifier`.
    pub bare: BareKey,
    /// Pinned modifiers.
    pub modifiers: BTreeSet<KeyModifier>,
    /// Exact bytes delivered alongside the identity.
    pub bytes: Vec<u8>,
}

/// Maps one parsed canonical key to its pinned representation.
///
/// Alternate/base-layout sources need alternate-keys support, which stock
/// Zellij cannot provide; they fail precisely rather than silently degrading.
pub fn map_canonical_key(key: &CanonicalKey) -> Result<MappedKey, KeyboardError> {
    let unsupported = |reason: &'static str| KeyboardError::Unsupported {
        key: key.to_string(),
        reason,
    };
    if key.source != KeyIdentitySource::Primary {
        return Err(unsupported(
            "alternate/base-layout identities need alternate-keys support, unavailable on Zellij",
        ));
    }
    let modifiers = key.modifiers;
    if [
        Modifiers::HYPER,
        Modifiers::META,
        Modifiers::CAPS_LOCK,
        Modifiers::NUM_LOCK,
    ]
    .into_iter()
    .any(|flag| modifiers.contains(flag))
    {
        return Err(unsupported(
            "unknown modifier; hyper, meta, caps-lock, and num-lock have no pinned KeyModifier",
        ));
    }
    let mut set = BTreeSet::new();
    for (flag, modifier) in [
        (Modifiers::CTRL, KeyModifier::Ctrl),
        (Modifiers::ALT, KeyModifier::Alt),
        (Modifiers::SHIFT, KeyModifier::Shift),
        (Modifiers::SUPER, KeyModifier::Super),
    ] {
        if modifiers.contains(flag) {
            set.insert(modifier);
        }
    }
    match &key.identity {
        KeyIdentity::Text(character) => map_text_key(set, modifiers, *character, unsupported),
        KeyIdentity::Named(identity) => map_named_key(set, modifiers, *identity, unsupported),
    }
}

fn map_text_key(
    mut modifiers: BTreeSet<KeyModifier>,
    flags: Modifiers,
    character: char,
    unsupported: impl Fn(&'static str) -> KeyboardError,
) -> Result<MappedKey, KeyboardError> {
    // Shift on an ASCII letter folds into the uppercase identity.
    let (bare_char, shift_folded) = match character {
        'a'..='z' if flags.contains(Modifiers::SHIFT) => (character.to_ascii_uppercase(), true),
        _ => (character, false),
    };
    if shift_folded {
        modifiers.remove(&KeyModifier::Shift);
    }
    let remaining = (
        flags.contains(Modifiers::SHIFT) && !shift_folded,
        flags.contains(Modifiers::ALT),
        flags.contains(Modifiers::CTRL),
        flags.contains(Modifiers::SUPER),
    );
    match remaining {
        (false, false, false, false) => Ok(MappedKey {
            bare: BareKey::Char(bare_char),
            modifiers,
            bytes: bare_char.to_string().into_bytes(),
        }),
        (false, false, true, false) => {
            let byte = control_byte(bare_char).ok_or_else(|| {
                unsupported("ctrl maps only ASCII letters and [ to C0 control bytes")
            })?;
            Ok(MappedKey {
                bare: BareKey::Char(bare_char),
                modifiers,
                bytes: vec![byte],
            })
        }
        (false, true, false, false) => {
            let mut bytes = Vec::with_capacity(1 + bare_char.len_utf8());
            bytes.push(0x1b);
            bytes.extend_from_slice(bare_char.encode_utf8(&mut [0; 4]).as_bytes());
            Ok(MappedKey {
                bare: BareKey::Char(bare_char),
                modifiers,
                bytes,
            })
        }
        (true, false, true, false) => {
            let byte = control_byte(bare_char).ok_or_else(|| {
                unsupported("ctrl+shift maps only ASCII letters to C0 control bytes")
            })?;
            Ok(MappedKey {
                bare: BareKey::Char(bare_char),
                modifiers,
                bytes: vec![byte],
            })
        }
        _ => Err(unsupported(
            "only bare, ctrl, alt, and ctrl+shift combinations have byte encodings on Zellij",
        )),
    }
}

/// C0 control byte for `ctrl` combinations, mirroring core's
/// `legacy_control_text_code`: uppercased ASCII letter minus `@`.
fn control_byte(character: char) -> Option<u8> {
    match character.to_ascii_uppercase() {
        'A'..='Z' => Some(character.to_ascii_uppercase() as u8 - b'@'),
        '[' => Some(0x1b),
        _ => None,
    }
}
/// Applies a CSI modifier code to a bare sequence: `ESC [ A` becomes
/// `ESC [ 1;5A`, `ESC [ 5~` becomes `ESC [ 5;5~`, and SS3 `ESC O P` becomes
/// CSI `ESC [ 1;5P` (modified function keys use the CSI form).
fn csi_modified_sequence(sequence: &[u8], code: u8) -> Option<Vec<u8>> {
    if sequence.first() != Some(&0x1b) {
        return None;
    }
    if sequence.get(1) == Some(&b'O') {
        let final_byte = *sequence.get(2)?;
        let mut modified = vec![0x1b, b'[', b'1', b';'];
        modified.extend_from_slice(code.to_string().as_bytes());
        modified.push(final_byte);
        return Some(modified);
    }
    if sequence.get(1) != Some(&b'[') {
        return None;
    }
    let rest = &sequence[2..];
    // Split trailing final byte(s) from numeric parameters.
    let split = rest
        .iter()
        .rposition(|byte| !(byte.is_ascii_digit() || *byte == b';'))?;
    let (params, final_byte) = rest.split_at(split);
    let mut modified = vec![0x1b, b'['];
    modified.extend_from_slice(params);
    if params.is_empty() {
        modified.extend_from_slice(b"1;");
    } else {
        modified.push(b';');
    }
    modified.extend_from_slice(code.to_string().as_bytes());
    modified.extend_from_slice(final_byte);
    Some(modified)
}
fn map_named_key(
    set: BTreeSet<KeyModifier>,
    modifiers: Modifiers,
    identity: NamedKey,
    unsupported: impl Fn(&'static str) -> KeyboardError,
) -> Result<MappedKey, KeyboardError> {
    let (bare, sequence) = named_sequence(identity).ok_or_else(|| {
        unsupported("keypad, media, modifier, and F13+ identities have no pinned representation")
    })?;
    if modifiers == Modifiers::empty() {
        return Ok(MappedKey {
            bare,
            modifiers: set,
            bytes: sequence,
        });
    }
    let code = csi_modifier_code(modifiers).ok_or_else(|| {
        unsupported("only ctrl, alt, and shift combine with special keys via CSI 1;<modifier>")
    })?;
    let bytes = csi_modified_sequence(&sequence, code)
        .ok_or_else(|| unsupported("this special key has no CSI-modifiable sequence"))?;
    Ok(MappedKey {
        bare,
        modifiers: set,
        bytes,
    })
}

/// Pinned identity plus standard bare sequence for named specials.
fn named_sequence(identity: NamedKey) -> Option<(BareKey, Vec<u8>)> {
    let sequence = match identity {
        NamedKey::Escape => (BareKey::Esc, vec![0x1b]),
        NamedKey::Enter => (BareKey::Enter, vec![0x0d]),
        NamedKey::Tab => (BareKey::Tab, vec![0x09]),
        NamedKey::Backspace => (BareKey::Backspace, vec![0x08]),
        NamedKey::Left => (BareKey::Left, b"\x1b[D".to_vec()),
        NamedKey::Right => (BareKey::Right, b"\x1b[C".to_vec()),
        NamedKey::Up => (BareKey::Up, b"\x1b[A".to_vec()),
        NamedKey::Down => (BareKey::Down, b"\x1b[B".to_vec()),
        NamedKey::Home => (BareKey::Home, b"\x1b[H".to_vec()),
        NamedKey::End => (BareKey::End, b"\x1b[F".to_vec()),
        NamedKey::Insert => (BareKey::Insert, b"\x1b[2~".to_vec()),
        NamedKey::Delete => (BareKey::Delete, b"\x1b[3~".to_vec()),
        NamedKey::PageUp => (BareKey::PageUp, b"\x1b[5~".to_vec()),
        NamedKey::PageDown => (BareKey::PageDown, b"\x1b[6~".to_vec()),
        NamedKey::Function(1) => (BareKey::F(1), b"\x1bOP".to_vec()),
        NamedKey::Function(2) => (BareKey::F(2), b"\x1bOQ".to_vec()),
        NamedKey::Function(3) => (BareKey::F(3), b"\x1bOR".to_vec()),
        NamedKey::Function(4) => (BareKey::F(4), b"\x1bOS".to_vec()),
        NamedKey::Function(5) => (BareKey::F(5), b"\x1b[15~".to_vec()),
        NamedKey::Function(6) => (BareKey::F(6), b"\x1b[17~".to_vec()),
        NamedKey::Function(7) => (BareKey::F(7), b"\x1b[18~".to_vec()),
        NamedKey::Function(8) => (BareKey::F(8), b"\x1b[19~".to_vec()),
        NamedKey::Function(9) => (BareKey::F(9), b"\x1b[20~".to_vec()),
        NamedKey::Function(10) => (BareKey::F(10), b"\x1b[21~".to_vec()),
        NamedKey::Function(11) => (BareKey::F(11), b"\x1b[23~".to_vec()),
        NamedKey::Function(12) => (BareKey::F(12), b"\x1b[24~".to_vec()),
        _ => return None,
    };
    Some(sequence)
}

/// CSI `1;<modifier>` code for modified specials.
fn csi_modifier_code(modifiers: Modifiers) -> Option<u8> {
    let (shift, alt, ctrl) = (
        modifiers.contains(Modifiers::SHIFT),
        modifiers.contains(Modifiers::ALT),
        modifiers.contains(Modifiers::CTRL),
    );
    if modifiers.contains(Modifiers::SUPER) {
        return None;
    }
    match (shift, alt, ctrl) {
        (true, false, false) => Some(2),
        (false, true, false) => Some(3),
        (true, true, false) => Some(4),
        (false, false, true) => Some(5),
        (true, false, true) => Some(6),
        (false, true, true) => Some(7),
        (true, true, true) => Some(8),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_input(input: &str) -> Result<MappedKey, KeyboardError> {
        map_canonical_key(&CanonicalKey::parse(input).expect("valid canonical input"))
    }

    #[test]
    fn text_ctrl_alt_and_shift_fold_map() {
        let key = map_input("a").expect("text maps");
        assert_eq!(key.bare, BareKey::Char('a'));
        assert!(key.modifiers.is_empty());
        assert_eq!(key.bytes, b"a");

        let key = map_input("ctrl+c").expect("ctrl maps to C0");
        assert_eq!(key.bytes, vec![0x03]);
        assert!(key.modifiers.contains(&KeyModifier::Ctrl));

        let key = map_input("ctrl+shift+c").expect("ctrl+shift keeps C0");
        assert_eq!(key.bytes, vec![0x03]);

        let key = map_input("alt+x").expect("alt prefixes ESC");
        assert_eq!(key.bytes, b"\x1bx".to_vec());

        let key = map_input("shift+a").expect("shift folds into uppercase");
        assert_eq!(key.bare, BareKey::Char('A'));
        assert_eq!(key.bytes, b"A");
    }

    #[test]
    fn legacy_control_bytes_match_core_projection() {
        // Core's legacy_control_code grounds these exact bytes.
        for (key, byte) in [
            ("esc", 0x1b),
            ("enter", 0x0d),
            ("tab", 0x09),
            ("backspace", 0x08),
        ] {
            let mapped = map_input(key).expect("legacy key maps");
            assert_eq!(mapped.bytes, vec![byte], "{key}");
        }
        let mapped = map_input("ctrl+[").expect("ctrl+[ maps");
        assert_eq!(mapped.bytes, vec![0x1b]);
    }

    #[test]
    fn bare_specials_carry_standard_sequences() {
        let key = map_input("up").expect("arrow maps");
        assert_eq!(key.bare, BareKey::Up);
        assert_eq!(key.bytes, b"\x1b[A");

        let key = map_input("f1").expect("F1 maps");
        assert_eq!(key.bare, BareKey::F(1));
        assert_eq!(key.bytes, b"\x1bOP");

        let key = map_input("pgdn").expect("pgdn maps");
        assert_eq!(key.bytes, b"\x1b[6~");
    }

    #[test]
    fn modified_specials_use_csi_modifier_codes() {
        let key = map_input("ctrl+up").expect("ctrl+arrow maps");
        assert_eq!(key.bytes, b"\x1b[1;5A");
        assert!(key.modifiers.contains(&KeyModifier::Ctrl));

        let key = map_input("shift+f1").expect("shift+F1 maps");
        assert_eq!(key.bytes, b"\x1b[1;2P".as_slice());

        let key = map_input("ctrl+f1").expect("ctrl+F1 maps");
        assert_eq!(key.bytes, b"\x1b[1;5P".as_slice());
    }

    #[test]
    fn unrepresentable_keys_fail_precisely() {
        for key in [
            "super+a",
            "hyper+a",
            "meta+a",
            "caps-lock+a",
            "keypad+1",
            "keypad+enter",
            "media-play",
            "left-ctrl",
            "f13",
            "alternate:a",
            "super+up",
        ] {
            assert!(map_input(key).is_err(), "{key} must fail precisely");
        }
    }

    #[test]
    fn unicode_text_maps_by_value() {
        let key = map_input("unicode+1f642").expect("emoji maps");
        assert_eq!(key.bare, BareKey::Char('🙂'));
        assert_eq!(key.bytes, "🙂".as_bytes());
    }
}
