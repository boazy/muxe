//! Finite canonical-key mapping for `keyboard:send`.
//!
//! Canonical keys arrive as strings (`ctrl+c`, `F1`, `pgdn`, `a`, …) per
//! `muxe-core/src/key.rs`: optional `alternate:`/`base:` source prefixes,
//! `+`-joined modifiers from `ctrl, alt, shift, super, hyper, meta, caps-lock,
//! num-lock`, and an identity (single graphic char, `unicode+hex`, named key,
//! `f1`-`f35`, or `keypad+…`). This module maps the finite subset with exact
//! pinned representations into `(BareKey, modifiers, bytes)`:
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Modifier {
    Ctrl,
    Alt,
    Shift,
    Super,
}

fn parse_modifiers(parts: &[&str]) -> Result<(Vec<Modifier>, BTreeSet<KeyModifier>), &'static str> {
    let mut ordered = Vec::new();
    let mut set = BTreeSet::new();
    for part in parts {
        let modifier = match *part {
            "ctrl" => Modifier::Ctrl,
            "alt" => Modifier::Alt,
            "shift" => Modifier::Shift,
            "super" => Modifier::Super,
            _ => {
                return Err(
                    "unknown modifier; hyper, meta, caps-lock, and num-lock have no pinned KeyModifier",
                );
            }
        };
        if !set.insert(match modifier {
            Modifier::Ctrl => KeyModifier::Ctrl,
            Modifier::Alt => KeyModifier::Alt,
            Modifier::Shift => KeyModifier::Shift,
            Modifier::Super => KeyModifier::Super,
        }) {
            return Err("duplicate modifier");
        }
        ordered.push(modifier);
    }
    Ok((ordered, set))
}

fn has(modifiers: &[Modifier], target: Modifier) -> bool {
    modifiers.contains(&target)
}

/// Maps one canonical key string to its pinned representation.
///
/// Alternate/base-layout sources need alternate-keys support, which stock
/// Zellij cannot provide; they fail precisely rather than silently degrading.
pub fn map_canonical_key(key: &str) -> Result<MappedKey, KeyboardError> {
    let unsupported = |reason: &'static str| KeyboardError::Unsupported {
        key: key.to_owned(),
        reason,
    };
    let (identity, modifiers) = split_key(key).map_err(unsupported)?;
    if identity.starts_with("alternate:") || identity.starts_with("base:") {
        return Err(unsupported(
            "alternate/base-layout identities need alternate-keys support, unavailable on Zellij",
        ));
    }
    let (ordered, set) = parse_modifiers(&modifiers).map_err(unsupported)?;
    // unicode+hex identities carry exact text.
    if let Some(codepoint) = identity.strip_prefix("unicode+") {
        let scalar = u32::from_str_radix(codepoint, 16)
            .ok()
            .and_then(char::from_u32)
            .ok_or(unsupported("invalid unicode+hex identity"))?;
        return map_text_key(&set, &ordered, scalar, key, unsupported);
    }
    if identity.chars().count() == 1 {
        let character = identity.chars().next().expect("count checked");
        if !character.is_control() {
            return map_text_key(&set, &ordered, character, key, unsupported);
        }
        return Err(unsupported(
            "control characters must use named or ctrl+ identities",
        ));
    }
    map_named_key(&set, &ordered, identity, unsupported)
}

fn split_key(key: &str) -> Result<(&str, Vec<&str>), &'static str> {
    // Split trailing identity from leading modifiers. unicode+/keypad+ markers
    // bind to the identity, never to a modifier boundary.
    for marker in ["unicode+", "keypad+"] {
        if let Some(start) = key.rfind(marker) {
            let (modifiers, identity) = key.split_at(start);
            let modifiers = modifiers.strip_suffix('+').unwrap_or(modifiers);
            let parts = if modifiers.is_empty() {
                Vec::new()
            } else {
                modifiers.split('+').collect()
            };
            if parts.iter().any(|part| part.is_empty()) {
                return Err("empty modifier segment");
            }
            return Ok((identity, parts));
        }
    }
    match key.rsplit_once('+') {
        Some((modifiers, identity)) if !identity.contains('+') && !modifiers.is_empty() => {
            Ok((identity, modifiers.split('+').collect()))
        }
        _ => Ok((key, Vec::new())),
    }
}

fn map_text_key(
    set: &BTreeSet<KeyModifier>,
    ordered: &[Modifier],
    character: char,
    _key: &str,
    unsupported: impl Fn(&'static str) -> KeyboardError,
) -> Result<MappedKey, KeyboardError> {
    // Shift on an ASCII letter folds into the uppercase identity.
    let (bare_char, shift_folded) = match character {
        'a'..='z' if has(ordered, Modifier::Shift) => (character.to_ascii_uppercase(), true),
        _ => (character, false),
    };
    let mut modifiers = set.clone();
    if shift_folded {
        modifiers.remove(&KeyModifier::Shift);
    }
    let remaining: Vec<Modifier> = ordered
        .iter()
        .copied()
        .filter(|modifier| *modifier != Modifier::Shift || !shift_folded)
        .collect();
    match remaining.as_slice() {
        [] => Ok(MappedKey {
            bare: BareKey::Char(bare_char),
            modifiers,
            bytes: bare_char.to_string().into_bytes(),
        }),
        [Modifier::Ctrl] => {
            let byte = control_byte(bare_char).ok_or_else(|| {
                unsupported("ctrl maps only ASCII letters and [ to C0 control bytes")
            })?;
            Ok(MappedKey {
                bare: BareKey::Char(bare_char),
                modifiers,
                bytes: vec![byte],
            })
        }
        [Modifier::Alt] => {
            let mut bytes = vec![0x1b];
            bytes.extend_from_slice(bare_char.to_string().as_bytes());
            Ok(MappedKey {
                bare: BareKey::Char(bare_char),
                modifiers,
                bytes,
            })
        }
        [Modifier::Ctrl, Modifier::Shift] | [Modifier::Shift, Modifier::Ctrl] => {
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
    set: &BTreeSet<KeyModifier>,
    ordered: &[Modifier],
    identity: &str,
    unsupported: impl Fn(&'static str) -> KeyboardError,
) -> Result<MappedKey, KeyboardError> {
    let (bare, sequence) = named_sequence(identity).ok_or_else(|| {
        unsupported("keypad, media, modifier, and F13+ identities have no pinned representation")
    })?;
    if ordered.is_empty() {
        return Ok(MappedKey {
            bare,
            modifiers: set.clone(),
            bytes: sequence,
        });
    }
    let code = csi_modifier_code(ordered).ok_or_else(|| {
        unsupported("only ctrl, alt, and shift combine with special keys via CSI 1;<modifier>")
    })?;
    let bytes = csi_modified_sequence(&sequence, code)
        .ok_or_else(|| unsupported("this special key has no CSI-modifiable sequence"))?;
    Ok(MappedKey {
        bare,
        modifiers: set.clone(),
        bytes,
    })
}

/// Pinned identity plus standard bare sequence for named specials.
fn named_sequence(identity: &str) -> Option<(BareKey, Vec<u8>)> {
    let sequence = match identity {
        "esc" => (BareKey::Esc, vec![0x1b]),
        "enter" => (BareKey::Enter, vec![0x0d]),
        "tab" => (BareKey::Tab, vec![0x09]),
        "backspace" => (BareKey::Backspace, vec![0x08]),
        "left" => (BareKey::Left, b"\x1b[D".to_vec()),
        "right" => (BareKey::Right, b"\x1b[C".to_vec()),
        "up" => (BareKey::Up, b"\x1b[A".to_vec()),
        "down" => (BareKey::Down, b"\x1b[B".to_vec()),
        "home" => (BareKey::Home, b"\x1b[H".to_vec()),
        "end" => (BareKey::End, b"\x1b[F".to_vec()),
        "insert" => (BareKey::Insert, b"\x1b[2~".to_vec()),
        "delete" => (BareKey::Delete, b"\x1b[3~".to_vec()),
        "pgup" => (BareKey::PageUp, b"\x1b[5~".to_vec()),
        "pgdn" => (BareKey::PageDown, b"\x1b[6~".to_vec()),
        "f1" => (BareKey::F(1), b"\x1bOP".to_vec()),
        "f2" => (BareKey::F(2), b"\x1bOQ".to_vec()),
        "f3" => (BareKey::F(3), b"\x1bOR".to_vec()),
        "f4" => (BareKey::F(4), b"\x1bOS".to_vec()),
        "f5" => (BareKey::F(5), b"\x1b[15~".to_vec()),
        "f6" => (BareKey::F(6), b"\x1b[17~".to_vec()),
        "f7" => (BareKey::F(7), b"\x1b[18~".to_vec()),
        "f8" => (BareKey::F(8), b"\x1b[19~".to_vec()),
        "f9" => (BareKey::F(9), b"\x1b[20~".to_vec()),
        "f10" => (BareKey::F(10), b"\x1b[21~".to_vec()),
        "f11" => (BareKey::F(11), b"\x1b[23~".to_vec()),
        "f12" => (BareKey::F(12), b"\x1b[24~".to_vec()),
        _ => return None,
    };
    Some(sequence)
}

/// CSI `1;<modifier>` code for modified specials.
fn csi_modifier_code(ordered: &[Modifier]) -> Option<u8> {
    let (shift, alt, ctrl) = (
        has(ordered, Modifier::Shift),
        has(ordered, Modifier::Alt),
        has(ordered, Modifier::Ctrl),
    );
    if has(ordered, Modifier::Super) {
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

    #[test]
    fn text_ctrl_alt_and_shift_fold_map() {
        let key = map_canonical_key("a").expect("text maps");
        assert_eq!(key.bare, BareKey::Char('a'));
        assert!(key.modifiers.is_empty());
        assert_eq!(key.bytes, b"a");

        let key = map_canonical_key("ctrl+c").expect("ctrl maps to C0");
        assert_eq!(key.bytes, vec![0x03]);
        assert!(key.modifiers.contains(&KeyModifier::Ctrl));

        let key = map_canonical_key("ctrl+shift+c").expect("ctrl+shift keeps C0");
        assert_eq!(key.bytes, vec![0x03]);

        let key = map_canonical_key("alt+x").expect("alt prefixes ESC");
        assert_eq!(key.bytes, b"\x1bx".to_vec());

        let key = map_canonical_key("shift+a").expect("shift folds into uppercase");
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
            let mapped = map_canonical_key(key).expect("legacy key maps");
            assert_eq!(mapped.bytes, vec![byte], "{key}");
        }
        let mapped = map_canonical_key("ctrl+[").expect("ctrl+[ maps");
        assert_eq!(mapped.bytes, vec![0x1b]);
    }

    #[test]
    fn bare_specials_carry_standard_sequences() {
        let key = map_canonical_key("up").expect("arrow maps");
        assert_eq!(key.bare, BareKey::Up);
        assert_eq!(key.bytes, b"\x1b[A");

        let key = map_canonical_key("f1").expect("F1 maps");
        assert_eq!(key.bare, BareKey::F(1));
        assert_eq!(key.bytes, b"\x1bOP");

        let key = map_canonical_key("pgdn").expect("pgdn maps");
        assert_eq!(key.bytes, b"\x1b[6~");
    }

    #[test]
    fn modified_specials_use_csi_modifier_codes() {
        let key = map_canonical_key("ctrl+up").expect("ctrl+arrow maps");
        assert_eq!(key.bytes, b"\x1b[1;5A");
        assert!(key.modifiers.contains(&KeyModifier::Ctrl));

        let key = map_canonical_key("shift+f1").expect("shift+F1 maps");
        assert_eq!(key.bytes, b"\x1b[1;2P".as_slice());

        let key = map_canonical_key("ctrl+f1").expect("ctrl+F1 maps");
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
            assert!(map_canonical_key(key).is_err(), "{key} must fail precisely");
        }
    }

    #[test]
    fn unicode_text_maps_by_value() {
        let key = map_canonical_key("unicode+1f642").expect("emoji maps");
        assert_eq!(key.bare, BareKey::Char('🙂'));
        assert_eq!(key.bytes, "🙂".as_bytes());
    }
}
