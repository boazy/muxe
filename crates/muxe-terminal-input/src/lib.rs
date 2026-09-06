#![forbid(unsafe_code)]

//! Pure, bounded decoding of legacy terminal and Kitty keyboard input.

use core::fmt;

/// Maximum bytes retained for a CSI sequence, excluding `ESC [` and its final byte.
pub const MAX_CSI_SEQUENCE_BYTES: usize = 96;
/// Maximum semicolon-delimited fields accepted in a CSI sequence.
pub const MAX_CSI_PARAMETERS: usize = 3;
/// Maximum colon-delimited associated-text scalars accepted by Kitty.
pub const MAX_ASSOCIATED_TEXT_SCALARS: usize = 16;

/// A lossless terminal input result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputEvent {
    /// A decoded key event.
    Key(RawKeyEvent),
    /// A syntactically complete control sequence which is not a supported key.
    Unknown(UnknownSequence),
    /// Malformed input. The parser has reached its documented recovery boundary.
    Malformed(MalformedInput),
}

/// A raw key event before configuration matching or host normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawKeyEvent {
    /// The key identity reported by the terminal.
    pub primary: KeyIdentity,
    /// The shifted identity, when Kitty alternate-key reporting supplied it.
    pub shifted: Option<KeyIdentity>,
    /// The base-layout identity, when Kitty alternate-key reporting supplied it.
    pub base: Option<KeyIdentity>,
    /// Non-lock modifiers reported for this event.
    pub modifiers: Modifiers,
    /// Press, repeat, or release, as reported by Kitty.
    pub kind: EventKind,
    /// Lock state reported separately from non-lock modifiers.
    pub locks: LockState,
    /// The keypad identity, when the event came from a keypad key.
    pub keypad: Option<KeypadKey>,
}

impl RawKeyEvent {
    const fn plain(primary: KeyIdentity) -> Self {
        Self {
            primary,
            shifted: None,
            base: None,
            modifiers: Modifiers::NONE,
            kind: EventKind::Press,
            locks: LockState::NONE,
            keypad: None,
        }
    }

    const fn with_modifiers(primary: KeyIdentity, modifiers: Modifiers) -> Self {
        Self {
            primary,
            shifted: None,
            base: None,
            modifiers,
            kind: EventKind::Press,
            locks: LockState::NONE,
            keypad: None,
        }
    }
}

/// A key identity reported by a terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyIdentity {
    /// A Unicode scalar value.
    Unicode(char),
    /// A documented non-Unicode functional key.
    Functional(FunctionalKey),
    /// A valid Kitty functional-key code not assigned by the canonical protocol table.
    ///
    /// It remains a raw identity but never claims to be a supported named key.
    UnknownFunctional(u32),
    /// Kitty's `0` key code: text exists, but no physical key is known.
    Unidentified,
}

/// A named functional key in the Kitty protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FunctionalKey {
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
    PageUp,
    PageDown,
    Home,
    End,
    Begin,
    CapsLock,
    ScrollLock,
    NumLock,
    PrintScreen,
    Pause,
    Menu,
    Function(u8),
    Keypad(KeypadKey),
    Media(MediaKey),
    Modifier(ModifierKey),
}

/// A media-key identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MediaKey {
    Play,
    Pause,
    PlayPause,
    Reverse,
    Stop,
    FastForward,
    Rewind,
    TrackNext,
    TrackPrevious,
    Record,
    LowerVolume,
    RaiseVolume,
    MuteVolume,
}

/// A physical modifier-key identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModifierKey {
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

/// A keypad identity, kept independently of the equivalent primary identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeypadKey {
    Digit(u8),
    Decimal,
    Divide,
    Multiply,
    Subtract,
    Add,
    Enter,
    Equal,
    Separator,
    Left,
    Right,
    Up,
    Down,
    PageUp,
    PageDown,
    Home,
    End,
    Insert,
    Delete,
    Begin,
}

/// Non-lock modifier bits reported by Kitty.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub struct Modifiers(u8);

impl Modifiers {
    pub const NONE: Self = Self(0);
    pub const SHIFT: Self = Self(0b000001);
    pub const ALT: Self = Self(0b000010);
    pub const CONTROL: Self = Self(0b000100);
    pub const SUPER: Self = Self(0b001000);
    pub const HYPER: Self = Self(0b010000);
    pub const META: Self = Self(0b100000);

    /// Returns the six protocol modifier bits, excluding lock state.
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Returns whether every bit in `other` is active.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl core::ops::BitOr for Modifiers {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl fmt::Debug for Modifiers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("Modifiers").field(&self.0).finish()
    }
}

/// Lock state kept separately from ordinary modifiers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LockState {
    pub caps_lock: bool,
    pub num_lock: bool,
}

impl LockState {
    pub const NONE: Self = Self {
        caps_lock: false,
        num_lock: false,
    };
}

/// The event kind supplied by Kitty event-type reporting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventKind {
    Press,
    Repeat,
    Release,
}

/// The class of a completed or incomplete terminal sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SequenceClass {
    Escape,
    Csi,
    Ss3,
    Utf8,
}

/// A well-formed terminal control sequence that was not a supported key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnknownSequence {
    pub class: SequenceClass,
    pub final_byte: u8,
    /// Number of bounded bytes observed after the introducer, before `final_byte`.
    pub parameter_bytes: u8,
}

/// A bounded malformed-input report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MalformedInput {
    pub class: SequenceClass,
    pub reason: MalformedReason,
    /// Bytes observed in the malformed unit, capped by parser limits.
    pub observed_bytes: u8,
}

/// Why input was malformed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MalformedReason {
    InvalidUtf8,
    InvalidSyntax,
    InvalidScalar,
    InvalidModifier,
    InvalidEventKind,
    InvalidAssociatedText,
    NumericOverflow,
    TooManyParameters,
    SequenceTooLong,
    IncompleteAtEof,
}

/// A pure streaming terminal input parser.
///
/// `push` accepts arbitrary byte chunks. It does not use a clock or allocate.
/// The caller resolves a pending bare Escape by calling [`Self::flush_pending_escape`]
/// at its profile-specific deadline, and calls [`Self::finish`] at end of input.
pub struct Parser {
    state: State,
    csi: [u8; MAX_CSI_SEQUENCE_BYTES],
    csi_len: usize,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    /// Creates a parser in its ground state.
    pub const fn new() -> Self {
        Self {
            state: State::Ground,
            csi: [0; MAX_CSI_SEQUENCE_BYTES],
            csi_len: 0,
        }
    }

    /// Pushes a chunk of terminal input bytes.
    pub fn push<F>(&mut self, bytes: &[u8], mut emit: F)
    where
        F: FnMut(InputEvent),
    {
        let mut index = 0;
        while index < bytes.len() {
            let byte = bytes[index];
            match self.state {
                State::Ground => {
                    self.consume_ground(byte, &mut emit);
                    index += 1;
                }
                State::Escape => {
                    self.consume_after_escape(byte, &mut emit);
                    index += 1;
                }
                State::Utf8(mut pending) => {
                    if byte & 0b1100_0000 != 0b1000_0000 {
                        self.state = State::Ground;
                        emit(malformed(
                            SequenceClass::Utf8,
                            MalformedReason::InvalidUtf8,
                            pending.len,
                        ));
                        continue;
                    }
                    pending.bytes[pending.len as usize] = byte;
                    pending.len += 1;
                    if pending.len == pending.expected {
                        let mut value =
                            (pending.bytes[0] & utf8_initial_mask(pending.expected)) as u32;
                        for continuation in &pending.bytes[1..pending.expected as usize] {
                            value = (value << 6) | (continuation & 0b0011_1111) as u32;
                        }
                        self.state = State::Ground;
                        match char::from_u32(value) {
                            Some(character) if !is_overlong_utf8(value, pending.expected) => {
                                emit(InputEvent::Key(RawKeyEvent::with_modifiers(
                                    KeyIdentity::Unicode(character),
                                    pending.modifiers,
                                )));
                            }
                            _ => emit(malformed(
                                SequenceClass::Utf8,
                                MalformedReason::InvalidUtf8,
                                pending.len,
                            )),
                        }
                    } else {
                        self.state = State::Utf8(pending);
                    }
                    index += 1;
                }
                State::Csi => {
                    self.consume_csi(byte, &mut emit);
                    index += 1;
                }
                State::Ss3 => {
                    self.consume_ss3(byte, &mut emit);
                    index += 1;
                }
                State::DiscardCsi => {
                    if is_csi_final(byte) {
                        self.state = State::Ground;
                    }
                    index += 1;
                }
            }
        }
    }

    /// Emits a bare Escape only when Escape is the sole pending byte.
    ///
    /// This is the explicit clock boundary: the parser does not own a timeout.
    pub fn flush_pending_escape<F>(&mut self, mut emit: F) -> bool
    where
        F: FnMut(InputEvent),
    {
        if !matches!(self.state, State::Escape) {
            return false;
        }
        self.state = State::Ground;
        emit(InputEvent::Key(RawKeyEvent::plain(
            KeyIdentity::Functional(FunctionalKey::Escape),
        )));
        true
    }

    /// Reports and clears incomplete input at an explicit end-of-input boundary.
    pub fn finish<F>(&mut self, mut emit: F)
    where
        F: FnMut(InputEvent),
    {
        match self.state {
            State::Ground => {}
            State::Escape => emit(InputEvent::Key(RawKeyEvent::plain(
                KeyIdentity::Functional(FunctionalKey::Escape),
            ))),
            State::Utf8(pending) => emit(malformed(
                SequenceClass::Utf8,
                MalformedReason::IncompleteAtEof,
                pending.len,
            )),
            State::Csi => emit(malformed(
                SequenceClass::Csi,
                MalformedReason::IncompleteAtEof,
                saturating_u8(self.csi_len),
            )),
            State::Ss3 => emit(malformed(
                SequenceClass::Ss3,
                MalformedReason::IncompleteAtEof,
                0,
            )),
            State::DiscardCsi => {}
        }
        self.reset();
    }

    /// Returns whether the only pending unit is a bare Escape byte.
    pub const fn has_pending_escape(&self) -> bool {
        matches!(self.state, State::Escape)
    }

    fn reset(&mut self) {
        self.state = State::Ground;
        self.csi_len = 0;
    }

    fn consume_ground<F>(&mut self, byte: u8, emit: &mut F)
    where
        F: FnMut(InputEvent),
    {
        match byte {
            0x1b => self.state = State::Escape,
            0x08 | 0x7f => emit(InputEvent::Key(RawKeyEvent::plain(
                KeyIdentity::Functional(FunctionalKey::Backspace),
            ))),
            b'\t' => emit(InputEvent::Key(RawKeyEvent::plain(
                KeyIdentity::Functional(FunctionalKey::Tab),
            ))),
            b'\r' | b'\n' => emit(InputEvent::Key(RawKeyEvent::plain(
                KeyIdentity::Functional(FunctionalKey::Enter),
            ))),
            byte if byte < 0x80 => emit(InputEvent::Key(RawKeyEvent::plain(KeyIdentity::Unicode(
                byte as char,
            )))),
            byte => self.begin_utf8(byte, Modifiers::NONE, emit),
        }
    }

    fn consume_after_escape<F>(&mut self, byte: u8, emit: &mut F)
    where
        F: FnMut(InputEvent),
    {
        match byte {
            b'[' => {
                self.csi_len = 0;
                self.state = State::Csi;
            }
            b'O' => self.state = State::Ss3,
            0x08 | 0x7f => {
                self.state = State::Ground;
                emit(InputEvent::Key(RawKeyEvent::with_modifiers(
                    KeyIdentity::Functional(FunctionalKey::Backspace),
                    Modifiers::ALT,
                )));
            }
            b'\t' => {
                self.state = State::Ground;
                emit(InputEvent::Key(RawKeyEvent::with_modifiers(
                    KeyIdentity::Functional(FunctionalKey::Tab),
                    Modifiers::ALT,
                )));
            }
            b'\r' | b'\n' => {
                self.state = State::Ground;
                emit(InputEvent::Key(RawKeyEvent::with_modifiers(
                    KeyIdentity::Functional(FunctionalKey::Enter),
                    Modifiers::ALT,
                )));
            }
            0x1b => {
                self.state = State::Ground;
                emit(InputEvent::Key(RawKeyEvent::with_modifiers(
                    KeyIdentity::Functional(FunctionalKey::Escape),
                    Modifiers::ALT,
                )));
            }
            byte if byte < 0x80 => {
                self.state = State::Ground;
                emit(InputEvent::Key(RawKeyEvent::with_modifiers(
                    KeyIdentity::Unicode(byte as char),
                    Modifiers::ALT,
                )));
            }
            byte => self.begin_utf8(byte, Modifiers::ALT, emit),
        }
    }

    fn begin_utf8<F>(&mut self, byte: u8, modifiers: Modifiers, emit: &mut F)
    where
        F: FnMut(InputEvent),
    {
        let expected = match byte {
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => {
                self.state = State::Ground;
                emit(malformed(
                    SequenceClass::Utf8,
                    MalformedReason::InvalidUtf8,
                    1,
                ));
                return;
            }
        };
        self.state = State::Utf8(Utf8Pending {
            bytes: [byte, 0, 0, 0],
            len: 1,
            expected,
            modifiers,
        });
    }

    fn consume_csi<F>(&mut self, byte: u8, emit: &mut F)
    where
        F: FnMut(InputEvent),
    {
        if is_csi_final(byte) {
            let result = decode_csi(&self.csi[..self.csi_len], byte);
            self.reset();
            emit(result);
            return;
        }
        if !is_csi_body(byte) {
            self.reset();
            emit(malformed(
                SequenceClass::Csi,
                MalformedReason::InvalidSyntax,
                saturating_u8(self.csi_len + 1),
            ));
            return;
        }
        if self.csi_len == MAX_CSI_SEQUENCE_BYTES {
            self.state = State::DiscardCsi;
            emit(malformed(
                SequenceClass::Csi,
                MalformedReason::SequenceTooLong,
                saturating_u8(self.csi_len),
            ));
            return;
        }
        self.csi[self.csi_len] = byte;
        self.csi_len += 1;
    }

    fn consume_ss3<F>(&mut self, byte: u8, emit: &mut F)
    where
        F: FnMut(InputEvent),
    {
        self.state = State::Ground;
        let primary = match byte {
            b'A' => Some(FunctionalKey::Up),
            b'B' => Some(FunctionalKey::Down),
            b'C' => Some(FunctionalKey::Right),
            b'D' => Some(FunctionalKey::Left),
            b'H' => Some(FunctionalKey::Home),
            b'F' => Some(FunctionalKey::End),
            b'P' => Some(FunctionalKey::Function(1)),
            b'Q' => Some(FunctionalKey::Function(2)),
            b'R' => Some(FunctionalKey::Function(3)),
            b'S' => Some(FunctionalKey::Function(4)),
            _ if is_csi_final(byte) => {
                emit(InputEvent::Unknown(UnknownSequence {
                    class: SequenceClass::Ss3,
                    final_byte: byte,
                    parameter_bytes: 0,
                }));
                return;
            }
            _ => {
                emit(malformed(
                    SequenceClass::Ss3,
                    MalformedReason::InvalidSyntax,
                    1,
                ));
                return;
            }
        };
        emit(InputEvent::Key(RawKeyEvent::plain(
            KeyIdentity::Functional(primary.expect("mapped above")),
        )));
    }
}

#[derive(Clone, Copy)]
enum State {
    Ground,
    Escape,
    Utf8(Utf8Pending),
    Csi,
    Ss3,
    DiscardCsi,
}

#[derive(Clone, Copy)]
struct Utf8Pending {
    bytes: [u8; 4],
    len: u8,
    expected: u8,
    modifiers: Modifiers,
}

#[derive(Clone, Copy)]
struct CsiParameter {
    values: [Option<u32>; MAX_ASSOCIATED_TEXT_SCALARS],
    len: u8,
}

impl CsiParameter {
    const EMPTY: Self = Self {
        values: [None; MAX_ASSOCIATED_TEXT_SCALARS],
        len: 0,
    };

    fn first(self) -> Option<u32> {
        if self.len == 0 { None } else { self.values[0] }
    }
}

#[derive(Clone, Copy)]
struct CsiParameters {
    prefix: Option<u8>,
    values: [CsiParameter; MAX_CSI_PARAMETERS],
    len: u8,
}

impl CsiParameters {
    const EMPTY: Self = Self {
        prefix: None,
        values: [CsiParameter::EMPTY; MAX_CSI_PARAMETERS],
        len: 0,
    };
}

fn decode_csi(bytes: &[u8], final_byte: u8) -> InputEvent {
    let parameters = match parse_csi_parameters(bytes) {
        Ok(parameters) => parameters,
        Err(reason) => return malformed(SequenceClass::Csi, reason, saturating_u8(bytes.len())),
    };

    if parameters.prefix.is_some() || bytes.iter().any(|byte| (0x20..=0x2f).contains(byte)) {
        return unknown_csi(final_byte, bytes.len());
    }

    match final_byte {
        b'u' => decode_kitty(parameters, final_byte, bytes.len()),
        b'~' => decode_tilde(parameters, final_byte, bytes.len()),
        b'A' => decode_direct(parameters, FunctionalKey::Up, final_byte, bytes.len()),
        b'B' => decode_direct(parameters, FunctionalKey::Down, final_byte, bytes.len()),
        b'C' => decode_direct(parameters, FunctionalKey::Right, final_byte, bytes.len()),
        b'D' => decode_direct(parameters, FunctionalKey::Left, final_byte, bytes.len()),
        b'H' => decode_direct(parameters, FunctionalKey::Home, final_byte, bytes.len()),
        b'F' => decode_direct(parameters, FunctionalKey::End, final_byte, bytes.len()),
        b'P' => decode_direct(
            parameters,
            FunctionalKey::Function(1),
            final_byte,
            bytes.len(),
        ),
        b'Q' => decode_direct(
            parameters,
            FunctionalKey::Function(2),
            final_byte,
            bytes.len(),
        ),
        b'R' => decode_direct(
            parameters,
            FunctionalKey::Function(3),
            final_byte,
            bytes.len(),
        ),
        b'S' => decode_direct(
            parameters,
            FunctionalKey::Function(4),
            final_byte,
            bytes.len(),
        ),
        b'E' => {
            let event = RawKeyEvent {
                primary: KeyIdentity::Functional(FunctionalKey::Begin),
                shifted: None,
                base: None,
                modifiers: Modifiers::NONE,
                kind: EventKind::Press,
                locks: LockState::NONE,
                keypad: Some(KeypadKey::Begin),
            };
            decode_direct_event(parameters, event, final_byte, bytes.len())
        }
        b'Z' => decode_shift_tab(parameters, final_byte, bytes.len()),
        _ => unknown_csi(final_byte, bytes.len()),
    }
}

fn parse_csi_parameters(bytes: &[u8]) -> Result<CsiParameters, MalformedReason> {
    let mut parsed = CsiParameters::EMPTY;
    if bytes.is_empty() {
        return Ok(parsed);
    }

    let mut index = 0;
    if matches!(bytes[0], b'<' | b'=' | b'>' | b'?') {
        parsed.prefix = Some(bytes[0]);
        index += 1;
        if index == bytes.len() {
            return Ok(parsed);
        }
    }

    let parameter_tail = &bytes[index..];
    let intermediate_start = parameter_tail
        .iter()
        .position(|byte| (0x20..=0x2f).contains(byte))
        .unwrap_or(parameter_tail.len());
    let (parameter_bytes, intermediates) = parameter_tail.split_at(intermediate_start);
    if intermediates
        .iter()
        .any(|byte| !(0x20..=0x2f).contains(byte))
    {
        return Err(MalformedReason::InvalidSyntax);
    }
    if parameter_bytes.is_empty() {
        return Ok(parsed);
    }

    parsed.len = 1;
    parsed.values[0].len = 1;
    let mut parameter_index = 0usize;
    let mut component_index = 0usize;
    let mut index = 0;

    while index < parameter_bytes.len() {
        match parameter_bytes[index] {
            digit @ b'0'..=b'9' => {
                let current = parsed.values[parameter_index].values[component_index].unwrap_or(0);
                let value = current
                    .checked_mul(10)
                    .and_then(|value| value.checked_add((digit - b'0') as u32))
                    .ok_or(MalformedReason::NumericOverflow)?;
                parsed.values[parameter_index].values[component_index] = Some(value);
            }
            b':' => {
                component_index += 1;
                if component_index == MAX_ASSOCIATED_TEXT_SCALARS {
                    return Err(MalformedReason::TooManyParameters);
                }
                parsed.values[parameter_index].len = (component_index + 1) as u8;
            }
            b';' => {
                parameter_index += 1;
                if parameter_index == MAX_CSI_PARAMETERS {
                    return Err(MalformedReason::TooManyParameters);
                }
                parsed.len = (parameter_index + 1) as u8;
                parsed.values[parameter_index].len = 1;
                component_index = 0;
            }
            _ => return Err(MalformedReason::InvalidSyntax),
        }
        index += 1;
    }

    Ok(parsed)
}

fn decode_kitty(parameters: CsiParameters, final_byte: u8, bytes: usize) -> InputEvent {
    if parameters.len == 0 {
        return unknown_csi(final_byte, bytes);
    }
    let primary_fields = parameters.values[0];
    if primary_fields.len == 0 || primary_fields.len > 3 || primary_fields.first().is_none() {
        return malformed(
            SequenceClass::Csi,
            MalformedReason::InvalidSyntax,
            saturating_u8(bytes),
        );
    }
    if parameters.len > 1 && parameters.values[1].len > 2 {
        return malformed(
            SequenceClass::Csi,
            MalformedReason::TooManyParameters,
            saturating_u8(bytes),
        );
    }

    let (modifiers, locks, kind) = match kitty_event_details(parameters) {
        Ok(details) => details,
        Err(reason) => return malformed(SequenceClass::Csi, reason, saturating_u8(bytes)),
    };
    if let Err(reason) = validate_associated_text(parameters) {
        return malformed(SequenceClass::Csi, reason, saturating_u8(bytes));
    }

    let (primary, keypad) = match primary_identity(primary_fields.values[0].expect("checked above"))
    {
        Ok(identity) => identity,
        Err(reason) => return malformed(SequenceClass::Csi, reason, saturating_u8(bytes)),
    };
    let shifted = match primary_fields.len {
        0 | 1 => None,
        _ => match optional_alternate_identity(primary_fields.values[1]) {
            Ok(identity) => identity,
            Err(reason) => return malformed(SequenceClass::Csi, reason, saturating_u8(bytes)),
        },
    };
    let base = match primary_fields.len {
        0..=2 => None,
        _ => match optional_alternate_identity(primary_fields.values[2]) {
            Ok(identity) => identity,
            Err(reason) => return malformed(SequenceClass::Csi, reason, saturating_u8(bytes)),
        },
    };

    InputEvent::Key(RawKeyEvent {
        primary,
        shifted,
        base,
        modifiers,
        kind,
        locks,
        keypad,
    })
}

fn kitty_event_details(
    parameters: CsiParameters,
) -> Result<(Modifiers, LockState, EventKind), MalformedReason> {
    if parameters.len < 2 {
        return Ok((Modifiers::NONE, LockState::NONE, EventKind::Press));
    }
    let modifiers = parameters.values[1];
    if modifiers.len == 0 || modifiers.len > 2 {
        return Err(MalformedReason::InvalidSyntax);
    }
    let modifier_value = modifiers.values[0].unwrap_or(1);
    if modifiers.len == 2 && modifiers.values[0].is_none() {
        return Err(MalformedReason::InvalidSyntax);
    }
    let (modifier_set, locks) = kitty_modifiers(modifier_value)?;
    let kind = match modifiers.values[1] {
        None | Some(1) => EventKind::Press,
        Some(2) => EventKind::Repeat,
        Some(3) => EventKind::Release,
        Some(_) => return Err(MalformedReason::InvalidEventKind),
    };
    Ok((modifier_set, locks, kind))
}

fn validate_associated_text(parameters: CsiParameters) -> Result<(), MalformedReason> {
    if parameters.len < 3 {
        return Ok(());
    }
    let text = parameters.values[2];
    if text.len == 0 {
        return Err(MalformedReason::InvalidAssociatedText);
    }
    for value in text.values[..text.len as usize].iter().copied() {
        let value = value.ok_or(MalformedReason::InvalidAssociatedText)?;
        let character = char::from_u32(value).ok_or(MalformedReason::InvalidScalar)?;
        if character.is_control() || (0x7f..=0x9f).contains(&value) {
            return Err(MalformedReason::InvalidAssociatedText);
        }
    }
    Ok(())
}

fn decode_tilde(parameters: CsiParameters, final_byte: u8, bytes: usize) -> InputEvent {
    if parameters.len == 0 || parameters.len > 2 || parameters.values[0].len != 1 {
        return unknown_csi(final_byte, bytes);
    }
    let Some(number) = parameters.values[0].first() else {
        return unknown_csi(final_byte, bytes);
    };
    let Some((primary, keypad)) = tilde_identity(number) else {
        return unknown_csi(final_byte, bytes);
    };
    let modifier_value = match parameters.len {
        1 => 1,
        2 if parameters.values[1].len == 1 => parameters.values[1].first().unwrap_or(1),
        _ => return unknown_csi(final_byte, bytes),
    };
    let (modifiers, locks) = match kitty_modifiers(modifier_value) {
        Ok(result) => result,
        Err(reason) => return malformed(SequenceClass::Csi, reason, saturating_u8(bytes)),
    };
    InputEvent::Key(RawKeyEvent {
        primary,
        shifted: None,
        base: None,
        modifiers,
        kind: EventKind::Press,
        locks,
        keypad,
    })
}

fn decode_direct(
    parameters: CsiParameters,
    key: FunctionalKey,
    final_byte: u8,
    bytes: usize,
) -> InputEvent {
    decode_direct_event(
        parameters,
        RawKeyEvent::plain(KeyIdentity::Functional(key)),
        final_byte,
        bytes,
    )
}

fn decode_direct_event(
    parameters: CsiParameters,
    mut event: RawKeyEvent,
    final_byte: u8,
    bytes: usize,
) -> InputEvent {
    let modifier_value = match parameters.len {
        0 => 1,
        1 if parameters.values[0].len == 1 && parameters.values[0].first() == Some(1) => 1,
        2 if parameters.values[0].len == 1
            && parameters.values[0].first().is_some()
            && parameters.values[1].len == 1 =>
        {
            parameters.values[1].first().unwrap_or(1)
        }
        _ => return unknown_csi(final_byte, bytes),
    };
    let (modifiers, locks) = match kitty_modifiers(modifier_value) {
        Ok(result) => result,
        Err(reason) => return malformed(SequenceClass::Csi, reason, saturating_u8(bytes)),
    };
    event.modifiers = modifiers;
    event.locks = locks;
    InputEvent::Key(event)
}

fn decode_shift_tab(parameters: CsiParameters, final_byte: u8, bytes: usize) -> InputEvent {
    let mut event = RawKeyEvent::plain(KeyIdentity::Functional(FunctionalKey::Tab));
    event.modifiers = Modifiers::SHIFT;
    match parameters.len {
        0 => InputEvent::Key(event),
        2 if parameters.values[0].first() == Some(1) && parameters.values[1].len == 1 => {
            let (modifiers, locks) =
                match kitty_modifiers(parameters.values[1].first().unwrap_or(1)) {
                    Ok(result) => result,
                    Err(reason) => {
                        return malformed(SequenceClass::Csi, reason, saturating_u8(bytes));
                    }
                };
            event.modifiers = modifiers;
            event.locks = locks;
            InputEvent::Key(event)
        }
        _ => unknown_csi(final_byte, bytes),
    }
}

fn primary_identity(value: u32) -> Result<(KeyIdentity, Option<KeypadKey>), MalformedReason> {
    let primary = reported_identity(value)?;
    let keypad = functional_identity(value).and_then(|(_, keypad)| keypad);
    Ok((primary, keypad))
}

fn reported_identity(value: u32) -> Result<KeyIdentity, MalformedReason> {
    if value == 0 {
        return Ok(KeyIdentity::Unidentified);
    }
    if let Some((primary, _)) = functional_identity(value) {
        return Ok(KeyIdentity::Functional(primary));
    }
    let character = char::from_u32(value).ok_or(MalformedReason::InvalidScalar)?;
    if (0xe000..=0xf8ff).contains(&value) {
        return Ok(KeyIdentity::UnknownFunctional(value));
    }
    Ok(KeyIdentity::Unicode(character))
}

fn optional_alternate_identity(value: Option<u32>) -> Result<Option<KeyIdentity>, MalformedReason> {
    value.map(reported_identity).transpose()
}

fn functional_identity(value: u32) -> Option<(FunctionalKey, Option<KeypadKey>)> {
    let key = match value {
        9 => (FunctionalKey::Tab, None),
        13 => (FunctionalKey::Enter, None),
        27 => (FunctionalKey::Escape, None),
        127 => (FunctionalKey::Backspace, None),
        57344 => (FunctionalKey::Escape, None),
        57345 => (FunctionalKey::Enter, None),
        57346 => (FunctionalKey::Tab, None),
        57347 => (FunctionalKey::Backspace, None),
        57348 => (FunctionalKey::Insert, None),
        57349 => (FunctionalKey::Delete, None),
        57350 => (FunctionalKey::Left, None),
        57351 => (FunctionalKey::Right, None),
        57352 => (FunctionalKey::Up, None),
        57353 => (FunctionalKey::Down, None),
        57354 => (FunctionalKey::PageUp, None),
        57355 => (FunctionalKey::PageDown, None),
        57356 => (FunctionalKey::Home, None),
        57357 => (FunctionalKey::End, None),
        57358 => (FunctionalKey::CapsLock, None),
        57359 => (FunctionalKey::ScrollLock, None),
        57360 => (FunctionalKey::NumLock, None),
        57361 => (FunctionalKey::PrintScreen, None),
        57362 => (FunctionalKey::Pause, None),
        57363 => (FunctionalKey::Menu, None),
        57364..=57398 => (FunctionalKey::Function((value - 57364 + 1) as u8), None),
        57399..=57408 => {
            let digit = (value - 57399) as u8;
            (
                FunctionalKey::from_keypad(KeypadKey::Digit(digit)),
                Some(KeypadKey::Digit(digit)),
            )
        }
        57409 => (
            FunctionalKey::from_keypad(KeypadKey::Decimal),
            Some(KeypadKey::Decimal),
        ),
        57410 => (
            FunctionalKey::from_keypad(KeypadKey::Divide),
            Some(KeypadKey::Divide),
        ),
        57411 => (
            FunctionalKey::from_keypad(KeypadKey::Multiply),
            Some(KeypadKey::Multiply),
        ),
        57412 => (
            FunctionalKey::from_keypad(KeypadKey::Subtract),
            Some(KeypadKey::Subtract),
        ),
        57413 => (
            FunctionalKey::from_keypad(KeypadKey::Add),
            Some(KeypadKey::Add),
        ),
        57414 => (
            FunctionalKey::from_keypad(KeypadKey::Enter),
            Some(KeypadKey::Enter),
        ),
        57415 => (
            FunctionalKey::from_keypad(KeypadKey::Equal),
            Some(KeypadKey::Equal),
        ),
        57416 => (
            FunctionalKey::from_keypad(KeypadKey::Separator),
            Some(KeypadKey::Separator),
        ),
        57417 => (
            FunctionalKey::from_keypad(KeypadKey::Left),
            Some(KeypadKey::Left),
        ),
        57418 => (
            FunctionalKey::from_keypad(KeypadKey::Right),
            Some(KeypadKey::Right),
        ),
        57419 => (
            FunctionalKey::from_keypad(KeypadKey::Up),
            Some(KeypadKey::Up),
        ),
        57420 => (
            FunctionalKey::from_keypad(KeypadKey::Down),
            Some(KeypadKey::Down),
        ),
        57421 => (
            FunctionalKey::from_keypad(KeypadKey::PageUp),
            Some(KeypadKey::PageUp),
        ),
        57422 => (
            FunctionalKey::from_keypad(KeypadKey::PageDown),
            Some(KeypadKey::PageDown),
        ),
        57423 => (
            FunctionalKey::from_keypad(KeypadKey::Home),
            Some(KeypadKey::Home),
        ),
        57424 => (
            FunctionalKey::from_keypad(KeypadKey::End),
            Some(KeypadKey::End),
        ),
        57425 => (
            FunctionalKey::from_keypad(KeypadKey::Insert),
            Some(KeypadKey::Insert),
        ),
        57426 => (
            FunctionalKey::from_keypad(KeypadKey::Delete),
            Some(KeypadKey::Delete),
        ),
        57427 => (
            FunctionalKey::from_keypad(KeypadKey::Begin),
            Some(KeypadKey::Begin),
        ),
        57428 => (FunctionalKey::Media(MediaKey::Play), None),
        57429 => (FunctionalKey::Media(MediaKey::Pause), None),
        57430 => (FunctionalKey::Media(MediaKey::PlayPause), None),
        57431 => (FunctionalKey::Media(MediaKey::Reverse), None),
        57432 => (FunctionalKey::Media(MediaKey::Stop), None),
        57433 => (FunctionalKey::Media(MediaKey::FastForward), None),
        57434 => (FunctionalKey::Media(MediaKey::Rewind), None),
        57435 => (FunctionalKey::Media(MediaKey::TrackNext), None),
        57436 => (FunctionalKey::Media(MediaKey::TrackPrevious), None),
        57437 => (FunctionalKey::Media(MediaKey::Record), None),
        57438 => (FunctionalKey::Media(MediaKey::LowerVolume), None),
        57439 => (FunctionalKey::Media(MediaKey::RaiseVolume), None),
        57440 => (FunctionalKey::Media(MediaKey::MuteVolume), None),
        57441 => (FunctionalKey::Modifier(ModifierKey::LeftShift), None),
        57442 => (FunctionalKey::Modifier(ModifierKey::LeftControl), None),
        57443 => (FunctionalKey::Modifier(ModifierKey::LeftAlt), None),
        57444 => (FunctionalKey::Modifier(ModifierKey::LeftSuper), None),
        57445 => (FunctionalKey::Modifier(ModifierKey::LeftHyper), None),
        57446 => (FunctionalKey::Modifier(ModifierKey::LeftMeta), None),
        57447 => (FunctionalKey::Modifier(ModifierKey::RightShift), None),
        57448 => (FunctionalKey::Modifier(ModifierKey::RightControl), None),
        57449 => (FunctionalKey::Modifier(ModifierKey::RightAlt), None),
        57450 => (FunctionalKey::Modifier(ModifierKey::RightSuper), None),
        57451 => (FunctionalKey::Modifier(ModifierKey::RightHyper), None),
        57452 => (FunctionalKey::Modifier(ModifierKey::RightMeta), None),
        57453 => (FunctionalKey::Modifier(ModifierKey::IsoLevel3Shift), None),
        57454 => (FunctionalKey::Modifier(ModifierKey::IsoLevel5Shift), None),
        _ => return None,
    };
    Some(key)
}

impl FunctionalKey {
    fn from_keypad(keypad: KeypadKey) -> Self {
        Self::Keypad(keypad)
    }
}

fn tilde_identity(value: u32) -> Option<(KeyIdentity, Option<KeypadKey>)> {
    let functional = match value {
        2 => FunctionalKey::Insert,
        3 => FunctionalKey::Delete,
        5 => FunctionalKey::PageUp,
        6 => FunctionalKey::PageDown,
        7 => FunctionalKey::Home,
        8 => FunctionalKey::End,
        11 => FunctionalKey::Function(1),
        12 => FunctionalKey::Function(2),
        13 => FunctionalKey::Function(3),
        14 => FunctionalKey::Function(4),
        15 => FunctionalKey::Function(5),
        17 => FunctionalKey::Function(6),
        18 => FunctionalKey::Function(7),
        19 => FunctionalKey::Function(8),
        20 => FunctionalKey::Function(9),
        21 => FunctionalKey::Function(10),
        23 => FunctionalKey::Function(11),
        24 => FunctionalKey::Function(12),
        29 => FunctionalKey::Menu,
        57427 => FunctionalKey::Begin,
        _ => return None,
    };
    let keypad = (value == 57427).then_some(KeypadKey::Begin);
    Some((KeyIdentity::Functional(functional), keypad))
}

fn kitty_modifiers(value: u32) -> Result<(Modifiers, LockState), MalformedReason> {
    if !(1..=256).contains(&value) {
        return Err(MalformedReason::InvalidModifier);
    }
    let bits = (value - 1) as u8;
    Ok((
        Modifiers(bits & 0b0011_1111),
        LockState {
            caps_lock: bits & 0b0100_0000 != 0,
            num_lock: bits & 0b1000_0000 != 0,
        },
    ))
}

const fn malformed(
    class: SequenceClass,
    reason: MalformedReason,
    observed_bytes: u8,
) -> InputEvent {
    InputEvent::Malformed(MalformedInput {
        class,
        reason,
        observed_bytes,
    })
}

const fn unknown_csi(final_byte: u8, bytes: usize) -> InputEvent {
    InputEvent::Unknown(UnknownSequence {
        class: SequenceClass::Csi,
        final_byte,
        parameter_bytes: saturating_u8(bytes),
    })
}

const fn saturating_u8(value: usize) -> u8 {
    if value > u8::MAX as usize {
        u8::MAX
    } else {
        value as u8
    }
}

const fn is_csi_final(byte: u8) -> bool {
    byte >= 0x40 && byte <= 0x7e
}

const fn is_csi_body(byte: u8) -> bool {
    byte >= 0x20 && byte <= 0x3f
}

const fn utf8_initial_mask(expected: u8) -> u8 {
    match expected {
        2 => 0b0001_1111,
        3 => 0b0000_1111,
        4 => 0b0000_0111,
        _ => 0,
    }
}

const fn is_overlong_utf8(value: u32, expected: u8) -> bool {
    match expected {
        2 => value < 0x80,
        3 => value < 0x800,
        4 => value < 0x1_0000,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn parse_chunks(chunks: &[&[u8]], finish: bool) -> Vec<InputEvent> {
        let mut parser = Parser::new();
        let mut events = Vec::new();
        for chunk in chunks {
            parser.push(chunk, |event| events.push(event));
        }
        if finish {
            parser.finish(|event| events.push(event));
        }
        events
    }

    fn kitty_functional_table_oracle(value: u32) -> Option<(FunctionalKey, Option<KeypadKey>)> {
        let key = match value {
            9 => (FunctionalKey::Tab, None),
            13 => (FunctionalKey::Enter, None),
            27 => (FunctionalKey::Escape, None),
            127 => (FunctionalKey::Backspace, None),
            57344 => (FunctionalKey::Escape, None),
            57345 => (FunctionalKey::Enter, None),
            57346 => (FunctionalKey::Tab, None),
            57347 => (FunctionalKey::Backspace, None),
            57348 => (FunctionalKey::Insert, None),
            57349 => (FunctionalKey::Delete, None),
            57350 => (FunctionalKey::Left, None),
            57351 => (FunctionalKey::Right, None),
            57352 => (FunctionalKey::Up, None),
            57353 => (FunctionalKey::Down, None),
            57354 => (FunctionalKey::PageUp, None),
            57355 => (FunctionalKey::PageDown, None),
            57356 => (FunctionalKey::Home, None),
            57357 => (FunctionalKey::End, None),
            57358 => (FunctionalKey::CapsLock, None),
            57359 => (FunctionalKey::ScrollLock, None),
            57360 => (FunctionalKey::NumLock, None),
            57361 => (FunctionalKey::PrintScreen, None),
            57362 => (FunctionalKey::Pause, None),
            57363 => (FunctionalKey::Menu, None),
            57364..=57398 => (FunctionalKey::Function((value - 57364 + 1) as u8), None),
            57399..=57408 => {
                let keypad = KeypadKey::Digit((value - 57399) as u8);
                (FunctionalKey::Keypad(keypad), Some(keypad))
            }
            57409 => (
                FunctionalKey::Keypad(KeypadKey::Decimal),
                Some(KeypadKey::Decimal),
            ),
            57410 => (
                FunctionalKey::Keypad(KeypadKey::Divide),
                Some(KeypadKey::Divide),
            ),
            57411 => (
                FunctionalKey::Keypad(KeypadKey::Multiply),
                Some(KeypadKey::Multiply),
            ),
            57412 => (
                FunctionalKey::Keypad(KeypadKey::Subtract),
                Some(KeypadKey::Subtract),
            ),
            57413 => (FunctionalKey::Keypad(KeypadKey::Add), Some(KeypadKey::Add)),
            57414 => (
                FunctionalKey::Keypad(KeypadKey::Enter),
                Some(KeypadKey::Enter),
            ),
            57415 => (
                FunctionalKey::Keypad(KeypadKey::Equal),
                Some(KeypadKey::Equal),
            ),
            57416 => (
                FunctionalKey::Keypad(KeypadKey::Separator),
                Some(KeypadKey::Separator),
            ),
            57417 => (
                FunctionalKey::Keypad(KeypadKey::Left),
                Some(KeypadKey::Left),
            ),
            57418 => (
                FunctionalKey::Keypad(KeypadKey::Right),
                Some(KeypadKey::Right),
            ),
            57419 => (FunctionalKey::Keypad(KeypadKey::Up), Some(KeypadKey::Up)),
            57420 => (
                FunctionalKey::Keypad(KeypadKey::Down),
                Some(KeypadKey::Down),
            ),
            57421 => (
                FunctionalKey::Keypad(KeypadKey::PageUp),
                Some(KeypadKey::PageUp),
            ),
            57422 => (
                FunctionalKey::Keypad(KeypadKey::PageDown),
                Some(KeypadKey::PageDown),
            ),
            57423 => (
                FunctionalKey::Keypad(KeypadKey::Home),
                Some(KeypadKey::Home),
            ),
            57424 => (FunctionalKey::Keypad(KeypadKey::End), Some(KeypadKey::End)),
            57425 => (
                FunctionalKey::Keypad(KeypadKey::Insert),
                Some(KeypadKey::Insert),
            ),
            57426 => (
                FunctionalKey::Keypad(KeypadKey::Delete),
                Some(KeypadKey::Delete),
            ),
            57427 => (
                FunctionalKey::Keypad(KeypadKey::Begin),
                Some(KeypadKey::Begin),
            ),
            57428 => (FunctionalKey::Media(MediaKey::Play), None),
            57429 => (FunctionalKey::Media(MediaKey::Pause), None),
            57430 => (FunctionalKey::Media(MediaKey::PlayPause), None),
            57431 => (FunctionalKey::Media(MediaKey::Reverse), None),
            57432 => (FunctionalKey::Media(MediaKey::Stop), None),
            57433 => (FunctionalKey::Media(MediaKey::FastForward), None),
            57434 => (FunctionalKey::Media(MediaKey::Rewind), None),
            57435 => (FunctionalKey::Media(MediaKey::TrackNext), None),
            57436 => (FunctionalKey::Media(MediaKey::TrackPrevious), None),
            57437 => (FunctionalKey::Media(MediaKey::Record), None),
            57438 => (FunctionalKey::Media(MediaKey::LowerVolume), None),
            57439 => (FunctionalKey::Media(MediaKey::RaiseVolume), None),
            57440 => (FunctionalKey::Media(MediaKey::MuteVolume), None),
            57441 => (FunctionalKey::Modifier(ModifierKey::LeftShift), None),
            57442 => (FunctionalKey::Modifier(ModifierKey::LeftControl), None),
            57443 => (FunctionalKey::Modifier(ModifierKey::LeftAlt), None),
            57444 => (FunctionalKey::Modifier(ModifierKey::LeftSuper), None),
            57445 => (FunctionalKey::Modifier(ModifierKey::LeftHyper), None),
            57446 => (FunctionalKey::Modifier(ModifierKey::LeftMeta), None),
            57447 => (FunctionalKey::Modifier(ModifierKey::RightShift), None),
            57448 => (FunctionalKey::Modifier(ModifierKey::RightControl), None),
            57449 => (FunctionalKey::Modifier(ModifierKey::RightAlt), None),
            57450 => (FunctionalKey::Modifier(ModifierKey::RightSuper), None),
            57451 => (FunctionalKey::Modifier(ModifierKey::RightHyper), None),
            57452 => (FunctionalKey::Modifier(ModifierKey::RightMeta), None),
            57453 => (FunctionalKey::Modifier(ModifierKey::IsoLevel3Shift), None),
            57454 => (FunctionalKey::Modifier(ModifierKey::IsoLevel5Shift), None),
            _ => return None,
        };
        Some(key)
    }

    #[test]
    fn kitty_event_keeps_each_reported_identity_and_state() {
        let events = parse_chunks(&[b"\x1b[97:65:113;198:2;65u"], true);
        assert_eq!(
            events,
            vec![InputEvent::Key(RawKeyEvent {
                primary: KeyIdentity::Unicode('a'),
                shifted: Some(KeyIdentity::Unicode('A')),
                base: Some(KeyIdentity::Unicode('q')),
                modifiers: Modifiers::SHIFT | Modifiers::CONTROL,
                kind: EventKind::Repeat,
                locks: LockState {
                    caps_lock: true,
                    num_lock: true,
                },
                keypad: None,
            })]
        );
    }

    #[test]
    fn legacy_escape_is_explicitly_flushed_and_alt_prefixes_are_not_lost() {
        let mut parser = Parser::new();
        let mut events = Vec::new();
        parser.push(b"\x1b", |event| events.push(event));
        assert!(parser.has_pending_escape());
        assert!(events.is_empty());
        assert!(parser.flush_pending_escape(|event| events.push(event)));
        parser.push(b"\x1b\xC3\xA5", |event| events.push(event));
        parser.finish(|event| events.push(event));

        assert_eq!(
            events,
            vec![
                InputEvent::Key(RawKeyEvent::plain(KeyIdentity::Functional(
                    FunctionalKey::Escape
                ))),
                InputEvent::Key(RawKeyEvent::with_modifiers(
                    KeyIdentity::Unicode('å'),
                    Modifiers::ALT,
                )),
            ]
        );
    }

    #[test]
    fn legacy_and_kitty_named_keys_cover_the_supported_paths() {
        let cases = [
            (b"\x1b[A".as_slice(), FunctionalKey::Up),
            (b"\x1bOD".as_slice(), FunctionalKey::Left),
            (b"\x1b[15~".as_slice(), FunctionalKey::Function(5)),
            (b"\x1b[57358;1:1u".as_slice(), FunctionalKey::CapsLock),
            (
                b"\x1b[57428;1:2u".as_slice(),
                FunctionalKey::Media(MediaKey::Play),
            ),
            (
                b"\x1b[57442;5:3u".as_slice(),
                FunctionalKey::Modifier(ModifierKey::LeftControl),
            ),
        ];

        for (bytes, expected) in cases {
            let events = parse_chunks(&[bytes], true);
            assert!(matches!(
                events.as_slice(),
                [InputEvent::Key(RawKeyEvent { primary: KeyIdentity::Functional(key), .. })] if *key == expected
            ));
        }
    }

    #[test]
    fn every_canonical_kitty_functional_key_preserves_identity_modifiers_and_event_kind() {
        for key_code in [9_u32, 13, 27, 127].into_iter().chain(57344..=57454) {
            let (expected_primary, expected_keypad) =
                kitty_functional_table_oracle(key_code).expect("canonical Kitty table entry");
            for modifier_value in 1..=256 {
                for (encoded_kind, expected_kind) in [
                    (1, EventKind::Press),
                    (2, EventKind::Repeat),
                    (3, EventKind::Release),
                ] {
                    let stream = format!("\x1b[{key_code};{modifier_value}:{encoded_kind}u");
                    let events = parse_chunks(&[stream.as_bytes()], true);
                    let [InputEvent::Key(event)] = events.as_slice() else {
                        panic!("canonical Kitty event did not decode: {stream:?}");
                    };
                    let bits = (modifier_value - 1) as u8;
                    assert_eq!(event.primary, KeyIdentity::Functional(expected_primary));
                    assert_eq!(event.keypad, expected_keypad);
                    assert_eq!(event.modifiers.bits(), bits & 0b0011_1111);
                    assert_eq!(event.locks.caps_lock, bits & 0b0100_0000 != 0);
                    assert_eq!(event.locks.num_lock, bits & 0b1000_0000 != 0);
                    assert_eq!(event.kind, expected_kind);
                }
            }
        }
    }

    #[test]
    fn kitty_keypad_identity_remains_separate_from_primary() {
        let events = parse_chunks(&[b"\x1b[57400;1:3u"], true);
        assert_eq!(
            events,
            vec![InputEvent::Key(RawKeyEvent {
                primary: KeyIdentity::Functional(FunctionalKey::Keypad(KeypadKey::Digit(1))),
                shifted: None,
                base: None,
                modifiers: Modifiers::NONE,
                kind: EventKind::Release,
                locks: LockState::NONE,
                keypad: Some(KeypadKey::Digit(1)),
            })]
        );
    }

    #[test]
    fn unknown_kitty_functional_keys_keep_their_raw_identity_and_metadata() {
        let events = parse_chunks(&[b"\x1b[57455:57456:57457;198:3;65uz"], true);
        assert_eq!(
            events,
            vec![
                InputEvent::Key(RawKeyEvent {
                    primary: KeyIdentity::UnknownFunctional(57455),
                    shifted: Some(KeyIdentity::UnknownFunctional(57456)),
                    base: Some(KeyIdentity::UnknownFunctional(57457)),
                    modifiers: Modifiers::SHIFT | Modifiers::CONTROL,
                    kind: EventKind::Release,
                    locks: LockState {
                        caps_lock: true,
                        num_lock: true,
                    },
                    keypad: None,
                }),
                InputEvent::Key(RawKeyEvent::plain(KeyIdentity::Unicode('z'))),
            ]
        );
    }

    #[test]
    fn valid_stream_is_invariant_to_fragmentation_at_every_byte_boundary() {
        let stream = b"a\xC3\xA5\x1b[97:65:113;6:2u\x1b[1;3A\x1bOP\r";
        let whole = parse_chunks(&[stream], true);
        for split in 0..=stream.len() {
            let fragmented = parse_chunks(&[&stream[..split], &stream[split..]], true);
            assert_eq!(fragmented, whole, "split at byte {split}");
        }
    }

    #[test]
    fn malformed_input_reports_once_and_recovers_at_the_next_unit() {
        let events = parse_chunks(&[b"\x1b[55296ux\x1b[42949672960uy"], true);
        assert_eq!(
            events,
            vec![
                malformed(SequenceClass::Csi, MalformedReason::InvalidScalar, 5),
                InputEvent::Key(RawKeyEvent::plain(KeyIdentity::Unicode('x'))),
                malformed(SequenceClass::Csi, MalformedReason::NumericOverflow, 11),
                InputEvent::Key(RawKeyEvent::plain(KeyIdentity::Unicode('y'))),
            ]
        );
    }

    #[test]
    fn overlong_csi_discards_to_its_final_boundary_then_recovers() {
        let mut stream = Vec::with_capacity(MAX_CSI_SEQUENCE_BYTES + 5);
        stream.extend_from_slice(b"\x1b[");
        stream.extend(core::iter::repeat_n(b'1', MAX_CSI_SEQUENCE_BYTES + 1));
        stream.extend_from_slice(b"ux");
        let events = parse_chunks(&[&stream], true);
        assert_eq!(
            events,
            vec![
                malformed(
                    SequenceClass::Csi,
                    MalformedReason::SequenceTooLong,
                    MAX_CSI_SEQUENCE_BYTES as u8,
                ),
                InputEvent::Key(RawKeyEvent::plain(KeyIdentity::Unicode('x'))),
            ]
        );
    }

    #[test]
    fn parameter_limits_and_unknown_well_formed_sequences_are_distinct() {
        let events = parse_chunks(&[b"\x1b[1;1;1;1ux\x1b[?1uy"], true);
        assert!(matches!(
            events.as_slice(),
            [
                InputEvent::Malformed(MalformedInput {
                    reason: MalformedReason::TooManyParameters,
                    ..
                }),
                InputEvent::Key(RawKeyEvent {
                    primary: KeyIdentity::Unicode('x'),
                    ..
                }),
                InputEvent::Unknown(UnknownSequence {
                    class: SequenceClass::Csi,
                    final_byte: b'u',
                    ..
                }),
                InputEvent::Key(RawKeyEvent {
                    primary: KeyIdentity::Unicode('y'),
                    ..
                }),
            ]
        ));
    }

    #[test]
    fn eof_distinguishes_pending_escape_from_other_incomplete_units() {
        let escape = parse_chunks(&[b"\x1b"], true);
        let csi = parse_chunks(&[b"\x1b[97"], true);
        assert!(matches!(
            escape.as_slice(),
            [InputEvent::Key(RawKeyEvent {
                primary: KeyIdentity::Functional(FunctionalKey::Escape),
                ..
            })]
        ));
        assert_eq!(
            csi,
            vec![malformed(
                SequenceClass::Csi,
                MalformedReason::IncompleteAtEof,
                2,
            )]
        );
    }

    fn ordinary_unicode_scalar() -> impl Strategy<Value = char> {
        any::<char>().prop_filter("not a Kitty functional code", |character| {
            !matches!(*character as u32, 0 | 9 | 13 | 27 | 127)
                && !('\u{e000}'..='\u{f8ff}').contains(character)
        })
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_never_panic_and_have_chunking_invariance(
            bytes in proptest::collection::vec(any::<u8>(), 0..512),
            boundaries in proptest::collection::vec(0usize..512, 0..32),
        ) {
            let whole = parse_chunks(&[&bytes], true);
            let mut parser = Parser::new();
            let mut fragmented = Vec::new();
            let mut start = 0;
            for end in boundaries {
                let end = end.min(bytes.len());
                if end < start {
                    continue;
                }
                parser.push(&bytes[start..end], |event| fragmented.push(event));
                start = end;
            }
            parser.push(&bytes[start..], |event| fragmented.push(event));
            parser.finish(|event| fragmented.push(event));
            prop_assert_eq!(fragmented, whole);
        }

        #[test]
        fn valid_unicode_and_alternate_identities_are_preserved(
            primary in ordinary_unicode_scalar(),
            shifted in prop::option::of(ordinary_unicode_scalar()),
            base in prop::option::of(ordinary_unicode_scalar()),
            modifier_value in 1u32..=256,
            encoded_kind in 1u32..=3,
        ) {
            let mut stream = format!("\x1b[{}", primary as u32);
            if shifted.is_some() || base.is_some() {
                stream.push(':');
                if let Some(shifted) = shifted {
                    stream.push_str(&(shifted as u32).to_string());
                }
                if let Some(base) = base {
                    stream.push(':');
                    stream.push_str(&(base as u32).to_string());
                }
            }
            stream.push_str(&format!(";{modifier_value}:{encoded_kind};65u"));

            let events = parse_chunks(&[stream.as_bytes()], true);
            let [InputEvent::Key(event)] = events.as_slice() else {
                prop_assert!(false, "valid Kitty Unicode event did not decode: {stream:?}");
                return Ok(());
            };
            let bits = (modifier_value - 1) as u8;
            prop_assert_eq!(event.primary, KeyIdentity::Unicode(primary));
            prop_assert_eq!(event.shifted, shifted.map(KeyIdentity::Unicode));
            prop_assert_eq!(event.base, base.map(KeyIdentity::Unicode));
            prop_assert_eq!(event.modifiers.bits(), bits & 0b0011_1111);
            prop_assert_eq!(event.locks.caps_lock, bits & 0b0100_0000 != 0);
            prop_assert_eq!(event.locks.num_lock, bits & 0b1000_0000 != 0);
            prop_assert_eq!(
                event.kind,
                match encoded_kind {
                    1 => EventKind::Press,
                    2 => EventKind::Repeat,
                    3 => EventKind::Release,
                    _ => unreachable!(),
                },
            );
        }

        #[test]
        fn unicode_scalar_boundaries_are_accepted_or_rejected_deterministically(
            scalar in prop_oneof![
                Just(0xd7ff_u32),
                Just(0xd800_u32),
                Just(0xdfff_u32),
                Just(0xe000_u32),
                Just(0xffff_u32),
                Just(0x1_0000_u32),
                Just(0x10_ffff_u32),
                Just(0x11_0000_u32),
            ],
        ) {
            let stream = format!("\x1b[{scalar}u");
            let events = parse_chunks(&[stream.as_bytes()], true);
            match char::from_u32(scalar) {
                Some(_) => {
                    prop_assert!(matches!(events.as_slice(), [InputEvent::Key(_)]));
                }
                None => {
                    let is_invalid_scalar = matches!(
                        events.as_slice(),
                        [InputEvent::Malformed(input)]
                            if input.reason == MalformedReason::InvalidScalar
                    );
                    prop_assert!(is_invalid_scalar);
                }
            }
        }
    }
}
