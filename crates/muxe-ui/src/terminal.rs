use std::{
    collections::VecDeque,
    io::{self, Write},
};

use crossterm::{
    QueueableCommand,
    cursor::{Hide, MoveTo, Show},
    terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
};
use muxe_core::{KeyCapabilities, KeyboardProfile};
use muxe_terminal_input::{Parser, ProtocolResponse};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
use thiserror::Error;
use tokio::time::Instant;
use unicode_width::UnicodeWidthStr;

use crate::{
    ConvertedInput, GridPlan, MenuGrid, MenuStatus, RenderedText, convert_input,
    widget::write_rendered,
};

/// Maximum parsed input events held while a Kitty response is pending.
pub const MAX_PENDING_NEGOTIATION_INPUT: usize = 64;

/// One deterministic arbitration decision for a VT100 Escape deadline.
///
/// The driver flushes the pending Escape first and then feeds newly arrived bytes, or feeds the
/// bytes into the still-pending sequence. The runner computes this from one shared timestamp so
/// a byte that is readable at the same moment the deadline expires always resolves the same way.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EscapeArrival {
    /// The arrival precedes the deadline, so the byte continues the pending Escape sequence.
    BeforeDeadline,
    /// The arrival is at or past the deadline, so the pending Escape flushes first.
    AtOrAfterDeadline,
}

/// Arbitrates one Escape-deadline boundary against a single shared arrival timestamp.
///
/// Rule: when the arrival timestamp reaches the pending Escape deadline (`now >= deadline`), the
/// pending Escape flushes first and the newly arrived bytes feed after it as their own keys; when
/// the arrival precedes the deadline (`now < deadline`), the bytes feed into the still-pending
/// sequence. An arrival exactly at the deadline counts as expired: the byte raced a deadline that
/// already elapsed, so it must not join the sequence it lost to. The pending Escape then emits as
/// a standalone key and the byte follows, which is also what a strictly later arrival produces.
/// The driver stays clock-free: the caller supplies both `now` and `deadline`.
#[must_use]
pub(crate) fn arbitrate_escape_arrival(now: Instant, deadline: Instant) -> EscapeArrival {
    if now >= deadline {
        EscapeArrival::AtOrAfterDeadline
    } else {
        EscapeArrival::BeforeDeadline
    }
}

/// UI-owned parser driving with an explicit effective keyboard profile.
///
/// The driver has no readiness loop of its own. Its caller supplies byte chunks and the shared
/// arrival timestamp, calls the vt100 Escape deadline boundary, and calls [`Self::finish`] when
/// stdin closes. [`Self::push_stdin`] records the pending Escape deadline from the caller-owned
/// clock; [`Self::push_at_escape_deadline`] arbitrates a both-ready boundary with the single
/// [`arbitrate_escape_arrival`] comparison instead of a task-scheduling race.
pub struct InputDriver {
    profile: KeyboardProfile,
    parser: Parser,
    pending_escape_since: Option<Instant>,
}

impl InputDriver {
    #[must_use]
    pub fn new(profile: KeyboardProfile) -> Self {
        Self {
            profile,
            parser: Parser::new(),
            pending_escape_since: None,
        }
    }

    /// Returns the arrival timestamp of the currently pending Escape, if any.
    #[must_use]
    pub(crate) const fn pending_escape_since(&self) -> Option<Instant> {
        self.pending_escape_since
    }

    /// Feeds one stdin chunk stamped with its shared arrival timestamp.
    ///
    /// The caller supplies `now` from its own clock (the same clock that owns the Escape
    /// deadline). When the chunk leaves a bare Escape pending, the driver remembers `now` as
    /// the sequence start so the runner can derive the deadline; any other outcome clears it.
    pub(crate) fn push_stdin<F>(&mut self, bytes: &[u8], now: Instant, mut emit: F)
    where
        F: FnMut(ConvertedInput),
    {
        self.parser.push(bytes, |input| emit(convert_input(input)));
        self.pending_escape_since = self.parser.has_pending_escape().then_some(now);
    }

    #[must_use]
    pub fn profile(&self) -> &KeyboardProfile {
        &self.profile
    }

    pub fn push<F>(&mut self, bytes: &[u8], mut emit: F)
    where
        F: FnMut(ConvertedInput),
    {
        self.parser.push(bytes, |input| emit(convert_input(input)));
        if !self.parser.has_pending_escape() {
            self.pending_escape_since = None;
        }
    }

    /// Feeds newly arrived bytes that share their readiness with an armed Escape deadline.
    ///
    /// The caller supplies the shared arrival timestamp `now` and the armed `deadline` from its
    /// own clock. The single [`arbitrate_escape_arrival`] comparison decides: an arrival at or
    /// past the deadline flushes the pending Escape first and feeds the bytes after it, while an
    /// earlier arrival feeds the bytes into the still-pending sequence. Returns the decision, or
    /// `None` when no VT100 Escape is pending and the bytes feed directly.
    pub(crate) fn push_at_escape_deadline<F>(
        &mut self,
        bytes: &[u8],
        now: Instant,
        deadline: Instant,
        mut emit: F,
    ) -> Option<EscapeArrival>
    where
        F: FnMut(ConvertedInput),
    {
        if !matches!(self.profile, KeyboardProfile::Vt100 { .. })
            || !self.parser.has_pending_escape()
        {
            self.parser.push(bytes, |input| emit(convert_input(input)));
            self.pending_escape_since = self.parser.has_pending_escape().then_some(now);
            return None;
        }
        match arbitrate_escape_arrival(now, deadline) {
            EscapeArrival::BeforeDeadline => {
                self.parser.push(bytes, |input| emit(convert_input(input)));
                self.pending_escape_since = self.parser.has_pending_escape().then_some(now);
                Some(EscapeArrival::BeforeDeadline)
            }
            EscapeArrival::AtOrAfterDeadline => {
                self.pending_escape_since = None;
                self.parser
                    .flush_pending_escape(|input| emit(convert_input(input)));
                self.parser.push(bytes, |input| emit(convert_input(input)));
                self.pending_escape_since = self.parser.has_pending_escape().then_some(now);
                Some(EscapeArrival::AtOrAfterDeadline)
            }
        }
    }

    /// Flushes a pending bare Escape only for the explicit vt100 deadline boundary.
    pub fn flush_vt100_escape<F>(&mut self, mut emit: F) -> bool
    where
        F: FnMut(ConvertedInput),
    {
        let flushed = match self.profile {
            KeyboardProfile::Vt100 { .. } => self
                .parser
                .flush_pending_escape(|input| emit(convert_input(input))),
            KeyboardProfile::Kitty(_) => false,
        };
        if flushed {
            self.pending_escape_since = None;
        }
        flushed
    }

    pub fn finish<F>(&mut self, mut emit: F)
    where
        F: FnMut(ConvertedInput),
    {
        self.parser.finish(|input| emit(convert_input(input)));
        self.pending_escape_since = None;
    }
}

/// A pending exact Kitty keyboard-mode confirmation.
pub struct KittyNegotiation {
    expected_flags: u32,
    confirmed: bool,
    pending_input: VecDeque<ConvertedInput>,
}

/// The state transition produced by one input result during negotiation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NegotiationUpdate {
    Waiting,
    Confirmed,
}

/// A runtime Kitty negotiation failure. Each failure requires terminal restoration.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum KittyNegotiationError {
    #[error("Kitty keyboard mode refused: expected flags {expected}, received {received}")]
    Refused { expected: u32, received: u32 },
    #[error("Kitty keyboard mode did not respond before the UI deadline")]
    TimedOut,
    #[error("too many key events arrived before Kitty mode was confirmed")]
    PendingInputOverflow,
}

impl KittyNegotiation {
    #[must_use]
    pub fn new(capabilities: KeyCapabilities) -> Self {
        Self {
            expected_flags: kitty_flags(capabilities),
            confirmed: false,
            pending_input: VecDeque::with_capacity(MAX_PENDING_NEGOTIATION_INPUT),
        }
    }

    #[must_use]
    pub const fn expected_flags(&self) -> u32 {
        self.expected_flags
    }

    #[must_use]
    pub const fn is_confirmed(&self) -> bool {
        self.confirmed
    }

    /// Consumes one parser result in stream order.
    ///
    /// Responses must exactly match the compiled capability flags. Every other input result is
    /// retained until confirmation, so a key that shares a read with the response is not lost.
    ///
    /// # Errors
    ///
    /// Returns [`KittyNegotiationError::Refused`] when the terminal reports Kitty flags that do
    /// not exactly match the compiled capabilities, or
    /// [`KittyNegotiationError::PendingInputOverflow`] when more than
    /// [`MAX_PENDING_NEGOTIATION_INPUT`] key events arrive before confirmation.
    pub fn observe(
        &mut self,
        input: ConvertedInput,
    ) -> Result<NegotiationUpdate, KittyNegotiationError> {
        if let ConvertedInput::ProtocolResponse(ProtocolResponse::KittyKeyboardFlags(flags)) = input
        {
            if flags != self.expected_flags {
                return Err(KittyNegotiationError::Refused {
                    expected: self.expected_flags,
                    received: flags,
                });
            }
            self.confirmed = true;
            return Ok(NegotiationUpdate::Confirmed);
        }
        if self.pending_input.len() == MAX_PENDING_NEGOTIATION_INPUT {
            return Err(KittyNegotiationError::PendingInputOverflow);
        }
        self.pending_input.push_back(input);
        Ok(NegotiationUpdate::Waiting)
    }

    /// Reports a caller-owned readiness deadline without accepting a degraded mode.
    ///
    /// # Errors
    ///
    /// Returns [`KittyNegotiationError::TimedOut`] when the Kitty response has not confirmed the
    /// requested mode before the caller's deadline.
    pub const fn timeout(&self) -> Result<(), KittyNegotiationError> {
        if self.confirmed {
            Ok(())
        } else {
            Err(KittyNegotiationError::TimedOut)
        }
    }

    /// Restores buffered stream-order input ahead of unread parser results after confirmation.
    pub fn prepend_pending_to(&mut self, input: &mut VecDeque<ConvertedInput>) {
        while let Some(pending) = self.pending_input.pop_back() {
            input.push_front(pending);
        }
    }
}

/// Converts a compiled Kitty profile to its required progressive-enhancement flag mask.
#[must_use]
pub const fn kitty_flags(capabilities: KeyCapabilities) -> u32 {
    0b1 | if capabilities.event_types { 0b10 } else { 0 }
        | if capabilities.alternate_keys {
            0b100
        } else {
            0
        }
        | if capabilities.all_keys_as_escape_codes {
            0b1000
        } else {
            0
        }
}

/// Padding around a menu grid after the surface heading has been reserved.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SurfacePadding {
    pub left: u16,
    pub right: u16,
    pub top: u16,
    pub bottom: u16,
}

/// Borrowed content for one terminal render.
#[derive(Clone, Copy)]
pub struct SurfaceFrame<'a> {
    pub title: &'a str,
    pub breadcrumb: &'a RenderedText,
    pub padding: SurfacePadding,
    pub plan: &'a GridPlan,
    pub cells: &'a [RenderedText],
    pub page: usize,
    pub pager: Option<&'a RenderedText>,
    pub status: Option<MenuStatus<'a>>,
}

/// A real raw-mode alternate-screen terminal surface.
///
/// It writes through the supplied terminal writer. Call [`Self::restore`] on orderly exits; `Drop`
/// also attempts restoration for handled error paths.
pub struct TerminalSurface<W: Write> {
    output: W,
    entered_alternate_screen: bool,
    raw_mode: bool,
    kitty_restore_needed: bool,
}

impl<W: Write> TerminalSurface<W> {
    /// Enters raw mode and the alternate screen before rendering input-sensitive UI content.
    ///
    /// # Errors
    ///
    /// Returns the underlying terminal error when raw mode cannot be enabled, or when the
    /// alternate screen, cursor-hide, or flush writes fail.
    pub fn enter(mut output: W) -> io::Result<Self> {
        terminal::enable_raw_mode()?;
        if let Err(error) = output
            .queue(EnterAlternateScreen)
            .and_then(|output| output.queue(Hide))
            .and_then(std::io::Write::flush)
        {
            let _ = terminal::disable_raw_mode();
            return Err(error);
        }
        Ok(Self {
            output,
            entered_alternate_screen: true,
            raw_mode: true,
            kitty_restore_needed: false,
        })
    }

    /// Pushes the exact compiled Kitty mode, then queries it for confirmation.
    ///
    /// The caller must feed stdin bytes through [`InputDriver`] and [`KittyNegotiation::observe`]
    /// before accepting menu input. A timeout, refusal, or disconnect must call [`Self::restore`].
    ///
    /// # Errors
    ///
    /// Returns the underlying terminal error when the Kitty push sequence or its flush fails.
    pub fn begin_kitty_negotiation(
        &mut self,
        capabilities: KeyCapabilities,
    ) -> io::Result<KittyNegotiation> {
        let negotiation = KittyNegotiation::new(capabilities);
        self.kitty_restore_needed = true;
        write!(
            self.output,
            "\x1b[>{}u\x1b[?u",
            negotiation.expected_flags()
        )?;
        self.output.flush()?;
        Ok(negotiation)
    }

    /// Clears and redraws one frame at the supplied terminal dimensions.
    ///
    /// # Errors
    ///
    /// Returns the underlying terminal error when clearing, cursor movement, row writes, or the
    /// final flush fails.
    pub fn render(&mut self, frame: SurfaceFrame<'_>, area: Rect) -> io::Result<()> {
        if area.width == 0 || area.height == 0 {
            return Ok(());
        }
        let mut buffer = Buffer::empty(area);
        buffer.set_stringn(
            area.x,
            area.y,
            frame.title,
            area.width as usize,
            ratatui::style::Style::default(),
        );
        let title_width =
            u16::try_from(UnicodeWidthStr::width(frame.title).min(usize::from(area.width)))
                .unwrap_or(u16::MAX);
        if !frame.breadcrumb.plain.is_empty() && title_width < area.width {
            let breadcrumb_x = area.x.saturating_add(title_width);
            buffer.set_stringn(
                breadcrumb_x,
                area.y,
                ": ",
                area.width.saturating_sub(title_width) as usize,
                ratatui::style::Style::default(),
            );
            write_rendered(
                &mut buffer,
                breadcrumb_x.saturating_add(2),
                area.y,
                area.width.saturating_sub(title_width.saturating_add(2)),
                frame.breadcrumb,
            );
        }
        let body = Rect {
            x: area.x,
            y: area.y.saturating_add(1),
            width: area.width,
            height: area.height.saturating_sub(1),
        };
        let grid_area = Rect {
            x: body.x.saturating_add(frame.padding.left.min(body.width)),
            y: body.y.saturating_add(frame.padding.top.min(body.height)),
            width: body
                .width
                .saturating_sub(frame.padding.left.saturating_add(frame.padding.right)),
            height: body
                .height
                .saturating_sub(frame.padding.top.saturating_add(frame.padding.bottom)),
        };
        MenuGrid {
            plan: frame.plan,
            cells: frame.cells,
            page: frame.page,
            pager: frame.pager,
            status: frame.status,
        }
        .render(grid_area, &mut buffer);

        self.output.queue(Clear(ClearType::All))?;
        for y in area.y..area.bottom() {
            self.output.queue(MoveTo(area.x, y))?;
            let mut line = String::with_capacity(area.width as usize);
            for x in area.x..area.right() {
                line.push_str(buffer[(x, y)].symbol());
            }
            self.output.write_all(line.as_bytes())?;
        }
        self.output.flush()
    }

    /// Restores Kitty mode, raw mode, and the alternate screen.
    ///
    /// # Errors
    ///
    /// Returns the first underlying terminal error when the Kitty reset, alternate-screen exit,
    /// or raw-mode teardown fails.
    pub fn restore(mut self) -> io::Result<()> {
        self.restore_inner()
    }

    fn restore_inner(&mut self) -> io::Result<()> {
        let mut first_error = None;
        if self.kitty_restore_needed {
            self.kitty_restore_needed = false;
            if let Err(error) = self
                .output
                .write_all(b"\x1b[<u")
                .and_then(|()| self.output.flush())
            {
                first_error = Some(error);
            }
        }
        if self.entered_alternate_screen {
            self.entered_alternate_screen = false;
            if let Err(error) = self
                .output
                .queue(Show)
                .and_then(|output| output.queue(LeaveAlternateScreen))
                .and_then(std::io::Write::flush)
            {
                first_error.get_or_insert(error);
            }
        }
        if self.raw_mode {
            self.raw_mode = false;
            if let Err(error) = terminal::disable_raw_mode() {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl<W: Write> Drop for TerminalSurface<W> {
    fn drop(&mut self) {
        let _ = self.restore_inner();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use muxe_core::{KeyIdentity, NamedKey};
    use muxe_terminal_input::ProtocolResponse;

    use super::*;

    #[test]
    fn vt100_escape_arrival_boundaries_are_explicit() {
        let profile = KeyboardProfile::Vt100 {
            escape_timeout: Duration::from_millis(25),
        };
        let start = Instant::now();
        let deadline = start + Duration::from_millis(25);

        // Genuinely early byte: strictly before the deadline, so the pending Escape continues.
        let mut before = InputDriver::new(profile.clone());
        let mut before_events = Vec::new();
        before.push_stdin(b"\x1b", start, |input| before_events.push(input));
        assert!(before_events.is_empty());
        assert_eq!(
            before.push_at_escape_deadline(
                b"a",
                start + Duration::from_millis(10),
                deadline,
                |input| {
                    before_events.push(input);
                }
            ),
            Some(EscapeArrival::BeforeDeadline)
        );
        assert!(matches!(
            before_events.as_slice(),
            [ConvertedInput::Key(event)]
                if event.event.primary == Some(KeyIdentity::Text('a'))
                    && event.event.modifiers.contains(muxe_core::Modifiers::ALT)
        ));

        // Boundary byte: exactly at the deadline counts as expired, so the production rule flushes
        // the pending Escape first and the byte follows as its own key. The old unbiased select
        // could instead feed the byte into the pending sequence and emit Alt-a; asserting the
        // two-key outcome plus the AtOrAfterDeadline decision pins the deterministic rule.
        let mut at = InputDriver::new(profile.clone());
        let mut at_events = Vec::new();
        at.push_stdin(b"\x1b", start, |input| at_events.push(input));
        assert!(at_events.is_empty());
        assert_eq!(
            at.push_at_escape_deadline(b"a", deadline, deadline, |input| at_events.push(input)),
            Some(EscapeArrival::AtOrAfterDeadline)
        );
        assert!(matches!(
            at_events.as_slice(),
            [
                ConvertedInput::Key(escape),
                ConvertedInput::Key(text),
            ] if escape.event.primary == Some(KeyIdentity::Named(NamedKey::Escape))
                && text.event.primary == Some(KeyIdentity::Text('a'))
        ));

        // Late byte: strictly after the deadline resolves identically to the boundary case.
        let mut after = InputDriver::new(profile.clone());
        let mut after_events = Vec::new();
        after.push_stdin(b"\x1b", start, |input| after_events.push(input));
        assert!(after_events.is_empty());
        assert_eq!(
            after.push_at_escape_deadline(
                b"a",
                deadline + Duration::from_millis(1),
                deadline,
                |input| after_events.push(input),
            ),
            Some(EscapeArrival::AtOrAfterDeadline)
        );
        assert_eq!(after_events, at_events);

        // Re-arm: a both-ready chunk that itself ends in a bare Escape re-arms the deadline from
        // the shared arrival timestamp, so the stranded Escape still flushes on its own timeout
        // instead of lingering and swallowing the next byte as Alt.
        let mut rearmed = InputDriver::new(profile);
        let mut rearmed_events = Vec::new();
        rearmed.push_stdin(b"\x1b", start, |input| rearmed_events.push(input));
        assert_eq!(
            rearmed.push_at_escape_deadline(b"b\x1b", deadline, deadline, |input| {
                rearmed_events.push(input);
            }),
            Some(EscapeArrival::AtOrAfterDeadline)
        );
        assert!(matches!(
            rearmed_events.as_slice(),
            [
                ConvertedInput::Key(escape),
                ConvertedInput::Key(text),
            ] if escape.event.primary == Some(KeyIdentity::Named(NamedKey::Escape))
                && text.event.primary == Some(KeyIdentity::Text('b'))
        ));
        let rearmed_deadline = rearmed
            .pending_escape_since()
            .expect("trailing Escape re-arms its deadline")
            + Duration::from_millis(25);
        assert_eq!(rearmed_deadline, deadline + Duration::from_millis(25));
        assert!(rearmed.flush_vt100_escape(|input| rearmed_events.push(input)));
        assert_eq!(rearmed_events.len(), 3);
    }
    #[test]
    fn kitty_negotiation_requires_exact_flags_and_preserves_interleaved_keys() {
        let capabilities = KeyCapabilities {
            event_types: true,
            alternate_keys: true,
            all_keys_as_escape_codes: false,
        };
        let mut driver = InputDriver::new(KeyboardProfile::Kitty(capabilities));
        let mut pending = Vec::new();
        driver.push(b"a", |input| pending.push(input));
        let mut negotiation = KittyNegotiation::new(capabilities);
        assert_eq!(negotiation.expected_flags(), 0b111);
        assert_eq!(
            negotiation.observe(pending.pop().expect("key event")),
            Ok(NegotiationUpdate::Waiting)
        );
        assert_eq!(
            negotiation.observe(ConvertedInput::ProtocolResponse(
                ProtocolResponse::KittyKeyboardFlags(0b111)
            )),
            Ok(NegotiationUpdate::Confirmed)
        );
        assert!(negotiation.is_confirmed());
        let mut preserved = VecDeque::new();
        negotiation.prepend_pending_to(&mut preserved);
        assert!(matches!(
            preserved.pop_front(),
            Some(ConvertedInput::Key(_))
        ));
    }

    #[test]
    fn kitty_negotiation_rejects_a_silent_downgrade_or_timeout() {
        let mut negotiation = KittyNegotiation::new(KeyCapabilities {
            event_types: true,
            alternate_keys: false,
            all_keys_as_escape_codes: false,
        });
        assert_eq!(
            negotiation.observe(ConvertedInput::ProtocolResponse(
                ProtocolResponse::KittyKeyboardFlags(0b1)
            )),
            Err(KittyNegotiationError::Refused {
                expected: 0b11,
                received: 0b1,
            })
        );
        assert_eq!(negotiation.timeout(), Err(KittyNegotiationError::TimedOut));
    }
}
