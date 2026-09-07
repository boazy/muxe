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
use unicode_width::UnicodeWidthStr;

use crate::{
    ConvertedInput, GridPlan, MenuGrid, MenuStatus, RenderedText, convert_input,
    widget::write_rendered,
};

/// Maximum parsed input events held while a Kitty response is pending.
pub const MAX_PENDING_NEGOTIATION_INPUT: usize = 64;

/// UI-owned parser driving with an explicit effective keyboard profile.
///
/// The driver has no readiness loop or clock. Its caller supplies byte chunks, calls the vt100
/// Escape deadline boundary, and calls [`Self::finish`] when stdin closes.
pub struct InputDriver {
    profile: KeyboardProfile,
    parser: Parser,
}

impl InputDriver {
    #[must_use]
    pub fn new(profile: KeyboardProfile) -> Self {
        Self {
            profile,
            parser: Parser::new(),
        }
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
    }

    /// Flushes a pending bare Escape only for the explicit vt100 deadline boundary.
    pub fn flush_vt100_escape<F>(&mut self, mut emit: F) -> bool
    where
        F: FnMut(ConvertedInput),
    {
        match self.profile {
            KeyboardProfile::Vt100 { .. } => self
                .parser
                .flush_pending_escape(|input| emit(convert_input(input))),
            KeyboardProfile::Kitty(_) => false,
        }
    }

    pub fn finish<F>(&mut self, mut emit: F)
    where
        F: FnMut(ConvertedInput),
    {
        self.parser.finish(|input| emit(convert_input(input)));
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
        let mut before = InputDriver::new(profile.clone());
        let mut before_events = Vec::new();
        before.push(b"\x1ba", |input| before_events.push(input));
        assert!(matches!(
            before_events.as_slice(),
            [ConvertedInput::Key(event)]
                if event.event.primary == Some(KeyIdentity::Text('a'))
                    && event.event.modifiers.contains(muxe_core::Modifiers::ALT)
        ));

        for _boundary in ["at", "after"] {
            let mut driver = InputDriver::new(profile.clone());
            let mut events = Vec::new();
            driver.push(b"\x1b", |input| events.push(input));
            assert!(driver.flush_vt100_escape(|input| events.push(input)));
            driver.push(b"a", |input| events.push(input));
            assert!(matches!(
                events.as_slice(),
                [
                    ConvertedInput::Key(escape),
                    ConvertedInput::Key(text),
                ] if escape.event.primary == Some(KeyIdentity::Named(NamedKey::Escape))
                    && text.event.primary == Some(KeyIdentity::Text('a'))
            ));
        }
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
