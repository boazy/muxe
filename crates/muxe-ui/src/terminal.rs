use std::{
    collections::VecDeque,
    io::{self, Write},
};

use crossterm::{
    QueueableCommand,
    cursor::{Hide, Show},
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use muxe_core::{KeyCapabilities, KeyboardProfile};
use muxe_terminal_input::{Parser, ProtocolResponse};
use ratatui::{
    Terminal, TerminalOptions, Viewport,
    backend::{Backend, ClearType as RatatuiClearType, CrosstermBackend, WindowSize},
    buffer::{Cell, CellWidth},
    layout::{Position, Rect, Size},
    widgets::Widget,
};
use thiserror::Error;
use tokio::time::Instant;
use unicode_width::UnicodeWidthStr;

use crate::{ConvertedInput, GridPlan, MenuGrid, MenuStatus, RenderedText, convert_input};

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
/// Rendering goes through a stored ratatui [`Terminal`] over [`CrosstermBackend`]: each
/// [`Self::render`] call draws the title row, breadcrumb spans, and [`MenuGrid`] as ratatui
/// widgets into the frame for the caller-supplied `area`, and ratatui diffs the frame against
/// the previous one to emit cursor moves, SGR style transitions, and wide-grapheme continuation
/// handling. Call [`Self::restore`] on orderly exits; `Drop` also attempts restoration for
/// handled error paths.
pub struct TerminalSurface<W: Write> {
    output: Option<W>,
    terminal: Option<Terminal<AreaBackend<W>>>,
    entered_alternate_screen: bool,
    raw_mode: bool,
    kitty_restore_needed: bool,
}

/// A [`Backend`] adapter over [`CrosstermBackend`] that reports the caller-supplied render area.
///
/// `CrosstermBackend::size` queries the live terminal, which is wrong on both render paths:
/// real runs render into the caller-supplied `area`, and tests render into an in-memory writer
/// with no terminal at all. The wrapper owns the writer, answers `size` from the cached area,
/// and answers cursor reads from the last position ratatui produced (querying the device would
/// emit `ESC[6n` and block on stdin). Every byte-emitting operation delegates to a transient
/// [`CrosstermBackend`] borrowing the owned writer, so cell serialization — cursor moves, SGR
/// style transitions, wide-grapheme continuation skipping — stays ratatui's supported path.
/// [`Terminal`] always positions the cursor explicitly, so the cached read never feeds stale
/// coordinates back into a render pass.
#[derive(Debug)]
struct AreaBackend<W: Write> {
    writer: W,
    area: Rect,
    cursor: Position,
}

impl<W: Write> AreaBackend<W> {
    fn new(writer: W, area: Rect) -> Self {
        Self {
            writer,
            area,
            cursor: Position::ORIGIN,
        }
    }

    const fn area(&self) -> Rect {
        self.area
    }

    fn set_area(&mut self, area: Rect) {
        self.area = area;
    }

    fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    fn crossterm(&mut self) -> CrosstermBackend<&mut W> {
        CrosstermBackend::new(&mut self.writer)
    }
}

impl<W: Write> Backend for AreaBackend<W> {
    type Error = io::Error;

    fn draw<'a, I>(&mut self, content: I) -> io::Result<()>
    where
        I: Iterator<Item = (u16, u16, &'a Cell)>,
    {
        let mut last: Option<Position> = None;
        let spy = content.inspect(|(x, y, cell)| {
            last = Some(Position::new(x.saturating_add(cell.cell_width()), *y));
        });
        self.crossterm().draw(spy)?;
        if let Some(position) = last {
            self.cursor = position;
        }
        Ok(())
    }

    fn hide_cursor(&mut self) -> io::Result<()> {
        self.crossterm().hide_cursor()
    }

    fn show_cursor(&mut self) -> io::Result<()> {
        self.crossterm().show_cursor()
    }

    fn get_cursor_position(&mut self) -> io::Result<Position> {
        Ok(self.cursor)
    }

    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        let position = position.into();
        self.cursor = position;
        self.crossterm().set_cursor_position(position)
    }

    fn clear(&mut self) -> io::Result<()> {
        self.crossterm().clear()
    }

    fn clear_region(&mut self, clear_type: RatatuiClearType) -> io::Result<()> {
        self.crossterm().clear_region(clear_type)
    }

    fn size(&self) -> io::Result<Size> {
        Ok(self.area.as_size())
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        Ok(WindowSize {
            columns_rows: self.area.as_size(),
            pixels: Size::new(0, 0),
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        Write::flush(&mut self.writer)
    }
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
            output: Some(output),
            terminal: None,
            entered_alternate_screen: true,
            raw_mode: true,
            kitty_restore_needed: false,
        })
    }

    /// Builds the writer-only surface used by tests: no raw mode, no alternate screen, no
    /// cursor commands — only the stored ratatui terminal over the supplied writer.
    #[cfg(test)]
    fn for_test(output: W, area: Rect) -> io::Result<Self> {
        let backend = AreaBackend::new(output, area);
        let terminal = Terminal::with_options(
            backend,
            TerminalOptions {
                viewport: Viewport::Fixed(area),
            },
        )?;
        Ok(Self {
            output: None,
            terminal: Some(terminal),
            entered_alternate_screen: false,
            raw_mode: false,
            kitty_restore_needed: false,
        })
    }

    /// Returns the writer currently owned by the surface.
    ///
    /// The writer lives inside the stored ratatui backend once the first frame renders, so
    /// callers that need raw access (Kitty negotiation, restoration) go through this helper.
    fn writer_mut(&mut self) -> &mut W {
        match &mut self.terminal {
            Some(terminal) => terminal.backend_mut().writer_mut(),
            None => self
                .output
                .as_mut()
                .expect("writer is stored on the surface before the first render"),
        }
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
        let writer = self.writer_mut();
        write!(writer, "\x1b[>{}u\x1b[?u", negotiation.expected_flags())?;
        writer.flush()?;
        Ok(negotiation)
    }

    /// Draws one frame at the supplied terminal dimensions through the stored ratatui terminal.
    ///
    /// The title row, breadcrumb spans, and [`MenuGrid`] render as ratatui widgets into the
    /// frame for `area`; ratatui diffs against the previous frame to emit cursor moves, SGR
    /// style transitions, and wide-grapheme continuation handling. A changed area resizes the
    /// terminal first, which clears through ratatui and forces a full redraw within the new
    /// bounds. The frame hides the cursor, matching the surface's cursor-hide lifecycle.
    ///
    /// # Errors
    ///
    /// Returns the underlying terminal error when the terminal cannot be created or resized,
    /// or when the frame draw fails.
    ///
    /// # Panics
    ///
    /// Panics if the stored terminal is missing after it is (re)created above, which cannot
    /// happen without an intervening panic during terminal construction.
    pub fn render(&mut self, frame: SurfaceFrame<'_>, area: Rect) -> io::Result<()> {
        if area.width == 0 || area.height == 0 {
            return Ok(());
        }
        if let Some(mut terminal) = self.terminal.take() {
            let result = if terminal.backend().area() == area {
                Ok(())
            } else {
                terminal.backend_mut().set_area(area);
                terminal.resize(area).and_then(|()| terminal.hide_cursor())
            };
            self.terminal = Some(terminal);
            result?;
        } else {
            let writer = self
                .output
                .take()
                .expect("writer is stored on the surface before the first render");
            // Fixed viewports use only the supplied area; `with_options` performs no backend I/O.
            let fresh = Terminal::with_options(
                AreaBackend::new(writer, area),
                TerminalOptions {
                    viewport: Viewport::Fixed(area),
                },
            )?;
            self.terminal = Some(fresh);
            self.terminal
                .as_mut()
                .expect("stored terminal above")
                .hide_cursor()?;
        }
        let terminal = self.terminal.as_mut().expect("stored terminal above");
        terminal.draw(|frame_context| {
            render_surface(frame_context, frame, area);
        })?;
        Ok(())
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
                .writer_mut()
                .write_all(b"\x1b[<u")
                .and_then(|()| self.writer_mut().flush())
            {
                first_error = Some(error);
            }
        }
        if self.entered_alternate_screen {
            self.entered_alternate_screen = false;
            if let Err(error) = self
                .writer_mut()
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

/// Renders the title row, breadcrumb spans, and [`MenuGrid`] body into the current frame.
///
/// The title keeps its historical default styling (`SurfaceFrame.title` is plain text; a
/// configured title style is a separate finding), while the breadcrumb already carries span
/// styles through `write_rendered`, and the grid cells carry theirs through [`MenuGrid`].
/// Geometry matches the previous hand-written path so callers and layout keep the same areas.
fn render_surface(frame_context: &mut ratatui::Frame<'_>, frame: SurfaceFrame<'_>, area: Rect) {
    let buffer = frame_context.buffer_mut();
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
        crate::widget::write_rendered(
            buffer,
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
    .render(grid_area, buffer);
}

impl<W: Write> Drop for TerminalSurface<W> {
    fn drop(&mut self) {
        let _ = self.restore_inner();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
        sync::Mutex,
        time::Duration,
    };

    use muxe_core::{KeyIdentity, NamedKey};
    use muxe_terminal_input::ProtocolResponse;

    use super::*;

    static BYTE_CAPTURE: Mutex<()> = Mutex::new(());

    struct AnsiColorGateRestore(bool);

    impl Drop for AnsiColorGateRestore {
        fn drop(&mut self) {
            crossterm::style::Colored::set_ansi_color_disabled(self.0);
        }
    }

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

    fn styled_cell(text: &str, style: ratatui::style::Style) -> RenderedText {
        RenderedText {
            plain: text.into(),
            spans: vec![crate::RenderedSpan {
                text: text.into(),
                style,
            }],
        }
    }

    fn empty_breadcrumb() -> RenderedText {
        RenderedText {
            plain: String::new(),
            spans: Vec::new(),
        }
    }

    #[derive(Debug)]
    struct ToggleFailWriter {
        bytes: Rc<RefCell<Vec<u8>>>,
        failing: Rc<Cell<bool>>,
    }

    impl ToggleFailWriter {
        fn new(bytes: Rc<RefCell<Vec<u8>>>, failing: Rc<Cell<bool>>) -> Self {
            Self { bytes, failing }
        }
    }

    impl std::io::Write for ToggleFailWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.failing.get() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "test writer failure",
                ));
            }
            self.bytes.borrow_mut().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn single_cell_plan(width: u16) -> GridPlan {
        GridPlan {
            columns: 1,
            row_gap: 0,
            rows_per_page: 1,
            page_count: 1,
            has_pager: false,
            pages: vec![crate::GridPage {
                columns: vec![crate::GridColumn { offset: 0, width }],
            }],
            slots: vec![crate::GridSlot {
                source_index: 0,
                page: 0,
                column: 0,
                row: 0,
            }],
        }
    }

    /// Strips CSI/CS/OSC escape sequences, leaving only printable row content.
    fn strip_escapes(bytes: &[u8]) -> String {
        let text = String::from_utf8_lossy(bytes);
        let mut visible = String::with_capacity(text.len());
        let mut chars = text.chars().peekable();
        while let Some(character) = chars.next() {
            if character == '\x1b' {
                match chars.peek() {
                    Some('[') => {
                        chars.next();
                        for control in chars.by_ref() {
                            if control.is_ascii_alphabetic() {
                                break;
                            }
                        }
                    }
                    Some(']') => {
                        chars.next();
                        for control in chars.by_ref() {
                            if control == '\x07' {
                                break;
                            }
                        }
                    }
                    Some('(' | ')') => {
                        chars.next();
                        chars.next();
                    }
                    _ => {}
                }
            } else if !character.is_control() {
                visible.push(character);
            }
        }
        visible
    }

    #[test]
    fn render_emits_sgr_for_styled_cells_and_leaves_plain_cells_bare() {
        use ratatui::style::{Color as RatatuiColor, Modifier, Style as RatatuiStyle};

        let _byte_capture = BYTE_CAPTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous_color_gate = crossterm::style::Colored::ansi_color_disabled_memoized();
        crossterm::style::Colored::set_ansi_color_disabled(false);
        let _restore_color_gate = AnsiColorGateRestore(previous_color_gate);

        // The harness runs with `NO_COLOR=1`, which makes crossterm's memoized ANSI gate
        // suppress color output process-wide; force it off first so this render emits SGR.
        let area = Rect::new(0, 0, 8, 2);
        let output = Vec::new();
        let mut surface = TerminalSurface::for_test(output, area).expect("test surface");
        let breadcrumb = empty_breadcrumb();
        let plan = GridPlan {
            columns: 2,
            row_gap: 0,
            rows_per_page: 1,
            page_count: 1,
            has_pager: false,
            pages: vec![crate::GridPage {
                columns: vec![
                    crate::GridColumn {
                        offset: 0,
                        width: 4,
                    },
                    crate::GridColumn {
                        offset: 4,
                        width: 4,
                    },
                ],
            }],
            slots: vec![
                crate::GridSlot {
                    source_index: 0,
                    page: 0,
                    column: 0,
                    row: 0,
                },
                crate::GridSlot {
                    source_index: 1,
                    page: 0,
                    column: 1,
                    row: 0,
                },
            ],
        };
        let styled = styled_cell(
            "ab",
            RatatuiStyle::default()
                .fg(RatatuiColor::Red)
                .add_modifier(Modifier::BOLD),
        );
        let unstyled = styled_cell("cd", RatatuiStyle::default());
        let frame = SurfaceFrame {
            title: "",
            breadcrumb: &breadcrumb,
            padding: SurfacePadding::default(),
            plan: &plan,
            cells: &[styled, unstyled],
            page: 0,
            pager: None,
            status: None,
        };
        surface.render(frame, area).expect("render succeeds");
        let bytes = surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clone();
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("\x1b[1m"), "bold SGR missing in {text:?}");
        assert!(
            text.contains("\x1b[38;5;1"),
            "red foreground SGR missing in {text:?}"
        );
        let styled_pos = text.find('a').expect("styled cell emitted");
        let plain_pos = text.find("cd").expect("plain cell emitted");
        let prefix = &text[..styled_pos];
        assert!(
            prefix.contains("\x1b[1m") && prefix.contains("\x1b[38;5;1"),
            "style sequences must precede the styled cell, got {text:?}"
        );
        let plain_span = &text[plain_pos..plain_pos + 2];
        assert_eq!(
            plain_span, "cd",
            "plain cell must follow its bytes directly"
        );
        let reset_before_plain = &text[styled_pos..plain_pos];
        assert!(
            reset_before_plain.contains("\x1b[22m") || reset_before_plain.contains("\x1b[0m"),
            "style reset missing between styled and plain cells in {text:?}"
        );
    }

    #[test]
    fn render_emits_a_wide_grapheme_once_with_no_continuation_column() {
        use unicode_width::UnicodeWidthStr;

        let _byte_capture = BYTE_CAPTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let area = Rect::new(0, 0, 4, 2);
        let output = Vec::new();
        let mut surface = TerminalSurface::for_test(output, area).expect("test surface");
        let breadcrumb = empty_breadcrumb();
        let plan = GridPlan {
            columns: 1,
            row_gap: 0,
            rows_per_page: 1,
            page_count: 1,
            has_pager: false,
            pages: vec![crate::GridPage {
                columns: vec![crate::GridColumn {
                    offset: 0,
                    width: 4,
                }],
            }],
            slots: vec![crate::GridSlot {
                source_index: 0,
                page: 0,
                column: 0,
                row: 0,
            }],
        };
        let wide = styled_cell("あa", ratatui::style::Style::default());
        let frame = SurfaceFrame {
            title: "",
            breadcrumb: &breadcrumb,
            padding: SurfacePadding::default(),
            plan: &plan,
            cells: std::slice::from_ref(&wide),
            page: 0,
            pager: None,
            status: None,
        };
        surface.render(frame, area).expect("render succeeds");
        let bytes = surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clone();
        let spaces = bytes.iter().fold(0_usize, |count, byte| {
            count.saturating_add(usize::from(*byte == b' '))
        });
        assert_eq!(
            spaces, 0,
            "wide continuation spaces must not be emitted: {bytes:?}"
        );
        let visible = strip_escapes(&bytes);
        assert!(
            visible.contains('あ'),
            "wide grapheme missing from {visible:?}"
        );
        assert_eq!(
            visible.matches('あ').count(),
            1,
            "wide grapheme must be emitted exactly once in {visible:?}"
        );
        // Diffing omits trailing blank cells, so the visible payload is exactly the wide
        // grapheme plus its follower: display width 3 in a width-4 row. The old serializer
        // emitted the reset continuation cell as an extra space, which the zero-space
        // assertion above and this width check together forbid.
        assert_eq!(
            UnicodeWidthStr::width(visible.as_str()),
            UnicodeWidthStr::width("あa"),
            "wide row must carry no continuation column in {visible:?}"
        );
    }

    #[test]
    fn render_fills_an_exact_width_row_with_wide_graphemes_without_continuation_columns() {
        use unicode_width::UnicodeWidthStr;

        let _byte_capture = BYTE_CAPTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let area = Rect::new(0, 0, 4, 2);
        let output = Vec::new();
        let mut surface = TerminalSurface::for_test(output, area).expect("test surface");
        let breadcrumb = empty_breadcrumb();
        let plan = single_cell_plan(area.width);
        let exact = styled_cell("界界", ratatui::style::Style::default());
        surface
            .render(
                SurfaceFrame {
                    title: "",
                    breadcrumb: &breadcrumb,
                    padding: SurfacePadding::default(),
                    plan: &plan,
                    cells: std::slice::from_ref(&exact),
                    page: 0,
                    pager: None,
                    status: None,
                },
                area,
            )
            .expect("exact-width render succeeds");
        let exact_bytes = surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clone();
        let exact_visible = strip_escapes(&exact_bytes);
        assert_eq!(
            exact_visible, "界界",
            "exact-width row must contain only its wide graphemes: {exact_visible:?}"
        );
        assert_eq!(
            UnicodeWidthStr::width(exact_visible.as_str()),
            area.width as usize,
            "exact-width row must have the requested visible width"
        );
        assert!(
            !exact_visible.contains(' '),
            "wide grapheme continuation columns must not become blanks: {exact_bytes:?}"
        );

        let narrow_area = Rect::new(0, 0, 3, 2);
        let narrow_output = Vec::new();
        let mut narrow_surface =
            TerminalSurface::for_test(narrow_output, narrow_area).expect("test surface");
        let narrow_plan = single_cell_plan(narrow_area.width);
        let clipped = styled_cell("界a界", ratatui::style::Style::default());
        narrow_surface
            .render(
                SurfaceFrame {
                    title: "",
                    breadcrumb: &breadcrumb,
                    padding: SurfacePadding::default(),
                    plan: &narrow_plan,
                    cells: std::slice::from_ref(&clipped),
                    page: 0,
                    pager: None,
                    status: None,
                },
                narrow_area,
            )
            .expect("clipped render succeeds");
        let narrow_bytes = narrow_surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clone();
        let narrow_visible = strip_escapes(&narrow_bytes);
        assert_eq!(
            narrow_visible, "界a",
            "the final wide grapheme must be clipped at the row edge: {narrow_visible:?}"
        );
        assert_eq!(
            UnicodeWidthStr::width(narrow_visible.as_str()),
            narrow_area.width as usize,
            "clipped row must have the requested visible width"
        );
        assert!(
            !narrow_visible.contains(' '),
            "wide grapheme continuation columns must not become blanks: {narrow_bytes:?}"
        );
    }

    #[test]
    fn second_render_moves_the_cursor_only_as_needed_and_resize_redraws_fully() {
        let _byte_capture = BYTE_CAPTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let area = Rect::new(0, 0, 6, 2);
        let output = Vec::new();
        let mut surface = TerminalSurface::for_test(output, area).expect("test surface");
        let breadcrumb = empty_breadcrumb();
        let plan = GridPlan {
            columns: 1,
            row_gap: 0,
            rows_per_page: 1,
            page_count: 1,
            has_pager: false,
            pages: vec![crate::GridPage {
                columns: vec![crate::GridColumn {
                    offset: 0,
                    width: 6,
                }],
            }],
            slots: vec![crate::GridSlot {
                source_index: 0,
                page: 0,
                column: 0,
                row: 0,
            }],
        };
        let first = styled_cell("abcdef", ratatui::style::Style::default());
        surface
            .render(
                SurfaceFrame {
                    title: "",
                    breadcrumb: &breadcrumb,
                    padding: SurfacePadding::default(),
                    plan: &plan,
                    cells: std::slice::from_ref(&first),
                    page: 0,
                    pager: None,
                    status: None,
                },
                area,
            )
            .expect("first render succeeds");
        surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clear();
        let second = styled_cell("abcXef", ratatui::style::Style::default());
        surface
            .render(
                SurfaceFrame {
                    title: "",
                    breadcrumb: &breadcrumb,
                    padding: SurfacePadding::default(),
                    plan: &plan,
                    cells: std::slice::from_ref(&second),
                    page: 0,
                    pager: None,
                    status: None,
                },
                area,
            )
            .expect("second render succeeds");
        let bytes = surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clone();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            text.contains('X'),
            "changed cell must be emitted in {text:?}"
        );
        assert_eq!(
            text.matches("\x1b[2J").count()
                + text.matches("\x1b[J").count()
                + text.matches("\x1b[0J").count(),
            0,
            "steady-state redraw must not clear the screen in {text:?}"
        );
        let escapes = text.matches("\x1b[").count();
        assert_eq!(
            escapes, 6,
            "one-cell change emits exactly the cursor move plus reset trailer, got {text:?}"
        );
        assert!(
            text.ends_with("\x1b[0m\x1b[?25l"),
            "frame must end in the documented hidden-cursor position in {text:?}"
        );
    }

    #[test]
    fn resize_forces_a_full_redraw_through_ratatui() {
        let _byte_capture = BYTE_CAPTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let area = Rect::new(0, 0, 6, 2);
        let output = Vec::new();
        let mut surface = TerminalSurface::for_test(output, area).expect("test surface");
        let breadcrumb = empty_breadcrumb();
        let plan = GridPlan {
            columns: 1,
            row_gap: 0,
            rows_per_page: 1,
            page_count: 1,
            has_pager: false,
            pages: vec![crate::GridPage {
                columns: vec![crate::GridColumn {
                    offset: 0,
                    width: 6,
                }],
            }],
            slots: vec![crate::GridSlot {
                source_index: 0,
                page: 0,
                column: 0,
                row: 0,
            }],
        };
        let first = styled_cell("abcdef", ratatui::style::Style::default());
        surface
            .render(
                SurfaceFrame {
                    title: "",
                    breadcrumb: &breadcrumb,
                    padding: SurfacePadding::default(),
                    plan: &plan,
                    cells: std::slice::from_ref(&first),
                    page: 0,
                    pager: None,
                    status: None,
                },
                area,
            )
            .expect("first render succeeds");
        surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clear();
        let grown = Rect::new(0, 0, 8, 3);
        let third = styled_cell("abcXefgh", ratatui::style::Style::default());
        surface
            .render(
                SurfaceFrame {
                    title: "",
                    breadcrumb: &breadcrumb,
                    padding: SurfacePadding::default(),
                    plan: &plan,
                    cells: std::slice::from_ref(&third),
                    page: 0,
                    pager: None,
                    status: None,
                },
                grown,
            )
            .expect("resize render succeeds");
        let resized = surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clone();
        let resized_text = String::from_utf8_lossy(&resized);
        assert!(
            resized_text.contains("abcXef"),
            "resized frame must draw the new area in {resized_text:?}"
        );
        assert!(
            resized_text.contains("\x1b[1;1H\x1b[J"),
            "resize must force a full redraw through ratatui in {resized_text:?}"
        );
    }

    #[test]
    fn a_failed_render_write_leaves_the_surface_usable_and_restorable() {
        let _byte_capture = BYTE_CAPTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let area = Rect::new(0, 0, 4, 2);
        let bytes = Rc::new(RefCell::new(Vec::new()));
        let failing = Rc::new(Cell::new(false));
        let writer = ToggleFailWriter::new(Rc::clone(&bytes), Rc::clone(&failing));
        let mut surface = TerminalSurface::for_test(writer, area).expect("test surface");
        surface.kitty_restore_needed = true;
        surface.entered_alternate_screen = true;
        let breadcrumb = empty_breadcrumb();
        let plan = single_cell_plan(area.width);
        let first = styled_cell("abcd", ratatui::style::Style::default());
        surface
            .render(
                SurfaceFrame {
                    title: "",
                    breadcrumb: &breadcrumb,
                    padding: SurfacePadding::default(),
                    plan: &plan,
                    cells: std::slice::from_ref(&first),
                    page: 0,
                    pager: None,
                    status: None,
                },
                area,
            )
            .expect("first render succeeds");

        failing.set(true);
        let grown = Rect::new(0, 0, 6, 3);
        let grown_plan = single_cell_plan(grown.width);
        let changed = styled_cell("abcdef", ratatui::style::Style::default());
        let error = surface.render(
            SurfaceFrame {
                title: "",
                breadcrumb: &breadcrumb,
                padding: SurfacePadding::default(),
                plan: &grown_plan,
                cells: std::slice::from_ref(&changed),
                page: 0,
                pager: None,
                status: None,
            },
            grown,
        );
        assert!(error.is_err(), "the resize write must fail");

        failing.set(false);
        surface
            .restore()
            .expect("restore succeeds after render failure");
        let emitted = bytes.borrow();
        let restore = b"\x1b[<u\x1b[?25h\x1b[?1049l";
        assert!(
            emitted
                .windows(restore.len())
                .any(|window| window == restore),
            "restore must emit Kitty reset, show-cursor, and leave-alternate-screen: {emitted:?}"
        );
    }

    #[test]
    fn first_render_write_failure_leaves_fresh_surface_usable_and_restorable() {
        let _byte_capture = BYTE_CAPTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let area = Rect::new(0, 0, 4, 2);
        let bytes = Rc::new(RefCell::new(Vec::new()));
        let failing = Rc::new(Cell::new(true));
        let writer = ToggleFailWriter::new(Rc::clone(&bytes), Rc::clone(&failing));
        let mut surface = TerminalSurface {
            output: Some(writer),
            terminal: None,
            entered_alternate_screen: true,
            raw_mode: false,
            kitty_restore_needed: true,
        };
        let breadcrumb = empty_breadcrumb();
        let plan = single_cell_plan(area.width);
        let first = styled_cell("abcd", ratatui::style::Style::default());
        let error = surface.render(
            SurfaceFrame {
                title: "",
                breadcrumb: &breadcrumb,
                padding: SurfacePadding::default(),
                plan: &plan,
                cells: std::slice::from_ref(&first),
                page: 0,
                pager: None,
                status: None,
            },
            area,
        );
        assert!(error.is_err(), "fresh render's cursor-hide write must fail");
        assert!(
            surface.terminal.is_some(),
            "fresh terminal must remain stored after cursor-hide failure"
        );

        failing.set(false);
        surface
            .restore_inner()
            .expect("restore succeeds after first render failure");
        let emitted = bytes.borrow();
        let restore = b"\x1b[<u\x1b[?25h\x1b[?1049l";
        assert!(
            emitted
                .windows(restore.len())
                .any(|window| window == restore),
            "restore must emit Kitty reset, show-cursor, and leave-alternate-screen: {emitted:?}"
        );
    }

    #[test]
    fn shrink_never_emits_cells_outside_the_new_area() {
        let _byte_capture = BYTE_CAPTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let wide_area = Rect::new(0, 0, 8, 3);
        let output = Vec::new();
        let mut surface = TerminalSurface::for_test(output, wide_area).expect("test surface");
        let breadcrumb = empty_breadcrumb();
        let plan = GridPlan {
            columns: 1,
            row_gap: 0,
            rows_per_page: 1,
            page_count: 1,
            has_pager: false,
            pages: vec![crate::GridPage {
                columns: vec![crate::GridColumn {
                    offset: 0,
                    width: 8,
                }],
            }],
            slots: vec![crate::GridSlot {
                source_index: 0,
                page: 0,
                column: 0,
                row: 0,
            }],
        };
        let full = styled_cell("abcdefgh", ratatui::style::Style::default());
        surface
            .render(
                SurfaceFrame {
                    title: "",
                    breadcrumb: &breadcrumb,
                    padding: SurfacePadding::default(),
                    plan: &plan,
                    cells: std::slice::from_ref(&full),
                    page: 0,
                    pager: None,
                    status: None,
                },
                wide_area,
            )
            .expect("wide render succeeds");
        surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clear();
        let narrow_area = Rect::new(0, 0, 4, 2);
        let narrow = styled_cell("abcd", ratatui::style::Style::default());
        surface
            .render(
                SurfaceFrame {
                    title: "",
                    breadcrumb: &breadcrumb,
                    padding: SurfacePadding::default(),
                    plan: &plan,
                    cells: std::slice::from_ref(&narrow),
                    page: 0,
                    pager: None,
                    status: None,
                },
                narrow_area,
            )
            .expect("shrink render succeeds");
        let bytes = surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clone();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            !text.contains("efgh"),
            "shrink must not emit columns outside the new area in {text:?}"
        );
        let visible = strip_escapes(&bytes);
        assert!(
            !visible.contains('e'),
            "shrunken frame must not show leftover wide columns in {visible:?}"
        );
    }

    #[test]
    fn restore_emits_show_cursor_leave_alternate_and_kitty_reset() {
        let _byte_capture = BYTE_CAPTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let area = Rect::new(0, 0, 4, 2);
        let output = Vec::new();
        let mut surface = TerminalSurface::for_test(output, area).expect("test surface");
        surface.kitty_restore_needed = true;
        surface.entered_alternate_screen = true;
        surface.restore_inner().expect("restore succeeds");
        let bytes = surface
            .terminal
            .as_mut()
            .expect("terminal stored")
            .backend_mut()
            .writer_mut()
            .clone();
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("\x1b[<u"), "Kitty reset missing in {text:?}");
        assert!(
            text.contains("\x1b[?25h"),
            "show-cursor missing in {text:?}"
        );
        assert!(
            text.contains("\x1b[?1049l"),
            "leave-alternate-screen missing in {text:?}"
        );
        assert!(
            text.find("\x1b[<u").expect("kitty reset emitted")
                < text.find("\x1b[?1049l").expect("alternate exit emitted"),
            "Kitty reset must precede the alternate-screen exit in {text:?}"
        );
    }
}
