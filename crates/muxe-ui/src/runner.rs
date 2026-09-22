use std::{
    collections::VecDeque,
    fs::OpenOptions,
    io,
    os::fd::{AsFd, OwnedFd},
    time::Duration,
};

use async_trait::async_trait;
use crossterm::terminal;
use muxe_core::KeyboardProfile;
use muxe_protocol::{
    BindingId, BrokerEvent, BrokerResponse, InvocationDisposition as WireInvocationDisposition,
    MenuControl, ProtocolDiagnostic,
};
use nix::{
    fcntl::{FcntlArg, OFlag, fcntl},
    unistd::{read as read_fd, ttyname},
};
use ratatui::layout::Rect;
use thiserror::Error;
use tokio::{
    io::unix::AsyncFd,
    signal::unix::{Signal, SignalKind, signal},
    time::Instant,
};

use crate::{
    ConvertedInput, InputDriver, InvocationDisposition, KittyNegotiation, KittyNegotiationError,
    TerminalSurface, UiCommand, UiError, UiRuntime,
};

/// The bounded wait for the terminal to confirm the requested Kitty mode.
pub const DEFAULT_KITTY_NEGOTIATION_TIMEOUT: Duration = Duration::from_millis(500);
/// Maximum time spent awaiting an orderly detach before terminal restoration begins.
pub const DETACH_CLEANUP_TIMEOUT: Duration = Duration::from_millis(250);

/// The broker operations needed by one already-attached terminal UI.
///
/// The composition root owns connection setup, `AttachUi`, request correlation, and session IDs.
/// This trait keeps the native terminal loop independent of a concrete broker client while keeping
/// every input-driven side effect on the existing UI connection.
#[async_trait]
pub trait UiControl {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Invokes a binding from the immutable generation attached to this UI session.
    async fn invoke(
        &mut self,
        generation: u64,
        binding: BindingId,
    ) -> Result<BrokerResponse, Self::Error>;

    /// Applies an explicit menu-control request to the attached session.
    async fn menu_control(&mut self, control: MenuControl) -> Result<BrokerResponse, Self::Error>;

    /// Releases the attached session during an orderly terminal exit.
    async fn detach(&mut self) -> Result<BrokerResponse, Self::Error>;

    /// Waits for one broker event for this connection.
    ///
    /// A disconnect is an error. The runner fails closed and restores terminal state rather than
    /// reconnecting or replaying input against another broker.
    async fn next_event(&mut self) -> Result<BrokerEvent, Self::Error>;
}

/// Terminal-session exit reason after terminal restoration has been attempted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UiExit {
    /// The broker detached this UI session.
    Detached,
    /// Stdin reached end-of-file and the runner sent `DetachUi`.
    InputClosed,
    /// A handled termination signal caused an orderly detach.
    Interrupted,
    /// The broker announced retirement.
    BrokerRetiring,
}

/// Failure while driving the attached native terminal UI.
#[derive(Debug, Error)]
pub enum UiRunError {
    #[error(transparent)]
    Ui(#[from] UiError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Kitty(#[from] KittyNegotiationError),
    #[error("broker UI connection failed: {0}")]
    Control(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("broker sent fatal diagnostic ({diagnostic:?})")]
    BrokerFatal { diagnostic: ProtocolDiagnostic },
    #[error("UI run failed: {primary}; terminal restoration failed: {restoration}")]
    RunAndRestore {
        #[source]
        primary: Box<Self>,
        restoration: Box<Self>,
    },
    #[error("broker detach did not complete before terminal restoration")]
    DetachTimedOut,
}

struct SignalHandlers {
    resize: Signal,
    interrupt: Signal,
    terminate: Signal,
    hangup: Signal,
}

impl SignalHandlers {
    fn install() -> io::Result<Self> {
        Ok(Self {
            resize: signal(SignalKind::window_change())?,
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
            hangup: signal(SignalKind::hangup())?,
        })
    }
}

/// A readiness-backed, cancellable raw terminal reader.
///
/// Reopening stdin's concrete terminal path creates an independent open-file
/// description. Using a duplicated stdin descriptor here would share
/// `O_NONBLOCK` with stdout and stderr in PTY hosts that dup one slave
/// descriptor across all three streams; a subsequent render could then fail
/// with `EAGAIN`.
struct NonblockingStdin {
    fd: AsyncFd<OwnedFd>,
}

impl NonblockingStdin {
    fn open() -> io::Result<Self> {
        let tty_path = ttyname(io::stdin().as_fd())
            .map_err(nix_io)
            .map_err(|error| {
                io::Error::new(error.kind(), format!("resolve stdin terminal: {error}"))
            })?;
        let tty = OpenOptions::new()
            .read(true)
            .open(&tty_path)
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("open stdin terminal {}: {error}", tty_path.display()),
                )
            })?;
        let fd = OwnedFd::from(tty);
        let flags =
            OFlag::from_bits_truncate(fcntl(&fd, FcntlArg::F_GETFL).map_err(nix_io).map_err(
                |error| io::Error::new(error.kind(), format!("read stdin terminal flags: {error}")),
            )?);
        fcntl(&fd, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))
            .map_err(nix_io)
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("set stdin terminal nonblocking: {error}"),
                )
            })?;
        Ok(Self {
            fd: AsyncFd::new(fd).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("register stdin terminal readiness: {error}"),
                )
            })?,
        })
    }

    async fn read(&self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut readiness = self.fd.readable().await?;
            if let Ok(result) = readiness.try_io(|fd| read_fd(fd.get_ref(), bytes).map_err(nix_io))
            {
                return result;
            }
        }
    }
}

fn nix_io(error: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}

/// Stateful UI input, event, and rendering logic that is reusable by a native runner or a PTY
/// harness. It owns no broker connection and does not enter terminal raw mode by itself.
pub struct UiSession {
    runtime: UiRuntime,
    input: InputDriver,
    pending_inputs: VecDeque<ConvertedInput>,
    kitty: Option<KittyNegotiation>,
    started: Instant,
}

impl UiSession {
    /// Builds a session from the checked `UiAttached` response frame supplied by the broker.
    ///
    /// # Errors
    ///
    /// Returns [`UiError`] when the frame cannot be decoded, is not a UI attachment response,
    /// carries an invalid binding key, or supplies templates that fail to compile.
    pub fn attach(frame: muxe_protocol::ArchivedFrame) -> Result<Self, UiError> {
        let started = Instant::now();
        let runtime = UiRuntime::attach_at(frame, muxe_core::SessionInstant(Duration::ZERO))?;
        let input = InputDriver::new(runtime.keyboard_profile()?);
        Ok(Self {
            runtime,
            input,
            pending_inputs: VecDeque::with_capacity(8),
            kitty: None,
            started,
        })
    }

    /// Returns the attached menu runtime without copying its archived snapshot.
    pub fn runtime(&self) -> &UiRuntime {
        &self.runtime
    }

    /// Returns whether Kitty confirmation still prevents ordinary menu input.
    pub fn is_negotiating_kitty(&self) -> bool {
        self.kitty
            .as_ref()
            .is_some_and(|negotiation| !negotiation.is_confirmed())
    }

    /// Starts terminal-dependent input behavior after raw mode and the alternate screen exist.
    ///
    /// # Errors
    ///
    /// Returns the underlying terminal error when the Kitty mode push or its flush fails.
    pub fn begin_terminal<W: io::Write>(
        &mut self,
        surface: &mut TerminalSurface<W>,
    ) -> Result<(), io::Error> {
        if let KeyboardProfile::Kitty(capabilities) = self.input.profile() {
            self.kitty = Some(surface.begin_kitty_negotiation(*capabilities)?);
        }
        Ok(())
    }

    /// Renders one fresh terminal area, resetting pager state if the area changed.
    ///
    /// # Errors
    ///
    /// Returns [`UiRunError::Ui`] when the menu cannot be prepared from the pinned attachment, or
    /// [`UiRunError::Io`] when the terminal surface write fails.
    pub fn render<W: io::Write>(
        &mut self,
        surface: &mut TerminalSurface<W>,
        area: Rect,
    ) -> Result<(), UiRunError> {
        let prepared = self.runtime.prepare(area)?;
        surface.render(prepared.surface_frame(), area)?;
        Ok(())
    }

    /// Decodes raw stdin bytes into the reusable stream-order input queue.
    ///
    /// The runner must call [`Self::next_command`] between each queued event so an accepted
    /// invocation or pending-control response changes `MenuSession` before the next key applies.
    pub fn push(&mut self, bytes: &[u8]) {
        let (input, pending_inputs) = (&mut self.input, &mut self.pending_inputs);
        input.push(bytes, |input| pending_inputs.push_back(input));
    }

    /// Feeds one stdin chunk stamped with its shared arrival timestamp.
    ///
    /// The runner passes its own clock reading so the driver can arm the pending Escape deadline
    /// in that same clock. Test harnesses drive this with a paused clock to pin the boundary.
    fn push_stdin_at(&mut self, bytes: &[u8], now: Instant) {
        let (input, pending_inputs) = (&mut self.input, &mut self.pending_inputs);
        input.push_stdin(bytes, now, |input| pending_inputs.push_back(input));
    }

    /// Feeds bytes that arrived together with an armed Escape deadline through the production rule.
    ///
    /// When the shared arrival timestamp reaches the armed deadline (`now >= deadline`), the
    /// pending Escape flushes first and the bytes follow as their own keys; otherwise the bytes
    /// continue the pending sequence. Returns the arbitration decision, or `None` when no Escape
    /// is pending.
    fn push_at_escape_deadline(
        &mut self,
        bytes: &[u8],
        now: Instant,
        deadline: Instant,
    ) -> Option<crate::terminal::EscapeArrival> {
        let (input, pending_inputs) = (&mut self.input, &mut self.pending_inputs);
        input.push_at_escape_deadline(bytes, now, deadline, |input| {
            pending_inputs.push_back(input);
        })
    }

    /// Returns the armed Escape deadline derived from the pending Escape start, if any.
    #[must_use]
    fn escape_deadline(&self) -> Option<Instant> {
        let since = self.input.pending_escape_since()?;
        let timeout = self.vt100_escape_timeout()?;
        Some(since + timeout)
    }

    /// Resolves a pending VT100 Escape only at the caller's explicit deadline.
    pub fn flush_vt100_escape(&mut self) {
        let (input, pending_inputs) = (&mut self.input, &mut self.pending_inputs);
        input.flush_vt100_escape(|input| pending_inputs.push_back(input));
    }

    /// Flushes parser input at an explicit stdin end boundary.
    pub fn finish(&mut self) {
        let (input, pending_inputs) = (&mut self.input, &mut self.pending_inputs);
        input.finish(|input| pending_inputs.push_back(input));
    }

    /// Fails the terminal setup when the Kitty confirmation deadline expires.
    ///
    /// # Errors
    ///
    /// Returns [`KittyNegotiationError::TimedOut`] when the Kitty response has not confirmed the
    /// requested mode. An already-confirmed or absent negotiation succeeds.
    pub fn kitty_timeout(&self) -> Result<(), KittyNegotiationError> {
        self.kitty
            .as_ref()
            .map_or(Ok(()), KittyNegotiation::timeout)
    }

    /// Applies one broker event to dynamic availability and session state.
    ///
    /// # Errors
    ///
    /// Returns [`UiError`] when the attached session ID or attachment cannot be read, or when a
    /// binding condition fails to evaluate.
    pub fn handle_broker_event(&mut self, event: &BrokerEvent) -> Result<UiCommand, UiError> {
        self.runtime.handle_broker_event(event, self.now())
    }

    fn invocation_accepted(
        &mut self,
        binding: BindingId,
        execution: muxe_protocol::ExecutionId,
        disposition: WireInvocationDisposition,
    ) -> Result<UiCommand, UiError> {
        let disposition = match disposition {
            WireInvocationDisposition::Awaited => InvocationDisposition::Await,
            WireInvocationDisposition::Detached => InvocationDisposition::Detached,
            WireInvocationDisposition::Dismissed => InvocationDisposition::Dismissed,
        };
        self.runtime
            .invocation_accepted(binding, execution, disposition, self.now())
    }

    fn pending_control_completed(
        &mut self,
        execution: &muxe_protocol::ExecutionId,
        control: MenuControl,
    ) -> UiCommand {
        self.runtime
            .pending_control_completed(execution, control, self.now())
    }

    /// Records a recoverable broker diagnostic for the next render.
    pub fn report_broker_error(&mut self, message: String) {
        self.runtime.report_broker_error(message);
    }

    /// Applies at most one decoded input after every preceding runner command has settled.
    fn next_command(&mut self) -> Result<Option<UiCommand>, UiRunError> {
        loop {
            let Some(input) = self.pending_inputs.pop_front() else {
                return Ok(None);
            };
            if let Some(negotiation) = self.kitty.as_mut()
                && !negotiation.is_confirmed()
            {
                if negotiation.observe(input)? == crate::NegotiationUpdate::Confirmed {
                    negotiation.prepend_pending_to(&mut self.pending_inputs);
                }
                continue;
            }
            return Ok(Some(self.runtime.handle_input_at(&input, self.now())?));
        }
    }

    fn vt100_escape_timeout(&self) -> Option<Duration> {
        match self.input.profile() {
            KeyboardProfile::Vt100 { escape_timeout } => Some(*escape_timeout),
            KeyboardProfile::Kitty(_) => None,
        }
    }

    fn inactivity_deadline(&self) -> Option<Instant> {
        self.runtime
            .inactivity_deadline()
            .map(|deadline| self.started + deadline.0)
    }

    fn tick(&mut self) -> UiCommand {
        self.runtime.tick(self.now())
    }

    fn now(&self) -> muxe_core::SessionInstant {
        muxe_core::SessionInstant(self.started.elapsed())
    }
}

/// Runs one checked broker attachment on the caller's real terminal.
///
/// The broker client remains injected so the composition root retains socket construction and
/// lifecycle ownership. Every path after [`TerminalSurface::enter`] attempts terminal restoration:
/// normal detach, stdin EOF, Kitty refusal or timeout, broker failure, and handled signals. During
/// panic unwinding, `TerminalSurface`'s drop guard performs the same best-effort restoration.
/// # Errors
///
/// Returns [`UiRunError`] when signal installation, session attachment, terminal setup,
/// input handling, broker communication, or terminal restoration fails.
pub async fn run_attached<C>(
    frame: muxe_protocol::ArchivedFrame,
    control: &mut C,
) -> Result<UiExit, UiRunError>
where
    C: UiControl + Send,
{
    let mut signals = SignalHandlers::install()?;
    let mut session = UiSession::attach(frame)?;
    let mut surface = TerminalSurface::enter(io::stdout())?;
    let result = run_loop(&mut session, &mut surface, control, &mut signals).await;
    finish_run(result, surface.restore())
}

fn finish_run(
    result: Result<UiExit, UiRunError>,
    restored: io::Result<()>,
) -> Result<UiExit, UiRunError> {
    match (result, restored) {
        (Ok(exit), Ok(())) => Ok(exit),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(restoration)) => Err(UiRunError::Io(restoration)),
        (Err(primary), Err(restoration)) => Err(UiRunError::RunAndRestore {
            primary: Box::new(primary),
            restoration: Box::new(UiRunError::Io(restoration)),
        }),
    }
}

fn broker_event_exit(event: &BrokerEvent) -> Result<Option<UiExit>, UiRunError> {
    match event {
        BrokerEvent::BrokerRetiring => Ok(Some(UiExit::BrokerRetiring)),
        BrokerEvent::Fatal(diagnostic) => Err(UiRunError::BrokerFatal {
            diagnostic: diagnostic.clone(),
        }),
        _ => Ok(None),
    }
}

async fn run_loop<C>(
    session: &mut UiSession,
    surface: &mut TerminalSurface<io::Stdout>,
    control: &mut C,
    signals: &mut SignalHandlers,
) -> Result<UiExit, UiRunError>
where
    C: UiControl + Send,
{
    session.begin_terminal(surface)?;
    let mut area = terminal_area()?;
    session.render(surface, area)?;

    let stdin = NonblockingStdin::open()?;
    let mut bytes = [0_u8; 4096];
    let mut escape_deadline: Option<Instant> = None;
    let mut kitty_deadline = session
        .is_negotiating_kitty()
        .then_some(Instant::now() + DEFAULT_KITTY_NEGOTIATION_TIMEOUT);

    loop {
        // The stdin arm owns every Escape boundary: a read that lands while a deadline is armed
        // routes through `push_at_escape_deadline`, so the single `now >= deadline` timestamp
        // comparison decides instead of the `select!` scheduler. The sleep arm only fires when no
        // byte arrived first; whichever branch wins, the pending Escape resolves exactly once.
        let now = Instant::now();
        let escape_wait = escape_deadline.map_or(Duration::MAX, |deadline| {
            deadline.saturating_duration_since(now)
        });
        let kitty_wait = kitty_deadline.map_or(Duration::MAX, |deadline| {
            deadline.saturating_duration_since(now)
        });
        let inactivity_deadline = session.inactivity_deadline();
        let inactivity_wait = inactivity_deadline.map_or(Duration::MAX, |deadline| {
            deadline.saturating_duration_since(now)
        });
        tokio::select! {
            read = stdin.read(&mut bytes) => {
                let read = read?;
                if read == 0 {
                    session.finish();
                    if let Some(exit) = dispatch_queued(session, surface, control).await? {
                        return Ok(exit);
                    }
                    detach_bounded(control).await?;
                    return Ok(UiExit::InputClosed);
                }
                let arrival = Instant::now();
                if let Some(deadline) = escape_deadline.take() {
                    session.push_at_escape_deadline(&bytes[..read], arrival, deadline);
                    escape_deadline = session.escape_deadline();
                } else {
                    session.push_stdin_at(&bytes[..read], arrival);
                    escape_deadline = session.escape_deadline();
                }
                if let Some(exit) = dispatch_queued(session, surface, control).await? {
                    return Ok(exit);
                }
                if !session.is_negotiating_kitty() {
                    kitty_deadline = None;
                }
            }
            () = tokio::time::sleep(escape_wait), if escape_deadline.is_some() => {
                escape_deadline = None;
                session.flush_vt100_escape();
                if let Some(exit) = dispatch_queued(session, surface, control).await? {
                    return Ok(exit);
                }
            }
            () = tokio::time::sleep(kitty_wait), if kitty_deadline.is_some() => {
                session.kitty_timeout()?;
                kitty_deadline = None;
            }
            () = tokio::time::sleep(inactivity_wait), if inactivity_deadline.is_some() => {
                if let Some(exit) = dispatch(session.tick(), session, surface, control).await? {
                    return Ok(exit);
                }
            }
            _ = signals.resize.recv() => {
                area = terminal_area()?;
                session.render(surface, area)?;
            }
            _ = signals.interrupt.recv() => {
                detach_bounded(control).await?;
                return Ok(UiExit::Interrupted);
            }
            _ = signals.terminate.recv() => {
                detach_bounded(control).await?;
                return Ok(UiExit::Interrupted);
            }
            _ = signals.hangup.recv() => {
                detach_bounded(control).await?;
                return Ok(UiExit::Interrupted);
            }
            event = control.next_event() => {
                let event = event.map_err(|error| UiRunError::Control(Box::new(error)))?;
                if let Some(exit) = broker_event_exit(&event)? {
                    return Ok(exit);
                }
                let command = session.handle_broker_event(&event)?;
                if let Some(exit) = dispatch(command, session, surface, control).await? {
                    return Ok(exit);
                }
            }
        }
    }
}

async fn dispatch_queued<C>(
    session: &mut UiSession,
    surface: &mut TerminalSurface<io::Stdout>,
    control: &mut C,
) -> Result<Option<UiExit>, UiRunError>
where
    C: UiControl + Send,
{
    while let Some(command) = session.next_command()? {
        if let Some(exit) = dispatch(command, session, surface, control).await? {
            return Ok(Some(exit));
        }
    }
    Ok(None)
}

async fn dispatch<C>(
    mut command: UiCommand,
    session: &mut UiSession,
    surface: &mut TerminalSurface<io::Stdout>,
    control: &mut C,
) -> Result<Option<UiExit>, UiRunError>
where
    C: UiControl + Send,
{
    loop {
        match command {
            UiCommand::Invoke {
                generation,
                binding,
            } => {
                let response = control
                    .invoke(generation, binding)
                    .await
                    .map_err(|error| UiRunError::Control(Box::new(error)))?;
                match response {
                    BrokerResponse::InvocationAccepted {
                        execution,
                        disposition,
                    } => {
                        command = session.invocation_accepted(binding, execution, disposition)?;
                    }
                    response => return observe_response(response, session, surface),
                }
            }
            UiCommand::MenuControl(menu_control) => {
                let response = control
                    .menu_control(menu_control)
                    .await
                    .map_err(|error| UiRunError::Control(Box::new(error)))?;
                match response {
                    BrokerResponse::PendingControlCompleted { execution, control } => {
                        command = session.pending_control_completed(&execution, control);
                    }
                    response => return observe_response(response, session, surface),
                }
            }
            UiCommand::Detach => {
                detach_bounded(control).await?;
                return Ok(Some(UiExit::Detached));
            }
            UiCommand::Redraw => {
                session.render(surface, terminal_area()?)?;
                return Ok(None);
            }
            UiCommand::Ignored => return Ok(None),
        }
    }
}

async fn detach_bounded<C>(control: &mut C) -> Result<(), UiRunError>
where
    C: UiControl + Send,
{
    match tokio::time::timeout(DETACH_CLEANUP_TIMEOUT, control.detach()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(UiRunError::Control(Box::new(error))),
        Err(_) => Err(UiRunError::DetachTimedOut),
    }
}

fn observe_response(
    response: BrokerResponse,
    session: &mut UiSession,
    surface: &mut TerminalSurface<io::Stdout>,
) -> Result<Option<UiExit>, UiRunError> {
    match response {
        BrokerResponse::Detached => Ok(Some(UiExit::Detached)),
        BrokerResponse::Error(diagnostic) => {
            session.report_broker_error(diagnostic.message);
            session.render(surface, terminal_area()?)?;
            Ok(None)
        }
        BrokerResponse::Acknowledged => Ok(None),
        unexpected => {
            session.report_broker_error(format!("unexpected UI broker response: {unexpected:?}"));
            session.render(surface, terminal_area()?)?;
            Ok(None)
        }
    }
}

fn terminal_area() -> io::Result<Rect> {
    let (width, height) = terminal::size()?;
    Ok(Rect::new(0, 0, width, height))
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use muxe_core::KeyCapabilities;
    use muxe_protocol::{
        AfterAction, DiagnosticCode, ExecutionId, ExecutionMode, KeyCapabilitiesWire,
        KeyboardProfileWire, LocalMenuActionWire, MenuControl, ProtocolDiagnostic,
    };

    use super::*;
    use crate::runtime::tests::{binding_with_policy, profiled_attachment_with_timeout};

    fn pending_control_session() -> UiSession {
        UiSession::attach(profiled_attachment_with_timeout(
            KeyboardProfileWire::Vt100 {
                escape_timeout_millis: 25,
            },
            vec![
                binding_with_policy(1, "a", AfterAction::Stay, ExecutionMode::Await, None),
                binding_with_policy(2, "b", AfterAction::Stay, ExecutionMode::Await, None),
                binding_with_policy(
                    3,
                    "r",
                    AfterAction::Stay,
                    ExecutionMode::Await,
                    Some(LocalMenuActionWire::Control(MenuControl::Return)),
                ),
            ],
            None,
        ))
        .expect("checked attachment creates a session")
    }

    fn accept_first_invocation(session: &mut UiSession) {
        assert_eq!(
            session.next_command().expect("first key decodes"),
            Some(UiCommand::Invoke {
                generation: 7,
                binding: BindingId {
                    generation: 7,
                    ordinal: 1,
                },
            })
        );
        assert_eq!(
            session
                .invocation_accepted(
                    BindingId {
                        generation: 7,
                        ordinal: 1,
                    },
                    ExecutionId([1; 16]),
                    WireInvocationDisposition::Awaited,
                )
                .expect("accepted invocation stages pending session"),
            UiCommand::Redraw
        );
    }

    #[test]
    fn coalesced_and_fragmented_keys_observe_acceptance_before_pending_control_priority() {
        let mut coalesced = pending_control_session();
        coalesced.push(b"abr");
        accept_first_invocation(&mut coalesced);
        let coalesced_remaining = [
            coalesced
                .next_command()
                .expect("second coalesced key is processed after acceptance"),
            coalesced
                .next_command()
                .expect("pending control is processed after swallowed binding"),
        ];

        let mut fragmented = pending_control_session();
        fragmented.push(b"a");
        accept_first_invocation(&mut fragmented);
        fragmented.push(b"br");
        let fragmented_remaining = [
            fragmented
                .next_command()
                .expect("second fragmented key is processed after acceptance"),
            fragmented
                .next_command()
                .expect("fragmented pending control follows swallowed binding"),
        ];

        let expected = [
            Some(UiCommand::Ignored),
            Some(UiCommand::MenuControl(MenuControl::Return)),
        ];
        assert_eq!(coalesced_remaining, expected);
        assert_eq!(fragmented_remaining, expected);
        assert_eq!(
            coalesced.pending_control_completed(&ExecutionId([1; 16]), MenuControl::Return),
            UiCommand::Detach
        );
    }

    #[test]
    fn coalesced_kitty_confirmation_replays_buffered_input_before_unread_bytes() {
        let capabilities = KeyCapabilities {
            event_types: true,
            alternate_keys: true,
            all_keys_as_escape_codes: false,
        };
        let mut session = UiSession::attach(profiled_attachment_with_timeout(
            KeyboardProfileWire::Kitty(KeyCapabilitiesWire {
                event_types: true,
                alternate_keys: true,
                all_keys_as_escape_codes: false,
            }),
            vec![binding_with_policy(
                1,
                "a",
                AfterAction::Stay,
                ExecutionMode::Await,
                None,
            )],
            None,
        ))
        .expect("checked Kitty attachment creates a session");
        session.kitty = Some(KittyNegotiation::new(capabilities));

        session.push(b"a\x1b[?7u");
        assert_eq!(
            session
                .next_command()
                .expect("confirmation replays buffered key"),
            Some(UiCommand::Invoke {
                generation: 7,
                binding: BindingId {
                    generation: 7,
                    ordinal: 1,
                },
            })
        );
    }
    /// Drives the production stdin/deadline arbitration with a paused clock.
    ///
    /// The scenario mirrors `run_loop`: the Escape chunk arms the deadline through `push_stdin_at`,
    /// the follow-up byte arrives via `push_at_escape_deadline` with the same shared timestamp the
    /// loop would supply, and the queued inputs drain in order. Identical arrival timestamps must
    /// produce identical results; the assertions below pin which outcome each timestamp selects.
    async fn arbitrate_with_paused_clock(
        escape_at: Duration,
        byte_at: Duration,
    ) -> (Option<crate::terminal::EscapeArrival>, Vec<UiCommand>) {
        let timeout = Duration::from_millis(25);
        let start = Instant::now();
        let mut session = pending_control_session();
        session.push_stdin_at(b"\x1b", start + escape_at);
        let deadline = session
            .escape_deadline()
            .expect("pending Escape arms a deadline");
        assert_eq!(deadline, start + escape_at + timeout);
        tokio::time::advance(byte_at.saturating_sub(escape_at)).await;
        let now = Instant::now();
        assert_eq!(now, start + byte_at);
        let decision = session.push_at_escape_deadline(b"b", now, deadline);
        let mut commands = Vec::new();
        while let Some(command) = session
            .next_command()
            .expect("queued input decodes without broker traffic")
        {
            commands.push(command);
        }
        (decision, commands)
    }

    #[tokio::test(start_paused = true)]
    async fn escape_deadline_arbitration_is_deterministic_at_the_boundary() {
        use crate::terminal::EscapeArrival;

        // Well before the deadline the byte continues the sequence: Alt-b matches no binding.
        let (before_decision, before_commands) =
            arbitrate_with_paused_clock(Duration::ZERO, Duration::from_millis(10)).await;
        assert_eq!(before_decision, Some(EscapeArrival::BeforeDeadline));
        assert_eq!(before_commands, vec![UiCommand::Ignored]);

        // Exactly at the deadline the pending Escape flushes first: standalone Escape is ignored
        // and the follow-up byte invokes its own binding. The old unbiased select could instead
        // feed the byte into the pending sequence and emit Alt-b (Ignored above); asserting the
        // two-command Invoke outcome pins the `now >= deadline` flush-first rule.
        let (at_decision, at_commands) =
            arbitrate_with_paused_clock(Duration::ZERO, Duration::from_millis(25)).await;
        assert_eq!(at_decision, Some(EscapeArrival::AtOrAfterDeadline));
        assert_eq!(
            at_commands,
            vec![
                UiCommand::Ignored,
                UiCommand::Invoke {
                    generation: 7,
                    binding: BindingId {
                        generation: 7,
                        ordinal: 2,
                    },
                },
            ]
        );

        // Strictly after the deadline resolves identically to the boundary: same decision, same
        // commands for the same inputs, so identical arrival timestamps never race.
        let (after_decision, after_commands) =
            arbitrate_with_paused_clock(Duration::ZERO, Duration::from_millis(40)).await;
        assert_eq!(after_decision, Some(EscapeArrival::AtOrAfterDeadline));
        assert_eq!(after_commands, at_commands);
    }
    #[test]
    fn loop_and_restore_errors_preserve_primary_then_cleanup() {
        let error = finish_run(
            Err(UiRunError::Io(io::Error::other("loop failed"))),
            Err(io::Error::other("restore failed")),
        )
        .expect_err("both failures must remain an error");

        let UiRunError::RunAndRestore {
            primary,
            restoration,
        } = &error
        else {
            panic!("expected structured primary and restoration errors: {error:?}");
        };
        assert_eq!(primary.to_string(), "loop failed");
        assert_eq!(restoration.to_string(), "restore failed");
        assert_eq!(
            error.to_string(),
            "UI run failed: loop failed; terminal restoration failed: restore failed"
        );
        assert_eq!(
            error.source().expect("primary is the source").to_string(),
            "loop failed"
        );
    }

    #[test]
    fn fatal_broker_event_preserves_diagnostic_as_a_run_error() {
        let diagnostic = ProtocolDiagnostic {
            code: DiagnosticCode::HostUnavailable,
            message: "host vanished".into(),
        };
        let error = broker_event_exit(&BrokerEvent::Fatal(diagnostic.clone()))
            .expect_err("fatal broker diagnostics must not become successful exits");

        let UiRunError::BrokerFatal {
            diagnostic: received,
        } = &error
        else {
            panic!("expected structured fatal diagnostic: {error:?}");
        };
        assert_eq!(received, &diagnostic);
        assert_eq!(
            error.to_string(),
            "broker sent fatal diagnostic (ProtocolDiagnostic { code: HostUnavailable, message: \"host vanished\" })"
        );
        assert!(
            error.source().is_none(),
            "wire diagnostics are the terminal error source"
        );
    }
}
