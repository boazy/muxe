use std::{
    collections::VecDeque,
    io,
    os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd},
    time::Duration,
};

use async_trait::async_trait;
use crossterm::terminal;
use muxe_core::KeyboardProfile;
use muxe_protocol::{
    BindingId, BrokerEvent, BrokerResponse, InvocationDisposition as WireInvocationDisposition,
    MenuControl,
};
use nix::{
    fcntl::{FcntlArg, OFlag, fcntl},
    unistd::{dup, read as read_fd},
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
    /// The broker sent a session-fatal diagnostic.
    BrokerFatal,
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

/// A duplicated stdin descriptor whose nonblocking flag is restored before it closes.
struct NonblockingStdinFd {
    fd: OwnedFd,
    original_flags: OFlag,
}

impl AsRawFd for NonblockingStdinFd {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.fd.as_raw_fd()
    }
}

impl AsFd for NonblockingStdinFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl Drop for NonblockingStdinFd {
    fn drop(&mut self) {
        let _ = fcntl(&self.fd, FcntlArg::F_SETFL(self.original_flags));
    }
}

/// A readiness-backed, cancellable raw terminal reader.
///
/// The duplicated descriptor shares the terminal's open-file description, so setting
/// `O_NONBLOCK` is restored by the descriptor's drop guard before the UI returns.
struct NonblockingStdin {
    fd: AsyncFd<NonblockingStdinFd>,
}

impl NonblockingStdin {
    fn open() -> io::Result<Self> {
        let fd = dup(io::stdin()).map_err(nix_io)?;
        let original_flags =
            OFlag::from_bits_truncate(fcntl(&fd, FcntlArg::F_GETFL).map_err(nix_io)?);
        fcntl(&fd, FcntlArg::F_SETFL(original_flags | OFlag::O_NONBLOCK)).map_err(nix_io)?;
        let fd = NonblockingStdinFd { fd, original_flags };
        Ok(Self {
            fd: AsyncFd::new(fd)?,
        })
    }

    async fn read(&self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut readiness = self.fd.readable().await?;
            match readiness.try_io(|fd| read_fd(fd.get_ref(), bytes).map_err(nix_io)) {
                Ok(result) => return result,
                Err(_) => continue,
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
    pub fn kitty_timeout(&self) -> Result<(), KittyNegotiationError> {
        self.kitty
            .as_ref()
            .map_or(Ok(()), KittyNegotiation::timeout)
    }

    /// Applies one broker event to dynamic availability and session state.
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
            if let Some(negotiation) = self.kitty.as_mut() {
                if !negotiation.is_confirmed() {
                    if negotiation.observe(input)? == crate::NegotiationUpdate::Confirmed {
                        negotiation.prepend_pending_to(&mut self.pending_inputs);
                    }
                    continue;
                }
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
    let restored = surface.restore();
    match (result, restored) {
        (Ok(exit), Ok(())) => Ok(exit),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(UiRunError::Io(error)),
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
        let now = Instant::now();
        let escape_wait = escape_deadline
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or(Duration::MAX);
        let kitty_wait = kitty_deadline
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or(Duration::MAX);
        let inactivity_deadline = session.inactivity_deadline();
        let inactivity_wait = inactivity_deadline
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or(Duration::MAX);
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
                session.push(&bytes[..read]);
                if let Some(exit) = dispatch_queued(session, surface, control).await? {
                    return Ok(exit);
                }
                if let Some(timeout) = session.vt100_escape_timeout() {
                    escape_deadline = Some(Instant::now() + timeout);
                }
                if !session.is_negotiating_kitty() {
                    kitty_deadline = None;
                }
            }
            _ = tokio::time::sleep(escape_wait), if escape_deadline.is_some() => {
                escape_deadline = None;
                session.flush_vt100_escape();
                if let Some(exit) = dispatch_queued(session, surface, control).await? {
                    return Ok(exit);
                }
            }
            _ = tokio::time::sleep(kitty_wait), if kitty_deadline.is_some() => {
                session.kitty_timeout()?;
                kitty_deadline = None;
            }
            _ = tokio::time::sleep(inactivity_wait), if inactivity_deadline.is_some() => {
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
                let exit = match event {
                    BrokerEvent::BrokerRetiring => Some(UiExit::BrokerRetiring),
                    BrokerEvent::Fatal(_) => Some(UiExit::BrokerFatal),
                    _ => None,
                };
                if let Some(exit) = exit {
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
                    .invoke(generation, binding.clone())
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
    use muxe_core::KeyCapabilities;
    use muxe_protocol::{
        AfterAction, ExecutionId, ExecutionMode, KeyCapabilitiesWire, KeyboardProfileWire,
        LocalMenuActionWire, MenuControl,
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
            UiCommand::Ignored
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
}
