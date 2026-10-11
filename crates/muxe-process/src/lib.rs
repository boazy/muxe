//! Exact current-user broker process capabilities for forced retirement.
//! No API signals a raw PID. Unix peer handles and OS incarnation checks bind
//! authorization; a separate exact exit observer proves hard-stop completion.

use serde::{Deserialize, Serialize};
use std::{
    io,
    num::NonZeroU32,
    os::fd::{AsFd, AsRawFd},
    time::Duration,
};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ProcessId(NonZeroU32);
impl ProcessId {
    /// Creates a positive POSIX process identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessError::Identity`] for zero or values exceeding `i32::MAX`.
    pub fn new(value: u32) -> Result<Self, ProcessError> {
        if value > i32::MAX as u32 {
            return Err(ProcessError::Identity);
        }
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(ProcessError::Identity)
    }
    #[must_use]
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

impl<'de> Deserialize<'de> for ProcessId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u32::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
struct BootId(String);

impl<'de> Deserialize<'de> for BootId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut value = String::deserialize(deserializer)?;
        if !valid_boot_id(&value) {
            return Err(serde::de::Error::custom("invalid boot UUID"));
        }
        value.make_ascii_lowercase();
        Ok(Self(value))
    }
}

impl BootId {
    fn current() -> Result<Self, ProcessError> {
        #[cfg(target_os = "linux")]
        let text = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        #[cfg(target_os = "macos")]
        let text = {
            let mut buffer = [0_u8; 64];
            // SAFETY: the helper writes at most the supplied buffer capacity.
            check_code(unsafe { muxe_process_boot_id(buffer.as_mut_ptr().cast(), 64) })?;
            let end = buffer
                .iter()
                .position(|byte| *byte == 0)
                .ok_or(ProcessError::Identity)?;
            std::str::from_utf8(&buffer[..end])
                .map_err(|_| ProcessError::Identity)?
                .to_owned()
        };
        let text = text.trim();
        if !valid_boot_id(text) {
            return Err(ProcessError::Identity);
        }
        Ok(Self(text.to_ascii_lowercase()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ProcessIdentity(RecordedIdentity);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "platform", rename_all = "snake_case")]
enum RecordedIdentity {
    #[cfg(target_os = "macos")]
    Darwin { boot: BootId, token: DarwinIdentity },
    #[cfg(target_os = "linux")]
    Linux {
        boot: BootId,
        namespace: LinuxPidNamespace,
        process: ProcessId,
        start: LinuxStartTime,
    },
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
struct DarwinIdentity([u32; 8]);
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
struct LinuxStartTime(u64);

#[cfg(any(target_os = "linux", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
struct LinuxPidNamespace([u64; 2]);

#[cfg(any(target_os = "linux", test))]
impl<'de> Deserialize<'de> for LinuxPidNamespace {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let [device, inode] = <[u64; 2]>::deserialize(deserializer)?;
        Self::new(device, inode).map_err(serde::de::Error::custom)
    }
}

#[cfg(any(target_os = "linux", test))]
impl LinuxPidNamespace {
    fn new(device: u64, inode: u64) -> Result<Self, ProcessError> {
        if inode == 0 {
            return Err(ProcessError::ObservationContext);
        }
        Ok(Self([device, inode]))
    }

    fn valid(self) -> bool {
        self.0[1] != 0
    }

    fn verify(self, current: Self) -> Result<(), ProcessError> {
        if self.valid() && self == current {
            Ok(())
        } else {
            Err(ProcessError::ObservationContext)
        }
    }

    #[cfg(target_os = "linux")]
    fn current() -> Result<Self, ProcessError> {
        use std::os::unix::fs::MetadataExt;

        let status = std::fs::read_to_string("/proc/self/status")?;
        validate_procfs_view(&status, ProcessId::new(std::process::id())?)?;
        let metadata = std::fs::metadata("/proc/self/ns/pid")?;
        Self::new(metadata.dev(), metadata.ino())
    }
}

#[cfg(any(target_os = "linux", test))]
fn validate_procfs_view(status: &str, process: ProcessId) -> Result<(), ProcessError> {
    let values = status
        .lines()
        .find_map(|line| line.strip_prefix("NStgid:"))
        .ok_or(ProcessError::ObservationContext)?;
    let mut values = values.split_ascii_whitespace();
    let observed = values.next().and_then(|value| value.parse::<u32>().ok());
    if observed != Some(process.get()) || values.next().is_some() {
        return Err(ProcessError::ObservationContext);
    }
    Ok(())
}

impl<'de> Deserialize<'de> for ProcessIdentity {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let identity = RecordedIdentity::deserialize(deserializer)?;
        let valid = match &identity {
            #[cfg(target_os = "macos")]
            RecordedIdentity::Darwin { token, .. } => {
                ProcessId::new(token.0[5]).is_ok()
                    && token.0[1] == nix::unistd::Uid::current().as_raw()
            }
            #[cfg(target_os = "linux")]
            RecordedIdentity::Linux {
                namespace, start, ..
            } => namespace.valid() && start.0 != 0,
        };
        if !valid {
            return Err(serde::de::Error::custom("invalid OS process incarnation"));
        }
        Ok(Self(identity))
    }
}

fn valid_boot_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

impl ProcessIdentity {
    /// Captures the calling broker's OS incarnation at its registration boundary.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessError`] if boot or own-process metadata is unavailable,
    /// invalid, or observed through an unsupported Linux namespace/procfs view.
    pub fn current() -> Result<Self, ProcessError> {
        let boot = BootId::current()?;
        #[cfg(target_os = "macos")]
        {
            let mut token = [0_u32; 8];
            // SAFETY: helper writes exactly eight words into this initialized array.
            check_code(unsafe { muxe_process_current_token(token.as_mut_ptr()) })?;
            Ok(Self(RecordedIdentity::Darwin {
                boot,
                token: DarwinIdentity(token),
            }))
        }
        #[cfg(target_os = "linux")]
        {
            let process = ProcessId::new(std::process::id())?;
            let namespace = LinuxPidNamespace::current()?;
            Ok(Self(RecordedIdentity::Linux {
                boot,
                namespace,
                process,
                start: linux_start(process)?,
            }))
        }
    }
    /// Returns the validated positive PID in this observation's OS context.
    ///
    /// # Panics
    ///
    /// Panics if an internal Darwin token violates the nonzero PID invariant.
    /// Public deserialization rejects that state before constructing this type.
    #[must_use]
    pub fn process(&self) -> ProcessId {
        match &self.0 {
            #[cfg(target_os = "macos")]
            RecordedIdentity::Darwin { token, .. } => {
                ProcessId(NonZeroU32::new(token.0[5]).expect("validated audit PID"))
            }
            #[cfg(target_os = "linux")]
            RecordedIdentity::Linux { process, .. } => *process,
        }
    }
    /// Whether persistence carries a kernel incarnation cookie strong enough
    /// to reauthorize this peer without a fresh broker role response.
    #[must_use]
    pub fn authorizes_stored_peer(&self) -> bool {
        #[cfg(target_os = "macos")]
        {
            true
        }
        #[cfg(target_os = "linux")]
        {
            false
        }
    }

    /// Proves that the recorded process has exited. A Darwin exec changes the
    /// audit token but is not exit, so a live PID with a changed token is ambiguous.
    /// A live Linux PID can remain unproven even if it replaced the old process;
    /// stored clock ticks never supply a positive exit certificate.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessError`] if the process incarnation is ambiguous, the
    /// Linux observation context differs, or the kernel metadata query fails.
    pub fn is_gone(&self) -> Result<bool, ProcessError> {
        match &self.0 {
            #[cfg(target_os = "macos")]
            RecordedIdentity::Darwin { boot, token } => {
                if *boot != BootId::current()? {
                    return Ok(true);
                }
                if validate_token(token).is_ok() {
                    return Ok(false);
                }
                // Zombie-aware direct lookup includes P_REF_DEAD until fd cleanup.
                // SAFETY: helper reads only the current PID's public short-BSD record.
                let completed = unsafe {
                    muxe_process_pid_completed(
                        i32::try_from(self.process().get()).map_err(|_| ProcessError::Identity)?,
                    )
                };
                completed_observation(completed)
            }
            #[cfg(target_os = "linux")]
            RecordedIdentity::Linux {
                boot,
                namespace,
                process,
                ..
            } => {
                if *boot != BootId::current()? {
                    return Ok(true);
                }
                namespace.verify(LinuxPidNamespace::current()?)?;
                let Some(pidfd) = open_pidfd(*process)? else {
                    return Ok(true);
                };
                // Start ticks are reader-time-namespace adjusted and are not a
                // persistent kernel incarnation cookie. Only this scoped
                // kernel handle's exit state is a positive death certificate.
                pidfd_ready(&pidfd, Duration::ZERO)
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("the OS did not provide a valid exact broker process identity")]
    Identity,
    #[error("the control peer does not belong to the current user or recorded broker process")]
    Peer,
    #[error("the OS could not prove that this process owns the accepted control connection")]
    SocketOwnership,
    #[error("the Linux PID namespace or procfs PID view cannot prove this process observation")]
    ObservationContext,
    #[error(
        "the recorded process incarnation is ambiguous; a changed executable token is not proof of exit"
    )]
    AmbiguousIncarnation,
    #[error("the exact broker process did not exit before the hard-stop deadline")]
    ExitTimeout,
    #[error("the OS process operation failed: {0}")]
    Io(#[from] io::Error),
}

/// Retained kernel authority. The caller must authenticate the broker's role
/// through its control reply or an exact self-recorded registration identity.
#[derive(Debug)]
pub struct BrokerStopHandle {
    identity: ProcessIdentity,
    #[cfg(target_os = "macos")]
    observer: nix::sys::event::Kqueue,
    #[cfg(target_os = "linux")]
    pidfd: std::os::fd::OwnedFd,
}

impl BrokerStopHandle {
    /// Rebinds previously authenticated durable authority, rechecking the OS
    /// incarnation before returning an exact kernel handle.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessError::AmbiguousIncarnation`] on Linux, where stored
    /// observations cannot authorize a new stop. On Darwin, returns
    /// [`ProcessError`] if the boot/token changed or the exact observer fails.
    pub fn resume_recorded(identity: &ProcessIdentity) -> Result<Self, ProcessError> {
        #[cfg(target_os = "macos")]
        {
            use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};
            let RecordedIdentity::Darwin { boot, token } = &identity.0;
            if *boot != BootId::current()? {
                return Err(ProcessError::AmbiguousIncarnation);
            }
            let observer = Kqueue::new().map_err(os_error)?;
            let event = KEvent::new(
                identity.process().get() as usize,
                EventFilter::EVFILT_PROC,
                EvFlags::EV_ADD | EvFlags::EV_ENABLE | EvFlags::EV_ONESHOT,
                FilterFlag::NOTE_EXIT,
                0,
                0,
            );
            observer
                .kevent(
                    &[event],
                    &mut [],
                    Some(nix::libc::timespec {
                        tv_sec: 0,
                        tv_nsec: 0,
                    }),
                )
                .map_err(os_error)?;
            validate_token(token)?;
            Ok(Self {
                identity: identity.clone(),
                observer,
            })
        }
        #[cfg(target_os = "linux")]
        {
            let _ = identity;
            Err(ProcessError::AmbiguousIncarnation)
        }
    }

    /// Captures an exact current-user peer before any authentication nonce is sent.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessError`] if the peer PID/user differs, an exact kernel
    /// capability is unavailable, the Linux procfs view is unsupported, or the
    /// peer exits before its observer is bound.
    pub fn capture(socket: &impl AsFd, expected: ProcessId) -> Result<Self, ProcessError> {
        #[cfg(target_os = "macos")]
        {
            use nix::sys::{
                event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue},
                socket::{getsockopt, sockopt},
            };
            let token = getsockopt(socket, sockopt::LocalPeerToken).map_err(os_error)?;
            // The audit-token layout is the public OS IPC boundary: euid and pid.
            if token.val[1] != nix::unistd::Uid::current().as_raw()
                || token.val[5] != expected.get()
            {
                return Err(ProcessError::Peer);
            }
            let token = DarwinIdentity(token.val);
            let observer = Kqueue::new().map_err(os_error)?;
            let event = KEvent::new(
                expected.get() as usize,
                EventFilter::EVFILT_PROC,
                EvFlags::EV_ADD | EvFlags::EV_ENABLE | EvFlags::EV_ONESHOT,
                FilterFlag::NOTE_EXIT,
                0,
                0,
            );
            observer
                .kevent(
                    &[event],
                    &mut [],
                    Some(nix::libc::timespec {
                        tv_sec: 0,
                        tv_nsec: 0,
                    }),
                )
                .map_err(os_error)?;
            // A successful token-bound query binds the PID-attached observer to
            // this incarnation. The observer is never authority for PID signaling.
            validate_token(&token)?;
            Ok(Self {
                identity: ProcessIdentity(RecordedIdentity::Darwin {
                    boot: BootId::current()?,
                    token,
                }),
                observer,
            })
        }
        #[cfg(target_os = "linux")]
        {
            use nix::sys::socket::{getsockopt, sockopt};
            let credentials = getsockopt(socket, sockopt::PeerCredentials).map_err(os_error)?;
            if credentials.uid() != nix::unistd::Uid::current().as_raw()
                || u32::try_from(credentials.pid()).ok() != Some(expected.get())
            {
                return Err(ProcessError::Peer);
            }
            let pidfd = getsockopt(socket, sockopt::PeerPidfd).map_err(os_error)?;
            if pidfd_ready(&pidfd, Duration::ZERO)? {
                return Err(ProcessError::AmbiguousIncarnation);
            }
            let namespace = LinuxPidNamespace::current()?;
            let start = linux_start(expected)?;
            let boot = BootId::current()?;
            if pidfd_ready(&pidfd, Duration::ZERO)? {
                return Err(ProcessError::AmbiguousIncarnation);
            }
            Ok(Self {
                identity: ProcessIdentity(RecordedIdentity::Linux {
                    boot,
                    namespace,
                    process: expected,
                    start,
                }),
                pidfd,
            })
        }
    }
    #[must_use]
    pub fn identity(&self) -> &ProcessIdentity {
        &self.identity
    }
    /// Provides independent OS proof for an accepted, unresponsive peer.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessError`] if the supplied socket cannot be bound to the
    /// retained peer, including an unaccepted Darwin connection, or required
    /// kernel peer/namespace information is unavailable.
    pub fn prove_socket_peer(&self, socket: &impl AsFd) -> Result<(), ProcessError> {
        #[cfg(target_os = "macos")]
        {
            let RecordedIdentity::Darwin { token, .. } = &self.identity.0;
            // SAFETY: fd is borrowed and live; the fixed token has eight words.
            check_code(unsafe {
                muxe_process_socket_peer(
                    socket.as_fd().as_raw_fd(),
                    i32::try_from(self.identity.process().get())
                        .map_err(|_| ProcessError::Identity)?,
                    token.0.as_ptr(),
                )
            })
            .map_err(|_| ProcessError::SocketOwnership)
        }
        #[cfg(target_os = "linux")]
        {
            let peer = Self::capture(socket, self.identity.process())?;
            if peer.identity != self.identity {
                return Err(ProcessError::SocketOwnership);
            }
            if pidfd_ready(&self.pidfd, Duration::ZERO)? {
                return Err(ProcessError::SocketOwnership);
            }
            Ok(())
        }
    }
    /// Hard-stops only the retained incarnation, then proves its exact OS exit.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessError`] if exact termination is unavailable or denied,
    /// the wait fails, or no exact exit is observed within the supplied deadline.
    pub fn kill_and_wait(&self, deadline: Duration) -> Result<(), ProcessError> {
        #[cfg(target_os = "macos")]
        {
            use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent};
            let RecordedIdentity::Darwin { token, .. } = &self.identity.0;
            // SAFETY: the helper receives a valid fixed-size token; it signals
            // through the identity-checked kernel API, never through a raw PID.
            let signal = check_code(unsafe { muxe_process_signal_token(token.0.as_ptr()) });
            if let Err(error) = signal {
                let wait = if matches!(&error, ProcessError::Io(source) if source.raw_os_error() == Some(nix::libc::ESRCH))
                {
                    deadline
                } else {
                    Duration::ZERO
                };
                if self.exit_observed(wait)? {
                    return Ok(());
                }
                return Err(error);
            }
            let mut events = [KEvent::new(
                0,
                EventFilter::EVFILT_PROC,
                EvFlags::empty(),
                FilterFlag::empty(),
                0,
                0,
            )];
            let timeout = nix::libc::timespec {
                tv_sec: i64::try_from(deadline.as_secs()).map_err(|_| ProcessError::Identity)?,
                tv_nsec: i64::from(deadline.subsec_nanos()),
            };
            let count = self
                .observer
                .kevent(&[], &mut events, Some(timeout))
                .map_err(os_error)?;
            if count == 1
                && events[0].ident() == self.identity.process().get() as usize
                && events[0].fflags().contains(FilterFlag::NOTE_EXIT)
            {
                Ok(())
            } else {
                Err(ProcessError::ExitTimeout)
            }
        }
        #[cfg(target_os = "linux")]
        {
            // SAFETY: the retained pidfd is an owned kernel handle; siginfo is
            // null as permitted for pidfd_send_signal, and flags are zero.
            let result = unsafe {
                nix::libc::syscall(
                    nix::libc::SYS_pidfd_send_signal,
                    self.pidfd.as_raw_fd(),
                    nix::libc::SIGKILL,
                    std::ptr::null::<nix::libc::siginfo_t>(),
                    0,
                )
            };
            if result < 0 {
                let error = io::Error::last_os_error();
                if !pidfd_ready(&self.pidfd, Duration::ZERO)? {
                    return Err(error.into());
                }
            }
            if pidfd_ready(&self.pidfd, deadline)? {
                Ok(())
            } else {
                Err(ProcessError::ExitTimeout)
            }
        }
    }
    #[cfg(target_os = "macos")]
    fn exit_observed(&self, deadline: Duration) -> Result<bool, ProcessError> {
        use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent};
        let mut events = [KEvent::new(
            0,
            EventFilter::EVFILT_PROC,
            EvFlags::empty(),
            FilterFlag::empty(),
            0,
            0,
        )];
        let count = self
            .observer
            .kevent(
                &[],
                &mut events,
                Some(nix::libc::timespec {
                    tv_sec: i64::try_from(deadline.as_secs())
                        .map_err(|_| ProcessError::Identity)?,
                    tv_nsec: i64::from(deadline.subsec_nanos()),
                }),
            )
            .map_err(os_error)?;
        Ok(count == 1
            && events[0].ident() == self.identity.process().get() as usize
            && events[0].fflags().contains(FilterFlag::NOTE_EXIT))
    }
}

fn os_error(error: nix::errno::Errno) -> ProcessError {
    io::Error::from_raw_os_error(error as i32).into()
}
#[cfg(target_os = "macos")]
fn check_code(code: i32) -> Result<(), ProcessError> {
    if code == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(code).into())
    }
}

#[cfg(any(target_os = "macos", test))]
fn completed_observation(code: i32) -> Result<bool, ProcessError> {
    match code {
        1 => Ok(true),
        0 => Err(ProcessError::AmbiguousIncarnation),
        code if code < 0 => {
            let errno = code.checked_neg().ok_or(ProcessError::Identity)?;
            Err(io::Error::from_raw_os_error(errno).into())
        }
        _ => Err(ProcessError::Identity),
    }
}
#[cfg(target_os = "macos")]
fn validate_token(token: &DarwinIdentity) -> Result<(), ProcessError> {
    // SAFETY: the helper only reads the eight words in this fixed-size token.
    check_code(unsafe { muxe_process_validate_token(token.0.as_ptr()) })
}
#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn muxe_process_boot_id(buffer: *mut std::ffi::c_char, capacity: u32) -> i32;
    fn muxe_process_validate_token(token: *const u32) -> i32;
    fn muxe_process_current_token(token: *mut u32) -> i32;
    fn muxe_process_socket_peer(fd: i32, pid: i32, token: *const u32) -> i32;
    fn muxe_process_signal_token(token: *const u32) -> i32;
    fn muxe_process_pid_completed(pid: i32) -> i32;
}
#[cfg(target_os = "linux")]
fn pidfd_ready(fd: &impl AsFd, deadline: Duration) -> Result<bool, ProcessError> {
    let mut event = nix::libc::pollfd {
        fd: fd.as_fd().as_raw_fd(),
        events: nix::libc::POLLIN,
        revents: 0,
    };
    let millis = i32::try_from(deadline.as_millis()).map_err(|_| ProcessError::Identity)?;
    // SAFETY: poll receives one initialized live borrowed fd and its exact length.
    let result = unsafe { nix::libc::poll(&raw mut event, 1, millis) };
    if result < 0 {
        return Err(io::Error::last_os_error().into());
    }
    if event.revents & (nix::libc::POLLERR | nix::libc::POLLNVAL) != 0 {
        return Err(ProcessError::Identity);
    }
    Ok(result == 1 && event.revents & nix::libc::POLLIN != 0)
}
#[cfg(target_os = "linux")]
fn linux_start(process: ProcessId) -> Result<LinuxStartTime, ProcessError> {
    let contents = std::fs::read_to_string(format!("/proc/{}/stat", process.get()))?;
    let command_end = contents.rfind(')').ok_or(ProcessError::Identity)?;
    let start_ticks = contents[command_end + 1..]
        .split_ascii_whitespace()
        .nth(19)
        .ok_or(ProcessError::Identity)?
        .parse()
        .map_err(|_| ProcessError::Identity)?;
    Ok(LinuxStartTime(start_ticks))
}

#[cfg(target_os = "linux")]
fn open_pidfd(process: ProcessId) -> Result<Option<std::os::fd::OwnedFd>, ProcessError> {
    use std::os::fd::FromRawFd;
    // SAFETY: pidfd_open creates an owned kernel handle, flags are zero.
    let fd = unsafe { nix::libc::syscall(nix::libc::SYS_pidfd_open, process.get(), 0) };
    if fd < 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(nix::libc::ESRCH) {
            return Ok(None);
        }
        return Err(error.into());
    }
    let fd = i32::try_from(fd).map_err(|_| ProcessError::Identity)?;
    // SAFETY: this fd was just created successfully and has one owner.
    Ok(Some(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) }))
}

#[cfg(test)]
mod tests;
