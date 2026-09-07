//! Fixed argv for the hidden `muxe broker serve-herdr` child mode.
//!
//! The repository-owned upgrade runner starts old and target Herdr brokers only
//! through [`ServeHerdrSpawn`]: typed absolute paths, no shell, no command hook.
//! The argv order is part of the runner contract: `broker serve-herdr --socket
//! {socket} --herdr-binary {herdr} --herdr-socket {herdr_socket} --config
//! {config} --cache-dir {cache}` with the target pair `--handoff {hex}
//! --activation-journal {journal}` appended together or not at all.

use std::{ffi::OsString, path::PathBuf};

use data_encoding::HEXLOWER;
use muxe_protocol::control::HandoffId;
use thiserror::Error;

/// Typed inputs for one `muxe broker serve-herdr` child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServeHerdrSpawn {
    /// The `muxe` executable to run (old or target installation).
    pub binary: PathBuf,
    /// Absolute normal broker endpoint socket the child must serve.
    pub socket: PathBuf,
    /// Absolute pinned Herdr binary the child must validate against.
    pub herdr_binary: PathBuf,
    /// Absolute live Herdr server socket the child must attach to.
    pub herdr_socket: PathBuf,
    /// Absolute broker configuration file.
    pub config: PathBuf,
    /// Absolute cache directory (registry, Herdr schema cache, journals).
    pub cache_dir: PathBuf,
    /// Target handoff; always paired with `activation_journal`.
    pub handoff: Option<HandoffId>,
    /// Durable activation journal authorizing the target; always paired with `handoff`.
    pub activation_journal: Option<PathBuf>,
}

/// Failure to render a [`ServeHerdrSpawn`] as argv.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum SpawnArgvError {
    /// The handoff/journal pair must be supplied together or not at all.
    #[error("broker target startup requires both --handoff and --activation-journal")]
    HalfHandoff,
}

impl ServeHerdrSpawn {
    /// Renders the exact child argv. The handoff pair is all-or-nothing, mirroring
    /// the CLI parser and the broker's target startup requirement.
    ///
    /// # Errors
    ///
    /// Returns `SpawnArgvError::HalfHandoff` when only one of the handoff pair is set.
    pub fn argv(&self) -> Result<Vec<OsString>, SpawnArgvError> {
        match (&self.handoff, &self.activation_journal) {
            (Some(_), None) | (None, Some(_)) => return Err(SpawnArgvError::HalfHandoff),
            _ => {}
        }
        let mut argv = vec![
            OsString::from("broker"),
            OsString::from("serve-herdr"),
            OsString::from("--socket"),
            self.socket.clone().into(),
            OsString::from("--herdr-binary"),
            self.herdr_binary.clone().into(),
            OsString::from("--herdr-socket"),
            self.herdr_socket.clone().into(),
            OsString::from("--config"),
            self.config.clone().into(),
            OsString::from("--cache-dir"),
            self.cache_dir.clone().into(),
        ];
        if let (Some(handoff), Some(journal)) = (&self.handoff, &self.activation_journal) {
            argv.push(OsString::from("--handoff"));
            argv.push(HEXLOWER.encode(&handoff.0).into());
            argv.push(OsString::from("--activation-journal"));
            argv.push(journal.clone().into());
        }
        Ok(argv)
    }
}

/// Typed inputs for one `muxe broker serve-zellij` child. Mirrors
/// [`ServeHerdrSpawn`] with the Zellij session name and executable in place of
/// the Herdr socket pair; the journal carries the group bridge path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServeZellijSpawn {
    /// The `muxe` executable to run (old or target installation).
    pub binary: PathBuf,
    /// Absolute normal broker endpoint socket the child must serve.
    pub socket: PathBuf,
    /// Absolute pinned Zellij binary the child must serve through.
    pub zellij_exe: PathBuf,
    /// Live Zellij session name the child serves.
    pub session: String,
    /// Absolute broker configuration file.
    pub config: PathBuf,
    /// Absolute cache directory (registry, journals).
    pub cache_dir: PathBuf,
    /// Target handoff; always paired with `activation_journal`.
    pub handoff: Option<HandoffId>,
    /// Durable activation journal authorizing the target; always paired with `handoff`.
    pub activation_journal: Option<PathBuf>,
}

impl ServeZellijSpawn {
    /// Renders the exact child argv, in CLI parser order. The handoff pair is
    /// all-or-nothing, mirroring the parser and the broker's target requirement.
    ///
    /// # Errors
    ///
    /// Returns `SpawnArgvError::HalfHandoff` when only one of the handoff pair is set.
    pub fn argv(&self) -> Result<Vec<OsString>, SpawnArgvError> {
        match (&self.handoff, &self.activation_journal) {
            (Some(_), None) | (None, Some(_)) => return Err(SpawnArgvError::HalfHandoff),
            _ => {}
        }
        let mut argv = vec![
            OsString::from("broker"),
            OsString::from("serve-zellij"),
            OsString::from("--socket"),
            self.socket.clone().into(),
            OsString::from("--zellij-exe"),
            self.zellij_exe.clone().into(),
            OsString::from("--session"),
            self.session.clone().into(),
            OsString::from("--config"),
            self.config.clone().into(),
            OsString::from("--cache-dir"),
            self.cache_dir.clone().into(),
        ];
        if let (Some(handoff), Some(journal)) = (&self.handoff, &self.activation_journal) {
            argv.push(OsString::from("--handoff"));
            argv.push(HEXLOWER.encode(&handoff.0).into());
            argv.push(OsString::from("--activation-journal"));
            argv.push(journal.clone().into());
        }
        Ok(argv)
    }
}
