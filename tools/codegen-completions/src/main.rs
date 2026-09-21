//! Release-only shell completion generator.
//!
//! Generates Bash, Zsh, and Fish completion files from the `clap` command
//! definition (`muxe::cli::Cli`, the same parser the executable uses) and
//! writes them under `share/muxe/completions/`. The generated files are
//! committed; CI runs `--check` and fails on any byte-for-byte diff.
//!
//! Completion-generation dependencies stay in this tool and are never linked
//! into the installed `muxe` executable.

use clap::{CommandFactory, Parser};
use clap_complete::{Shell, generate_to};
use muxe::cli::Cli;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

/// File names are the exact `clap_complete::generate_to` outputs: Zsh uses
/// the conventional `_muxe` fpath name, not `muxe.zsh`.
const FILES: [(Shell, &str); 3] = [
    (Shell::Bash, "muxe.bash"),
    (Shell::Zsh, "_muxe"),
    (Shell::Fish, "muxe.fish"),
];
static NEXT_STAGING_ID: AtomicU64 = AtomicU64::new(0);
/// Generate completions from the clap definition.
#[derive(Debug, Parser)]
#[command(name = "codegen-completions")]
struct Args {
    /// Directory receiving the generated files.
    #[arg(long, default_value = "share/muxe/completions")]
    out: PathBuf,
    /// Verify committed files match generation instead of writing.
    #[arg(long)]
    check: bool,
}

fn main() {
    if let Err(error) = run(&Args::parse()) {
        eprintln!("codegen-completions: {error}");
        std::process::exit(1);
    }
}

fn run(args: &Args) -> Result<(), String> {
    let mut command = Cli::command();
    if args.check {
        verify(&args.out, &mut command)
    } else {
        generate(&args.out, &mut command)
    }
}

fn generate(out: &Path, command: &mut clap::Command) -> Result<(), String> {
    fs::create_dir_all(out).map_err(|error| format!("cannot create {}: {error}", out.display()))?;
    for (shell, file) in FILES {
        generate_to(shell, &mut command.clone(), "muxe", out)
            .map_err(|error| format!("cannot generate {file}: {error}"))?;
    }
    sync_dir(out)?;
    Ok(())
}

struct StagingDir {
    path: PathBuf,
}

impl StagingDir {
    fn new() -> Result<Self, String> {
        let base = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("cannot determine staging timestamp: {error}"))?
            .as_nanos();
        let process_id = std::process::id();

        for attempt in 0..100 {
            let sequence = NEXT_STAGING_ID.fetch_add(1, Ordering::Relaxed);
            let path = base.join(format!(
                "muxe-codegen-check-{process_id}-{timestamp}-{sequence}-{attempt}"
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(format!("cannot create {}: {error}", path.display()));
                }
            }
        }

        Err(format!(
            "cannot create a unique staging directory in {}",
            base.display()
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for StagingDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn verify(out: &Path, command: &mut clap::Command) -> Result<(), String> {
    let staging = StagingDir::new()?;
    let result = (|| {
        for (shell, file) in FILES {
            generate_to(shell, &mut command.clone(), "muxe", staging.path())
                .map_err(|error| format!("cannot generate {file}: {error}"))?;
        }
        for (_, file) in FILES {
            let expected = fs::read(staging.path().join(file))
                .map_err(|error| format!("cannot read staged {file}: {error}"))?;
            let actual = fs::read(out.join(file)).map_err(|error| {
                format!(
                    "committed {} missing or unreadable: {error}",
                    out.join(file).display()
                )
            })?;
            if expected != actual {
                return Err(format!(
                    "completion file {file} differs from generation; regenerate and commit"
                ));
            }
        }
        Ok(())
    })();
    drop(staging);
    result
}

fn sync_dir(directory: &Path) -> Result<(), String> {
    fs::File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|error| format!("cannot sync {}: {error}", directory.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_matches_parser_tree() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        generate(temp.path(), &mut Cli::command()).expect("generate");
        for (_, file) in FILES {
            let text = fs::read_to_string(temp.path().join(file)).expect("read");
            assert!(text.contains("muxe"), "{file} lacks the command name");
        }
    }

    #[test]
    fn staging_directories_are_unique_for_concurrent_checks() {
        use std::{collections::HashSet, thread};

        let staging_dirs = thread::scope(|scope| {
            let handles = (0..8)
                .map(|_| scope.spawn(StagingDir::new))
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("staging thread").expect("staging dir"))
                .collect::<Vec<_>>()
        });
        let paths = staging_dirs
            .iter()
            .map(StagingDir::path)
            .collect::<HashSet<_>>();

        assert_eq!(paths.len(), staging_dirs.len());
    }
}
