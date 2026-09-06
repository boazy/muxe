//! Release-only shell completion generator.
//!
//! Generates Bash, Zsh, and Fish completion files from the `clap` command
//! definition (`muxe::cli::Cli`, the same parser the executable uses) and
//! writes them under `share/muxe/completions/`. The generated files are
//! committed; CI runs `--check` and fails on any byte-for-byte diff.
//!
//! Completion-generation dependencies stay in this tool and are never linked
//! into the installed `muxe` executable.

use std::{
    fs,
    path::{Path, PathBuf},
};

use clap::{CommandFactory, Parser};
use clap_complete::{Shell, generate_to};
use muxe::cli::Cli;

const FILES: [(Shell, &str); 3] = [
    (Shell::Bash, "muxe.bash"),
    (Shell::Zsh, "muxe.zsh"),
    (Shell::Fish, "muxe.fish"),
];

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
    if let Err(error) = run(Args::parse()) {
        eprintln!("codegen-completions: {error}");
        std::process::exit(1);
    }
}

fn run(args: Args) -> Result<(), String> {
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

fn verify(out: &Path, command: &mut clap::Command) -> Result<(), String> {
    let staging = out.join(".codegen-check.tmp");
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .map_err(|error| format!("cannot clear {}: {error}", staging.display()))?;
    }
    fs::create_dir_all(&staging)
        .map_err(|error| format!("cannot create {}: {error}", staging.display()))?;
    let result = (|| {
        for (shell, file) in FILES {
            generate_to(shell, &mut command.clone(), "muxe", &staging)
                .map_err(|error| format!("cannot generate {file}: {error}"))?;
        }
        for (_, file) in FILES {
            let expected = fs::read(staging.join(file))
                .map_err(|error| format!("cannot read staged {file}: {error}"))?;
            let actual = fs::read(out.join(file)).map_err(|error| {
                format!("committed {} missing or unreadable: {error}", out.join(file).display())
            })?;
            if expected != actual {
                return Err(format!(
                    "completion file {file} differs from generation; regenerate and commit"
                ));
            }
        }
        Ok(())
    })();
    let _ = fs::remove_dir_all(&staging);
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
}
