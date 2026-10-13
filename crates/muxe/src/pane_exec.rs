//! Fixed persistent Herdr plugin entrypoint. The host supplies cwd and this process
//! replaces itself with the exact executable: no shell, terminal input, or launch files.

use std::os::unix::process::CommandExt;

use color_eyre::eyre::{Result, WrapErr, bail};
use muxe_adapter_herdr::{COMMAND_PANE_ARGV_ENV, COMMAND_PANE_ENTRYPOINT, COMMAND_PANE_PLUGIN_ID};

fn command(argv_json: &str) -> Result<std::process::Command> {
    let argv: Vec<String> = serde_json::from_str(argv_json)
        .wrap_err("the Herdr command-pane plugin supplied an invalid program and argument list")?;
    let Some(program) = argv.first().filter(|program| !program.is_empty()) else {
        bail!("the Herdr command-pane plugin did not supply a program to run");
    };
    if argv.iter().any(|word| word.contains('\0')) {
        bail!("the program name and arguments for a command pane must not contain NUL bytes");
    }
    let mut command = std::process::Command::new(program);
    command.args(&argv[1..]).env_remove(COMMAND_PANE_ARGV_ENV);
    Ok(command)
}

pub fn execute() -> Result<()> {
    if std::env::var("HERDR_PLUGIN_ID").as_deref() != Ok(COMMAND_PANE_PLUGIN_ID)
        || std::env::var("HERDR_PLUGIN_ENTRYPOINT_ID").as_deref() != Ok(COMMAND_PANE_ENTRYPOINT)
    {
        bail!(
            "`muxe pane exec` is reserved for the Herdr command-pane plugin; use `muxe pane open -- program arguments` to open a command pane"
        );
    }
    let payload = std::env::var(COMMAND_PANE_ARGV_ENV)
        .wrap_err("the Herdr command-pane plugin did not supply the MUXE_COMMAND_PANE_ARGV environment variable as valid text")?;
    let mut command = command(&payload)?;
    Err(command.exec()).wrap_err("could not execute the Muxe pane command")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_empty_and_nul_payloads_fail_before_exec() {
        for payload in ["{}", "[]", r#"[""]"#, r#"["echo","\u0000"]"#, "not json"] {
            assert!(command(payload).is_err(), "{payload}");
        }
    }
}
