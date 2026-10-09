use muxe_adapter_herdr::{
    CONFIG_OVERRIDE_FILENAME as HERDR_CONFIG_OVERRIDE_FILENAME, HerdrConfigValidator,
};
use muxe_adapter_zellij::{
    CONFIG_OVERRIDE_FILENAME as ZELLIJ_CONFIG_OVERRIDE_FILENAME, ZellijValidator,
};
use muxe_broker::{ConfigError, load_effective_config};
use muxe_core::{ActionValidator, KeyCapabilities};
use std::{
    io::{self, Write},
    path::Path,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CheckError {
    #[error("could not render configuration diagnostics: {0}")]
    Render(#[from] io::Error),
}

/// Checks the base configuration with each supported host override and validator.
///
/// `true` means every effective host configuration compiled successfully. Invalid configuration
/// is reported to `output` and returns `false`; only diagnostic rendering failures return an error.
pub async fn check(
    config_path: &Path,
    herdr_binary: &Path,
    cache_dir: &Path,
    output: &mut dyn Write,
) -> Result<bool, CheckError> {
    let zellij = ZellijValidator;
    let mut valid = check_host(
        config_path,
        "zellij",
        ZELLIJ_CONFIG_OVERRIDE_FILENAME,
        &zellij,
        output,
    )?;
    match HerdrConfigValidator::load(herdr_binary, cache_dir).await {
        Ok(herdr) => {
            valid &= check_host(
                config_path,
                "herdr",
                HERDR_CONFIG_OVERRIDE_FILENAME,
                &herdr,
                output,
            )?;
        }
        Err(error) => {
            writeln!(
                output,
                "herdr configuration: could not load installed schema: {error}"
            )?;
            valid = false;
        }
    }
    Ok(valid)
}

#[cfg(test)]
fn check_hosts(
    config_path: &Path,
    checks: &[(&str, &'static str, &dyn ActionValidator)],
    output: &mut dyn Write,
) -> Result<bool, CheckError> {
    let mut valid = true;
    for &(host_name, override_filename, validator) in checks {
        valid &= check_host(config_path, host_name, override_filename, validator, output)?;
    }
    Ok(valid)
}

fn check_host(
    config_path: &Path,
    host_name: &str,
    override_filename: &'static str,
    validator: &dyn ActionValidator,
    output: &mut dyn Write,
) -> Result<bool, CheckError> {
    match load_effective_config(
        config_path,
        override_filename,
        KeyCapabilities::default(),
        validator,
    ) {
        Ok(_) => Ok(true),
        Err(error) => {
            render_config_error(host_name, &error, output)?;
            Ok(false)
        }
    }
}

fn render_config_error(
    host_name: &str,
    error: &ConfigError,
    output: &mut dyn Write,
) -> Result<(), CheckError> {
    match error {
        ConfigError::Diagnostic(report) | ConfigError::Diagnostics(report) => {
            report.write(Some(host_name), output)?;
            Ok(())
        }
        _ => {
            writeln!(output, "{host_name} configuration: {error}")?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    fn write_config(directory: &TempDir, name: &str, content: &str) {
        fs::write(directory.path().join(name), content).expect("configuration fixture writes");
    }

    fn base_config() -> &'static str {
        "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n"
    }

    #[test]
    fn checks_each_host_override() {
        let directory = tempfile::tempdir().expect("configuration directory");
        write_config(&directory, "config.yml", base_config());
        write_config(&directory, "zellij.yml", "invalid-zellij: true\n");
        write_config(&directory, "herdr.yml", "invalid-herdr: true\n");

        let zellij = ZellijValidator;
        let checks: [(&str, &'static str, &dyn ActionValidator); 2] = [
            ("zellij", ZELLIJ_CONFIG_OVERRIDE_FILENAME, &zellij),
            ("herdr", HERDR_CONFIG_OVERRIDE_FILENAME, &zellij),
        ];
        let mut output = Vec::new();
        assert!(
            !check_hosts(&directory.path().join("config.yml"), &checks, &mut output)
                .expect("invalid host configurations report")
        );
        let output = String::from_utf8(output).expect("diagnostics are UTF-8");
        assert!(output.contains("zellij.yml"));
        assert!(output.contains("herdr.yml"));
    }

    #[test]
    fn anchors_missing_merged_root_field_in_base_document() {
        let directory = tempfile::tempdir().expect("configuration directory");
        let config = directory.path().join("config.yml");
        fs::write(&config, "{}\n").expect("configuration fixture writes");

        let zellij = ZellijValidator;
        let mut output = Vec::new();
        assert!(
            !check_host(
                &config,
                "zellij",
                ZELLIJ_CONFIG_OVERRIDE_FILENAME,
                &zellij,
                &mut output
            )
            .expect("missing root field reports")
        );
        let output = String::from_utf8(output).expect("diagnostics are UTF-8");
        assert!(output.contains("[zellij/missing_field]"), "{output}");
        assert!(
            output.contains(&format!("{}:1:1", config.display())),
            "{output}"
        );
        assert!(!output.contains("<muxe built-in>"), "{output}");
    }

    #[test]
    fn renders_user_location_for_collision_with_virtual_builtin_binding() {
        let directory = tempfile::tempdir().expect("configuration directory");
        let config = directory.path().join("config.yml");
        write_config(
            &directory,
            "config.yml",
            "version: 1\nmenus:\n  main:\n    bindings:\n      ctrl+[:\n        label: user escape\n        action: menu:quit\n",
        );

        let zellij = ZellijValidator;
        let mut output = Vec::new();
        assert!(
            !check_host(
                &config,
                "zellij",
                ZELLIJ_CONFIG_OVERRIDE_FILENAME,
                &zellij,
                &mut output
            )
            .expect("VT100 key collision reports")
        );
        let output = String::from_utf8(output).expect("diagnostics are UTF-8");
        assert!(output.contains("[zellij/key_collision]"), "{output}");
        assert!(
            output.contains(&format!("{}:5:", config.display())),
            "{output}"
        );
        assert!(
            output.contains("first indistinguishable binding is here"),
            "{output}"
        );
    }

    #[test]
    fn reports_menu_cycles_at_the_closing_user_action() {
        let directory = tempfile::tempdir().expect("configuration directory");
        let config = directory.path().join("config.yml");
        write_config(
            &directory,
            "config.yml",
            "version: 1\nmenus:\n  a:\n    bindings:\n      a:\n        label: to b\n        action: menu:open b\n  b:\n    bindings:\n      b:\n        label: to a\n        action: menu:open a\n",
        );

        let zellij = ZellijValidator;
        let checks = [(
            "zellij",
            ZELLIJ_CONFIG_OVERRIDE_FILENAME,
            &zellij as &dyn ActionValidator,
        )];
        let mut output = Vec::new();
        assert!(!check_hosts(&config, &checks, &mut output).expect("cycle reports"));
        let output = String::from_utf8(output).expect("diagnostics are UTF-8");
        assert!(output.contains("config.yml:12:"));
    }
}
