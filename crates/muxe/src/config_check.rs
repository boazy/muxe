use std::{
    collections::BTreeMap,
    fs,
    io::{self, Write},
    path::Path,
};

use ariadne::{Config, IndexType, Label, Report, ReportKind, sources};
use muxe_adapter_api::HostKind as AdapterHostKind;
use muxe_adapter_herdr::HerdrConfigValidator;
use muxe_adapter_zellij::ZellijValidator;
use muxe_broker::{ConfigError, load_effective_config};
use muxe_core::{ActionValidator, ConfigDiagnostic, DiagnosticSeverity, KeyCapabilities};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CheckError {
    #[error("could not read diagnostic source {path}: {source}")]
    Source { path: String, source: io::Error },
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
        AdapterHostKind::Zellij,
        &zellij,
        output,
    )?;
    match HerdrConfigValidator::load(herdr_binary, cache_dir).await {
        Ok(herdr) => {
            valid &= check_host(config_path, "herdr", AdapterHostKind::Herdr, &herdr, output)?;
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
    checks: &[(&str, AdapterHostKind, &dyn ActionValidator)],
    output: &mut dyn Write,
) -> Result<bool, CheckError> {
    let mut valid = true;
    for &(host_name, host, validator) in checks {
        valid &= check_host(config_path, host_name, host, validator, output)?;
    }
    Ok(valid)
}

fn check_host(
    config_path: &Path,
    host_name: &str,
    host: AdapterHostKind,
    validator: &dyn ActionValidator,
    output: &mut dyn Write,
) -> Result<bool, CheckError> {
    match load_effective_config(config_path, host, KeyCapabilities::default(), validator) {
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
        ConfigError::Diagnostic(diagnostic) => {
            render_diagnostics(host_name, std::slice::from_ref(diagnostic), output)
        }
        ConfigError::Diagnostics(diagnostics) => render_diagnostics(host_name, diagnostics, output),
        _ => {
            writeln!(output, "{host_name} configuration: {error}")?;
            Ok(())
        }
    }
}

fn render_diagnostics(
    host_name: &str,
    diagnostics: &[ConfigDiagnostic],
    output: &mut dyn Write,
) -> Result<(), CheckError> {
    let source_text = diagnostic_sources(diagnostics)?;
    for diagnostic in diagnostics {
        let Some((primary_index, primary)) = diagnostic
            .labels
            .iter()
            .enumerate()
            .find(|(_, label)| source_text.contains_key(label.span.source.as_str()))
        else {
            render_source_less_diagnostic(host_name, diagnostic, output)?;
            continue;
        };
        let kind = match diagnostic.severity {
            DiagnosticSeverity::Error => ReportKind::Error,
            DiagnosticSeverity::Warning => ReportKind::Warning,
        };
        let mut report = Report::build(
            kind,
            (
                primary.span.source.as_str().to_owned(),
                primary.span.start..primary.span.end,
            ),
        )
        .with_config(
            Config::default()
                .with_color(false)
                .with_index_type(IndexType::Byte),
        )
        .with_code(format!("{host_name}/{}", diagnostic.code.as_str()))
        .with_message(diagnostic.message.clone());
        for (index, label) in diagnostic
            .labels
            .iter()
            .enumerate()
            .filter(|(_, label)| source_text.contains_key(label.span.source.as_str()))
        {
            let rendered = Label::new((
                label.span.source.as_str().to_owned(),
                label.span.start..label.span.end,
            ));
            report = if label.message.is_empty() {
                if index == primary_index {
                    report.with_label(rendered.with_message("here"))
                } else {
                    report.with_label(rendered)
                }
            } else {
                report.with_label(rendered.with_message(label.message.clone()))
            };
        }
        for note in &diagnostic.notes {
            report = report.with_note(note.clone());
        }
        if let Some(help) = &diagnostic.help {
            report = report.with_help(help.clone());
        }
        report
            .finish()
            .write(sources(source_text.clone()), &mut *output)?;
    }
    Ok(())
}

fn render_source_less_diagnostic(
    host_name: &str,
    diagnostic: &ConfigDiagnostic,
    output: &mut dyn Write,
) -> Result<(), CheckError> {
    writeln!(
        output,
        "{host_name} configuration {}: {}",
        diagnostic.code.as_str(),
        diagnostic.message
    )?;
    for note in &diagnostic.notes {
        writeln!(output, "note: {note}")?;
    }
    if let Some(help) = &diagnostic.help {
        writeln!(output, "help: {help}")?;
    }
    Ok(())
}

fn is_virtual_source(source: &str) -> bool {
    source.starts_with('<') && source.ends_with('>')
}

fn diagnostic_sources(
    diagnostics: &[ConfigDiagnostic],
) -> Result<BTreeMap<String, String>, CheckError> {
    let mut source_text = BTreeMap::new();
    for diagnostic in diagnostics {
        for label in &diagnostic.labels {
            let path = label.span.source.as_str();
            if source_text.contains_key(path) {
                continue;
            }
            if is_virtual_source(path) {
                continue;
            }
            let text = fs::read_to_string(path).map_err(|source| CheckError::Source {
                path: path.to_owned(),
                source,
            })?;
            source_text.insert(path.to_owned(), text);
        }
    }
    Ok(source_text)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use muxe_core::{ConfigDiagnostic, DiagnosticCode, SourceId, SourceSpan};
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
        let checks: [(&str, AdapterHostKind, &dyn ActionValidator); 2] = [
            ("zellij", AdapterHostKind::Zellij, &zellij),
            ("herdr", AdapterHostKind::Herdr, &zellij),
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
                AdapterHostKind::Zellij,
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
                AdapterHostKind::Zellij,
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
        assert!(!output.contains("<muxe built-in>"), "{output}");
    }

    #[test]
    fn ariadne_uses_byte_offsets_for_non_ascii_source_text() {
        let directory = tempfile::tempdir().expect("diagnostic source directory");
        let path = directory.path().join("config.yml");
        fs::write(&path, "é: x\n").expect("diagnostic source writes");
        let diagnostic = ConfigDiagnostic::error(
            DiagnosticCode::InvalidValue,
            "invalid value",
            SourceSpan::new(SourceId::new(path.display().to_string()), 4, 5),
        );

        let mut output = Vec::new();
        render_diagnostics("zellij", &[diagnostic], &mut output).expect("diagnostic renders");
        let output = String::from_utf8(output).expect("diagnostic is UTF-8");
        assert!(
            output.contains(&format!("{}:1:4", path.display())),
            "{output}"
        );
        assert!(output.contains("here"), "{output}");
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
            AdapterHostKind::Zellij,
            &zellij as &dyn ActionValidator,
        )];
        let mut output = Vec::new();
        assert!(!check_hosts(&config, &checks, &mut output).expect("cycle reports"));
        let output = String::from_utf8(output).expect("diagnostics are UTF-8");
        assert!(output.contains("config.yml:12:"));
    }
}
