//! Host-independent command shapes used by native launch composition.

use std::{ffi::OsStr, path::Path};

/// Reports whether an argv vector begins with Muxe's canonical UI command,
/// `muxe ui menu`.
///
/// The first element may be `muxe` itself or a path whose file name is exactly
/// `muxe`. The next two elements must be exactly `ui` and `menu`. Remaining
/// arguments are intentionally left to the selected launch path to validate.
/// This recognizes command shape only; it does not authorize a launch.
#[must_use]
pub fn is_ui_argv<T: AsRef<OsStr>>(argv: &[T]) -> bool {
    let [program, first, second, ..] = argv else {
        return false;
    };
    Path::new(program.as_ref())
        .file_name()
        .is_some_and(|name| name == "muxe")
        && first.as_ref() == OsStr::new("ui")
        && second.as_ref() == OsStr::new("menu")
}

#[cfg(test)]
mod tests {
    use super::is_ui_argv;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| (*arg).to_owned()).collect()
    }

    #[test]
    fn recognizes_only_the_canonical_ui_argv_prefix() {
        assert!(is_ui_argv(&argv(&["muxe", "ui", "menu"])));
        assert!(is_ui_argv(&argv(&["muxe", "ui", "menu", "main"])));
        assert!(is_ui_argv(&argv(&[
            "/opt/mise/shims/muxe",
            "ui",
            "menu",
            "main"
        ])));
        assert!(is_ui_argv(&argv(&["./muxe", "ui", "menu", "main"])));

        for args in [
            &[][..],
            &["muxe"][..],
            &["muxe", "ui"][..],
            &["muxe", "menu", "main"][..],
            &["muxe", "ui", "other"][..],
            &["sh", "ui", "menu"][..],
            &["/opt/muxe/not-muxe", "ui", "menu"][..],
            &["muxe", "ui", "menu main"][..],
        ] {
            let args = argv(args);
            assert!(!is_ui_argv(&args), "not a UI invocation: {args:?}");
        }
    }
}
