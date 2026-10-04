use std::io;
use std::path::Path;

/// Keep executable writers out of this process: a concurrent fork can retain
/// a writable inode even after our File closes, making direct exec fail with
/// ETXTBSY. Reap the isolated writer before making the completed file runnable.
pub(super) fn write_executable_script(
    path: &Path,
    write: impl FnOnce(&mut std::process::ChildStdin) -> io::Result<()>,
) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    use std::process::Stdio;

    let mut child = std::process::Command::new("/bin/sh")
        .args([
            "-c",
            "umask 077; set -C; exec /bin/cat > \"$1\"",
            "fixture-writer",
        ])
        .arg(path)
        .env_clear()
        .current_dir(
            path.parent()
                .ok_or_else(|| io::Error::other("script has no parent"))?,
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let written = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("script writer has no stdin"))
        .and_then(|mut stdin| write(&mut stdin));
    // The writer sees EOF here, including when writing failed. Always reap it.
    let output = child.wait_with_output().map_err(|error| {
        io::Error::other(format!(
            "script writer wait failed: {error}; write: {written:?}"
        ))
    })?;
    if !output.status.success() || written.is_err() {
        return Err(io::Error::other(format!(
            "script writer for {} failed ({}); write: {written:?}; stderr: {}",
            path.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr),
        )));
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}
