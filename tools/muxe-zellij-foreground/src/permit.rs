//! Test-only Zellij plugin permission seeder for the release-owned host runners.
//!
//! The pinned server grants a plugin's `request_permission` silently only
//! when its location string is already cached
//! (`zellij-server/src/plugins/zellij_exports.rs`, `request_permission`
//! → `PermissionCache::from_path_or_default(None)`), otherwise the grant
//! is an interactive pane prompt no unattended runner can answer. The
//! cache lives at the pinned default
//! (`ZELLIJ_CACHE_DIR/permissions.kdl`, KDL `"location" { Permission… }`
//! via `PermissionCache::{from_string,to_string}`), which follows the
//! process environment (scoped `XDG_CACHE_HOME` on Linux, scoped `HOME`
//! on macOS), so a seed written under the runner's scoped environment
//! lands exactly where the owned server reads — never in ambient user
//! state.
//!
//! This peer writes the grant with the pinned code itself
//! (`PermissionCache::from_path_or_default(None)` + `cache` +
//! `write_to_file`): path and format are never retyped. The runner
//! supplies only the exact managed-bridge location string (the bare
//! absolute path: `Display for RunPluginLocation::File` writes the path
//! with no `file:` prefix, and `parse` applies no normalization beyond
//! percent-decode/shellexpand) and exactly the three permissions the
//! bridge requests (`ReadApplicationState`, `ChangeApplicationState`,
//! `ReadCliPipes`). Existing grants for other plugins merge through
//! untouched. This is a scoped, URL-pinned, workflow-authorized grant
//! for the isolated fixture only — not a broad approval, and the test
//! guard `MUXE_LIVE_HOSTS_APPROVED` alone never implies it.
//!
//! Typed inputs only: one `--plugin` location string plus one or more
//! `--permission` names (parsed by the pinned `PermissionType`). There
//! is no command hook and no shell. The runner owns this process
//! (bounded timeout, preserved diagnostics).
//!
//! This binary is a test fixture only. It is never shipped in a Muxe
//! release archive; core resolves it through an independent lockfile.

use std::str::FromStr as _;

use zellij_utils::consts::ZELLIJ_PLUGIN_PERMISSIONS_CACHE;
use zellij_utils::data::PermissionType;
use zellij_utils::input::permission::PermissionCache;

fn usage() -> ! {
    eprintln!(
        "usage: muxe-zellij-permit --plugin <location-string> --permission <Name> [--permission <Name>...]"
    );
    std::process::exit(2);
}

fn main() {
    let mut plugin: Option<std::ffi::OsString> = None;
    let mut permissions: Vec<PermissionType> = Vec::new();
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--plugin" {
            if plugin.is_some() {
                usage();
            }
            plugin = Some(args.next().unwrap_or_else(|| usage()));
        } else if arg == "--permission" {
            let raw = args.next().unwrap_or_else(|| usage());
            let text = raw.to_string_lossy();
            permissions.push(PermissionType::from_str(text.as_ref()).unwrap_or_else(|_| {
                eprintln!("muxe-zellij-permit rejects unknown permission {text:?}");
                std::process::exit(2);
            }));
        } else {
            usage();
        }
    }
    let plugin = plugin.unwrap_or_else(|| usage());
    let key = plugin.to_string_lossy().into_owned();
    if key.is_empty() || permissions.is_empty() {
        usage();
    }

    let mut cache = PermissionCache::from_path_or_default(None);
    cache.cache(key.clone(), permissions.clone());
    if let Some(parent) = ZELLIJ_PLUGIN_PERMISSIONS_CACHE.parent() {
        if let Err(error) = std::fs::create_dir_all(parent) {
            eprintln!(
                "muxe-zellij-permit cannot create cache parent {}: {error}",
                parent.display()
            );
            std::process::exit(1);
        }
    }
    if let Err(error) = cache.write_to_file() {
        eprintln!("muxe-zellij-permit cannot write the permission cache: {error}");
        std::process::exit(1);
    }
    println!(
        "permitted {key} ({} permissions) at {}",
        permissions.len(),
        ZELLIJ_PLUGIN_PERMISSIONS_CACHE.display()
    );
}
