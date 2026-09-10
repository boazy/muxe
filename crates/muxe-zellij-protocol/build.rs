use std::{
    env, fs,
    io::Read,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

const DOMAIN: &[u8] = b"muxe-bridge-build-id/v1";
// Keep this list fixed and explicit. Directory walks would hash editor or OS
// files and would make the identity depend on unreviewed workspace contents.
const INPUTS: &[&str] = &[
    "Cargo.lock",
    "Cargo.toml",
    "crates/muxe-protocol/Cargo.toml",
    "crates/muxe-protocol/src/bridge.rs",
    "crates/muxe-protocol/src/control.rs",
    "crates/muxe-protocol/src/frame.rs",
    "crates/muxe-protocol/src/lib.rs",
    "crates/muxe-protocol/src/wire.rs",
    "crates/muxe-zellij-protocol/Cargo.toml",
    "crates/muxe-zellij-protocol/build.rs",
    "crates/muxe-zellij-protocol/src/compat.rs",
    "crates/muxe-zellij-protocol/src/generated.rs",
    "crates/muxe-zellij-protocol/src/ids.rs",
    "crates/muxe-zellij-protocol/src/lib.rs",
    "crates/muxe-zellij-protocol/src/pipe.rs",
    "crates/muxe-zellij-wasm/Cargo.toml",
    "crates/muxe-zellij-wasm/src/bridge.rs",
    "crates/muxe-zellij-wasm/src/dispatcher.rs",
    "crates/muxe-zellij-wasm/src/focus.rs",
    "crates/muxe-zellij-wasm/src/main.rs",
    "crates/muxe-zellij-wasm/src/outcome.rs",
    "fixtures/zellij/0.46.0/action-converters.policy",
    "fixtures/zellij/0.46.0/action-inventory.rs",
    "fixtures/zellij/0.46.0/public-functions.policy",
    "fixtures/zellij/0.46.0/source/source-inputs.sha256",
    "mise.toml",
    "pins/zellij.toml",
];

fn main() {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let mut hasher = Sha256::new();
    frame(&mut hasher, DOMAIN);
    frame(&mut hasher, b"inputs-v1");
    for relative in INPUTS {
        let path = workspace_root.join(relative);
        let metadata = fs::symlink_metadata(&path)
            .unwrap_or_else(|error| panic!("stat {}: {error}", path.display()));
        assert!(
            metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
            "bridge build ID input must be a regular non-symlink file: {}",
            path.display()
        );
        frame(&mut hasher, relative.as_bytes());
        frame_length(&mut hasher, metadata.len());
        let mut file = fs::File::open(&path)
            .unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
        let expected_len = metadata.len();
        let mut read_len = 0_u64;
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            let count = file
                .read(&mut buffer)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            if count == 0 {
                break;
            }
            read_len = read_len
                .checked_add(u64::try_from(count).expect("read chunk fits u64"))
                .expect("input length fits u64");
            assert!(
                read_len <= expected_len,
                "bridge build ID input changed while hashing: {}",
                path.display()
            );
            hasher.update(&buffer[..count]);
        }
        assert_eq!(
            read_len,
            expected_len,
            "bridge build ID input changed while hashing: {}",
            path.display()
        );
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    fs::write(
        out_dir.join("bridge_build_id.rs"),
        format!(
            "pub const BRIDGE_BUILD_ID: [u8; 32] = {digest:?};\n\
             pub const BRIDGE_BUILD_ID_HEX: &str = \"{hex}\";\n"
        ),
    )
    .expect("write bridge build ID");
}

fn frame(hasher: &mut Sha256, value: &[u8]) {
    frame_length(
        hasher,
        u64::try_from(value.len()).expect("framed input fits u64"),
    );
    hasher.update(value);
}

fn frame_length(hasher: &mut Sha256, length: u64) {
    hasher.update(length.to_be_bytes());
}
