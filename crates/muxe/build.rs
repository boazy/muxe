//! Build script for the native `muxe` executable.
//!
//! `TARGET` is Cargo-required. Release packages additionally embed the
//! producer-verified SHA-256 of the staged Zellij bridge through
//! `MUXE_WASM_SHA256`; a development Herdr-only build records the bridge as
//! unavailable rather than inventing an artifact identity.

const UNAVAILABLE: &str = "unavailable";

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && value.bytes().any(|byte| byte != b'0')
}

fn wasm_digest(profile: &str) -> String {
    match std::env::var("MUXE_WASM_SHA256") {
        Ok(digest) if is_sha256_hex(&digest) => digest,
        Ok(_) => panic!("MUXE_WASM_SHA256 must be a non-zero 64-character lowercase SHA-256"),
        Err(std::env::VarError::NotPresent)
            if profile == "release"
                || std::env::var("MUXE_REQUIRE_PACKAGED_WASM_SHA256").as_deref() == Ok("1") =>
        {
            panic!("release native builds require MUXE_WASM_SHA256 from the verified staged bridge")
        }
        Err(std::env::VarError::NotPresent) => UNAVAILABLE.to_owned(),
        Err(error) => panic!("cannot read MUXE_WASM_SHA256: {error}"),
    }
}

fn main() {
    let target = std::env::var("TARGET").expect("Cargo must set TARGET for the native build");
    let profile = std::env::var("PROFILE").expect("Cargo must set PROFILE for the native build");
    let digest = wasm_digest(&profile);
    println!("cargo:rustc-env=MUXE_TARGET_TRIPLE={target}");
    println!("cargo:rustc-env=MUXE_WASM_SHA256={digest}");
    println!("cargo:rerun-if-env-changed=TARGET");
    println!("cargo:rerun-if-env-changed=PROFILE");
    println!("cargo:rerun-if-env-changed=MUXE_WASM_SHA256");
    println!("cargo:rerun-if-env-changed=MUXE_REQUIRE_PACKAGED_WASM_SHA256");
}
