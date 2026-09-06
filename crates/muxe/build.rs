//! Build script for the native `muxe` executable.
//!
//! Derives the compiler target triple from the build environment (`TARGET`)
//! so the embedded compatibility record reports the real build triple instead
//! of a runtime `arch-os` approximation.

fn main() {
    let target =
        std::env::var("TARGET").unwrap_or_else(|_| "unknown-unknown-unknown".to_owned());
    println!("cargo:rustc-env=MUXE_TARGET_TRIPLE={target}");
    println!("cargo:rerun-if-env-changed=TARGET");
}
