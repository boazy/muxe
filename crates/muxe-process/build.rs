fn main() {
    println!("cargo:rerun-if-changed=src/darwin.c");
    let target = std::env::var("TARGET").expect("Cargo provides the target triple");
    if target.ends_with("-apple-darwin") {
        cc::Build::new()
            .file("src/darwin.c")
            .warnings(true)
            .extra_warnings(true)
            .compile("muxe_process_darwin");
    }
}
