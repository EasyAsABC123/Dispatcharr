use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf, process::Command};

fn run(command: &mut Command) {
    let status = command.status().expect("start native XML build step");
    assert!(
        status.success(),
        "native XML build step failed: {command:?}"
    );
}
fn main() {
    assert_eq!(
        env::var("CARGO_CFG_TARGET_OS").unwrap(),
        "linux",
        "native XML parser currently supports Linux builds"
    );
    assert_eq!(
        env::var("HOST").unwrap(),
        env::var("TARGET").unwrap(),
        "XML cross compilation is not yet verified"
    );
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let archive = root.join("vendor/libxml2-2.14.6.tar.xz");
    let expected = "7ce458a0affeb83f0b55f1f4f9e0e55735dbfc1a9de124ee86fb4a66b597203a";
    assert_eq!(
        format!("{:x}", Sha256::digest(fs::read(&archive).unwrap())),
        expected,
        "vendored original XML parser checksum mismatch"
    );
    let source = out.join("libxml2-2.14.6");
    if !source.join(".libs/libxml2.a").exists() {
        if source.exists() {
            fs::remove_dir_all(&source).unwrap();
        }
        run(Command::new("tar")
            .args(["--extract", "--xz", "--no-same-owner", "--file"])
            .arg(&archive)
            .arg("--directory")
            .arg(&out));
        run(Command::new(source.join("configure"))
            .current_dir(&source)
            .args([
                "--disable-shared",
                "--enable-static",
                "--with-pic",
                "--without-python",
                "--without-zlib",
                "--without-lzma",
                "--without-http",
            ]));
        let jobs = env::var("NUM_JOBS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1)
            .clamp(1, 4);
        run(Command::new("make")
            .current_dir(&source)
            .arg(format!("-j{jobs}"))
            .arg("libxml2.la"));
    }
    cc::Build::new()
        .file(root.join("native/shim.c"))
        .include(source.join("include"))
        .flag_if_supported("-std=c11")
        .compile("dispatcharr_xmltv_bridge");
    println!(
        "cargo:rustc-link-search=native={}",
        source.join(".libs").display()
    );
    println!("cargo:rustc-link-lib=static=xml2");
    println!("cargo:rustc-link-lib=m");
    for name in [
        "native/shim.c",
        "native/memory.h",
        "vendor/libxml2-2.14.6.tar.xz",
    ] {
        println!("cargo:rerun-if-changed={name}");
    }
}
