use std::env;
use std::fs;
use std::path::Path;

const PREBUILT_GUEST_DIR: &str = "ENCRYPTED_SPACES_PREBUILT_GUEST_DIR";

pub fn embed(package: &str) -> bool {
    println!("cargo:rerun-if-env-changed={PREBUILT_GUEST_DIR}");
    let Some(root) = env::var_os(PREBUILT_GUEST_DIR) else {
        return false;
    };
    let root = fs::canonicalize(Path::new(&root)).expect("canonicalize prebuilt guest directory");
    let source = root.join(package).join("methods.rs");
    let output = Path::new(&env::var_os("OUT_DIR").expect("OUT_DIR is set")).join("methods.rs");
    assert!(
        source.is_file(),
        "prebuilt guest methods are missing for {package}: {}",
        source.display()
    );
    fs::copy(&source, &output).expect("copy prebuilt guest methods");
    println!("cargo:rerun-if-changed={}", source.display());
    println!(
        "cargo:rustc-env={PREBUILT_GUEST_DIR}={}",
        root.to_str().expect("prebuilt guest directory is UTF-8")
    );
    true
}
