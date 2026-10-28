fn main() {
    let target = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if matches!(target.as_str(), "linux" | "freebsd")
        && let Ok(directory) = std::env::var("DEP_GUFI_ASGI_PYTHON_LIB_DIR")
    {
        println!("cargo:rustc-link-arg-bins=-Wl,-rpath,{directory}");
    }
}
