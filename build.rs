//! Chooses the I/O backend at build time.
//!
//! * Linux (default): `nexio_uring`, the io_uring backend.
//! * Every other OS, or Linux with the `poll` feature: `nexio_poll`, the
//!   portable readiness backend (epoll / kqueue / IOCP via the `polling` crate).

fn main() {
    println!("cargo:rustc-check-cfg=cfg(nexio_uring)");
    println!("cargo:rustc-check-cfg=cfg(nexio_poll)");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_POLL");

    let linux = std::env::var("CARGO_CFG_TARGET_OS")
        .map(|os| os == "linux")
        .unwrap_or(false);
    let force_poll = std::env::var_os("CARGO_FEATURE_POLL").is_some();
    if linux && !force_poll {
        println!("cargo:rustc-cfg=nexio_uring");
    } else {
        println!("cargo:rustc-cfg=nexio_poll");
    }
}
