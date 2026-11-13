//! nexio — a verified, single-core io_uring runtime.
//!
//! # Design constraints
//!
//! 1. **Owned buffers only.** The kernel holds a raw pointer from SQE
//!    submission until the matching CQE arrives. Borrowed `&mut [u8]`
//!    cannot model an external, uncancellable writer.
//!
//! 2. **Leak-until-real-completion cancellation.** Dropping an in-flight
//!    op submits a best-effort `ASYNC_CANCEL` and moves the buffer into
//!    the reactor slab. Buffer freed only when the real CQE lands.
//!
//! 3. **`!Send` tasks, one reactor per thread.** No work-stealing.
//!    Multi-core: N independent reactors + SO_REUSEPORT sharding.
//!
//! 4. **Verification-first.** Pure-Rust layer (slab, Op state machine)
//!    targets Miri + Kani. Syscall-adjacent layer targets integration
//!    tests at high queue depth.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs, unreachable_pub)]

pub mod addr;
pub mod blocking;
pub mod buf;
pub mod compat;
pub mod executor;
#[cfg(all(nexio_uring, not(nexio_runtime_fallback)))]
pub mod fs;
#[cfg(nexio_poll)]
#[path = "poll/fs.rs"]
pub mod fs;
#[cfg(nexio_runtime_fallback)]
#[path = "fallback/fs.rs"]
pub mod fs;
mod fs_ops;
#[cfg(feature = "http")]
pub mod http;
#[macro_use]
mod net_split;
#[cfg(all(nexio_uring, not(nexio_runtime_fallback)))]
pub mod net;
#[cfg(nexio_poll)]
#[path = "poll/net.rs"]
pub mod net;
#[cfg(nexio_runtime_fallback)]
#[path = "fallback/net.rs"]
pub mod net;
pub mod process;
pub mod profile;
#[cfg(all(nexio_uring, not(nexio_runtime_fallback)))]
pub mod reactor;
#[cfg(nexio_poll)]
#[path = "poll/reactor.rs"]
pub mod reactor;
#[cfg(nexio_runtime_fallback)]
#[path = "fallback/reactor.rs"]
pub mod reactor;
mod shard;
pub mod signal;
pub mod sync;
pub mod time;
#[cfg(feature = "tls")]
pub mod tls;

pub use blocking::spawn_blocking;
pub use executor::{block_on, join2, select2, spawn, yield_now, JoinSet};
#[cfg(feature = "macros")]
pub use nexio_macros::{main, test};
pub use profile::HardwareProfile;
pub use reactor::Reactor;
pub use shard::{run_sharded, run_sharded_auto};

/// Return `true` if `io_uring` is compiled in and permitted by the kernel/sandbox
/// in the current process.
///
/// The probe result is cached on first call. Returns `false` on non-Linux targets,
/// when compiled with `--features poll`, or when `io_uring_setup` is blocked
/// (e.g. `EPERM`/`ENOSYS` in restricted containers).
pub fn io_uring_available() -> bool {
    #[cfg(nexio_uring)]
    {
        static AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *AVAILABLE.get_or_init(|| {
            if std::env::var_os("NEXIO_FORCE_POLL").is_some() {
                return false;
            }
            io_uring::IoUring::new(2).is_ok()
        })
    }
    #[cfg(not(nexio_uring))]
    {
        false
    }
}
