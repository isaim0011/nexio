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
pub mod executor;
#[cfg(nexio_uring)]
pub mod fs;
#[cfg(nexio_poll)]
#[path = "poll/fs.rs"]
pub mod fs;
#[cfg(nexio_uring)]
pub mod net;
#[cfg(nexio_poll)]
#[path = "poll/net.rs"]
pub mod net;
pub mod process;
#[cfg(nexio_uring)]
pub mod reactor;
#[cfg(nexio_poll)]
#[path = "poll/reactor.rs"]
pub mod reactor;
mod shard;
pub mod signal;
pub mod time;

pub use blocking::spawn_blocking;
pub use executor::{block_on, join2, select2, spawn, yield_now, JoinSet};
pub use reactor::Reactor;
pub use shard::run_sharded;
