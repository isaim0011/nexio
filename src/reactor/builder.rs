//! Reactor builder for configurable queue depth, SQPOLL, and kernel taskrun flags.

use std::io;

use io_uring::IoUring;

use super::{slab::CompletionSlab, Reactor};

/// Configurable builder for [`Reactor`].
#[derive(Debug, Clone)]
pub struct ReactorBuilder {
    pub(crate) entries: u32,
    pub(crate) entries_explicit: bool,
    pub(crate) coop_taskrun: bool,
    pub(crate) sqpoll: Option<u32>,
}

impl Default for ReactorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReactorBuilder {
    /// Create a new `ReactorBuilder` with hardware-adaptive default parameters:
    /// - `entries`: derived from [`HardwareProfile::get().ring_entries()`](crate::HardwareProfile::ring_entries)
    /// - `coop_taskrun`: false (opt-in via [`coop_taskrun(true)`](Self::coop_taskrun) or `coop-taskrun` feature)
    /// - `sqpoll`: None (opt-in, default off)
    #[allow(unexpected_cfgs)]
    pub fn new() -> Self {
        #[cfg(feature = "coop-taskrun")]
        let coop = true;
        #[cfg(not(feature = "coop-taskrun"))]
        let coop = false;

        Self {
            entries: crate::profile::HardwareProfile::get().ring_entries(),
            entries_explicit: false,
            coop_taskrun: coop,
            sqpoll: None,
        }
    }

    /// Set the submission and completion queue depth.
    pub fn entries(mut self, entries: u32) -> Self {
        self.entries = entries;
        self.entries_explicit = true;
        self
    }

    /// Enable or disable cooperative taskrun (`COOP_TASKRUN`, `DEFER_TASKRUN`, `SINGLE_ISSUER`).
    pub fn coop_taskrun(mut self, enabled: bool) -> Self {
        self.coop_taskrun = enabled;
        self
    }

    /// Opt-in to kernel SQPOLL with the given idle timeout in milliseconds.
    ///
    /// Pass `None` to disable SQPOLL (default).
    pub fn sqpoll(mut self, idle: Option<u32>) -> Self {
        self.sqpoll = idle;
        self
    }

    /// Build the [`Reactor`] instance.
    pub fn build(&self) -> io::Result<Reactor> {
        let mut entries = self.entries.max(4);
        loop {
            match self.try_build_ring(entries) {
                Ok(ring) => {
                    return Ok(Reactor {
                        ring,
                        slab: CompletionSlab::new(),
                        multishot_ops: ::slab::Slab::new(),
                        timers: std::collections::BTreeMap::new(),
                        next_timer_id: 0,
                    });
                }
                Err(err)
                    if !self.entries_explicit
                        && err.raw_os_error() == Some(libc::ENOMEM)
                        && entries > 16 =>
                {
                    // Step down adaptive ring depth if RLIMIT_MEMLOCK / pinned memory is tight.
                    entries = (entries / 2).max(16);
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn try_build_ring(&self, entries: u32) -> io::Result<IoUring> {
        let mut builder = IoUring::builder();

        if self.coop_taskrun && self.sqpoll.is_none() {
            builder.setup_coop_taskrun();
            builder.setup_defer_taskrun();
            builder.setup_single_issuer();
        }

        if let Some(idle) = self.sqpoll {
            builder.setup_sqpoll(idle);
        }

        builder.build(entries).or_else(|err| {
            // Clean fallback to basic ring if coop flags fail on older kernels
            if self.coop_taskrun && self.sqpoll.is_none() {
                IoUring::new(entries)
            } else {
                Err(err)
            }
        })
    }
}
