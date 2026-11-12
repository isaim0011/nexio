//! Reactor builder for configurable queue depth, SQPOLL, and kernel taskrun flags.

use std::io;

use io_uring::IoUring;

use super::{slab::CompletionSlab, Reactor};

/// Configurable builder for [`Reactor`].
#[derive(Debug, Clone)]
pub struct ReactorBuilder {
    pub(crate) entries: u32,
    pub(crate) coop_taskrun: bool,
    pub(crate) sqpoll: Option<u32>,
}

impl Default for ReactorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReactorBuilder {
    /// Create a new `ReactorBuilder` with default parameters:
    /// - `entries`: 1024
    /// - `coop_taskrun`: true (unless `no-coop-taskrun` feature is set)
    /// - `sqpoll`: None (opt-in, default off)
    #[allow(unexpected_cfgs)]
    pub fn new() -> Self {
        #[cfg(feature = "no-coop-taskrun")]
        let coop = false;
        #[cfg(not(feature = "no-coop-taskrun"))]
        let coop = true;

        Self {
            entries: 1024,
            coop_taskrun: coop,
            sqpoll: None,
        }
    }

    /// Set the submission and completion queue depth.
    pub fn entries(mut self, entries: u32) -> Self {
        self.entries = entries;
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
        let mut builder = IoUring::builder();

        if self.coop_taskrun && self.sqpoll.is_none() {
            builder.setup_coop_taskrun();
            builder.setup_defer_taskrun();
            builder.setup_single_issuer();
        }

        if let Some(idle) = self.sqpoll {
            builder.setup_sqpoll(idle);
        }

        let ring = builder.build(self.entries).or_else(|err| {
            // Clean fallback to basic ring if coop flags fail on older kernels
            if self.coop_taskrun && self.sqpoll.is_none() {
                IoUring::new(self.entries)
            } else {
                Err(err)
            }
        })?;

        Ok(Reactor {
            ring,
            slab: CompletionSlab::new(),
        })
    }
}
