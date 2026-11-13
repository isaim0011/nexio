//! Self-adaptive hardware and OS profile.
//!
//! Probes the host's CPU budget (respecting Linux cgroup quotas), physical
//! memory, resource limits (`RLIMIT_MEMLOCK`, `RLIMIT_NOFILE`), and kernel
//! socket backlog (`somaxconn`) once per process, then derives tuning defaults
//! for the reactor, completion slab, executor, blocking pool, listeners, and
//! sharded workers.

use std::sync::OnceLock;

/// Cached hardware and OS capability profile for the current process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardwareProfile {
    cpus: usize,
    total_ram_bytes: Option<u64>,
    memlock_limit_bytes: Option<u64>,
    nofile_limit: Option<u64>,
    somaxconn: i32,
    ring_entries: u32,
    slab_capacity: usize,
    executor_capacity: usize,
    executor_min_batch: usize,
    blocking_max_threads: usize,
    listen_backlog: i32,
}

static PROFILE: OnceLock<HardwareProfile> = OnceLock::new();

impl HardwareProfile {
    /// Return the process-wide hardware profile, probing the host once on first call.
    #[inline]
    #[allow(unexpected_cfgs)]
    pub fn get() -> &'static Self {
        #[cfg(any(miri, kani))]
        {
            static SYNTHETIC: HardwareProfile =
                HardwareProfile::from_specs_const(2, None, None, None, 1024);
            &SYNTHETIC
        }
        #[cfg(not(any(miri, kani)))]
        {
            PROFILE.get_or_init(Self::probe)
        }
    }

    /// Build a [`HardwareProfile`] from explicit hardware/OS measurements.
    ///
    /// Useful for testing or inspecting how `nexio` tunes itself for a target
    /// environment without mutating global state.
    pub fn from_specs(
        cpus: usize,
        total_ram_bytes: Option<u64>,
        memlock_limit_bytes: Option<u64>,
        nofile_limit: Option<u64>,
        somaxconn: i32,
    ) -> Self {
        Self::from_specs_const(
            cpus,
            total_ram_bytes,
            memlock_limit_bytes,
            nofile_limit,
            somaxconn,
        )
    }

    const fn from_specs_const(
        cpus: usize,
        total_ram_bytes: Option<u64>,
        memlock_limit_bytes: Option<u64>,
        nofile_limit: Option<u64>,
        somaxconn: i32,
    ) -> Self {
        let cpus = if cpus == 0 { 1 } else { cpus };
        let low_ram = match total_ram_bytes {
            Some(ram) => ram < 512 * 1024 * 1024,
            None => false,
        };
        let modest_ram = match total_ram_bytes {
            Some(ram) => ram < 1024 * 1024 * 1024,
            None => false,
        };

        // Base io_uring SQ/CQ depth scaled to CPU tier & memory.
        let mut ring_entries: u32 = if low_ram || cpus == 1 {
            512
        } else if cpus <= 4 {
            1024
        } else if cpus <= 16 {
            2048
        } else {
            4096
        };

        // Respect tight RLIMIT_MEMLOCK in containers / unprivileged environments
        // (e.g. older distros / kernels < 5.12 with default 64 KiB ulimit -l).
        if let Some(memlock) = memlock_limit_bytes {
            if memlock <= 64 * 1024 {
                ring_entries = 16;
            } else if memlock <= 128 * 1024 {
                ring_entries = 32;
            } else if memlock <= 256 * 1024 {
                ring_entries = 64;
            } else if memlock < 512 * 1024 {
                ring_entries = 256;
            } else if memlock < 2 * 1024 * 1024 && ring_entries > 512 {
                ring_entries = 512;
            } else if memlock < 8 * 1024 * 1024 && ring_entries > 1024 {
                ring_entries = 1024;
            }
        }

        // Clamp slab capacity to [128, 1024].
        let raw_slab = (ring_entries / 4) as usize;
        let slab_capacity = if raw_slab < 128 {
            128
        } else if raw_slab > 1024 {
            1024
        } else {
            raw_slab
        };

        // Initial executor task/queue capacity scaled to CPU count, clamped to [128, 1024].
        let raw_exec = cpus.saturating_mul(64);
        let executor_capacity = if raw_exec < 128 {
            128
        } else if raw_exec > 1024 {
            1024
        } else {
            raw_exec
        };

        // Minimum ready-queue drain steps per tick before polling the reactor again.
        let raw_batch = cpus.saturating_mul(16);
        let executor_min_batch = if raw_batch < 64 {
            64
        } else if raw_batch > 256 {
            256
        } else {
            raw_batch
        };

        // Blocking pool cap: scale with CPUs, avoid thread explosion on small-RAM hosts.
        let raw_blocking = cpus.saturating_mul(32);
        let mut blocking_max_threads = if raw_blocking < 32 {
            32
        } else if raw_blocking > 512 {
            512
        } else {
            raw_blocking
        };
        if modest_ram && blocking_max_threads > 128 {
            blocking_max_threads = 128;
        }

        // Kernel listen backlog clamped to [128, 8192].
        let listen_backlog = if somaxconn < 128 {
            128
        } else if somaxconn > 8192 {
            8192
        } else {
            somaxconn
        };

        Self {
            cpus,
            total_ram_bytes,
            memlock_limit_bytes,
            nofile_limit,
            somaxconn,
            ring_entries,
            slab_capacity,
            executor_capacity,
            executor_min_batch,
            blocking_max_threads,
            listen_backlog,
        }
    }

    fn probe() -> Self {
        let host_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        #[cfg(target_os = "linux")]
        let cpus = match probe_cgroup_cpus() {
            Some(quota) if quota >= 1 => host_cpus.min(quota),
            _ => host_cpus,
        };
        #[cfg(not(target_os = "linux"))]
        let cpus = host_cpus;

        let total_ram_bytes = probe_total_ram();
        let memlock_limit_bytes = probe_rlimit_memlock();
        let nofile_limit = probe_rlimit_nofile();
        let somaxconn = probe_somaxconn();

        Self::from_specs(
            cpus,
            total_ram_bytes,
            memlock_limit_bytes,
            nofile_limit,
            somaxconn,
        )
    }

    /// Effective CPU count available to the process (respecting cgroup quotas on Linux).
    #[inline]
    pub fn cpus(&self) -> usize {
        self.cpus
    }

    /// Recommended shard count for [`crate::run_sharded`] / [`crate::run_sharded_auto`].
    #[inline]
    pub fn recommended_shards(&self) -> usize {
        self.cpus
    }

    /// Total physical RAM in bytes, if reported by the OS.
    #[inline]
    pub fn total_ram_bytes(&self) -> Option<u64> {
        self.total_ram_bytes
    }

    /// Soft `RLIMIT_MEMLOCK` limit in bytes (`None` if unlimited or unavailable).
    #[inline]
    pub fn memlock_limit_bytes(&self) -> Option<u64> {
        self.memlock_limit_bytes
    }

    /// Soft `RLIMIT_NOFILE` limit (`None` if unlimited or unavailable).
    #[inline]
    pub fn nofile_limit(&self) -> Option<u64> {
        self.nofile_limit
    }

    /// Raw kernel `somaxconn` value (or default `1024` off Linux).
    #[inline]
    pub fn somaxconn(&self) -> i32 {
        self.somaxconn
    }

    /// Adaptive default `io_uring` SQ/CQ depth.
    #[inline]
    pub fn ring_entries(&self) -> u32 {
        self.ring_entries
    }

    /// Adaptive initial capacity for `CompletionSlab`.
    #[inline]
    pub fn slab_capacity(&self) -> usize {
        self.slab_capacity
    }

    /// Adaptive initial capacity for the thread-local executor task slab and ready queue.
    #[inline]
    pub fn executor_capacity(&self) -> usize {
        self.executor_capacity
    }

    /// Adaptive minimum task-drain steps per `block_on` tick.
    #[inline]
    pub fn executor_min_batch(&self) -> usize {
        self.executor_min_batch
    }

    /// Adaptive maximum thread count for the blocking thread pool.
    #[inline]
    pub fn blocking_max_threads(&self) -> usize {
        self.blocking_max_threads
    }

    /// Adaptive TCP/Unix listener backlog derived from kernel `somaxconn`.
    #[inline]
    pub fn listen_backlog(&self) -> i32 {
        self.listen_backlog
    }
}

// ── OS probes ────────────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn probe_total_ram() -> Option<u64> {
    // SAFETY: sysconf with _SC_PHYS_PAGES / _SC_PAGESIZE is always safe to call.
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages > 0 && page_size > 0 {
        (pages as u64).checked_mul(page_size as u64)
    } else {
        None
    }
}

#[cfg(not(target_os = "linux"))]
fn probe_total_ram() -> Option<u64> {
    None
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
type RlimitResource = libc::__rlimit_resource_t;
#[cfg(all(unix, not(all(target_os = "linux", target_env = "gnu"))))]
type RlimitResource = libc::c_int;

#[cfg(unix)]
fn probe_rlimit(resource: RlimitResource) -> Option<u64> {
    let mut rlim: libc::rlimit = unsafe { std::mem::zeroed() };
    // SAFETY: `rlim` is a valid out-pointer to `libc::rlimit`.
    let ret = unsafe { libc::getrlimit(resource, &mut rlim) };
    if ret != 0 || rlim.rlim_cur == libc::RLIM_INFINITY {
        None
    } else {
        Some(rlim.rlim_cur as u64)
    }
}

#[cfg(unix)]
fn probe_rlimit_memlock() -> Option<u64> {
    probe_rlimit(libc::RLIMIT_MEMLOCK)
}

#[cfg(not(unix))]
fn probe_rlimit_memlock() -> Option<u64> {
    None
}

#[cfg(unix)]
fn probe_rlimit_nofile() -> Option<u64> {
    probe_rlimit(libc::RLIMIT_NOFILE)
}

#[cfg(not(unix))]
fn probe_rlimit_nofile() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn probe_somaxconn() -> i32 {
    std::fs::read_to_string("/proc/sys/net/core/somaxconn")
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(1024)
}

#[cfg(not(target_os = "linux"))]
fn probe_somaxconn() -> i32 {
    1024
}

#[cfg(target_os = "linux")]
fn probe_cgroup_cpus() -> Option<usize> {
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        if let Some(cpus) = parse_cgroup_v2_cpu_max(&s) {
            return Some(cpus);
        }
    }
    let quota = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").ok()?;
    let period = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us").ok()?;
    parse_cgroup_v1_quota(&quota, &period)
}

/// Parse cgroup v2 `cpu.max` contents (`"$MAX $PERIOD"` or `"max $PERIOD"`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_cgroup_v2_cpu_max(contents: &str) -> Option<usize> {
    let mut parts = contents.split_whitespace();
    let quota_str = parts.next()?;
    if quota_str == "max" {
        return None;
    }
    let quota: u64 = quota_str.parse().ok()?;
    let period: u64 = parts.next()?.parse().ok()?;
    if period == 0 || quota == 0 {
        return None;
    }
    // Round up fractional CPU quotas (e.g. 1.5 -> 2) with a floor of 1.
    Some(quota.div_ceil(period).max(1) as usize)
}

/// Parse cgroup v1 `cpu.cfs_quota_us` and `cpu.cfs_period_us`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_cgroup_v1_quota(quota_str: &str, period_str: &str) -> Option<usize> {
    let quota: i64 = quota_str.trim().parse().ok()?;
    let period: i64 = period_str.trim().parse().ok()?;
    if quota <= 0 || period <= 0 {
        return None;
    }
    Some((quota as u64).div_ceil(period as u64).max(1) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_profile_is_sane() {
        let p = HardwareProfile::get();
        assert!(p.cpus() >= 1);
        assert!(p.recommended_shards() >= 1);
        assert!(p.ring_entries().is_power_of_two());
        assert!(p.ring_entries() >= 16);
        assert!(p.slab_capacity() >= 128);
        assert!(p.executor_capacity() >= 128);
        assert!(p.executor_min_batch() >= 64);
        assert!(p.blocking_max_threads() >= 32);
        assert!(p.listen_backlog() >= 128);
    }

    #[test]
    fn profile_scales_across_hardware_tiers() {
        let tiny = HardwareProfile::from_specs(1, Some(256 * 1024 * 1024), None, None, 128);
        assert_eq!(tiny.ring_entries(), 512);
        assert_eq!(tiny.slab_capacity(), 128);
        assert_eq!(tiny.blocking_max_threads(), 32);
        assert_eq!(tiny.listen_backlog(), 128);

        let vm =
            HardwareProfile::from_specs(2, Some(4 * 1024 * 1024 * 1024), None, Some(65536), 4096);
        assert_eq!(vm.ring_entries(), 1024);
        assert_eq!(vm.slab_capacity(), 256);
        assert_eq!(vm.executor_capacity(), 128);
        assert_eq!(vm.blocking_max_threads(), 64);
        assert_eq!(vm.listen_backlog(), 4096);

        let server = HardwareProfile::from_specs(
            64,
            Some(256 * 1024 * 1024 * 1024),
            None,
            Some(1_048_576),
            16384,
        );
        assert_eq!(server.ring_entries(), 4096);
        assert_eq!(server.slab_capacity(), 1024);
        assert_eq!(server.executor_capacity(), 1024);
        assert_eq!(server.executor_min_batch(), 256);
        assert_eq!(server.blocking_max_threads(), 512);
        assert_eq!(server.listen_backlog(), 8192);
    }

    #[test]
    fn tight_memlock_clamps_ring_entries() {
        let tiny_ulimit = HardwareProfile::from_specs(
            32,
            Some(64 * 1024 * 1024 * 1024),
            Some(64 * 1024),
            None,
            4096,
        );
        assert_eq!(tiny_ulimit.ring_entries(), 16);
        assert_eq!(tiny_ulimit.slab_capacity(), 128);

        let locked = HardwareProfile::from_specs(
            32,
            Some(64 * 1024 * 1024 * 1024),
            Some(256 * 1024),
            None,
            4096,
        );
        assert_eq!(locked.ring_entries(), 64);

        let med_locked = HardwareProfile::from_specs(
            32,
            Some(64 * 1024 * 1024 * 1024),
            Some(1024 * 1024),
            None,
            4096,
        );
        assert_eq!(med_locked.ring_entries(), 512);
    }

    #[test]
    fn cgroup_parsers_handle_valid_and_unlimited_inputs() {
        assert_eq!(parse_cgroup_v2_cpu_max("max 100000\n"), None);
        assert_eq!(parse_cgroup_v2_cpu_max("200000 100000\n"), Some(2));
        assert_eq!(parse_cgroup_v2_cpu_max("150000 100000\n"), Some(2));
        assert_eq!(parse_cgroup_v2_cpu_max("50000 100000\n"), Some(1));
        assert_eq!(parse_cgroup_v1_quota("-1", "100000"), None);
        assert_eq!(parse_cgroup_v1_quota("400000\n", "100000\n"), Some(4));
    }
}
