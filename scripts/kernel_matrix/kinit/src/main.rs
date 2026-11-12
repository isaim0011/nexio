use std::{ffi::CString, process::Command};

fn mount(src: &str, target: &str, fs: &str) {
    let _ = std::fs::create_dir_all(target);
    let (s, t, f) = (
        CString::new(src).unwrap(),
        CString::new(target).unwrap(),
        CString::new(fs).unwrap(),
    );
    unsafe { libc::mount(s.as_ptr(), t.as_ptr(), f.as_ptr(), 0, std::ptr::null()) };
}

fn main() {
    mount("proc", "/proc", "proc");
    mount("sysfs", "/sys", "sysfs");
    mount("devtmpfs", "/dev", "devtmpfs");
    mount("tmpfs", "/tmp", "tmpfs");
    unsafe {
        let s = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        let mut ifr: libc::ifreq = std::mem::zeroed();
        for (i, b) in b"lo".iter().enumerate() {
            ifr.ifr_name[i] = *b as libc::c_char;
        }
        ifr.ifr_ifru.ifru_flags = (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
        libc::ioctl(s, libc::SIOCSIFFLAGS as _, &ifr);
        let lim = libc::rlimit {
            rlim_cur: 65535,
            rlim_max: 65535,
        };
        libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        let mut u: libc::utsname = std::mem::zeroed();
        libc::uname(&mut u);
        let rel = std::ffi::CStr::from_ptr(u.release.as_ptr())
            .to_string_lossy()
            .into_owned();
        println!("KTEST-KERNEL {rel}");
    }
    std::env::set_var("TMPDIR", "/tmp");
    let mut tests: Vec<_> = std::fs::read_dir("/tests")
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .collect();
    tests.sort();
    let (mut failures, total) = (0, tests.len());
    for t in tests {
        println!("KTEST-RUN {}", t.display());
        match Command::new(&t).status() {
            Ok(s) if s.success() => println!("KTEST-OK {}", t.display()),
            other => {
                failures += 1;
                println!("KTEST-FAIL {} {:?}", t.display(), other);
            }
        }
    }
    println!("KTEST-DONE failures={failures} total={total}");
    unsafe {
        libc::sync();
        libc::reboot(libc::LINUX_REBOOT_CMD_POWER_OFF);
    }
}
