//! Kernel-bypass feasibility experiment: AF_XDP receive versus a normal
//! kernel UDP socket, on a veth pair between two network namespaces.
//! Standalone, raw syscalls only (no libbpf/clang available on the host):
//! the XDP program that redirects packets into the AF_XDP socket is
//! hand-assembled eBPF, six instructions, loaded through bpf(2).
//!
//! Must run as root. Modes:
//!   xdp send <ip> <port> <secs>      kernel UDP flood sender (sendmmsg)
//!   xdp recv-udp <port>              kernel UDP receiver (recvmmsg)
//!   xdp recv-xsk <ifname>            AF_XDP receiver (copy mode, busy poll)
//! Receivers print delivered packets/sec between first and last packet.

#![allow(clippy::missing_safety_doc)]

use std::{
    ffi::CString,
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant},
};

const PAYLOAD: usize = 64;
const PORT_DEFAULT: u16 = 9000;

fn pin(core: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn die(msg: &str) -> ! {
    eprintln!("{msg}: {}", std::io::Error::last_os_error());
    std::process::exit(1);
}

// ── sender ───────────────────────────────────────────────────────────────────

fn send(ip: &str, port: u16, secs: u64) {
    unsafe {
        let s = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        let mut addr: libc::sockaddr_in = std::mem::zeroed();
        addr.sin_family = libc::AF_INET as u16;
        addr.sin_port = port.to_be();
        let v4: std::net::Ipv4Addr = ip.parse().expect("ipv4 address");
        addr.sin_addr.s_addr = u32::from_ne_bytes(v4.octets());
        let payload = [0x37u8; PAYLOAD];
        const B: usize = 64;
        let mut iov: Vec<libc::iovec> = (0..B)
            .map(|_| libc::iovec {
                iov_base: payload.as_ptr() as *mut _,
                iov_len: PAYLOAD,
            })
            .collect();
        let mut msgs: Vec<libc::mmsghdr> = (0..B)
            .map(|i| {
                let mut m: libc::mmsghdr = std::mem::zeroed();
                m.msg_hdr.msg_name = &mut addr as *mut _ as *mut _;
                m.msg_hdr.msg_namelen = std::mem::size_of::<libc::sockaddr_in>() as u32;
                m.msg_hdr.msg_iov = &mut iov[i] as *mut _;
                m.msg_hdr.msg_iovlen = 1;
                m
            })
            .collect();
        let end = Instant::now() + Duration::from_secs(secs);
        let mut sent = 0u64;
        while Instant::now() < end {
            let n = libc::sendmmsg(s, msgs.as_mut_ptr(), B as u32, 0);
            if n > 0 {
                sent += n as u64;
            }
        }
        println!("sent={sent} pps={:.0}", sent as f64 / secs as f64);
    }
}

// ── kernel UDP receiver ──────────────────────────────────────────────────────

fn recv_udp(port: u16) {
    unsafe {
        let s = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        let big: libc::c_int = 16 << 20;
        libc::setsockopt(
            s,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &big as *const _ as *const _,
            4,
        );
        let tv = libc::timeval {
            tv_sec: 1,
            tv_usec: 0,
        };
        libc::setsockopt(
            s,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const _ as *const _,
            16,
        );
        let mut addr: libc::sockaddr_in = std::mem::zeroed();
        addr.sin_family = libc::AF_INET as u16;
        addr.sin_port = port.to_be();
        if libc::bind(
            s,
            &addr as *const _ as *const _,
            std::mem::size_of::<libc::sockaddr_in>() as u32,
        ) < 0
        {
            die("bind");
        }
        const B: usize = 64;
        let mut bufs = vec![[0u8; 2048]; B];
        let mut iov: Vec<libc::iovec> = bufs
            .iter_mut()
            .map(|b| libc::iovec {
                iov_base: b.as_mut_ptr() as *mut _,
                iov_len: 2048,
            })
            .collect();
        let mut msgs: Vec<libc::mmsghdr> = (0..B)
            .map(|i| {
                let mut m: libc::mmsghdr = std::mem::zeroed();
                m.msg_hdr.msg_iov = &mut iov[i] as *mut _;
                m.msg_hdr.msg_iovlen = 1;
                m
            })
            .collect();
        println!("READY");
        let mut count = 0u64;
        let mut first: Option<Instant> = None;
        let mut last = Instant::now();
        let cpu0 = thread_cpu();
        loop {
            let n = libc::recvmmsg(
                s,
                msgs.as_mut_ptr(),
                B as u32,
                libc::MSG_WAITFORONE,
                std::ptr::null_mut(),
            );
            if n <= 0 {
                if first.is_some() {
                    break;
                }
                continue;
            }
            if first.is_none() {
                first = Some(Instant::now());
            }
            count += n as u64;
            last = Instant::now();
        }
        report("kernel-udp", count, first, last, thread_cpu() - cpu0);
    }
}

fn thread_cpu() -> Duration {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_THREAD, &mut ru) };
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

fn report(name: &str, count: u64, first: Option<Instant>, last: Instant, cpu: Duration) {
    let dur = first
        .map(|f| last.duration_since(f))
        .unwrap_or_default()
        .as_secs_f64();
    println!(
        "{name} delivered={count} window_s={dur:.3} pps={:.0} recv_thread_cpu_s={:.2}",
        count as f64 / dur.max(1e-9),
        cpu.as_secs_f64()
    );
}

// ── bpf(2) helpers ───────────────────────────────────────────────────────────

unsafe fn bpf(cmd: i32, attr: &mut [u8; 128]) -> i64 {
    libc::syscall(libc::SYS_bpf, cmd, attr.as_mut_ptr(), 128)
}
fn put32(a: &mut [u8; 128], off: usize, v: u32) {
    a[off..off + 4].copy_from_slice(&v.to_ne_bytes());
}
fn put64(a: &mut [u8; 128], off: usize, v: u64) {
    a[off..off + 8].copy_from_slice(&v.to_ne_bytes());
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Insn {
    code: u8,
    regs: u8, // dst | src << 4
    off: i16,
    imm: i32,
}

// ── AF_XDP receiver ──────────────────────────────────────────────────────────

const AF_XDP: i32 = 44;
const SOL_XDP: i32 = 283;
const XDP_MMAP_OFFSETS: i32 = 1;
const XDP_RX_RING: i32 = 2;
const XDP_UMEM_REG: i32 = 4;
const XDP_UMEM_FILL_RING: i32 = 5;
const XDP_UMEM_COMPLETION_RING: i32 = 6;
const FRAMES: usize = 4096;
const CHUNK: usize = 2048;
const RING: usize = 2048;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct RingOff {
    producer: u64,
    consumer: u64,
    desc: u64,
    flags: u64,
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct MmapOffsets {
    rx: RingOff,
    tx: RingOff,
    fr: RingOff,
    cr: RingOff,
}

fn recv_xsk(ifname: &str) {
    unsafe {
        let cname = CString::new(ifname).unwrap();
        let ifindex = libc::if_nametoindex(cname.as_ptr());
        if ifindex == 0 {
            die("if_nametoindex");
        }

        // XSKMAP with one entry, then the redirect program.
        let mut a = [0u8; 128];
        put32(&mut a, 0, 17); // BPF_MAP_TYPE_XSKMAP
        put32(&mut a, 4, 4);
        put32(&mut a, 8, 4);
        put32(&mut a, 12, 1);
        let map_fd = bpf(0, &mut a);
        if map_fd < 0 {
            die("BPF_MAP_CREATE xskmap");
        }

        let prog = [
            Insn {
                code: 0x61,
                regs: 0x12,
                off: 16,
                imm: 0,
            }, // r2 = *(u32*)(r1 + 16)  rx_queue_index
            Insn {
                code: 0x18,
                regs: 0x11,
                off: 0,
                imm: map_fd as i32,
            }, // r1 = map (pseudo map fd)
            Insn {
                code: 0,
                regs: 0,
                off: 0,
                imm: 0,
            },
            Insn {
                code: 0xb7,
                regs: 0x03,
                off: 0,
                imm: 2,
            }, // r3 = XDP_PASS (fallback)
            Insn {
                code: 0x85,
                regs: 0,
                off: 0,
                imm: 51,
            }, // call bpf_redirect_map
            Insn {
                code: 0x95,
                regs: 0,
                off: 0,
                imm: 0,
            }, // exit
        ];
        let license = CString::new("GPL").unwrap();
        let mut log = vec![0u8; 8192];
        let mut a = [0u8; 128];
        put32(&mut a, 0, 6); // BPF_PROG_TYPE_XDP
        put32(&mut a, 4, prog.len() as u32);
        put64(&mut a, 8, prog.as_ptr() as u64);
        put64(&mut a, 16, license.as_ptr() as u64);
        put32(&mut a, 24, 1);
        put32(&mut a, 28, log.len() as u32);
        put64(&mut a, 32, log.as_mut_ptr() as u64);
        let prog_fd = bpf(5, &mut a);
        if prog_fd < 0 {
            eprintln!("{}", String::from_utf8_lossy(&log));
            die("BPF_PROG_LOAD");
        }

        // UMEM
        let umem = libc::mmap(
            std::ptr::null_mut(),
            FRAMES * CHUNK,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if umem == libc::MAP_FAILED {
            die("mmap umem");
        }
        let xsk = libc::socket(AF_XDP, libc::SOCK_RAW, 0);
        if xsk < 0 {
            die("socket(AF_XDP)");
        }
        let mut reg = [0u64; 3];
        reg[0] = umem as u64;
        reg[1] = (FRAMES * CHUNK) as u64;
        reg[2] = CHUNK as u64; // chunk_size (low 32) + headroom 0 (high 32)
        if libc::setsockopt(xsk, SOL_XDP, XDP_UMEM_REG, reg.as_ptr() as *const _, 24) < 0 {
            die("XDP_UMEM_REG");
        }
        let n: i32 = RING as i32;
        for opt in [XDP_UMEM_FILL_RING, XDP_UMEM_COMPLETION_RING, XDP_RX_RING] {
            if libc::setsockopt(xsk, SOL_XDP, opt, &n as *const _ as *const _, 4) < 0 {
                die("ring size setsockopt");
            }
        }
        let mut off = MmapOffsets::default();
        let mut len = std::mem::size_of::<MmapOffsets>() as u32;
        if libc::getsockopt(
            xsk,
            SOL_XDP,
            XDP_MMAP_OFFSETS,
            &mut off as *mut _ as *mut _,
            &mut len,
        ) < 0
        {
            die("XDP_MMAP_OFFSETS");
        }
        let rx_map = libc::mmap(
            std::ptr::null_mut(),
            off.rx.desc as usize + RING * 16,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            xsk,
            0,
        );
        let fill_map = libc::mmap(
            std::ptr::null_mut(),
            off.fr.desc as usize + RING * 8,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            xsk,
            0x1_0000_0000i64,
        );
        if rx_map == libc::MAP_FAILED || fill_map == libc::MAP_FAILED {
            die("mmap rings");
        }

        // bind(sockaddr_xdp): family u16, flags u16, ifindex u32, queue_id u32, shared u32
        let mut sa = [0u8; 16];
        sa[0..2].copy_from_slice(&(AF_XDP as u16).to_ne_bytes());
        sa[4..8].copy_from_slice(&ifindex.to_ne_bytes());
        if libc::bind(xsk, sa.as_ptr() as *const _, 16) < 0 {
            die("bind AF_XDP");
        }

        // map[0] = xsk fd
        let key: u32 = 0;
        let val: u32 = xsk as u32;
        let mut a = [0u8; 128];
        put32(&mut a, 0, map_fd as u32);
        put64(&mut a, 8, &key as *const _ as u64);
        put64(&mut a, 16, &val as *const _ as u64);
        if bpf(2, &mut a) < 0 {
            die("BPF_MAP_UPDATE_ELEM");
        }

        // attach XDP via a bpf link (BPF_LINK_CREATE, attach type BPF_XDP = 37)
        let mut a = [0u8; 128];
        put32(&mut a, 0, prog_fd as u32);
        put32(&mut a, 4, ifindex);
        put32(&mut a, 8, 37);
        let link_fd = bpf(28, &mut a);
        if link_fd < 0 {
            die("BPF_LINK_CREATE xdp");
        }

        // Fill ring with every frame address.
        let fp = &*((fill_map as *mut u8).add(off.fr.producer as usize) as *const AtomicU32);
        let fill_desc = (fill_map as *mut u8).add(off.fr.desc as usize) as *mut u64;
        for i in 0..RING {
            *fill_desc.add(i) = (i * CHUNK) as u64;
        }
        fp.store(RING as u32, Ordering::Release);
        let mut fill_prod = RING as u32;

        let rp = &*((rx_map as *mut u8).add(off.rx.producer as usize) as *const AtomicU32);
        let rc = &*((rx_map as *mut u8).add(off.rx.consumer as usize) as *const AtomicU32);
        let rx_desc = (rx_map as *mut u8).add(off.rx.desc as usize) as *const [u64; 2]; // addr, len|options

        println!("READY");
        let mut count = 0u64;
        let mut first: Option<Instant> = None;
        let mut last = Instant::now();
        let mut idle_spins = 0u32;
        let mut cons = rc.load(Ordering::Relaxed);
        let cpu0 = thread_cpu();
        loop {
            let prod = rp.load(Ordering::Acquire);
            let avail = prod.wrapping_sub(cons);
            if avail == 0 {
                idle_spins += 1;
                if first.is_some()
                    && idle_spins % 1_000_000 == 0
                    && last.elapsed() > Duration::from_secs(1)
                {
                    break;
                }
                std::hint::spin_loop();
                continue;
            }
            idle_spins = 0;
            if first.is_none() {
                first = Some(Instant::now());
            }
            for _ in 0..avail {
                let d = *rx_desc.add((cons as usize) & (RING - 1));
                // recycle the frame
                *fill_desc.add((fill_prod as usize) & (RING - 1)) = d[0];
                fill_prod = fill_prod.wrapping_add(1);
                cons = cons.wrapping_add(1);
            }
            fp.store(fill_prod, Ordering::Release);
            rc.store(cons, Ordering::Release);
            count += avail as u64;
            last = Instant::now();
        }
        let cpu = thread_cpu() - cpu0;
        report("af_xdp", count, first, last, cpu);
        libc::close(link_fd as i32);
    }
}

const XDP_TX_RING: i32 = 3;

fn ip_checksum(h: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    for c in h.chunks(2) {
        sum += u16::from_be_bytes([c[0], c[1]]) as u32;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// Ethernet + IPv4 + UDP frame (UDP checksum 0, legal for IPv4) carrying
/// `PAYLOAD` bytes, 10.9.0.1:12345 -> 10.9.0.2:9000, between the fixed MACs
/// the benchmark script assigns to the veth pair.
fn build_frame() -> Vec<u8> {
    let mut f = Vec::with_capacity(14 + 20 + 8 + PAYLOAD);
    f.extend_from_slice(&[0x02, 0, 0, 0, 0, 0x02]); // dst MAC (vb)
    f.extend_from_slice(&[0x02, 0, 0, 0, 0, 0x01]); // src MAC (va)
    f.extend_from_slice(&0x0800u16.to_be_bytes());
    let mut ip = vec![0x45, 0];
    ip.extend_from_slice(&((20 + 8 + PAYLOAD) as u16).to_be_bytes());
    ip.extend_from_slice(&[0, 0, 0, 0, 64, 17, 0, 0]);
    ip.extend_from_slice(&[10, 9, 0, 1]);
    ip.extend_from_slice(&[10, 9, 0, 2]);
    let c = ip_checksum(&ip);
    ip[10..12].copy_from_slice(&c.to_be_bytes());
    f.extend_from_slice(&ip);
    f.extend_from_slice(&12345u16.to_be_bytes());
    f.extend_from_slice(&PORT_DEFAULT.to_be_bytes());
    f.extend_from_slice(&((8 + PAYLOAD) as u16).to_be_bytes());
    f.extend_from_slice(&[0, 0]);
    f.extend(std::iter::repeat(0x37u8).take(PAYLOAD));
    f
}

/// AF_XDP transmit: every UMEM frame holds the same packet, so frames can be
/// re-queued as soon as their completion is reaped, and nothing needs the
/// kernel UDP stack on the sending side.
fn send_xsk(ifname: &str, secs: u64) {
    unsafe {
        let cname = CString::new(ifname).unwrap();
        let ifindex = libc::if_nametoindex(cname.as_ptr());
        if ifindex == 0 {
            die("if_nametoindex");
        }
        let umem = libc::mmap(
            std::ptr::null_mut(),
            FRAMES * CHUNK,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if umem == libc::MAP_FAILED {
            die("mmap umem");
        }
        let frame = build_frame();
        for i in 0..FRAMES {
            std::ptr::copy_nonoverlapping(
                frame.as_ptr(),
                (umem as *mut u8).add(i * CHUNK),
                frame.len(),
            );
        }
        let xsk = libc::socket(AF_XDP, libc::SOCK_RAW, 0);
        if xsk < 0 {
            die("socket(AF_XDP)");
        }
        let mut reg = [0u64; 3];
        reg[0] = umem as u64;
        reg[1] = (FRAMES * CHUNK) as u64;
        reg[2] = CHUNK as u64;
        if libc::setsockopt(xsk, SOL_XDP, XDP_UMEM_REG, reg.as_ptr() as *const _, 24) < 0 {
            die("XDP_UMEM_REG");
        }
        let n: i32 = RING as i32;
        for opt in [XDP_UMEM_FILL_RING, XDP_UMEM_COMPLETION_RING, XDP_TX_RING] {
            if libc::setsockopt(xsk, SOL_XDP, opt, &n as *const _ as *const _, 4) < 0 {
                die("ring size setsockopt");
            }
        }
        let mut off = MmapOffsets::default();
        let mut len = std::mem::size_of::<MmapOffsets>() as u32;
        if libc::getsockopt(
            xsk,
            SOL_XDP,
            XDP_MMAP_OFFSETS,
            &mut off as *mut _ as *mut _,
            &mut len,
        ) < 0
        {
            die("XDP_MMAP_OFFSETS");
        }
        let tx_map = libc::mmap(
            std::ptr::null_mut(),
            off.tx.desc as usize + RING * 16,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            xsk,
            0x8000_0000i64,
        );
        let cr_map = libc::mmap(
            std::ptr::null_mut(),
            off.cr.desc as usize + RING * 8,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            xsk,
            0x1_8000_0000i64,
        );
        if tx_map == libc::MAP_FAILED || cr_map == libc::MAP_FAILED {
            die("mmap tx/completion rings");
        }
        let mut sa = [0u8; 16];
        sa[0..2].copy_from_slice(&(AF_XDP as u16).to_ne_bytes());
        sa[4..8].copy_from_slice(&ifindex.to_ne_bytes());
        if libc::bind(xsk, sa.as_ptr() as *const _, 16) < 0 {
            die("bind AF_XDP (tx)");
        }

        let tp = &*((tx_map as *mut u8).add(off.tx.producer as usize) as *const AtomicU32);
        let tc = &*((tx_map as *mut u8).add(off.tx.consumer as usize) as *const AtomicU32);
        let tx_desc = (tx_map as *mut u8).add(off.tx.desc as usize) as *mut [u64; 2];
        let cp = &*((cr_map as *mut u8).add(off.cr.producer as usize) as *const AtomicU32);
        let cc = &*((cr_map as *mut u8).add(off.cr.consumer as usize) as *const AtomicU32);

        let end = Instant::now() + Duration::from_secs(secs);
        let mut prod = 0u32;
        let mut next_frame = 0usize;
        let mut kicks = 0u64;
        let cpu0 = thread_cpu();
        while Instant::now() < end {
            let cons = tc.load(Ordering::Acquire);
            let free = RING as u32 - prod.wrapping_sub(cons);
            let batch = free.min(64);
            for _ in 0..batch {
                *tx_desc.add((prod as usize) & (RING - 1)) =
                    [(next_frame * CHUNK) as u64, frame.len() as u64];
                next_frame = (next_frame + 1) % FRAMES;
                prod = prod.wrapping_add(1);
            }
            tp.store(prod, Ordering::Release);
            libc::sendto(
                xsk,
                std::ptr::null(),
                0,
                libc::MSG_DONTWAIT,
                std::ptr::null(),
                0,
            );
            kicks += 1;
            // reap completions so the kernel can keep reserving completion slots
            cc.store(cp.load(Ordering::Acquire), Ordering::Release);
        }
        let done = tc.load(Ordering::Acquire) as u64;
        let cpu = thread_cpu() - cpu0;
        println!(
            "sent={done} pps={:.0} kicks={kicks} sender_thread_cpu_s={:.2}",
            done as f64 / secs as f64,
            cpu.as_secs_f64()
        );
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a.get(1).map(String::as_str) {
        Some("send-xsk") => {
            pin(1);
            send_xsk(
                a.get(2).map(String::as_str).unwrap_or("va"),
                a.get(3).and_then(|s| s.parse().ok()).unwrap_or(5),
            );
        }
        Some("send") => {
            pin(1);
            send(
                &a[2],
                a[3].parse().unwrap_or(PORT_DEFAULT),
                a[4].parse().unwrap_or(5),
            );
        }
        Some("recv-udp") => {
            pin(0);
            recv_udp(
                a.get(2)
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(PORT_DEFAULT),
            );
        }
        Some("recv-xsk") => {
            pin(0);
            recv_xsk(a.get(2).map(String::as_str).unwrap_or("vb"));
        }
        _ => eprintln!("usage: xdp <send ip port secs | recv-udp port | recv-xsk ifname>"),
    }
}
