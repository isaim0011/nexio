//! I/O loop and test harness for the `utcp` stack (Linux only, run as root).
//!
//!   utcp_xdp server-xdp <ifname> <ip> <mac> <port>   AF_XDP echo server on the userspace stack
//!   utcp_xdp server-nexio <ip> <port> <msg_size>     nexio (kernel TCP, io_uring) echo baseline
//!   utcp_xdp client <ip> <port> <conns> <msgs> <size>  kernel-TCP client, verifies every echoed byte

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("utcp_xdp is Linux-only");
}

#[cfg(target_os = "linux")]
fn main() {
    linux::run();
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        ffi::CString,
        sync::atomic::{AtomicU32, Ordering},
        time::{Duration, Instant},
    };
    use utcp::{Stack, TxBatch};

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

    fn cpu_secs() -> f64 {
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
        let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        tv(ru.ru_utime) + tv(ru.ru_stime)
    }

    pub fn run() {
        let a: Vec<String> = std::env::args().collect();
        match a.get(1).map(String::as_str) {
            Some("server-xdp") => {
                pin(0);
                let mac = parse_mac(&a[4]);
                let ip: std::net::Ipv4Addr = a[3].parse().expect("ip");
                server_xdp(&a[2], ip.octets(), mac, a[5].parse().expect("port"));
            }
            Some("server-nexio") => {
                pin(0);
                server_nexio(&a[2], a[3].parse().unwrap(), a[4].parse().unwrap());
            }
            Some("client") => {
                pin(1);
                client(&a[2], a[3].parse().unwrap(), a[4].parse().unwrap(), a[5].parse().unwrap(), a[6].parse().unwrap());
            }
            _ => eprintln!("usage: utcp_xdp <server-xdp|server-nexio|client> ..."),
        }
    }

    fn parse_mac(s: &str) -> [u8; 6] {
        let mut m = [0u8; 6];
        for (i, part) in s.split(':').enumerate() {
            m[i] = u8::from_str_radix(part, 16).expect("mac");
        }
        m
    }

    // ── nexio baseline ───────────────────────────────────────────────────────

    fn server_nexio(ip: &str, port: u16, size: usize) {
        let ip = ip.to_string();
        nexio::block_on(async move {
            let listener = nexio::net::TcpListener::bind((ip.as_str(), port)).await.unwrap();
            println!("READY");
            loop {
                let Ok(stream) = listener.accept().await else { break };
                drop(nexio::executor::spawn(async move {
                    let mut buf = vec![0u8; size];
                    loop {
                        let (r, b) = stream.read_exact(buf).await;
                        buf = b;
                        if r.is_err() {
                            break;
                        }
                        let (w, b) = stream.write_all(buf).await;
                        buf = b;
                        if w.is_err() {
                            break;
                        }
                    }
                }));
            }
        });
    }

    // ── client (kernel TCP, verifies every byte) ─────────────────────────────

    fn client(ip: &str, port: u16, conns: usize, msgs: usize, size: usize) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let ip = ip.to_string();
        rt.block_on(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(conns + 1));
            let mut hs = Vec::new();
            for c in 0..conns {
                let ip = ip.clone();
                let barrier = barrier.clone();
                hs.push(tokio::spawn(async move {
                    let mut s = tokio::net::TcpStream::connect((ip.as_str(), port)).await.expect("connect");
                    s.set_nodelay(true).unwrap();
                    barrier.wait().await;
                    let mut payload = vec![0u8; size];
                    let mut echo = vec![0u8; size];
                    let mut bad = 0usize;
                    for i in 0..msgs {
                        for (j, b) in payload.iter_mut().enumerate() {
                            *b = (c as u8).wrapping_mul(31) ^ (i as u8).wrapping_mul(7) ^ j as u8;
                        }
                        s.write_all(&payload).await.expect("write");
                        s.read_exact(&mut echo).await.expect("read");
                        if echo != payload {
                            bad += 1;
                        }
                    }
                    bad
                }));
            }
            barrier.wait().await;
            let t0 = Instant::now();
            let mut bad = 0;
            for h in hs {
                bad += h.await.unwrap();
            }
            let secs = t0.elapsed().as_secs_f64();
            let total = conns * msgs;
            println!(
                "RESULT total={total} secs={secs:.3} msgs/s={:.0} mismatches={bad} client_cpu_s={:.2}",
                total as f64 / secs,
                cpu_secs()
            );
        });
    }

    // ── AF_XDP server ────────────────────────────────────────────────────────

    const AF_XDP: i32 = 44;
    const SOL_XDP: i32 = 283;
    const XDP_MMAP_OFFSETS: i32 = 1;
    const XDP_RX_RING: i32 = 2;
    const XDP_TX_RING: i32 = 3;
    const XDP_UMEM_REG: i32 = 4;
    const XDP_UMEM_FILL_RING: i32 = 5;
    const XDP_UMEM_COMPLETION_RING: i32 = 6;
    const FRAMES: usize = 4096;
    const RX_FRAMES: usize = 2048;
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

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Insn {
        code: u8,
        regs: u8,
        off: i16,
        imm: i32,
    }

    unsafe fn bpf(cmd: i32, attr: &mut [u8; 128]) -> i64 {
        libc::syscall(libc::SYS_bpf, cmd, attr.as_mut_ptr(), 128)
    }
    fn put32(a: &mut [u8; 128], off: usize, v: u32) {
        a[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    }
    fn put64(a: &mut [u8; 128], off: usize, v: u64) {
        a[off..off + 8].copy_from_slice(&v.to_ne_bytes());
    }

    fn atom(base: *mut u8, off: u64) -> &'static AtomicU32 {
        unsafe { &*(base.add(off as usize) as *const AtomicU32) }
    }

    fn server_xdp(ifname: &str, ip: [u8; 4], mac: [u8; 6], port: u16) {
        unsafe {
            let cname = CString::new(ifname).unwrap();
            let ifindex = libc::if_nametoindex(cname.as_ptr());
            if ifindex == 0 {
                die("if_nametoindex");
            }

            // XSKMAP + hand-assembled redirect program (see benches/xdp.rs).
            let mut a = [0u8; 128];
            put32(&mut a, 0, 17);
            put32(&mut a, 4, 4);
            put32(&mut a, 8, 4);
            put32(&mut a, 12, 1);
            let map_fd = bpf(0, &mut a);
            if map_fd < 0 {
                die("BPF_MAP_CREATE");
            }
            let prog = [
                Insn { code: 0x61, regs: 0x12, off: 16, imm: 0 },
                Insn { code: 0x18, regs: 0x11, off: 0, imm: map_fd as i32 },
                Insn { code: 0, regs: 0, off: 0, imm: 0 },
                Insn { code: 0xb7, regs: 0x03, off: 0, imm: 2 },
                Insn { code: 0x85, regs: 0, off: 0, imm: 51 },
                Insn { code: 0x95, regs: 0, off: 0, imm: 0 },
            ];
            let license = CString::new("GPL").unwrap();
            let mut log = vec![0u8; 8192];
            let mut a = [0u8; 128];
            put32(&mut a, 0, 6);
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

            let umem = libc::mmap(
                std::ptr::null_mut(),
                FRAMES * CHUNK,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            ) as *mut u8;
            if umem as isize == -1 {
                die("mmap umem");
            }
            let xsk = libc::socket(AF_XDP, libc::SOCK_RAW, 0);
            if xsk < 0 {
                die("socket(AF_XDP)");
            }
            let reg = [umem as u64, (FRAMES * CHUNK) as u64, CHUNK as u64];
            if libc::setsockopt(xsk, SOL_XDP, XDP_UMEM_REG, reg.as_ptr() as *const _, 24) < 0 {
                die("XDP_UMEM_REG");
            }
            let n: i32 = RING as i32;
            for opt in [XDP_UMEM_FILL_RING, XDP_UMEM_COMPLETION_RING, XDP_RX_RING, XDP_TX_RING] {
                if libc::setsockopt(xsk, SOL_XDP, opt, &n as *const _ as *const _, 4) < 0 {
                    die("ring size");
                }
            }
            let mut off = MmapOffsets::default();
            let mut len = std::mem::size_of::<MmapOffsets>() as u32;
            if libc::getsockopt(xsk, SOL_XDP, XDP_MMAP_OFFSETS, &mut off as *mut _ as *mut _, &mut len) < 0 {
                die("XDP_MMAP_OFFSETS");
            }
            let map = |pgoff: i64, bytes: usize| {
                let p = libc::mmap(
                    std::ptr::null_mut(),
                    bytes,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED | libc::MAP_POPULATE,
                    xsk,
                    pgoff,
                );
                if p == libc::MAP_FAILED {
                    die("mmap ring");
                }
                p as *mut u8
            };
            let rx_map = map(0, off.rx.desc as usize + RING * 16);
            let tx_map = map(0x8000_0000, off.tx.desc as usize + RING * 16);
            let fill_map = map(0x1_0000_0000, off.fr.desc as usize + RING * 8);
            let comp_map = map(0x1_8000_0000, off.cr.desc as usize + RING * 8);

            let mut sa = [0u8; 16];
            sa[0..2].copy_from_slice(&(AF_XDP as u16).to_ne_bytes());
            sa[4..8].copy_from_slice(&ifindex.to_ne_bytes());
            if libc::bind(xsk, sa.as_ptr() as *const _, 16) < 0 {
                die("bind AF_XDP");
            }
            let key: u32 = 0;
            let val: u32 = xsk as u32;
            let mut a = [0u8; 128];
            put32(&mut a, 0, map_fd as u32);
            put64(&mut a, 8, &key as *const _ as u64);
            put64(&mut a, 16, &val as *const _ as u64);
            if bpf(2, &mut a) < 0 {
                die("BPF_MAP_UPDATE_ELEM");
            }
            let mut a = [0u8; 128];
            put32(&mut a, 0, prog_fd as u32);
            put32(&mut a, 4, ifindex);
            put32(&mut a, 8, 37);
            let link_fd = bpf(28, &mut a);
            if link_fd < 0 {
                die("BPF_LINK_CREATE");
            }

            let fill_prod = atom(fill_map, off.fr.producer);
            let fill_desc = fill_map.add(off.fr.desc as usize) as *mut u64;
            let rx_prod = atom(rx_map, off.rx.producer);
            let rx_cons = atom(rx_map, off.rx.consumer);
            let rx_desc = rx_map.add(off.rx.desc as usize) as *const [u64; 2];
            let tx_prod = atom(tx_map, off.tx.producer);
            let tx_cons = atom(tx_map, off.tx.consumer);
            let tx_desc = tx_map.add(off.tx.desc as usize) as *mut [u64; 2];
            let comp_prod = atom(comp_map, off.cr.producer);
            let comp_cons = atom(comp_map, off.cr.consumer);
            let comp_desc = comp_map.add(off.cr.desc as usize) as *const u64;

            for i in 0..RX_FRAMES {
                *fill_desc.add(i) = (i * CHUNK) as u64;
            }
            fill_prod.store(RX_FRAMES as u32, Ordering::Release);
            let mut fill_p = RX_FRAMES as u32;
            let mut free_tx: Vec<u64> = (RX_FRAMES..FRAMES).map(|i| (i * CHUNK) as u64).collect();
            let mut tx_p = 0u32;
            let mut rx_c = rx_cons.load(Ordering::Relaxed);
            let mut comp_c = 0u32;

            let mut stack = Stack::new(mac, ip, port, Instant::now());
            let mut out = TxBatch::new();
            let mut echoed = 0u64;
            let mut echoed_bytes = 0u64;
            let mut rx_frames = 0u64;
            let mut dropped_tx = 0u64;
            let mut busy = Duration::ZERO;
            let mut last_timer = Instant::now();
            let mut last_traffic: Option<Instant> = None;
            let mut spins = 0u32;

            println!("READY");
            loop {
                let prod = rx_prod.load(Ordering::Acquire);
                let avail = prod.wrapping_sub(rx_c);
                if avail > 0 {
                    let t = Instant::now();
                    out.clear();
                    let mut app = |d: &[u8], reply: &mut Vec<u8>| {
                        reply.extend_from_slice(d);
                        echoed += 1;
                        echoed_bytes += d.len() as u64;
                    };
                    for _ in 0..avail {
                        let d = *rx_desc.add((rx_c as usize) & (RING - 1));
                        let addr = d[0] as usize;
                        let flen = (d[1] & 0xffff_ffff) as usize;
                        let frame = std::slice::from_raw_parts(umem.add(addr), flen);
                        stack.handle_frame(frame, t, &mut app, &mut out);
                        *fill_desc.add((fill_p as usize) & (RING - 1)) = d[0];
                        fill_p = fill_p.wrapping_add(1);
                        rx_c = rx_c.wrapping_add(1);
                        rx_frames += 1;
                    }
                    fill_prod.store(fill_p, Ordering::Release);
                    rx_cons.store(rx_c, Ordering::Release);
                    transmit(
                        &out, umem, &mut free_tx, &mut tx_p, tx_prod, tx_cons, tx_desc, comp_prod, comp_cons,
                        comp_desc, &mut comp_c, xsk, &mut dropped_tx,
                    );
                    busy += t.elapsed();
                    last_traffic = Some(Instant::now());
                    spins = 0;
                    continue;
                }
                spins += 1;
                if spins % 256 == 0 {
                    let now = Instant::now();
                    if now.duration_since(last_timer) >= Duration::from_millis(1) {
                        last_timer = now;
                        out.clear();
                        stack.poll_timers(now, &mut out);
                        if !out.is_empty() {
                            transmit(
                                &out, umem, &mut free_tx, &mut tx_p, tx_prod, tx_cons, tx_desc, comp_prod, comp_cons,
                                comp_desc, &mut comp_c, xsk, &mut dropped_tx,
                            );
                        }
                    }
                    if let Some(lt) = last_traffic {
                        if now.duration_since(lt) > Duration::from_secs(3) && stack.conn_count() == 0 {
                            break;
                        }
                        if now.duration_since(lt) > Duration::from_secs(12) {
                            break;
                        }
                    }
                }
                std::hint::spin_loop();
            }
            println!(
                "STATS echoed_msgs={echoed} echoed_bytes={echoed_bytes} rx_frames={rx_frames} tx_dropped={dropped_tx} busy_s={:.3} busy_us_per_msg={:.3} open_conns_at_exit={}",
                busy.as_secs_f64(),
                busy.as_secs_f64() * 1e6 / echoed.max(1) as f64,
                stack.conn_count()
            );
            libc::close(link_fd as i32);
        }
    }

    #[allow(clippy::too_many_arguments)]
    unsafe fn transmit(
        out: &TxBatch,
        umem: *mut u8,
        free_tx: &mut Vec<u64>,
        tx_p: &mut u32,
        tx_prod: &AtomicU32,
        tx_cons: &AtomicU32,
        tx_desc: *mut [u64; 2],
        comp_prod: &AtomicU32,
        comp_cons: &AtomicU32,
        comp_desc: *const u64,
        comp_c: &mut u32,
        xsk: i32,
        dropped: &mut u64,
    ) {
        if out.is_empty() {
            return;
        }
        let reap = |free_tx: &mut Vec<u64>, comp_c: &mut u32| {
            let p = comp_prod.load(Ordering::Acquire);
            while *comp_c != p {
                free_tx.push(*comp_desc.add((*comp_c as usize) & (RING - 1)));
                *comp_c = comp_c.wrapping_add(1);
            }
            comp_cons.store(*comp_c, Ordering::Release);
        };
        reap(free_tx, comp_c);
        for f in out.iter() {
            let in_ring = tx_p.wrapping_sub(tx_cons.load(Ordering::Acquire));
            if in_ring as usize >= RING || f.len() > CHUNK {
                *dropped += 1;
                continue;
            }
            let Some(addr) = free_tx.pop() else {
                *dropped += 1;
                continue;
            };
            std::ptr::copy_nonoverlapping(f.as_ptr(), umem.add(addr as usize), f.len());
            *tx_desc.add((*tx_p as usize) & (RING - 1)) = [addr, f.len() as u64];
            *tx_p = tx_p.wrapping_add(1);
        }
        tx_prod.store(*tx_p, Ordering::Release);
        libc::sendto(xsk, std::ptr::null(), 0, libc::MSG_DONTWAIT, std::ptr::null(), 0);
        reap(free_tx, comp_c);
    }
}
