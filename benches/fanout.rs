//! Fan-out experiment: publish one payload to N connected sockets, the
//! core operation of a rooms/pub-sub server. Standalone on purpose (talks
//! to io_uring directly, no nexio slab).
//!
//! Modes:
//!   sync   - N blocking send(2) calls per round (what a per-connection loop costs)
//!   uring  - N Send SQEs queued, one io_uring_enter per round
//!
//! Usage: fanout <sync|uring> <conns> <size> <rounds>
//! Output: publisher-thread CPU microseconds per delivery and deliveries/sec.

use io_uring::{opcode, types, IoUring};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    os::fd::AsRawFd,
    time::{Duration, Instant},
};

fn thread_cpu() -> Duration {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_THREAD, &mut ru) };
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

fn pin(core: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 5 {
        eprintln!("usage: fanout <sync|uring> <conns> <size> <rounds>");
        std::process::exit(2);
    }
    let uring = a[1] == "uring";
    let conns: usize = a[2].parse().unwrap();
    let size: usize = a[3].parse().unwrap();
    let rounds: usize = a[4].parse().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let per_conn_bytes = size * rounds;

    // Receivers: one thread on core 1 draining all N sockets via epoll.
    let rx = std::thread::spawn(move || {
        pin(1);
        let mut socks: Vec<TcpStream> = (0..conns).map(|_| TcpStream::connect(addr).unwrap()).collect();
        for s in &socks {
            s.set_nonblocking(true).unwrap();
        }
        let ep = unsafe { libc::epoll_create1(0) };
        for (i, s) in socks.iter().enumerate() {
            let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: i as u64 };
            unsafe { libc::epoll_ctl(ep, libc::EPOLL_CTL_ADD, s.as_raw_fd(), &mut ev) };
        }
        let mut got = vec![0usize; conns];
        let mut done = 0usize;
        let mut buf = vec![0u8; 1 << 16];
        let mut evs = vec![libc::epoll_event { events: 0, u64: 0 }; 256];
        while done < conns {
            let n = unsafe { libc::epoll_wait(ep, evs.as_mut_ptr(), evs.len() as i32, 5000) };
            if n <= 0 {
                break;
            }
            for e in &evs[..n as usize] {
                let i = e.u64 as usize;
                loop {
                    match socks[i].read(&mut buf) {
                        Ok(0) => break,
                        Ok(k) => {
                            let before = got[i];
                            got[i] += k;
                            if before < per_conn_bytes && got[i] >= per_conn_bytes {
                                done += 1;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        got.iter().all(|&g| g == per_conn_bytes)
    });

    let mut pubs: Vec<TcpStream> = (0..conns).map(|_| listener.accept().unwrap().0).collect();
    for s in &pubs {
        s.set_nodelay(true).unwrap();
    }
    pin(0);
    let payload = vec![0x42u8; size];
    let mut ring = IoUring::new((conns.next_power_of_two() * 2).max(64) as u32).unwrap();
    let fds: Vec<types::Fd> = pubs.iter().map(|s| types::Fd(s.as_raw_fd())).collect();

    let cpu0 = thread_cpu();
    let t0 = Instant::now();
    for _ in 0..rounds {
        if uring {
            {
                let mut sq = ring.submission();
                for (i, fd) in fds.iter().enumerate() {
                    let e = opcode::Send::new(*fd, payload.as_ptr(), size as u32).build().user_data(i as u64);
                    unsafe { sq.push(&e).unwrap() };
                }
            }
            ring.submit_and_wait(conns).unwrap();
            let mut short = Vec::new();
            for cqe in ring.completion() {
                let r = cqe.result();
                if r < 0 {
                    eprintln!("send error {}", std::io::Error::from_raw_os_error(-r));
                    std::process::exit(1);
                }
                if (r as usize) < size {
                    short.push((cqe.user_data() as usize, r as usize));
                }
            }
            for (i, off) in short {
                pubs[i].write_all(&payload[off..]).unwrap();
            }
        } else {
            for s in pubs.iter_mut() {
                s.write_all(&payload).unwrap();
            }
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    let cpu = (thread_cpu() - cpu0).as_secs_f64();
    drop(pubs);
    let ok = rx.join().unwrap();
    let deliveries = (conns * rounds) as f64;
    println!(
        "{} conns={} size={} rounds={} secs={:.3} deliveries/s={:.0} pub_cpu_us_per_delivery={:.3} rx_ok={}",
        a[1],
        conns,
        size,
        rounds,
        secs,
        deliveries / secs,
        cpu * 1e6 / deliveries,
        ok
    );
}
