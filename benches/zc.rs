//! Zero-copy send experiment, deliberately standalone: talks to io_uring
//! directly so nexio's verified completion slab is not touched. SendZc
//! produces two CQEs per send (the result, then a buffer-release
//! notification), which would need slab changes to support properly.
//!
//! Usage: zc <send|zc> <msg_size> <count>
//! Output: mode size count secs MB/s sender_cpu_us_per_msg

use io_uring::{cqueue, opcode, types, IoUring};
use std::{
    io::Read,
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
    if a.len() < 4 {
        eprintln!("usage: zc <send|zc> <msg_size> <count>");
        std::process::exit(2);
    }
    let zc = a[1] == "zc";
    let size: usize = a[2].parse().unwrap();
    let count: usize = a[3].parse().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let total_bytes = size * count;

    let rx = std::thread::spawn(move || {
        pin(1);
        let (mut s, _) = listener.accept().unwrap();
        let mut buf = vec![0u8; 1 << 20];
        let mut got = 0usize;
        while got < total_bytes {
            match s.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => got += n,
            }
        }
        got
    });

    pin(0);
    let stream = TcpStream::connect(addr).unwrap();
    stream.set_nodelay(true).unwrap();
    let fd = types::Fd(stream.as_raw_fd());
    let mut ring = IoUring::new(64).unwrap();
    let buf = vec![0x5Au8; size];

    let cpu0 = thread_cpu();
    let t0 = Instant::now();
    for _ in 0..count {
        let sent_ok;
        if zc {
            let e = opcode::SendZc::new(fd, buf.as_ptr(), size as u32).build().user_data(1);
            unsafe { ring.submission().push(&e).unwrap() };
            ring.submit_and_wait(1).unwrap();
            let mut notif = false;
            let mut res = 0i32;
            let mut more = false;
            // First CQE carries the result (F_MORE set => a notification follows),
            // second carries F_NOTIF once the kernel no longer needs the buffer.
            while !(notif || (!more && res != 0)) {
                if ring.completion().is_empty() {
                    ring.submit_and_wait(1).unwrap();
                }
                for cqe in ring.completion() {
                    // IORING_CQE_F_NOTIF = 1 << 3 (cqueue::notif only exists in io-uring >= 0.7)
                    if cqe.flags() & 8 != 0 {
                        notif = true;
                    } else {
                        res = cqe.result();
                        more = cqueue::more(cqe.flags());
                    }
                }
            }
            sent_ok = res;
        } else {
            let e = opcode::Send::new(fd, buf.as_ptr(), size as u32).build().user_data(1);
            unsafe { ring.submission().push(&e).unwrap() };
            ring.submit_and_wait(1).unwrap();
            let cqe = ring.completion().next().unwrap();
            sent_ok = cqe.result();
        }
        if sent_ok < 0 {
            eprintln!("send failed: {}", std::io::Error::from_raw_os_error(-sent_ok));
            std::process::exit(1);
        }
        // Short sends are possible with large sizes; finish the message synchronously.
        let mut off = sent_ok as usize;
        while off < size {
            use std::io::Write;
            (&stream).write_all(&buf[off..]).unwrap();
            off = size;
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    let cpu = (thread_cpu() - cpu0).as_secs_f64();
    drop(stream);
    let got = rx.join().unwrap();
    println!(
        "{} size={} count={} secs={:.3} MB/s={:.0} sender_cpu_us_per_msg={:.2} rx_bytes_ok={}",
        a[1],
        size,
        count,
        secs,
        total_bytes as f64 / 1e6 / secs,
        cpu * 1e6 / count as f64,
        got == total_bytes
    );
}
