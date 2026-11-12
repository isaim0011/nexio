//! Standalone io_uring TCP echo server for A/B-ing two techniques before any
//! of them touches nexio's verified completion slab (multishot ops deliver
//! several CQEs per SQE, which the slab does not model).
//!
//!   base   - single-shot accept (re-armed), Recv then Send per message (what nexio does today)
//!   fixed  - same, but ReadFixed/WriteFixed on pre-registered buffers
//!   multi  - multishot accept + multishot recv with a provided-buffer ring, Send per message
//!
//! Usage: mshot <base|fixed|multi> <port> <msg_size>
//! Prints `READY <pid>` then serves forever; measure CPU externally via /proc.

use io_uring::{cqueue, opcode, squeue, types, IoUring};
use std::{
    net::TcpListener,
    os::fd::AsRawFd,
    sync::atomic::{AtomicU16, Ordering},
};

const ACCEPT: u64 = 1;
const RECV: u64 = 2;
const SEND: u64 = 3;
const MAX_CONNS: usize = 4096;
const BGID: u16 = 7;

fn ud(kind: u64, id: usize) -> u64 {
    (kind << 32) | id as u64
}

fn push(ring: &mut IoUring, e: &squeue::Entry) {
    loop {
        if unsafe { ring.submission().push(e) }.is_ok() {
            return;
        }
        ring.submit().unwrap();
    }
}

struct Conn {
    fd: i32,
    buf: Vec<u8>,
    pending_bid: u16,
}

fn nodelay(fd: i32) {
    let one: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &one as *const _ as *const _,
            4,
        );
    }
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
        eprintln!("usage: mshot <base|fixed|multi> <port> <msg_size>");
        std::process::exit(2);
    }
    let mode = a[1].as_str();
    let port: u16 = a[2].parse().unwrap();
    let size: usize = a[3].parse().unwrap();
    pin(0);

    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    let lfd = types::Fd(listener.as_raw_fd());
    let mut ring: IoUring = IoUring::builder()
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .setup_single_issuer()
        .build(1024)
        .unwrap();

    let mut conns: Vec<Option<Conn>> = (0..MAX_CONNS).map(|_| None).collect();
    let mut free: Vec<usize> = (0..MAX_CONNS).rev().collect();

    // fixed: one registered buffer per connection slot
    // One contiguous registered region (registering thousands of separate
    // buffers pins a page each and exceeds RLIMIT_MEMLOCK); every slot's
    // pointer lies inside it and all ops use buf_index 0.
    let mut pool: Vec<u8> = Vec::new();
    if mode == "fixed" {
        pool = vec![0u8; MAX_CONNS * size];
        let iov = [libc::iovec {
            iov_base: pool.as_mut_ptr() as *mut _,
            iov_len: pool.len(),
        }];
        unsafe { ring.submitter().register_buffers(&iov).unwrap() };
    }

    // multi: provided-buffer ring
    let mut bufs: Vec<u8> = Vec::new();
    let mut br_base: *mut types::BufRingEntry = std::ptr::null_mut();
    let mut br_tail: u16 = 0;
    if mode == "multi" {
        bufs = vec![0u8; MAX_CONNS * size];
        let bytes = MAX_CONNS * std::mem::size_of::<types::BufRingEntry>();
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        assert!(p != libc::MAP_FAILED);
        br_base = p as *mut types::BufRingEntry;
        unsafe {
            ring.submitter()
                .register_buf_ring(br_base as u64, MAX_CONNS as u16, BGID)
                .unwrap()
        };
        for bid in 0..MAX_CONNS {
            unsafe {
                let e = &mut *br_base.add(bid);
                e.set_addr(bufs.as_ptr() as u64 + (bid * size) as u64);
                e.set_len(size as u32);
                e.set_bid(bid as u16);
            }
        }
        br_tail = MAX_CONNS as u16;
        unsafe {
            (*(types::BufRingEntry::tail(br_base) as *const AtomicU16))
                .store(br_tail, Ordering::Release)
        };
    }
    let give_back = |bid: u16, tail: &mut u16| unsafe {
        let e = &mut *br_base.add((*tail as usize) & (MAX_CONNS - 1));
        e.set_addr(bufs.as_ptr() as u64 + (bid as usize * size) as u64);
        e.set_len(size as u32);
        e.set_bid(bid);
        *tail = tail.wrapping_add(1);
        (*(types::BufRingEntry::tail(br_base) as *const AtomicU16)).store(*tail, Ordering::Release);
    };

    let arm_accept = |ring: &mut IoUring| {
        let e = if mode == "multi" {
            opcode::AcceptMulti::new(lfd)
                .build()
                .user_data(ud(ACCEPT, 0))
        } else {
            opcode::Accept::new(lfd, std::ptr::null_mut(), std::ptr::null_mut())
                .build()
                .user_data(ud(ACCEPT, 0))
        };
        push(ring, &e);
    };
    let arm_recv = |ring: &mut IoUring, id: usize, fd: i32, buf: *mut u8| {
        let f = types::Fd(fd);
        let e = match mode {
            "fixed" => opcode::ReadFixed::new(f, buf, size as u32, 0).build(),
            "multi" => opcode::RecvMulti::new(f, BGID).build(),
            _ => opcode::Recv::new(f, buf, size as u32).build(),
        };
        push(ring, &e.user_data(ud(RECV, id)));
    };

    arm_accept(&mut ring);
    println!("READY {}", std::process::id());

    let mut batch: Vec<(u64, i32, u32)> = Vec::with_capacity(512);
    loop {
        ring.submit_and_wait(1).unwrap();
        batch.clear();
        batch.extend(
            ring.completion()
                .map(|c| (c.user_data(), c.result(), c.flags())),
        );
        for &(u, res, flags) in &batch {
            let (kind, id) = (u >> 32, (u & 0xffff_ffff) as usize);
            match kind {
                ACCEPT => {
                    if res >= 0 {
                        let Some(slot) = free.pop() else {
                            unsafe { libc::close(res) };
                            continue;
                        };
                        nodelay(res);
                        let mut buf = if mode == "fixed" {
                            Vec::new()
                        } else {
                            vec![0u8; if mode == "multi" { 0 } else { size }]
                        };
                        let ptr = if mode == "fixed" {
                            unsafe { pool.as_mut_ptr().add(slot * size) }
                        } else {
                            buf.as_mut_ptr()
                        };
                        conns[slot] = Some(Conn {
                            fd: res,
                            buf: std::mem::take(&mut buf),
                            pending_bid: 0,
                        });
                        let ptr = if mode == "base" {
                            conns[slot].as_mut().unwrap().buf.as_mut_ptr()
                        } else {
                            ptr
                        };
                        arm_recv(&mut ring, slot, res, ptr);
                    }
                    if mode != "multi" || !cqueue::more(flags) {
                        arm_accept(&mut ring);
                    }
                }
                RECV => {
                    let Some(c) = conns[id].as_mut() else {
                        continue;
                    };
                    if res <= 0 {
                        if mode != "multi" || !cqueue::more(flags) {
                            unsafe { libc::close(c.fd) };
                            conns[id] = None;
                            free.push(id);
                        }
                        continue;
                    }
                    let f = types::Fd(c.fd);
                    let e = match mode {
                        "fixed" => opcode::WriteFixed::new(
                            f,
                            unsafe { pool.as_mut_ptr().add(id * size) },
                            res as u32,
                            0,
                        )
                        .build(),
                        "multi" => {
                            let bid = cqueue::buffer_select(flags).expect("buffer id");
                            c.pending_bid = bid;
                            let p = unsafe { bufs.as_ptr().add(bid as usize * size) };
                            opcode::Send::new(f, p, res as u32).build()
                        }
                        _ => opcode::Send::new(f, c.buf.as_ptr(), res as u32).build(),
                    };
                    push(&mut ring, &e.user_data(ud(SEND, id)));
                    if mode == "multi" && !cqueue::more(flags) {
                        arm_recv(&mut ring, id, c.fd, std::ptr::null_mut());
                    }
                }
                SEND => {
                    let Some(c) = conns[id].as_mut() else {
                        continue;
                    };
                    if mode == "multi" {
                        give_back(c.pending_bid, &mut br_tail);
                    }
                    if res < 0 {
                        unsafe { libc::close(c.fd) };
                        conns[id] = None;
                        free.push(id);
                        continue;
                    }
                    if mode != "multi" {
                        let ptr = match mode {
                            "fixed" => unsafe { pool.as_mut_ptr().add(id * size) },
                            _ => c.buf.as_mut_ptr(),
                        };
                        let fd = c.fd;
                        arm_recv(&mut ring, id, fd, ptr);
                    }
                }
                _ => {}
            }
        }
    }
}
