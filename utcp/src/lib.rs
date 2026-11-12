//! A deliberately small user-space TCP *server* stack for AF_XDP experiments.
//!
//! It is pure protocol logic: frames in (`Stack::handle_frame`), frames out
//! (`TxBatch`), timers polled by the caller. No sockets, no syscalls, so it
//! is unit-testable on any platform. The AF_XDP I/O loop lives in
//! `src/bin/utcp_xdp.rs`.
//!
//! What it implements: IPv4, ARP replies, passive open (3-way handshake),
//! in-order data delivery to an application callback, cumulative ACKs,
//! sequence-number wraparound, a per-connection retransmission timer with
//! exponential backoff, passive close (FIN), and RST for unknown flows.
//!
//! What it does NOT implement, so do not use it as a general TCP:
//! congestion control (it can burst up to the peer's advertised window),
//! out-of-order buffering (out-of-order segments are dropped and answered
//! with a duplicate ACK), window scaling or SACK, TCP options other than
//! sending MSS (the peer's MSS is ignored), zero-window probing, delayed
//! ACKs, TIME_WAIT, IP fragment reassembly, IPv6, active open, or any
//! SYN-flood defense. Initial sequence numbers are predictable.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

pub const MSS: usize = 1460;
const WINDOW: u16 = 65535;
const RTO_INIT: Duration = Duration::from_millis(200);
const RTO_MAX: Duration = Duration::from_secs(4);
const MAX_RETRIES: u32 = 8;
const MAX_CONNS: usize = 65536;

pub const FIN: u8 = 0x01;
pub const SYN: u8 = 0x02;
pub const RST: u8 = 0x04;
pub const PSH: u8 = 0x08;
pub const ACK: u8 = 0x10;

fn seq_lt(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}
fn seq_le(a: u32, b: u32) -> bool {
    a == b || seq_lt(a, b)
}

// ── transmit batch ───────────────────────────────────────────────────────────

/// Frames produced by the stack, stored back to back in one arena so a
/// busy receive loop does not allocate per packet.
#[derive(Default)]
pub struct TxBatch {
    buf: Vec<u8>,
    frames: Vec<(usize, usize)>,
}

impl TxBatch {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn clear(&mut self) {
        self.buf.clear();
        self.frames.clear();
    }
    pub fn len(&self) -> usize {
        self.frames.len()
    }
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
    pub fn frame(&self, i: usize) -> &[u8] {
        let (s, l) = self.frames[i];
        &self.buf[s..s + l]
    }
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.frames.iter().map(move |&(s, l)| &self.buf[s..s + l])
    }
    fn push(&mut self, f: impl FnOnce(&mut Vec<u8>)) {
        let start = self.buf.len();
        f(&mut self.buf);
        self.frames.push((start, self.buf.len() - start));
    }
}

// ── addresses, checksums, packet build/parse ─────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Addr {
    pub mac: [u8; 6],
    pub ip: [u8; 4],
    pub port: u16,
}

fn csum_add(mut sum: u32, data: &[u8]) -> u32 {
    let mut i = 0;
    while i + 1 < data.len() {
        sum += u16::from_be_bytes([data[i], data[i + 1]]) as u32;
        i += 2;
    }
    if i < data.len() {
        sum += (data[i] as u32) << 8;
    }
    sum
}

fn csum_fold(mut sum: u32) -> u16 {
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn tcp_pseudo_sum(src: &[u8; 4], dst: &[u8; 4], tcp_len: usize) -> u32 {
    let mut s = csum_add(0, src);
    s = csum_add(s, dst);
    s + 6 + tcp_len as u32
}

/// Append one Ethernet + IPv4 + TCP frame to `buf`.
#[allow(clippy::too_many_arguments)]
pub fn build_tcp(
    buf: &mut Vec<u8>,
    src: &Addr,
    dst: &Addr,
    seq: u32,
    ack: u32,
    flags: u8,
    mss_opt: bool,
    payload: &[u8],
) {
    let start = buf.len();
    buf.extend_from_slice(&dst.mac);
    buf.extend_from_slice(&src.mac);
    buf.extend_from_slice(&[0x08, 0x00]);
    let ip_start = buf.len();
    let tcp_hdr_len = if mss_opt { 24 } else { 20 };
    let total = 20 + tcp_hdr_len + payload.len();
    buf.extend_from_slice(&[0x45, 0]);
    buf.extend_from_slice(&(total as u16).to_be_bytes());
    buf.extend_from_slice(&[0, 0, 0x40, 0]); // id 0, DF
    buf.extend_from_slice(&[64, 6, 0, 0]);
    buf.extend_from_slice(&src.ip);
    buf.extend_from_slice(&dst.ip);
    let ipc = csum_fold(csum_add(0, &buf[ip_start..ip_start + 20]));
    buf[ip_start + 10..ip_start + 12].copy_from_slice(&ipc.to_be_bytes());

    let tcp_start = buf.len();
    buf.extend_from_slice(&src.port.to_be_bytes());
    buf.extend_from_slice(&dst.port.to_be_bytes());
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(&ack.to_be_bytes());
    buf.push(((tcp_hdr_len / 4) as u8) << 4);
    buf.push(flags);
    buf.extend_from_slice(&WINDOW.to_be_bytes());
    buf.extend_from_slice(&[0, 0, 0, 0]); // checksum, urgent pointer
    if mss_opt {
        buf.extend_from_slice(&[2, 4]);
        buf.extend_from_slice(&(MSS as u16).to_be_bytes());
    }
    buf.extend_from_slice(payload);
    let s = tcp_pseudo_sum(&src.ip, &dst.ip, buf.len() - tcp_start);
    let c = csum_fold(csum_add(s, &buf[tcp_start..]));
    buf[tcp_start + 16..tcp_start + 18].copy_from_slice(&c.to_be_bytes());
    while buf.len() - start < 60 {
        buf.push(0); // Ethernet minimum frame size
    }
}

#[derive(Debug)]
pub struct Segment<'a> {
    pub src_mac: [u8; 6],
    pub src_ip: [u8; 4],
    pub dst_ip: [u8; 4],
    pub sport: u16,
    pub dport: u16,
    pub seq: u32,
    pub ack: u32,
    pub flags: u8,
    pub window: u16,
    pub payload: &'a [u8],
}

/// Parse and fully validate (lengths, fragmentation, both checksums) an
/// Ethernet/IPv4/TCP frame. Anything malformed returns `None`.
pub fn parse_tcp(f: &[u8]) -> Option<Segment<'_>> {
    if f.len() < 14 + 20 + 20 || f[12..14] != [0x08, 0x00] {
        return None;
    }
    let ip = &f[14..];
    let ihl = ((ip[0] & 0x0f) * 4) as usize;
    if ip[0] >> 4 != 4 || ihl < 20 || ip.len() < ihl {
        return None;
    }
    let total = u16::from_be_bytes([ip[2], ip[3]]) as usize;
    if total < ihl + 20 || total > ip.len() || ip[9] != 6 {
        return None;
    }
    if u16::from_be_bytes([ip[6], ip[7]]) & 0x3fff != 0 {
        return None; // fragment: no reassembly
    }
    if csum_fold(csum_add(0, &ip[..ihl])) != 0 {
        return None;
    }
    let tcp = &ip[ihl..total];
    let doff = ((tcp[12] >> 4) * 4) as usize;
    if doff < 20 || tcp.len() < doff {
        return None;
    }
    let src_ip: [u8; 4] = ip[12..16].try_into().ok()?;
    let dst_ip: [u8; 4] = ip[16..20].try_into().ok()?;
    let s = tcp_pseudo_sum(&src_ip, &dst_ip, tcp.len());
    if csum_fold(csum_add(s, tcp)) != 0 {
        return None;
    }
    Some(Segment {
        src_mac: f[6..12].try_into().ok()?,
        src_ip,
        dst_ip,
        sport: u16::from_be_bytes([tcp[0], tcp[1]]),
        dport: u16::from_be_bytes([tcp[2], tcp[3]]),
        seq: u32::from_be_bytes([tcp[4], tcp[5], tcp[6], tcp[7]]),
        ack: u32::from_be_bytes([tcp[8], tcp[9], tcp[10], tcp[11]]),
        flags: tcp[13],
        window: u16::from_be_bytes([tcp[14], tcp[15]]),
        payload: &tcp[doff..],
    })
}

#[allow(clippy::too_many_arguments)]
fn emit(
    local: &Addr,
    peer: &Addr,
    out: &mut TxBatch,
    seq: u32,
    ack: u32,
    flags: u8,
    mss: bool,
    payload: &[u8],
) {
    out.push(|b| build_tcp(b, local, peer, seq, ack, flags, mss, payload));
}

// ── connections ──────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct ConnKey {
    ip: [u8; 4],
    port: u16,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    SynRcvd,
    Established,
    LastAck,
}

struct Conn {
    peer: Addr,
    state: State,
    iss: u32,
    snd_una: u32,
    snd_nxt: u32,
    rcv_nxt: u32,
    snd_wnd: u32,
    /// Unacknowledged bytes first, then not-yet-sent bytes. Excludes SYN/FIN.
    send_buf: VecDeque<u8>,
    fin_queued: bool,
    fin_seq: Option<u32>,
    last_tx: Instant,
    rto: Duration,
    retries: u32,
}

impl Conn {
    fn data_in_flight(&self) -> usize {
        let mut n = self.snd_nxt.wrapping_sub(self.snd_una);
        if self.state == State::SynRcvd {
            n = n.saturating_sub(1); // the SYN
        }
        if self.fin_seq.is_some() {
            n = n.saturating_sub(1);
        }
        n as usize
    }

    fn send_synack(&self, local: &Addr, out: &mut TxBatch) {
        emit(local, &self.peer, out, self.iss, self.rcv_nxt, SYN | ACK, true, &[]);
    }

    /// Send whatever the window allows, then FIN if requested and drained.
    /// Returns whether anything carrying an ACK was sent.
    fn flush(&mut self, now: Instant, local: &Addr, out: &mut TxBatch) -> bool {
        let mut sent = false;
        loop {
            let in_flight = self.data_in_flight();
            let unsent = self.send_buf.len() - in_flight;
            let room = (self.snd_wnd as usize).saturating_sub(in_flight);
            let n = unsent.min(room).min(MSS);
            if n == 0 {
                break;
            }
            let (seq, rcv_nxt, peer) = (self.snd_nxt, self.rcv_nxt, self.peer);
            let buf = self.send_buf.make_contiguous();
            emit(local, &peer, out, seq, rcv_nxt, ACK | PSH, false, &buf[in_flight..in_flight + n]);
            self.snd_nxt = self.snd_nxt.wrapping_add(n as u32);
            sent = true;
        }
        let drained = self.send_buf.len() == self.data_in_flight();
        if self.fin_queued && self.fin_seq.is_none() && drained {
            emit(local, &self.peer, out, self.snd_nxt, self.rcv_nxt, FIN | ACK, false, &[]);
            self.fin_seq = Some(self.snd_nxt);
            self.snd_nxt = self.snd_nxt.wrapping_add(1);
            sent = true;
        }
        if sent {
            self.last_tx = now;
        }
        sent
    }

    /// Returns true if the connection should be removed.
    fn on_segment<F: FnMut(&[u8], &mut Vec<u8>)>(
        &mut self,
        p: &Segment<'_>,
        now: Instant,
        local: &Addr,
        app: &mut F,
        out: &mut TxBatch,
    ) -> bool {
        if p.flags & RST != 0 {
            // Accept a reset if it hits the receive window (or any reset during the handshake).
            return self.state == State::SynRcvd
                || (seq_le(self.rcv_nxt, p.seq) && seq_lt(p.seq, self.rcv_nxt.wrapping_add(WINDOW as u32)));
        }

        if self.state == State::SynRcvd {
            if p.flags & SYN != 0 {
                self.send_synack(local, out); // peer retransmitted its SYN
                return false;
            }
            if p.flags & ACK != 0 && p.ack == self.iss.wrapping_add(1) {
                self.snd_una = p.ack;
                self.state = State::Established;
                self.retries = 0;
                self.rto = RTO_INIT;
            } else {
                return false;
            }
        }

        let mut fin_acked = false;
        if p.flags & ACK != 0 {
            if seq_lt(self.snd_una, p.ack) && seq_le(p.ack, self.snd_nxt) {
                let acked = p.ack.wrapping_sub(self.snd_una) as usize;
                let mut data_acked = acked;
                if let Some(fs) = self.fin_seq {
                    if seq_lt(fs, p.ack) {
                        data_acked -= 1;
                        fin_acked = true;
                    }
                }
                self.send_buf.drain(..data_acked.min(self.send_buf.len()));
                self.snd_una = p.ack;
                self.retries = 0;
                self.rto = RTO_INIT;
                self.last_tx = now;
            }
            self.snd_wnd = p.window as u32;
        }
        if fin_acked && self.state == State::LastAck {
            return true;
        }

        let fin = p.flags & FIN != 0;
        let mut need_ack = false;
        if !p.payload.is_empty() || fin {
            need_ack = true;
            let fin_pos = p.seq.wrapping_add(p.payload.len() as u32);
            let mut payload = p.payload;
            let mut seq = p.seq;
            if seq_lt(seq, self.rcv_nxt) {
                let overlap = self.rcv_nxt.wrapping_sub(seq) as usize;
                if overlap >= payload.len() {
                    payload = &[];
                } else {
                    payload = &payload[overlap..];
                }
                seq = self.rcv_nxt;
            }
            if seq == self.rcv_nxt {
                if !payload.is_empty() && self.state == State::Established {
                    let mut reply = Vec::new();
                    app(payload, &mut reply);
                    self.rcv_nxt = self.rcv_nxt.wrapping_add(payload.len() as u32);
                    self.send_buf.extend(reply);
                }
                if fin && self.rcv_nxt == fin_pos && self.state == State::Established {
                    self.rcv_nxt = self.rcv_nxt.wrapping_add(1);
                    self.fin_queued = true;
                    self.state = State::LastAck;
                }
            }
            // seq beyond rcv_nxt: out of order, dropped; the ACK below is a duplicate ACK.
        }

        let sent = self.flush(now, local, out);
        if need_ack && !sent {
            emit(local, &self.peer, out, self.snd_nxt, self.rcv_nxt, ACK, false, &[]);
        }
        false
    }
}

// ── the stack ────────────────────────────────────────────────────────────────

pub struct Stack {
    local: Addr,
    conns: HashMap<ConnKey, Conn>,
    epoch: Instant,
    iss_counter: u32,
}

impl Stack {
    pub fn new(mac: [u8; 6], ip: [u8; 4], port: u16, now: Instant) -> Self {
        Self {
            local: Addr { mac, ip, port },
            conns: HashMap::new(),
            epoch: now,
            iss_counter: 0,
        }
    }

    pub fn conn_count(&self) -> usize {
        self.conns.len()
    }

    fn new_iss(&mut self, now: Instant) -> u32 {
        self.iss_counter = self.iss_counter.wrapping_add(64_007);
        ((now.duration_since(self.epoch).as_nanos() / 4) as u32).wrapping_add(self.iss_counter)
    }

    fn arp(&self, f: &[u8], out: &mut TxBatch) {
        // Ethernet(14) + ARP(28): htype 1, ptype 0x0800, hlen 6, plen 4, op 1 (request)
        if f.len() < 42 || f[14..20] != [0, 1, 8, 0, 6, 4] || f[20..22] != [0, 1] || f[38..42] != self.local.ip {
            return;
        }
        let local = self.local;
        let sender_mac = &f[22..28];
        let sender_ip = &f[28..32];
        out.push(|b| {
            b.extend_from_slice(sender_mac);
            b.extend_from_slice(&local.mac);
            b.extend_from_slice(&[0x08, 0x06, 0, 1, 8, 0, 6, 4, 0, 2]);
            b.extend_from_slice(&local.mac);
            b.extend_from_slice(&local.ip);
            b.extend_from_slice(sender_mac);
            b.extend_from_slice(sender_ip);
            while b.len() % 60 != 0 && b.len() < 60 {
                b.push(0);
            }
        });
    }

    pub fn handle_frame<F: FnMut(&[u8], &mut Vec<u8>)>(
        &mut self,
        frame: &[u8],
        now: Instant,
        app: &mut F,
        out: &mut TxBatch,
    ) {
        if frame.len() < 14 {
            return;
        }
        if frame[12..14] == [0x08, 0x06] {
            self.arp(frame, out);
            return;
        }
        let Some(seg) = parse_tcp(frame) else { return };
        let local = self.local;
        if seg.dst_ip != local.ip || seg.dport != local.port {
            return;
        }
        if frame[0..6] != local.mac && frame[0..6] != [0xff; 6] {
            return;
        }
        let key = ConnKey { ip: seg.src_ip, port: seg.sport };
        let peer = Addr { mac: seg.src_mac, ip: seg.src_ip, port: seg.sport };

        if !self.conns.contains_key(&key) {
            if seg.flags & RST != 0 {
                return;
            }
            if seg.flags & SYN != 0 && seg.flags & ACK == 0 {
                if self.conns.len() >= MAX_CONNS {
                    return;
                }
                let iss = self.new_iss(now);
                let conn = Conn {
                    peer,
                    state: State::SynRcvd,
                    iss,
                    snd_una: iss,
                    snd_nxt: iss.wrapping_add(1),
                    rcv_nxt: seg.seq.wrapping_add(1),
                    snd_wnd: seg.window as u32,
                    send_buf: VecDeque::new(),
                    fin_queued: false,
                    fin_seq: None,
                    last_tx: now,
                    rto: RTO_INIT,
                    retries: 0,
                };
                conn.send_synack(&local, out);
                self.conns.insert(key, conn);
                return;
            }
            // Segment for a connection we do not have: reset it (RFC 793).
            if seg.flags & ACK != 0 {
                emit(&local, &peer, out, seg.ack, 0, RST, false, &[]);
            } else {
                let len = seg.payload.len() as u32 + (seg.flags & SYN != 0) as u32 + (seg.flags & FIN != 0) as u32;
                emit(&local, &peer, out, 0, seg.seq.wrapping_add(len), RST | ACK, false, &[]);
            }
            return;
        }

        let remove = match self.conns.get_mut(&key) {
            Some(conn) => conn.on_segment(&seg, now, &local, app, out),
            None => false,
        };
        if remove {
            self.conns.remove(&key);
        }
    }

    /// Retransmit anything whose timer expired; drop connections that gave up.
    pub fn poll_timers(&mut self, now: Instant, out: &mut TxBatch) {
        let local = self.local;
        self.conns.retain(|_, c| {
            if c.snd_una == c.snd_nxt || now.duration_since(c.last_tx) < c.rto {
                return true;
            }
            if c.retries >= MAX_RETRIES {
                return false;
            }
            c.retries += 1;
            c.rto = (c.rto * 2).min(RTO_MAX);
            c.last_tx = now;
            if c.state == State::SynRcvd {
                c.send_synack(&local, out);
                return true;
            }
            let in_flight = c.data_in_flight();
            if in_flight > 0 {
                let n = in_flight.min(MSS);
                let (una, rcv_nxt, peer) = (c.snd_una, c.rcv_nxt, c.peer);
                let buf = c.send_buf.make_contiguous();
                emit(&local, &peer, out, una, rcv_nxt, ACK | PSH, false, &buf[..n]);
            } else if let Some(fs) = c.fin_seq {
                emit(&local, &c.peer, out, fs, c.rcv_nxt, FIN | ACK, false, &[]);
            }
            true
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SRV: Addr = Addr { mac: [2, 0, 0, 0, 0, 2], ip: [10, 9, 0, 2], port: 9000 };
    const CLI: Addr = Addr { mac: [2, 0, 0, 0, 0, 1], ip: [10, 9, 0, 1], port: 40000 };

    fn echo(data: &[u8], reply: &mut Vec<u8>) {
        reply.extend_from_slice(data);
    }

    struct Rig {
        stack: Stack,
        out: TxBatch,
        t0: Instant,
        seq: u32, // client's next sequence number
        ack: u32, // client's ack (server seq + consumed)
    }

    impl Rig {
        fn new(client_iss: u32) -> Self {
            let t0 = Instant::now();
            Rig { stack: Stack::new(SRV.mac, SRV.ip, SRV.port, t0), out: TxBatch::new(), t0, seq: client_iss, ack: 0 }
        }
        fn send(&mut self, seq: u32, ack: u32, flags: u8, payload: &[u8], at: Duration) {
            let mut f = Vec::new();
            build_tcp(&mut f, &CLI, &SRV, seq, ack, flags, flags & SYN != 0, payload);
            self.out.clear();
            self.stack.handle_frame(&f, self.t0 + at, &mut echo, &mut self.out);
        }
        fn handshake(&mut self) {
            let iss = self.seq;
            self.send(iss, 0, SYN, &[], Duration::ZERO);
            assert_eq!(self.out.len(), 1);
            let s = parse_tcp(self.out.frame(0)).expect("valid synack");
            assert_eq!(s.flags, SYN | ACK);
            assert_eq!(s.ack, iss.wrapping_add(1));
            self.ack = s.seq.wrapping_add(1);
            self.seq = iss.wrapping_add(1);
            self.send(self.seq, self.ack, ACK, &[], Duration::ZERO);
            assert!(self.out.is_empty());
            assert_eq!(self.stack.conn_count(), 1);
        }
    }

    #[test]
    fn handshake_then_echo() {
        let mut r = Rig::new(1000);
        r.handshake();
        r.send(r.seq, r.ack, ACK | PSH, b"hello", Duration::ZERO);
        assert_eq!(r.out.len(), 1);
        let s = parse_tcp(r.out.frame(0)).unwrap();
        assert_eq!(s.payload, b"hello");
        assert_eq!(s.ack, r.seq + 5);
        assert_eq!(s.seq, r.ack);
    }

    #[test]
    fn sequence_wraparound_is_handled() {
        let mut r = Rig::new(0xFFFF_FFF0);
        r.handshake();
        let big = vec![7u8; 100]; // crosses the 2^32 boundary
        r.send(r.seq, r.ack, ACK | PSH, &big, Duration::ZERO);
        let s = parse_tcp(r.out.frame(0)).unwrap();
        assert_eq!(s.payload, &big[..]);
        assert_eq!(s.ack, 0xFFFF_FFF1u32.wrapping_add(100));
    }

    #[test]
    fn out_of_order_segment_is_dropped_with_duplicate_ack() {
        let mut r = Rig::new(1);
        r.handshake();
        let expect_ack = r.seq;
        r.send(r.seq + 10, r.ack, ACK | PSH, b"later", Duration::ZERO);
        assert_eq!(r.out.len(), 1);
        let s = parse_tcp(r.out.frame(0)).unwrap();
        assert!(s.payload.is_empty(), "out-of-order data must not be echoed");
        assert_eq!(s.ack, expect_ack, "duplicate ACK points at the missing byte");
        r.send(r.seq, r.ack, ACK | PSH, b"first", Duration::ZERO);
        assert_eq!(parse_tcp(r.out.frame(0)).unwrap().payload, b"first");
    }

    #[test]
    fn duplicate_data_is_not_echoed_twice() {
        let mut r = Rig::new(1);
        r.handshake();
        r.send(r.seq, r.ack, ACK | PSH, b"once", Duration::ZERO);
        let acked = r.seq + 4;
        r.send(r.seq, r.ack, ACK | PSH, b"once", Duration::ZERO); // retransmitted by peer
        assert_eq!(r.out.len(), 1);
        let s = parse_tcp(r.out.frame(0)).unwrap();
        assert!(s.payload.is_empty());
        assert_eq!(s.ack, acked);
    }

    #[test]
    fn retransmits_unacked_data_after_rto() {
        let mut r = Rig::new(1);
        r.handshake();
        r.send(r.seq, r.ack, ACK | PSH, b"data", Duration::ZERO);
        assert_eq!(r.out.len(), 1); // the echo, never acknowledged
        r.out.clear();
        r.stack.poll_timers(r.t0 + Duration::from_millis(100), &mut r.out);
        assert!(r.out.is_empty(), "timer must not fire early");
        r.stack.poll_timers(r.t0 + Duration::from_millis(250), &mut r.out);
        assert_eq!(r.out.len(), 1);
        let s = parse_tcp(r.out.frame(0)).unwrap();
        assert_eq!(s.payload, b"data");
        assert_eq!(s.seq, r.ack, "retransmission restarts at the first unacked byte");
    }

    #[test]
    fn ack_stops_retransmission() {
        let mut r = Rig::new(1);
        r.handshake();
        r.send(r.seq, r.ack, ACK | PSH, b"data", Duration::ZERO);
        let echo_ack = r.ack + 4;
        r.send(r.seq + 4, echo_ack, ACK, &[], Duration::from_millis(10));
        r.out.clear();
        r.stack.poll_timers(r.t0 + Duration::from_secs(1), &mut r.out);
        assert!(r.out.is_empty());
    }

    #[test]
    fn fin_exchange_removes_connection() {
        let mut r = Rig::new(1);
        r.handshake();
        r.send(r.seq, r.ack, FIN | ACK, &[], Duration::ZERO);
        assert_eq!(r.out.len(), 1);
        let s = parse_tcp(r.out.frame(0)).unwrap();
        assert_eq!(s.flags & (FIN | ACK), FIN | ACK, "server closes right after the client");
        assert_eq!(s.ack, r.seq + 1);
        let fin_seq = s.seq;
        r.send(r.seq + 1, fin_seq + 1, ACK, &[], Duration::ZERO);
        assert_eq!(r.stack.conn_count(), 0);
    }

    #[test]
    fn unknown_connection_gets_rst() {
        let mut r = Rig::new(1);
        r.send(5, 77, ACK, b"x", Duration::ZERO);
        let s = parse_tcp(r.out.frame(0)).unwrap();
        assert_ne!(s.flags & RST, 0);
        assert_eq!(s.seq, 77);
    }

    #[test]
    fn corrupted_checksum_is_dropped() {
        let mut r = Rig::new(1);
        let mut f = Vec::new();
        build_tcp(&mut f, &CLI, &SRV, 1, 0, SYN, true, &[]);
        let last = f.len() - 1 - 6; // inside the TCP header/options, before padding
        f[last] ^= 0x01;
        r.stack.handle_frame(&f, r.t0, &mut echo, &mut r.out);
        assert!(r.out.is_empty());
        assert_eq!(r.stack.conn_count(), 0);
    }

    #[test]
    fn answers_arp_request_for_its_address() {
        let mut r = Rig::new(1);
        let mut f = vec![0xff; 6];
        f.extend_from_slice(&CLI.mac);
        f.extend_from_slice(&[0x08, 0x06, 0, 1, 8, 0, 6, 4, 0, 1]);
        f.extend_from_slice(&CLI.mac);
        f.extend_from_slice(&CLI.ip);
        f.extend_from_slice(&[0; 6]);
        f.extend_from_slice(&SRV.ip);
        r.stack.handle_frame(&f, r.t0, &mut echo, &mut r.out);
        assert_eq!(r.out.len(), 1);
        let a = r.out.frame(0);
        assert_eq!(&a[0..6], &CLI.mac);
        assert_eq!(&a[20..22], &[0, 2]);
        assert_eq!(&a[22..28], &SRV.mac);
    }

    #[test]
    fn large_echo_is_split_into_mss_segments() {
        let mut r = Rig::new(1);
        r.handshake();
        let data = vec![9u8; 3000];
        r.send(r.seq, r.ack, ACK | PSH, &data, Duration::ZERO);
        let lens: Vec<usize> = r.out.iter().map(|f| parse_tcp(f).unwrap().payload.len()).collect();
        assert_eq!(lens, vec![1460, 1460, 80]);
    }
}
