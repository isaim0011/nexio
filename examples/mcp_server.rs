//! Minimal Model Context Protocol (MCP) stdio server for AI coding agents.
//!
//! Run directly with:
//! ```text
//! cargo run --quiet --example mcp_server
//! ```
//!
//! Exposes four read-only tools over JSON-RPC 2.0 (line-delimited stdin/stdout):
//! - `nexio_runtime_info`: active backend, `io_uring_available()`, OS/arch, and `ReactorStats`.
//! - `nexio_platform_matrix`: full Linux (`io_uring` / `epoll` / `runtime-fallback`), Windows (`IOCP`),
//!   macOS (`kqueue`), and Unix capability matrix.
//! - `nexio_api_reference`: concise API & snippet reference for every nexio module (`runtime`,
//!   `macros`, `net`, `unix`, `fs`, `sync`, `time`, `process`, `signal`, `dns`, `http`, `tls`,
//!   `compat`, `multishot`, `fallback`, `platforms`, or `all`).
//! - `nexio_check_workspace`: inspects a `Cargo.toml` for `nexio` features and migration notes.

use std::{
    env, fs,
    io::{self, BufRead, Write},
    path::Path,
};

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = io::BufWriter::new(stdout.lock());

    for line in stdin.lock().lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(resp) = handle_jsonrpc(trimmed) {
            writeln!(out, "{resp}")?;
            out.flush()?;
        }
    }
    Ok(())
}

fn handle_jsonrpc(msg: &str) -> Option<String> {
    let id = extract_id(msg);
    let method = extract_string_field(msg, "method").unwrap_or_default();

    // Notifications (no `id`) do not receive a JSON-RPC response.
    let id_json = id?;

    let result = match method.as_str() {
        "initialize" => Ok(concat!(
            r#"{"protocolVersion":"2024-11-05","#,
            r#""capabilities":{"tools":{}},"#,
            r#""serverInfo":{"name":"nexio-mcp","version":""#,
            env!("CARGO_PKG_VERSION"),
            r#""}}"#
        )
        .to_string()),
        "ping" => Ok("{}".to_string()),
        "tools/list" => Ok(tools_list_json().to_string()),
        "tools/call" => {
            let name = extract_nested_string(msg, "params", "name").unwrap_or_default();
            match name.as_str() {
                "nexio_runtime_info" => Ok(tool_text_result(&runtime_info_text())),
                "nexio_platform_matrix" => {
                    let target =
                        extract_string_field(msg, "platform").unwrap_or_else(|| "all".into());
                    Ok(tool_text_result(&platform_matrix_text(&target)))
                }
                "nexio_api_reference" => {
                    let topic = extract_string_field(msg, "topic").unwrap_or_else(|| "all".into());
                    Ok(tool_text_result(&api_reference_text(&topic)))
                }
                "nexio_check_workspace" => {
                    let path = extract_string_field(msg, "manifest_path")
                        .unwrap_or_else(|| "Cargo.toml".into());
                    Ok(tool_text_result(&check_workspace_text(&path)))
                }
                other => Err((-32602, format!("unknown tool: {other}"))),
            }
        }
        other => Err((-32601, format!("method not found: {other}"))),
    };

    Some(match result {
        Ok(res) => format!(r#"{{"jsonrpc":"2.0","id":{id_json},"result":{res}}}"#),
        Err((code, err)) => format!(
            r#"{{"jsonrpc":"2.0","id":{id_json},"error":{{"code":{code},"message":"{}"}}}}"#,
            json_escape(&err)
        ),
    })
}

fn tools_list_json() -> &'static str {
    concat!(
        r#"{"tools":["#,
        r#"{"name":"nexio_runtime_info","description":"Return nexio version, OS/arch, io_uring_available(), active backend name, and ReactorStats.","inputSchema":{"type":"object","properties":{},"additionalProperties":false}},"#,
        r#"{"name":"nexio_platform_matrix","description":"Return nexio backend and feature support matrix for Linux (io_uring/epoll/runtime-fallback), Windows (IOCP), macOS (kqueue), or all.","inputSchema":{"type":"object","properties":{"platform":{"type":"string","description":"Platform filter: linux, windows, macos, unix, or all"}},"additionalProperties":false}},"#,
        r#"{"name":"nexio_api_reference","description":"Return concise nexio API usage and feature flags for any module or topic.","inputSchema":{"type":"object","properties":{"topic":{"type":"string","description":"Topic: runtime, macros, net, unix, fs, sync, time, process, signal, dns, http, tls, compat, multishot, fallback, platforms, or all"}},"additionalProperties":false}},"#,
        r#"{"name":"nexio_check_workspace","description":"Inspect a Cargo.toml manifest for nexio dependencies, features, and upgrade notes.","inputSchema":{"type":"object","properties":{"manifest_path":{"type":"string","description":"Path to Cargo.toml (defaults to Cargo.toml)"}},"additionalProperties":false}}"#,
        r#"]}"#
    )
}

fn tool_text_result(text: &str) -> String {
    format!(
        r#"{{"content":[{{"type":"text","text":"{}"}}]}}"#,
        json_escape(text)
    )
}

fn runtime_info_text() -> String {
    let uring_ok = nexio::io_uring_available();
    let backend = nexio::Reactor::backend_name();
    let stats = nexio::block_on(async { nexio::reactor::current_stats() });
    format!(
        "nexio version: {}\nos/arch: {}/{}\nio_uring_available(): {}\nactive backend: {}\ncurrent_stats: {:?}",
        env!("CARGO_PKG_VERSION"),
        env::consts::OS,
        env::consts::ARCH,
        uring_ok,
        backend,
        stats
    )
}

fn platform_matrix_text(platform: &str) -> String {
    match platform.trim().to_ascii_lowercase().as_str() {
        "linux" => concat!(
            "Linux:\n",
            "  - Default backend: io_uring (kernel 5.10+, DEFER_TASKRUN/COOP_TASKRUN optional via feature `coop-taskrun`).\n",
            "  - Portable override: `--features poll` forces epoll via `polling`.\n",
            "  - Runtime fallback: `--features runtime-fallback` probes `io_uring_available()` and falls back to epoll on EPERM/ENOSYS/EACCES.\n",
            "  - Exclusive on io_uring: `ProvidedBufRing`, `TcpListener::accept_multishot`, `TcpStream::recv_multishot`, native async file I/O.\n",
            "  - All other APIs (TCP/UDP, Unix sockets, timers, sync, fs helpers, process, signal, DNS, TLS, HTTP/1.1+HTTP/2, tokio-compat, run_sharded): supported."
        )
        .to_string(),
        "windows" => concat!(
            "Windows:\n",
            "  - Backend: IOCP via `polling` (`poll` backend).\n",
            "  - Supported: TCP (`split`/`into_split`/vectored), UDP, timers, sync, fs (blocking pool), process, `ctrl_c`, DNS, TLS, HTTP/1.1+HTTP/2, `tokio-compat`, `#[nexio::main]`/`#[nexio::test]`.\n",
            "  - Not available on Windows: Unix domain sockets (`cfg(unix)`), Unix `signal()`, Linux `io_uring` multishot/`ProvidedBufRing`, `SO_REUSEPORT` sharding."
        )
        .to_string(),
        "macos" | "darwin" => concat!(
            "macOS:\n",
            "  - Backend: kqueue via `polling` (`poll` backend).\n",
            "  - Supported: TCP (`split`/`into_split`/vectored), UDP, Unix domain sockets (`UnixStream::into_split`, `UnixDatagram`), timers, sync, fs (blocking pool), process, `ctrl_c` & Unix `signal()`, DNS, TLS, HTTP/1.1+HTTP/2, `tokio-compat`, `run_sharded` (`SO_REUSEPORT`), `#[nexio::main]`/`#[nexio::test]`.\n",
            "  - Not available on macOS: Linux `io_uring` multishot/`ProvidedBufRing`."
        )
        .to_string(),
        "unix" => concat!(
            "Other Unix (FreeBSD, OpenBSD, NetBSD, etc.):\n",
            "  - Backend: kqueue / epoll / event ports via `polling`.\n",
            "  - Same portable + Unix feature set as macOS (Unix sockets, Unix signals, `SO_REUSEPORT` sharding)."
        )
        .to_string(),
        _ => [
            platform_matrix_text("linux"),
            platform_matrix_text("windows"),
            platform_matrix_text("macos"),
            platform_matrix_text("unix"),
        ]
        .join("\n\n"),
    }
}

fn api_reference_text(topic: &str) -> String {
    match topic.trim().to_ascii_lowercase().as_str() {
        "runtime" => concat!(
            "nexio runtime core:\n",
            "  - `block_on`, `spawn`, `JoinSet`, `join2`, `select2`, `run_sharded`, `spawn_blocking`\n",
            "  - `Reactor::new()`, `Reactor::backend_name()`, `Reactor::stats()`, `nexio::reactor::current_stats()` (`ReactorStats`)"
        )
        .to_string(),
        "macros" => concat!(
            "#[nexio::main] and #[nexio::test] (feature `macros`, enabled by default):\n",
            "  #[nexio::main]\n",
            "  async fn main() -> std::io::Result<()> { Ok(()) }\n",
            "  #[nexio::main(shards = \"auto\")] // or shards = 4\n"
        )
        .to_string(),
        "net" => concat!(
            "nexio::net::{TcpListener, TcpStream, UdpSocket}:\n",
            "  - Owned buffers: let (res, buf) = stream.read(buf).await;\n",
            "  - Split halves: let (rd, wr) = stream.into_split(); rd.reunite(wr)?;\n",
            "  - Vectored I/O: read_vectored / write_vectored / write_all_vectored\n"
        )
        .to_string(),
        "unix" => concat!(
            "nexio::net::{UnixListener, UnixStream, UnixDatagram} (Unix only, `cfg(unix)`):\n",
            "  - Supports `split()`, `into_split()`, `reunite()`, and `UnixDatagram` send/recv.\n"
        )
        .to_string(),
        "fs" => concat!(
            "nexio::fs::{File, OpenOptions, read, write, copy, rename, metadata, symlink_metadata, ",
            "create_dir, create_dir_all, remove_file, remove_dir, remove_dir_all, set_permissions, read_dir}:\n",
            "  - Positional & vectored File I/O: read_at, write_at, read_vectored_at, write_vectored_at, write_all_vectored_at\n",
            "  - Metadata & sizing: File::{metadata, set_len, set_permissions}\n"
        )
        .to_string(),
        "sync" => {
            "nexio::sync::{Notify, Semaphore, Mutex, RwLock, watch, broadcast, CancellationToken, oneshot, mpsc}\n"
                .to_string()
        }
        "time" => concat!(
            "nexio::time::{sleep, sleep_until, timeout, timeout_at, interval, interval_at, Sleep, Interval, MissedTickBehavior}:\n",
            "  - Sleep::{deadline, is_elapsed, reset}\n",
            "  - MissedTickBehavior::{Burst, Delay, Skip}\n"
        )
        .to_string(),
        "process" => concat!(
            "nexio::process::Command:\n",
            "  - Async child spawning with piped stdin/stdout/stderr and `wait()` / `output()`.\n"
        )
        .to_string(),
        "signal" => concat!(
            "nexio::signal:\n",
            "  - Cross-platform `ctrl_c().await`\n",
            "  - Unix `nexio::signal::unix::signal(SignalKind)` (`cfg(unix)`)\n"
        )
        .to_string(),
        "dns" => concat!(
            "Async DNS:\n",
            "  - `nexio::net::lookup_host(host_port).await` and `ToAddrs` on `TcpStream::connect` / `TcpListener::bind`.\n"
        )
        .to_string(),
        "http" => concat!(
            "nexio::http (feature `http`): HTTP/1.1 & HTTP/2 over hyper:\n",
            "  - High-level: Server::bind(addr).await?.serve(|_stream| service_fn(...)).await?;\n",
            "  - Low-level: serve_connection, serve_http2_connection, LocalExec, connect\n"
        )
        .to_string(),
        "tls" => {
            "nexio::tls (feature `tls`): rustls TlsAcceptor / TlsConnector wrapping TcpStream.\n"
                .to_string()
        }
        "compat" => concat!(
            "nexio::compat::Compat (feature `tokio-compat` or `http`):\n",
            "  - Adapts `TcpStream`, `UnixStream`, `TlsStream`, and split halves to tokio `AsyncRead`/`AsyncWrite` without a tokio runtime.\n"
        )
        .to_string(),
        "multishot" => concat!(
            "Linux io_uring multishot & provided buffer rings:\n",
            "  - Reactor::register_buf_ring / ProvidedBufRing (IORING_REGISTER_PBUF_RING)\n",
            "  - TcpListener::accept_multishot, TcpStream::recv_multishot\n"
        )
        .to_string(),
        "fallback" => concat!(
            "Runtime fallback (Linux):\n",
            "  - nexio::io_uring_available() probes io_uring_setup at runtime.\n",
            "  - Feature `runtime-fallback` compiles both io_uring and epoll, falling back automatically on EPERM/ENOSYS/EACCES.\n"
        )
        .to_string(),
        "platforms" => platform_matrix_text("all"),
        _ => [
            api_reference_text("runtime"),
            api_reference_text("macros"),
            api_reference_text("net"),
            api_reference_text("unix"),
            api_reference_text("fs"),
            api_reference_text("sync"),
            api_reference_text("time"),
            api_reference_text("process"),
            api_reference_text("signal"),
            api_reference_text("dns"),
            api_reference_text("http"),
            api_reference_text("tls"),
            api_reference_text("compat"),
            api_reference_text("multishot"),
            api_reference_text("fallback"),
        ]
        .join("\n"),
    }
}

fn check_workspace_text(manifest_path: &str) -> String {
    let path = Path::new(manifest_path);
    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => return format!("failed to read {}: {e}", path.display()),
    };
    let mut notes = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("nexio") {
            notes.push(format!("dependency line: {trimmed}"));
        }
    }
    if content.contains("no-coop-taskrun") {
        notes.push(
            "warning: replace deprecated `no-coop-taskrun` with `coop-taskrun` (default is off)."
                .into(),
        );
    }
    if notes.is_empty() {
        format!("No `nexio` dependency line found in {}", path.display())
    } else {
        notes.join("\n")
    }
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

fn extract_id(json: &str) -> Option<String> {
    let idx = find_key(json, "id")?;
    let rest = json[idx..].trim_start();
    if let Some(after_quote) = rest.strip_prefix('"') {
        let end = after_quote.find('"')?;
        Some(format!("\"{}\"", &after_quote[..end]))
    } else {
        let end = rest
            .find(|c: char| !c.is_ascii_digit() && c != '-')
            .unwrap_or(rest.len());
        if end == 0 {
            None
        } else {
            Some(rest[..end].to_string())
        }
    }
}

fn extract_string_field(json: &str, key: &str) -> Option<String> {
    let idx = find_key(json, key)?;
    let rest = json[idx..].trim_start();
    let after_quote = rest.strip_prefix('"')?;
    let mut out = String::new();
    let mut chars = after_quote.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some(out),
            '\\' => {
                if let Some(esc) = chars.next() {
                    match esc {
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        other => out.push(other),
                    }
                }
            }
            other => out.push(other),
        }
    }
    None
}

fn extract_nested_string(json: &str, parent: &str, key: &str) -> Option<String> {
    let idx = find_key(json, parent)?;
    extract_string_field(&json[idx..], key)
}

fn find_key(json: &str, key: &str) -> Option<usize> {
    let pattern = format!("\"{key}\"");
    let mut pos = 0;
    while let Some(found) = json[pos..].find(&pattern) {
        let after = pos + found + pattern.len();
        let trimmed = json[after..].trim_start();
        if let Some(after_colon) = trimmed.strip_prefix(':') {
            let offset = json.len() - after_colon.len();
            return Some(offset);
        }
        pos = after;
    }
    None
}
