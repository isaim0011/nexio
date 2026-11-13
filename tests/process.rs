//! Child-process tests. They run on every backend and platform, using the shell and
//! standard tools that exist on each (`sh` on Unix, `cmd` on Windows).

use nexio::{
    block_on,
    process::{Command, Stdio},
    spawn, time,
};
use std::{io, time::Duration};

/// A command that runs `script` through the platform shell.
fn shell(script: &str) -> Command {
    #[cfg(unix)]
    {
        let mut c = Command::new("sh");
        c.arg("-c").arg(script);
        c
    }
    #[cfg(windows)]
    {
        let mut c = Command::new("cmd");
        c.arg("/C").arg(script);
        c
    }
}

/// A shell script that sleeps roughly `secs` seconds.
fn sleep_script(secs: u32) -> String {
    #[cfg(unix)]
    {
        format!("sleep {secs}")
    }
    #[cfg(windows)]
    {
        // `ping` fails if ICMP is blocked by Windows Firewall; PowerShell Start-Sleep is reliable.
        format!("powershell -NoProfile -Command \"Start-Sleep -Seconds {secs}\"")
    }
}

#[test]
fn output_captures_stdout_and_exit_status() {
    block_on(async {
        let out = shell("echo hello").output().await.unwrap();
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "hello");
        assert!(out.stderr.is_empty());
    });
}

#[test]
fn a_nonzero_exit_code_is_reported() {
    block_on(async {
        let out = shell("exit 3").output().await.unwrap();
        assert_eq!(out.status.code(), Some(3));
    });
}

#[test]
fn stderr_is_captured_separately_from_stdout() {
    block_on(async {
        let out = shell("echo to-out && echo to-err 1>&2")
            .output()
            .await
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "to-out");
        assert_eq!(String::from_utf8_lossy(&out.stderr).trim(), "to-err");
    });
}

#[test]
fn spawning_a_missing_program_fails_cleanly() {
    block_on(async {
        let err = Command::new("nexio-no-such-program-xyz")
            .spawn()
            .err()
            .expect("must fail");
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err:?}");
    });
}

#[test]
fn environment_and_working_directory_are_applied() {
    let dir = std::env::temp_dir().join(format!("nexio_proc_dir_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    block_on(async {
        #[cfg(unix)]
        let script = "echo $NEXIO_VAR && pwd";
        #[cfg(windows)]
        let script = "echo %NEXIO_VAR% && cd";
        let out = shell(script)
            .env("NEXIO_VAR", "from-parent")
            .current_dir(&dir)
            .output()
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout).to_string();
        let mut lines = text.lines().map(str::trim);
        assert_eq!(lines.next(), Some("from-parent"));
        let cwd = lines.next().expect("a working directory line");
        let name = dir.file_name().unwrap().to_string_lossy().to_string();
        assert!(cwd.ends_with(&name), "cwd {cwd:?} should end with {name:?}");
    });
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn a_megabyte_goes_through_stdin_to_stdout_without_deadlock() {
    // Writing and reading at the same time: a pipe's own buffer is far smaller than this, so a
    // naive "write everything, then read" would hang.
    block_on(async {
        let mut child = Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let data: Vec<u8> = (0..(1 << 20)).map(|i| (i % 251) as u8).collect();
        let expect = data.clone();
        let writer = spawn(async move {
            for chunk in data.chunks(8192) {
                let (res, _) = stdin.write_all(chunk.to_vec()).await;
                res.unwrap();
            }
            stdin.shutdown().await.unwrap();
        });
        let mut got = Vec::new();
        stdout.read_to_end(&mut got).await.unwrap();
        writer.await.unwrap();
        assert_eq!(got.len(), expect.len());
        assert!(got == expect, "bytes differ");
        assert!(child.wait().await.unwrap().success());
    });
}

#[cfg(windows)]
#[test]
fn stdin_reaches_the_child_and_its_reply_comes_back() {
    block_on(async {
        let mut child = Command::new("findstr")
            .arg("^")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (res, _) = stdin.write_all(b"hello\r\n".to_vec()).await;
        res.unwrap();
        stdin.shutdown().await.unwrap();
        let mut got = Vec::new();
        stdout.read_to_end(&mut got).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&got).trim(), "hello");
        assert!(child.wait().await.unwrap().success());
    });
}

#[cfg(unix)]
#[test]
fn three_mebibytes_of_output_respect_back_pressure() {
    // The queue per pipe is capped at 1 MiB, so this child is slowed down rather than buffered.
    block_on(async {
        let mut child = shell("head -c 3145728 /dev/zero")
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut got = Vec::new();
        stdout.read_to_end(&mut got).await.unwrap();
        assert_eq!(got.len(), 3 * 1024 * 1024);
        assert!(child.wait().await.unwrap().success());
    });
}

#[test]
fn a_cancelled_read_does_not_lose_the_childs_output() {
    block_on(async {
        // The child stays silent for about a second, then prints.
        #[cfg(unix)]
        let script = "sleep 1; echo first";
        #[cfg(windows)]
        let script = "ping -n 2 127.0.0.1 >NUL & echo first";
        let mut child = shell(script).stdout(Stdio::piped()).spawn().unwrap();
        let stdout = child.stdout.take().unwrap();

        let r = time::timeout(Duration::from_millis(100), stdout.read(vec![0u8; 64])).await;
        assert!(r.is_err(), "the child has not printed yet");

        // The data arrives after the abandoned read; the next read must still get it.
        let mut got = Vec::new();
        stdout.read_to_end(&mut got).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&got).trim(), "first");
        child.wait().await.unwrap();
    });
}

#[test]
fn waiting_for_a_child_does_not_stall_the_reactor() {
    block_on(async {
        let script = sleep_script(1);
        let mut child = shell(&script).spawn().unwrap();
        let waiter = spawn(async move { child.wait().await });
        let t = std::time::Instant::now();
        for _ in 0..5 {
            time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            t.elapsed() < Duration::from_millis(400),
            "timers stalled while waiting: {:?}",
            t.elapsed()
        );
        assert!(waiter.await.unwrap().unwrap().success());
    });
}

#[test]
fn kill_stops_a_long_running_child_promptly() {
    block_on(async {
        let script = sleep_script(30);
        let mut child = shell(&script).spawn().unwrap();
        time::sleep(Duration::from_millis(100)).await;
        let t = std::time::Instant::now();
        child.kill().await.unwrap();
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "kill took {:?}",
            t.elapsed()
        );
        assert!(!child.try_wait().unwrap().expect("it has exited").success());
    });
}

#[cfg(unix)]
#[test]
fn kill_on_drop_kills_the_child() {
    block_on(async {
        let child = Command::new("sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id() as i32;
        drop(child);
        // Give the kill and the background reaper a moment.
        for _ in 0..50 {
            time::sleep(Duration::from_millis(20)).await;
            // SAFETY: signal 0 only checks that the process exists.
            if unsafe { libc::kill(pid, 0) } == -1 {
                return;
            }
        }
        panic!("the child was still alive after being dropped with kill_on_drop");
    });
}

#[cfg(target_os = "linux")]
#[test]
fn a_dropped_child_is_reaped_and_does_not_stay_a_zombie() {
    block_on(async {
        let child = Command::new("true").spawn().unwrap();
        let pid = child.id();
        drop(child);
        for _ in 0..100 {
            time::sleep(Duration::from_millis(20)).await;
            // A zombie still has a /proc entry; a reaped process does not.
            if !std::path::Path::new(&format!("/proc/{pid}")).exists() {
                return;
            }
        }
        panic!("process {pid} was never reaped");
    });
}

#[cfg(unix)]
#[test]
fn writing_to_a_child_that_exited_is_an_error_not_a_crash() {
    block_on(async {
        let mut child = Command::new("true").stdin(Stdio::piped()).spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        child.wait().await.unwrap();
        let mut failed = false;
        for _ in 0..200 {
            let (res, _) = stdin.write_all(vec![0u8; 65536]).await;
            if res.is_err() {
                failed = true;
                break;
            }
            if stdin.flush().await.is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed, "writes to a dead child must eventually fail");
    });
}

#[test]
fn many_children_at_once() {
    block_on(async {
        let mut handles = Vec::new();
        for i in 0..20 {
            handles.push(spawn(async move {
                let out = shell(&format!("echo child-{i}")).output().await.unwrap();
                assert_eq!(
                    String::from_utf8_lossy(&out.stdout).trim(),
                    format!("child-{i}")
                );
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });
}
