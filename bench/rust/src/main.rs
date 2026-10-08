//! FastIPC's Rust benchmark, through the binding (`fipc`): one-way throughput between this process, a server
//! that receives and times, and a client process (this program again) that sends.
//!
//! ```text
//!   copy      Connection::send / receive_into a buffer the server reuses
//!   zerocopy  send_acquire + commit / receive_acquire, released when dropped (a message of several pieces through
//!             the copying calls); the server copies each message out, as a consumer would
//!   rpc       rpc_submit / rpc_receive_into a buffer the server reuses
//! ```
//!
//! Each case first sends messages untimed for 0.2 s (the warm-up; at least its count), then a start marker, then its
//! count, and the server times from the last start marker to the end marker (see [`client`]). Each case prints "Test i/n: name" and "Throughput: n messages/sec" (devtool bench-compare reads them).

use std::env;
use std::process::{self, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fipc::{Connection, Error, Listener, Result};

const CONNECT: Duration = Duration::from_secs(15);
const WARM_UP: Duration = Duration::from_millis(200);
const MARKER_EVERY: usize = 1000;
const DATA_OPCODE: u32 = 1;
const END_OPCODE: u32 = 0xFB00_0001;
const GO_OPCODE: u32 = 0xFB00_0002;
const GO: &[u8] = b"GO!";
const END: &[u8] = b"END";

struct Case {
    count: usize,
    ring: usize,
    size: usize,
    name: &'static str,
}

const CASES: [Case; 6] = [
    Case { count: 2_000_000, ring: 512 * 1024, size: 16, name: "Tiny messages (16B)" },
    Case { count: 2_000_000, ring: 512 * 1024, size: 64, name: "Small messages (64B)" },
    Case { count: 1_000_000, ring: 512 * 1024, size: 256, name: "Medium messages (256B)" },
    Case { count: 1000, ring: 512 * 1024, size: 64 * 1024, name: "Large messages (64KB)" },
    Case { count: 1000, ring: 2 * 1024 * 1024, size: 512 * 1024, name: "Large messages (512KB)" },
    Case { count: 10, ring: 512 * 1024, size: 1024 * 1024, name: "Exceeds buffer (1MB msg, 512KB buffer)" },
];

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("copy");
    if mode == "client" {
        let (size, count) = (args[3].parse().unwrap(), args[4].parse().unwrap());
        if let Err(e) = client(&args[1], &args[2], size, count) {
            eprintln!("client: {e}");
            process::exit(1);
        }
        return;
    }
    if !["copy", "zerocopy", "rpc"].contains(&mode) {
        eprintln!("usage: fipc-bench copy|zerocopy|rpc");
        process::exit(2);
    }
    println!("FastIPC Rust benchmark: {mode}\n");
    let mut passed = 0;
    for (i, case) in CASES.iter().enumerate() {
        println!("Test {}/{}: {}", i + 1, CASES.len(), case.name);
        if run_case(mode, case) {
            passed += 1;
        } else {
            println!("FAILED");
        }
        println!();
    }
    println!("Summary: {passed}/{} tests passed", CASES.len());
    process::exit(if passed == CASES.len() { 0 } else { 1 });
}

fn run_case(mode: &str, case: &Case) -> bool {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let name = format!("rsbench_{}_{millis}", process::id());
    let mut listener = match Listener::listen(&name, case.ring) {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("listen: {e}");
            return false;
        }
    };
    let mut client = Command::new(env::current_exe().unwrap())
        .args(["client", mode, &name, &case.size.to_string(), &case.count.to_string()])
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let served = listener.accept(CONNECT).and_then(|mut conn| serve(mode, &mut conn, case.size));
    drop(listener);
    let (messages, bytes, seconds) = match served {
        Ok(result) => result,
        Err(e) => {
            let _ = client.kill();
            let _ = client.wait();
            eprintln!("server: {e}");
            return false;
        }
    };
    let status = client.wait().unwrap();
    if !status.success() {
        eprintln!("the client failed: {status}");
        return false;
    }
    if messages != case.count || bytes != case.count * case.size {
        eprintln!("expected {} messages of {} B, got {messages} ({bytes} B)", case.count, case.size);
        return false;
    }
    println!("Messages: {} | Ring: {}KB | Size: {}B", case.count, case.ring / 1024, case.size);
    println!("Duration: {seconds:.3}s");
    let mb = bytes as f64 / seconds / f64::from(1 << 20);
    println!("Throughput: {:.0} messages/sec, {mb:.1} MB/sec", messages as f64 / seconds);
    true
}

/// One message received: its length, or a marker.
enum Got {
    Message(usize),
    Go,
    End,
}

/// The server: skips the warm-up up to the last start marker, then receives until the end marker; the messages, their
/// bytes and the seconds.
fn serve(mode: &str, conn: &mut Connection, size: usize) -> Result<(usize, usize, f64)> {
    let mut buf = vec![0u8; size.max(END.len())];
    let mut sink = vec![0u8; size];
    let (mut messages, mut bytes, mut timing) = (0, 0, false);
    let mut start = Instant::now();
    loop {
        let got = match mode {
            "rpc" => {
                let header = conn.rpc_receive_into(&mut buf, None)?;
                match header.opcode {
                    END_OPCODE => Got::End,
                    GO_OPCODE => Got::Go,
                    _ => Got::Message(header.len),
                }
            }
            "zerocopy" => {
                // The slot is used up in the closure, so the result holds no borrow of the connection
                let acquired = conn.receive_acquire(None).map(|message| {
                    marker(&message).unwrap_or_else(|| {
                        sink[..message.len()].copy_from_slice(&message);
                        Got::Message(message.len())
                    })
                });
                match acquired {
                    Ok(got) => got,
                    Err(Error::TooLarge { .. }) => Got::Message(conn.receive_into(&mut buf, None)?), // several pieces
                    Err(e) => return Err(e),
                }
            }
            _ => {
                let len = conn.receive_into(&mut buf, None)?;
                marker(&buf[..len]).unwrap_or(Got::Message(len))
            }
        };
        match got {
            Got::End => break,
            Got::Go => {
                (messages, bytes, timing) = (0, 0, true);
                start = Instant::now();
            }
            Got::Message(len) if timing => {
                messages += 1;
                bytes += len;
            }
            Got::Message(_) => {}
        }
    }
    Ok((messages, bytes, start.elapsed().as_secs_f64()))
}

/// The marker a message is, if it is one.
fn marker(message: &[u8]) -> Option<Got> {
    match message {
        END => Some(Got::End),
        GO => Some(Got::Go),
        _ => None,
    }
}

/// The client process: connects, sends messages untimed for the warm-up (the count, and for at least `WARM_UP`: the
/// first moments of a process run slower, on macOS much slower for a newly written executable), the start marker,
/// `count` messages and the end marker, and drops the connection (the server still receives everything sent before).
/// The warm-up sends a start marker every `MARKER_EVERY` messages too, as the other languages' benchmarks do.
fn client(mode: &str, name: &str, size: usize, count: usize) -> Result<()> {
    let mut conn = Connection::connect(name, CONNECT)?;
    let payload = vec![b'x'; size];
    let one_piece = size <= conn.max_piece();
    let warm = Instant::now() + WARM_UP;
    let mut i = 0;
    while i < count || Instant::now() < warm {
        if i % MARKER_EVERY == 0 {
            send_marker(mode, &mut conn, GO, GO_OPCODE)?;
        }
        send_one(mode, &mut conn, &payload, one_piece)?;
        i += 1;
    }
    send_marker(mode, &mut conn, GO, GO_OPCODE)?;
    for _ in 0..count {
        send_one(mode, &mut conn, &payload, one_piece)?;
    }
    send_marker(mode, &mut conn, END, END_OPCODE)
}

fn send_marker(mode: &str, conn: &mut Connection, marker: &[u8], opcode: u32) -> Result<()> {
    if mode == "rpc" { conn.rpc_submit(opcode, &[], None).map(|_| ()) } else { conn.send(marker, None) }
}

fn send_one(mode: &str, conn: &mut Connection, payload: &[u8], one_piece: bool) -> Result<()> {
    match mode {
        "rpc" => conn.rpc_submit(DATA_OPCODE, payload, None).map(|_| ()),
        "zerocopy" if one_piece => {
            let mut slot = conn.send_acquire(payload.len(), None)?;
            slot.copy_from_slice(payload);
            slot.commit(payload.len())
        }
        _ => conn.send(payload, None),
    }
}
