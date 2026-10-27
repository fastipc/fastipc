//! alt_bench's roles for iceoryx2 through its native Rust API: the same command line, control lines, messages and
//! results as the C++ harness (src/main.cpp), whose comments describe them. iceoryx2 is a Rust library, and its Rust
//! API is faster than its C++ binding, so the comparison runs it here.
//!
//! The endpoints: publish-subscribe services of byte slices (a stream: one subscriber, and a service each way for a
//! round trip; a broadcast: many), subscriber buffers of 512 KiB of messages (8 to 1024 of them; with `--buffer`, its
//! bytes of messages, at least 8), no overflow (a full
//! subscriber holds the publisher back, `RetryUntilDelivered`) or, lossy, with safe overflow; request-response for a
//! service, with one server (iceoryx2 delivers each request to every server). The sender writes into a loaned
//! sample; the receiver copies the payload out. Every receive polls: iceoryx2's receives don't wait.

use iceoryx2::prelude::*;
use iceoryx2::service::port_factory::publish_subscribe::PortFactory as PubSubFactory;
use iceoryx2::service::port_factory::request_response::PortFactory as ReqResFactory;
use std::io::{BufRead, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

type Ipc = ipc::Service;

// macOS doesn't give a process the generic timer's own register (cntvct_el0 reads a stale value that differs between
// cores): mach_absolute_time reads the same 24 MHz counter.
#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn mach_absolute_time() -> u64;
    fn mach_timebase_info(info: *mut [u32; 2]) -> i32;
}

fn ticks() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_rdtsc()
    }
    #[cfg(target_os = "macos")]
    unsafe {
        mach_absolute_time()
    }
    #[cfg(all(target_arch = "aarch64", not(target_os = "macos")))]
    {
        let value: u64;
        unsafe { core::arch::asm!("isb", "mrs {}, cntvct_el0", out(reg) value) };
        value
    }
}

fn ticks_per_second() -> f64 {
    static RATE: OnceLock<f64> = OnceLock::new();
    *RATE.get_or_init(|| {
        #[cfg(target_os = "macos")]
        {
            let mut timebase = [0u32; 2];
            unsafe { mach_timebase_info(&mut timebase) };
            1e9 * timebase[1] as f64 / timebase[0] as f64
        }
        #[cfg(all(target_arch = "aarch64", not(target_os = "macos")))]
        {
            let frequency: u64;
            unsafe { core::arch::asm!("mrs {}, cntfrq_el0", out(reg) frequency) };
            frequency as f64
        }
        #[cfg(not(target_arch = "aarch64"))]
        {
            let t0 = Instant::now();
            let c0 = ticks();
            std::thread::sleep(Duration::from_millis(50));
            let c1 = ticks();
            (c1 - c0) as f64 / t0.elapsed().as_secs_f64()
        }
    })
}

fn fail(what: &str) -> ! {
    eprintln!("alt_bench_iceoryx2: {what}");
    std::process::exit(2)
}

fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

#[derive(Clone)]
struct Options {
    name: String,
    size: usize,
    subscribers: usize,
    clients: usize,
    servers: usize,
    lossy: bool,
    warmup_ms: u64,
    duration_ms: u64,
    poll: bool,
    buffer: usize,
}

static GO: AtomicBool = AtomicBool::new(false);
static STOP: AtomicBool = AtomicBool::new(false);
static QUIT: AtomicBool = AtomicBool::new(false);

fn read_control_lines() {
    for line in std::io::stdin().lock().lines() {
        match line.as_deref() {
            Ok("go") => GO.store(true, Ordering::SeqCst),
            Ok("stop") => STOP.store(true, Ordering::SeqCst),
            Ok("quit") => QUIT.store(true, Ordering::SeqCst),
            Ok(_) => {}
            Err(_) => break,
        }
    }
    STOP.store(true, Ordering::SeqCst);
    QUIT.store(true, Ordering::SeqCst);
}

fn wait_for(flag: &AtomicBool) {
    while !flag.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn depth_for(options: &Options) -> usize {
    if options.buffer > 0 {
        return (options.buffer / options.size).max(8);
    }
    ((512 * 1024) / options.size).clamp(8, 1024)
}

fn write_header(message: &mut [u8], seq: u64, phase: u64) {
    message[0..8].copy_from_slice(&seq.to_ne_bytes());
    message[8..16].copy_from_slice(&phase.to_ne_bytes());
}

fn header_of(data: &[u8]) -> (u64, u64) {
    (u64::from_ne_bytes(data[0..8].try_into().unwrap()), u64::from_ne_bytes(data[8..16].try_into().unwrap()))
}

fn span_json(t0: u64, t1: u64) -> String {
    format!("\"t0\":{t0},\"t1\":{t1},\"hz\":{:.0}", ticks_per_second())
}

struct Phases {
    warmup: u64,
    duration: u64,
    start: u64,
    measured_start: u64,
    end: u64,
    phase: u64,
}

impl Phases {
    fn new(options: &Options) -> Self {
        let hz = ticks_per_second();
        Phases {
            warmup: (options.warmup_ms as f64 * hz / 1000.0) as u64,
            duration: (options.duration_ms as f64 * hz / 1000.0) as u64,
            start: ticks(),
            measured_start: 0,
            end: 0,
            phase: 0,
        }
    }

    fn done(&mut self) -> bool {
        let now = ticks();
        if self.phase == 0 && now - self.start >= self.warmup {
            self.phase = 1;
            self.measured_start = now;
        } else if self.phase == 1 && now - self.measured_start >= self.duration {
            self.end = now;
            return true;
        }
        false
    }
}

struct Counter {
    expected: usize,
    count: u64,
    count_at_stamp: u64,
    first: u64,
    stamp: u64,
    last_seq: u64,
    lost: u64,
}

impl Counter {
    fn new(expected: usize) -> Self {
        Counter { expected, count: 0, count_at_stamp: 0, first: 0, stamp: 0, last_seq: 0, lost: 0 }
    }

    #[inline]
    fn on(&mut self, data: &[u8]) {
        if data.len() != self.expected {
            fail(&format!("a message of the wrong length: {} for {}", data.len(), self.expected));
        }
        let (seq, phase) = header_of(data);
        if phase != 1 {
            return;
        }
        if self.count == 0 {
            self.first = ticks();
            self.stamp = self.first;
            self.count = 1;
            self.count_at_stamp = 1;
            self.last_seq = seq;
            return;
        }
        if seq > self.last_seq + 1 {
            self.lost += seq - self.last_seq - 1;
        }
        if seq > self.last_seq {
            self.last_seq = seq;
        }
        self.count += 1;
        if self.count & 63 == 0 {
            self.stamp = ticks();
            self.count_at_stamp = self.count;
        }
    }
}

/// Polls `attempt` until it delivers or timeout_ms has passed, reading the clock once per 256 attempts.
fn poll_for(timeout_ms: u64, mut attempt: impl FnMut() -> bool) -> bool {
    if attempt() {
        return true;
    }
    if timeout_ms == 0 {
        return false;
    }
    let deadline = ticks() + (ticks_per_second() * timeout_ms as f64 / 1000.0) as u64;
    loop {
        for _ in 0..256 {
            if attempt() {
                return true;
            }
        }
        if ticks() >= deadline {
            return false;
        }
    }
}

fn run_sender(options: &Options, mut send: impl FnMut(&[u8])) {
    let mut message = vec![0xA5u8; options.size];
    wait_for(&GO);
    let mut phases = Phases::new(options);
    let (mut seq, mut sent) = (0u64, 0u64);
    loop {
        for _ in 0..64 {
            write_header(&mut message, seq, phases.phase);
            seq += 1;
            send(&message);
        }
        if phases.phase == 1 {
            sent += 64;
        }
        if phases.done() {
            break;
        }
    }
    say(&format!("result {{\"sent\":{sent},{}}}", span_json(phases.measured_start, phases.end)));
    wait_for(&QUIT);
}

/// Receives (a replier: and replies) until "stop" and 50 ms without a message. `receive` delivers at most one message
/// to the counter within the timeout and returns whether it did.
fn run_receiver(options: &Options, mut receive: impl FnMut(&mut Counter, u64) -> bool) {
    let mut counter = Counter::new(options.size);
    wait_for(&GO);
    let timeout = if options.poll { 0 } else { 100 };
    let idle_limit = (ticks_per_second() * 0.05) as u64;
    let mut last_message = ticks();
    let mut received = 0u64;
    loop {
        if receive(&mut counter, timeout) {
            received += 1;
            if received & 1023 == 0 {
                last_message = ticks();
            }
            continue;
        }
        if STOP.load(Ordering::SeqCst) {
            if ticks() - last_message > idle_limit {
                break;
            }
        } else if options.poll {
            last_message = ticks();
        }
    }
    say(&format!(
        "result {{\"count\":{},\"lost\":{},\"received\":{received},{}}}",
        counter.count_at_stamp,
        counter.lost,
        span_json(counter.first, counter.stamp)
    ));
}

/// Round trips: `call` sends the message and waits for the reply, returning its length. One round trip in 16 is
/// sampled, chosen at random, so that the samples don't fall into step with a period of the library's own.
fn run_caller(options: &Options, mut call: impl FnMut(&[u8], u64) -> usize) {
    let mut message = vec![0xA5u8; options.size];
    let mut samples: Vec<u64> = Vec::with_capacity(1 << 22);
    wait_for(&GO);
    let timeout = if options.poll { 0 } else { 100 };
    let mut phases = Phases::new(options);
    let (mut seq, mut calls) = (0u64, 0u64);
    let mut random = 0x9E37_79B9_7F4A_7C15u64; // xorshift64
    loop {
        for _ in 0..64 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let sampled = random & 15 == 0 && phases.phase == 1;
            let t0 = if sampled { ticks() } else { 0 };
            write_header(&mut message, seq, phases.phase);
            seq += 1;
            let len = call(&message, timeout);
            if len != options.size {
                fail("a reply of the wrong length");
            }
            if sampled && samples.len() < samples.capacity() {
                samples.push(ticks() - t0);
            }
        }
        if phases.phase == 1 {
            calls += 64;
        }
        if phases.done() {
            break;
        }
    }
    samples.sort_unstable();
    let ns = 1e9 / ticks_per_second();
    let percentile = |p: f64| {
        if samples.is_empty() { 0.0 } else { samples[(p * (samples.len() - 1) as f64) as usize] as f64 * ns }
    };
    say(&format!(
        "result {{\"calls\":{calls},\"p50_ns\":{:.1},\"p99_ns\":{:.1},\"p999_ns\":{:.1},{}}}",
        percentile(0.5),
        percentile(0.99),
        percentile(0.999),
        span_json(phases.measured_start, phases.end)
    ));
    wait_for(&QUIT);
}

fn pubsub(node: &Node<Ipc>, name: &str, options: &Options, subscribers: usize) -> PubSubFactory<Ipc, [u8], ()> {
    let service_name = ServiceName::new(name).unwrap_or_else(|_| fail("a bad service name"));
    node.service_builder(&service_name)
        .publish_subscribe::<[u8]>()
        .max_publishers(1)
        .max_subscribers(subscribers)
        .history_size(0)
        .subscriber_max_buffer_size(depth_for(options))
        .enable_safe_overflow(options.lossy)
        .open_or_create()
        .unwrap_or_else(|e| fail(&format!("publish_subscribe: {e:?}")))
}

fn reqres(node: &Node<Ipc>, options: &Options) -> ReqResFactory<Ipc, [u8], (), [u8], ()> {
    let service_name = ServiceName::new(&options.name).unwrap_or_else(|_| fail("a bad service name"));
    node.service_builder(&service_name)
        .request_response::<[u8], [u8]>()
        .max_clients(options.clients)
        .max_servers(1)
        .max_active_requests_per_client(1)
        .enable_safe_overflow_for_requests(false)
        .enable_safe_overflow_for_responses(false)
        .open_or_create()
        .unwrap_or_else(|e| fail(&format!("request_response: {e:?}")))
}

fn publisher(
    service: &PubSubFactory<Ipc, [u8], ()>,
    options: &Options,
    subscribers: usize,
) -> iceoryx2::port::publisher::Publisher<Ipc, [u8], ()> {
    let strategy =
        if options.lossy { BackpressureStrategy::DiscardData } else { BackpressureStrategy::RetryUntilDelivered };
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(options.size)
        .backpressure_strategy(strategy)
        .create()
        .unwrap_or_else(|e| fail(&format!("publisher: {e:?}")));
    while service.dynamic_config().number_of_subscribers() < subscribers {
        std::thread::sleep(Duration::from_millis(1));
    }
    publisher
}

fn publish(publisher: &iceoryx2::port::publisher::Publisher<Ipc, [u8], ()>, data: &[u8]) {
    let sample = publisher.loan_slice_uninit(data.len()).unwrap_or_else(|e| fail(&format!("loan: {e:?}")));
    sample.write_from_slice(data).send().unwrap_or_else(|e| fail(&format!("send: {e:?}")));
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        fail("usage: alt_bench_iceoryx2 iceoryx2 <pattern> <role> [options]");
    }
    let supports_only = args[2] == "supports";
    let pattern = args[if supports_only { 3 } else { 2 }].clone();
    let mut options = Options {
        name: String::new(),
        size: 16,
        subscribers: 1,
        clients: 1,
        servers: 1,
        lossy: false,
        warmup_ms: 300,
        duration_ms: 1000,
        poll: false,
        buffer: 0,
    };
    let mut i = 4;
    while i < args.len() {
        let arg = args[i].as_str();
        let mut value = || {
            i += 1;
            args.get(i).cloned().unwrap_or_else(|| fail("a value is missing"))
        };
        match arg {
            "--size" => options.size = value().parse().unwrap(),
            "--name" => options.name = value(),
            "--subscribers" => options.subscribers = value().parse().unwrap(),
            "--clients" => options.clients = value().parse().unwrap(),
            "--servers" => options.servers = value().parse().unwrap(),
            "--lossy" => options.lossy = true,
            "--warmup-ms" => options.warmup_ms = value().parse().unwrap(),
            "--duration-ms" => options.duration_ms = value().parse().unwrap(),
            "--poll" => options.poll = true,
            "--buffer" => options.buffer = value().parse().unwrap(),
            "--dir" | "--index" | "--producers" | "--consumers" | "--port" => {
                value();
            }
            other => fail(&format!("unknown option: {other}")),
        }
        i += 1;
    }
    let supported = match pattern.as_str() {
        "stream" | "broadcast" => true,
        "service" => options.servers == 1,
        _ => false,
    };
    if supports_only {
        std::process::exit(if supported { 0 } else { 1 });
    }
    if !supported {
        fail("iceoryx2 lacks this case");
    }
    let role = args[3].as_str();
    ticks_per_second();
    set_log_level(LogLevel::Error);
    let node = NodeBuilder::new().create::<Ipc>().unwrap_or_else(|e| fail(&format!("node: {e:?}")));
    let forward = format!("{}-ab", options.name);
    let back = format!("{}-ba", options.name);
    let subscribers = if pattern == "broadcast" { options.subscribers } else { 1 };
    std::thread::spawn(read_control_lines);
    let mut buffer = vec![0u8; options.size];
    let cap = buffer.len();

    match role {
        "tx" | "pub" => {
            let service = pubsub(&node, &forward, &options, subscribers);
            let publisher = publisher(&service, &options, subscribers);
            say("ready");
            run_sender(&options, |data| publish(&publisher, data));
        }
        "rx" | "sub" | "echo" => {
            let service = pubsub(&node, &forward, &options, subscribers);
            let subscriber = service.subscriber_builder().create().unwrap_or_else(|e| fail(&format!("{e:?}")));
            let reply_service = (role == "echo").then(|| pubsub(&node, &back, &options, 1));
            let reply_publisher = reply_service.as_ref().map(|s| publisher(s, &options, 1));
            let reply = vec![0x5Au8; options.size];
            say("ready");
            run_receiver(&options, |counter, timeout| {
                let got = poll_for(timeout, || match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload: &[u8] = sample.payload();
                        let len = payload.len().min(buffer.len());
                        buffer[..len].copy_from_slice(&payload[..len]);
                        counter.on(&buffer[..payload.len()]);
                        true
                    }
                    Ok(None) => false,
                    Err(e) => fail(&format!("receive: {e:?}")),
                });
                if got {
                    if let Some(p) = &reply_publisher {
                        publish(p, &reply);
                    }
                }
                got
            });
        }
        "ping" => {
            let reply_service = pubsub(&node, &back, &options, 1);
            let subscriber = reply_service.subscriber_builder().create().unwrap_or_else(|e| fail(&format!("{e:?}")));
            let service = pubsub(&node, &forward, &options, 1);
            let publisher = publisher(&service, &options, 1);
            say("ready");
            run_caller(&options, |data, timeout| {
                publish(&publisher, data);
                let mut len = 0;
                while !poll_for(timeout, || match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload: &[u8] = sample.payload();
                        len = payload.len();
                        let n = len.min(cap);
                        buffer[..n].copy_from_slice(&payload[..n]);
                        true
                    }
                    Ok(None) => false,
                    Err(e) => fail(&format!("receive: {e:?}")),
                }) {}
                len
            });
        }
        "client" => {
            let service = reqres(&node, &options);
            let client = service
                .client_builder()
                .initial_max_slice_len(options.size)
                .create()
                .unwrap_or_else(|e| fail(&format!("client: {e:?}")));
            while service.dynamic_config().number_of_servers() < 1 {
                std::thread::sleep(Duration::from_millis(1));
            }
            say("ready");
            run_caller(&options, |data, timeout| {
                let request = client.loan_slice_uninit(data.len()).unwrap_or_else(|e| fail(&format!("loan: {e:?}")));
                let pending =
                    request.write_from_slice(data).send().unwrap_or_else(|e| fail(&format!("request: {e:?}")));
                let mut len = 0;
                while !poll_for(timeout, || match pending.receive() {
                    Ok(Some(response)) => {
                        let payload: &[u8] = response.payload();
                        len = payload.len();
                        let n = len.min(cap);
                        buffer[..n].copy_from_slice(&payload[..n]);
                        true
                    }
                    Ok(None) => false,
                    Err(e) => fail(&format!("response: {e:?}")),
                }) {}
                len
            });
        }
        "server" => {
            let service = reqres(&node, &options);
            let server = service
                .server_builder()
                .initial_max_slice_len(options.size)
                .create()
                .unwrap_or_else(|e| fail(&format!("server: {e:?}")));
            let reply = vec![0x5Au8; options.size];
            say("ready");
            run_receiver(&options, |counter, timeout| {
                poll_for(timeout, || match server.receive() {
                    Ok(Some(active)) => {
                        let payload: &[u8] = active.payload();
                        let len = payload.len().min(buffer.len());
                        buffer[..len].copy_from_slice(&payload[..len]);
                        counter.on(&buffer[..payload.len()]);
                        let response =
                            active.loan_slice_uninit(reply.len()).unwrap_or_else(|e| fail(&format!("loan: {e:?}")));
                        response.write_from_slice(&reply).send().unwrap_or_else(|e| fail(&format!("reply: {e:?}")));
                        true
                    }
                    Ok(None) => false,
                    Err(e) => fail(&format!("server receive: {e:?}")),
                })
            });
        }
        other => fail(&format!("unknown role: {other}")),
    }
    let _ = std::io::stdout().flush();
    // The node's cleanup can outlast the run: end the process without it.
    std::process::exit(0);
}
