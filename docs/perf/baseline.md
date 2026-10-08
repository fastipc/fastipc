# Performance

What the benchmark suite (`bench/zig`, `fipc_bench`) measures, how it runs, the library's numbers on the baseline
machine, and how much two runs of the same library differ. The C, C++, C#, Java, Python, Rust, Lua, JavaScript and
Go benchmarks' reference medians are in [`bindings-baseline.md`](bindings-baseline.md).

## The numbers

The library on the baseline machine (below), ReleaseFast for x86-64-v3, with the stream pace (`stream_paces`: Windows 64,
Linux 32; "The stream pace on x86" below): the medians of `devtool bench-compare --ab --runs 5` (5 alternating rounds of
0.3 s per gate case against the library without it), 2026-10-07, `main` eeb6676's library, Windows and then WSL. Messages per second, the round
trip's p50, or connection cycles per second.

| Scenario | Case | Windows | WSL | macOS\* | Linux VM on the M1† |
|---|---|---|---|---|---|
| fastipc | 16 B | 48.25M | 47.89M | 54.06M | 49.64M |
| fastipc | 64 B | 40.83M | 46.12M | 49.89M | 53.92M |
| fastipc | 1 KiB | 16.78M | 21.45M | 26.99M | 26.09M |
| fastipc | 64 KiB | 432.8K | 487.6K | 652.9K | 643.5K |
| fastipc | 1.5 MiB | 8.8K | 9.8K | 14.0K | 14.1K |
| fastipc-zerocopy | 16 B | 47.91M | 57.47M | 56.22M | 54.69M |
| fastipc-zerocopy | 64 B | 47.94M | 51.95M | 49.05M | 41.26M |
| fastipc-zerocopy | 1 KiB | 21.21M | 23.07M | 25.32M | 21.81M |
| fastipc-zerocopy | 64 KiB | 442.4K | 484.7K | 433.5K | 542.5K |
| fastipc-zerocopy | 1.5 MiB | 8.7K | 9.6K | 14.3K | 14.2K |
| rpc | 16 B | 44.03M | 43.90M | 51.43M | 42.64M |
| rpc | 64 B | 33.46M | 32.90M | 48.15M | 39.60M |
| rpc | 1 KiB | 15.69M | 17.75M | 25.86M | 25.33M |
| rpc | 64 KiB | 431.6K | 476.2K | 649.4K | 648.6K |
| rpc | 1.5 MiB | 8.8K | 9.8K | 13.2K | 14.1K |
| latency-spin | 32 B p50 | 282 ns | 254 ns | 167 ns | 212 ns |
| latency-spin | 16 B p50\*\* | 264 ns | 237 ns | 167 ns | 208 ns |
| setup | 1 MiB ring | 7.4K | 2.6K | 3.5K | 2.3K |

\*\* At 16 B, which ties 8 and 48 B as the quickest size: from "Latency by message size" below (2026-10-07, a session of its
own); the macOS and Linux VM columns' from "On the M1" below (2026-10-06), where 16 and 32 B tie on both.

\* macOS: MacBook Air (M1, 8 GB RAM, 8 cores), macOS 26.5.1, on power; different hardware from the baseline machine,
so compare across columns with care. ReleaseFast for `apple_m1`, unpinned (macOS has no thread pinning), with the
stream pace (256 hints on arm64, below) and `main` 6102beb's receive wait with the store of `head_sent` before the
publish (f5a96c1; "The stream pace on the M1" below): the medians of `devtool bench-compare --ab --runs 5 --duration 1
--ref cb60494` (5 alternating rounds of 1 s per gate case, where the Windows and WSL columns ran 0.3 s, against `main`
before any pacing), 2026-10-07. The rounds' medians spread most at zero-copy 64 B (46.57M-61.55M, bimodal: below),
copy 64 B (45.24M-51.16M) and RPC 1.5 MiB (12.8K-15.2K); 1 KiB within about 1% (copy 26.81M-27.15M). Against the
unpaced library in the same rounds: 1 KiB 3.3 times as fast (copy 26.99M against 8.12M, zero-copy 25.32M against
7.72M, RPC 25.86M against 7.82M); 16 B, RPC 64 B and 64 KiB within 5%, 1.5 MiB within its noise; copy 64 B 10% faster
and zero-copy 64 B 10% slower (49.05M against 54.51M: code placement, below); the round trip's p50 167 ns and p99
250 ns unchanged. The earlier column (2026-10-04, `devtool bench zig-gates`, before the pace) had 1 KiB at 8.32M
(copy), 8.03M (zero-copy) and 7.98M (RPC), and RPC 16 and 64 B at 54.2M and 57.6M, which no run since has repeated
(the unpaced library here: 49.42M and 49.37M). Three things of the platform show in them:

- **The timer ticks at 24 MHz** (about 42 ns: the ARM generic timer, `cntvct_el0`, which the suite reads for latency
  samples), so latencies are quantized to that step: the round trip's 167 ns is 4 ticks, and its p99 of 250 ns 6.
- **The M1's cache line is 128 bytes** for the lines that matter here (`hw.cachelinesize`): a ring's `head` and
  `tail`, 64 bytes apart in the segment (docs/protocol.md §5.1), share one line.
- **From 256 B to 1.5 KiB, without the stream pace, a receiver that reads each message runs at about half the rate of
  one that doesn't.** A sweep of the copy scenario (2026-10-05, before the pace): 256 B 31.7M/s, 512 B 16.9M/s, 1 KiB 9.0M/s, 1.5 KiB 6.9M/s, then 2 KiB
  13.9M/s (27 GiB/s) and 16 KiB 2.4M/s (36 GiB/s). A zero-copy receiver that doesn't read the payload
  (`--no-touch`) runs 512 B at 35.4M/s and 1 KiB at 18.6M/s (8.0M/s when it copies each message out), and 2 KiB
  at 13.4M/s, about the copy scenario's 13.9M/s. The ring's size moved it (1 KiB with a 64 KiB ring: 10.7M/s). The
  cause is a receiver that has caught up looking at the head after every spin hint: each look takes the line of
  `head` and `tail` (one 128-byte line) from the sender, whose every publish must win it back, so the receiver stays
  caught up. So a receive that waits for a stream makes no look for 256 hints on arm64 (macOS and Linux; x86 below,
  `stream_paces` in `data/ring_wait.zig`), then one after every hint: copy 1 KiB about 27M/s, 256 B about 55M/s,
  1.5 KiB about 19M/s, 2 KiB about 10% more, the other cases within their noise (1.5 MiB: "On the M1" below). The
  columns above are with it. `devtool bench-pace` measures a platform's paces.

† Linux in a VM on the macOS column's MacBook Air: Lima 2.2.1 (Apple Virtualization framework), Ubuntu 24.04.5 arm64,
kernel 6.8.0, glibc 2.39, 4 vCPUs and 4 GiB; Zig 0.17.0, ReleaseFast for generic ARMv8.0-A (the build's CPU floor
for Linux arm64). The macOS column's tree and method, 2026-10-07: the rounds' medians spread most at zero-copy 64 B
(31.77M-43.19M) and copy 16 B (46.22M-51.45M); 1 KiB within 5%. Against the unpaced library in the same rounds: copy
and RPC 1 KiB 3.9 and 4.2 times as fast (26.09M against 6.66M, 25.33M against 6.08M), zero-copy 1 KiB 1.05 (21.81M
against 20.74M: its receiver's copy keeps it behind its sender either way); 16 and 64 B 1.06-1.10; 64 KiB within
1.5%, 1.5 MiB 0.95-0.97; the round trip (p50 212 ns, p99 255 ns) and setup unchanged; 17 of 17 gate cases passed. The
same OS as the WSL column on other hardware: compare it with WSL for the CPU's part, with macOS for the
OS's. Its clock is the same 24 MHz counter (`arch_sys_counter`), so its latencies have the same 42 ns steps.

On the baseline machine:

- **Short messages: the receiver's pace, not the API, sets the rate.** A copied and a zero-copy message of 16-256 B
  cost about as much; what sets the rate is whether the receiver keeps up:
  - **A receiver slower than its sender:** the ring fills, the writer waits for a share of it to free up and then
    writes in bursts, and neither side waits for the other's cache lines.
  - **A receiver that keeps up:** it looks at the head after every message, and every publish's swap of the sleep
    flag (the wake-up check, docs/protocol.md §6.1) then waits for the head's and the frame's lines to come back from
    the reader's core: about 50 ns a message.

  The copying scenarios' receiver does nothing with a message and would keep up; the zero-copy one copies each
  message out and doesn't. Without the stream pace (`-Dstream_pace=1`), copy and RPC at 16-64 B run at about half the
  zero-copy rate for that reason (the table below, pace 1). A consumer that does work on its messages runs in the fast
  regime. The swap is what keeps a sleeping receiver from missing a wake-up, so it stays; instead, a receiver that
  waits for a stream looks at the head less often (the stream pace, below). One that sent since its last wait, and so
  waits for an answer, looks after every pause: looking less often would slow the round trip.
- A message of three rings goes in pieces through the copying calls (zero-copy takes one piece), so its three
  scenarios run alike.

### The stream pace on x86

A receiver that hasn't sent since its last wait (a stream's) looks at the ring once, then makes no look for its first
`stream_pace` pauses (`stream_paces`, `data/ring_wait.zig`), then looks after every pause; the writer meanwhile
publishes without losing the index line to the reader. `devtool bench-pace` builds the library once per pace and runs
the copy and RPC streams against each, in alternating rounds. 2026-10-06, unpinned; each cell the median of all runs
(3 rounds of 5 runs of 1 s), and the range of the rounds' medians:

| Pace | Windows copy 64 B | Windows RPC 64 B | WSL copy 64 B | WSL RPC 64 B |
|---|---|---|---|---|
| 1 (off) | 18.59M (17.22M-19.06M) | 14.61M (13.50M-14.81M) | 19.57M (18.25M-19.83M) | 17.30M (16.98M-17.70M) |
| 2 | 21.25M (18.60M-22.38M) | 15.84M (15.42M-16.35M) | 21.12M (19.40M-21.28M) | 19.45M (17.23M-21.41M) |
| 4 | 23.86M (22.61M-24.71M) | 16.12M (14.99M-17.10M) | 21.59M (20.95M-23.16M) | 20.78M (18.35M-21.77M) |
| 8 | 23.80M (23.13M-25.08M) | 18.92M (18.38M-19.25M) | 25.29M (24.33M-25.47M) | 21.57M (21.37M-21.58M) |
| 16 | 28.50M (27.83M-29.06M) | 21.83M (21.78M-21.86M) | 35.93M (35.14M-36.43M) | 23.89M (23.63M-24.56M) |
| 32 | 34.98M (34.12M-35.85M) | 30.13M (29.94M-30.52M) | 46.18M (43.96M-46.31M) | 32.27M (31.14M-32.38M) |
| 64 | 40.12M (37.57M-41.15M) | 34.53M (33.80M-34.87M) | 51.25M (50.95M-51.69M) | 35.82M (31.26M-36.31M) |

Unpaced against paced, from the A/B runs of the table at the top (`main` cb60494, which has no pace, against the
library with it, alternating, 2026-10-07):

| Case | Windows, no pace | Windows, 64 | WSL, no pace | WSL, 32 |
|---|---|---|---|---|
| fastipc 16 B | 36.68M | 48.25M | 30.75M | 47.89M |
| fastipc 64 B | 21.39M | 40.83M | 20.98M | 46.12M |
| fastipc-zerocopy 64 B | 42.33M | 47.94M | 48.85M | 51.95M |
| rpc 16 B | 24.59M | 44.03M | 24.81M | 43.90M |
| rpc 64 B | 14.39M | 33.46M | 18.40M | 32.90M |
| latency-spin 32 B p50 | 268 ns | 282 ns | 252 ns | 254 ns |

Every other gate case within 2%, but for setup on Windows (5% slower) and zero-copy 16 B on WSL (10% faster).

- **The 16-256 B streams gain more the larger the pace, up to 64, the top of the sweep; 1 KiB and up gain nothing at
  any pace here**, unlike on the M1: copy 1 KiB stays at 17M/s on Windows and 21M/s on WSL. What limits the pace is
  the round trip. A run of paces 1, 16, 32 and 64 (4 rounds of 5 runs of 1 s, the four libraries in each round, in a
  new order each round; the round trip in two runs of 20 rounds), each cell the median of all runs:

  | Case | Windows 1 | 16 | 32 | 64 | WSL 1 | 16 | 32 | 64 |
  |---|---|---|---|---|---|---|---|---|
  | copy 16 B | 29.07M | 38.64M | 42.20M | 49.44M | 36.15M | 44.11M | 49.14M | 57.62M |
  | copy 64 B | 20.89M | 29.23M | 36.97M | 42.35M | 22.20M | 37.71M | 47.02M | 52.18M |
  | copy 256 B | 24.42M | 29.02M | 31.39M | 32.80M | 20.21M | 36.46M | 39.36M | 41.28M |
  | RPC 16 B | 21.10M | 28.56M | 39.31M | 45.49M | 23.91M | 31.95M | 44.51M | 49.44M |
  | RPC 64 B | 14.52M | 22.84M | 30.14M | 36.11M | 18.67M | 25.32M | 31.82M | 36.16M |
  | RPC 256 B | 16.62M | 22.53M | 24.67M | 28.37M | 21.70M | 25.30M | 29.47M | 31.97M |
  | round trip p50, run 1 | 283 ns | 282 ns | 279 ns | 285 ns | 244 ns | 257 ns | 258 ns | 266 ns |
  | round trip p50, run 2 | 272 ns | 275 ns | 268 ns | 270 ns | 246 ns | 254 ns | 254 ns | 262 ns |

- **Windows 64, Linux 32.** On Windows no pace up to 64 moved the round trip (its p50 within 1% of pace 1's in both
  runs, its p99 2-5% lower). On WSL a round's median lands near 240 ns or near 270 ns, and the larger the pace, the
  more rounds land high: pace 1 in 4 and 7 of 20, 32 in 10 and 8, 64 in 13 and 10. That makes 32's p50 some 3%
  slower (1-6% by the measure taken: the median of all runs, the mean of the rounds' medians, the A/B above), 64's
  2-9%; 16 cost no less than 32. The p99 stays within 2% at every pace.
- **What else the paces cost: nothing measurable.** 64 KiB and 1.5 MiB (5 rounds of 3 s runs) within 2% of pace 1;
  latency-sleep, wakeup and wakeup-poll within their noise (on WSL, wakeup over 12 rounds: its p999 spans
  143 µs-2 ms at pace 1 itself). wakeup-poll's p50 is 7-15 ns higher on WSL at every pace, as it is with a build
  whose code differs but never paces: code placement.
- **The receiver must tell a stream from an answer without touching the segment.** Loading the send ring's head (the
  line the peer spins on) to see whether it sent made every paced build's unpinned round trip 7-10% slower, in 18 of
  20 rounds, whatever the pace; the endpoint's own copy of that head (`SendLine.head_sent`) costs nothing.
- 64 pauses take about as long as the M1's 256 `isb`s (some 2 µs). Other hardware may want another pace: `zig build
  -Dstream_pace=N` builds one (1 turns the pacing off), and `devtool bench-pace` measures them.

### Latency by message size

latency-spin at 8 to 64 B (`--sizes`), unpinned, 2026-10-07, `main` eeb6676's library:
7 rounds, each running every size once (1 s of samples after a 0.2 s warm-up) in a rotated order, so
drift reaches every size alike; the medians over the rounds.

| Size | Windows p50 | Windows p99 | WSL p50 | WSL p99 |
|---|---|---|---|---|
| 8 B | 263 ns | 351 ns | 240 ns | 334 ns |
| 16 B | 264 ns | 331 ns | 237 ns | 298 ns |
| 32 B | 280 ns | 386 ns | 270 ns | 361 ns |
| 48 B | 266 ns | 341 ns | 240 ns | 314 ns |
| 64 B | 318 ns | 426 ns | 296 ns | 377 ns |

- **The frame's fit in a cache line sets the order.** A frame is the message plus its 16-byte header, aligned to 16
  bytes: 8 and 16 B messages make 32-byte frames, which never straddle a 64-byte line, and 48 B makes 64-byte frames;
  32 B makes 48-byte frames, half of which straddle one, and 64 B 80-byte frames, which always span two. 8, 16 and
  48 B tie; 32 B is 6-14% slower at the p50 and 64 B 21-25%. The rounds' ranges overlap between 8, 16 and 48 B.
- **No pinned numbers.** The docs report the two sides on separate cores, as two processes that each do work run.
  Pinned to two separate P-cores, Windows measures as unpinned (16 B: 270-281 ns, neighbouring cores or not;
  2026-10-07); on WSL pinning changes nothing measurable (the VM's vCPUs aren't fixed to cores).
- The website and the README headline each OS's fastest size: 16 B on WSL, 8 B on Windows (the macOS and Linux VM
  columns' 16 B from "On the M1" below, where 16 and 32 B tie).

### On the M1: macOS and Linux, before and after the stream pace

Every scenario on macOS and in the Linux VM: `main` 0fa402b (before) and 4f50e8f (after: the
stream pace, 256 hints on arm64, before the `head_sent` changes of "The stream pace on the M1" below), 3 rounds
alternating between them, each round `fipc_bench all --runs 5` and latency-spin at 16 B; the median of the 15 runs,
and in brackets the range of the 3 rounds' medians. macOS first (2026-10-06, the VM stopped), then Linux in the VM
(the same day). Messages per second, ns or µs, cycles per second. The macOS and Linux VM columns at the top are later
runs (2026-10-07, f5a96c1).

| Scenario | Case | macOS before | macOS after | Linux VM before | Linux VM after |
|---|---|---|---|---|---|
| fastipc | 16 B | 53.43M (52.17M-54.82M) | 52.33M (52.09M-55.40M) | 46.85M (46.77M-47.93M) | 46.61M (46.24M-47.89M) |
| fastipc | 64 B | 44.02M (42.08M-45.63M) | 44.45M (43.24M-47.80M) | 50.65M (48.49M-52.41M) | 50.56M (50.27M-51.21M) |
| fastipc | 256 B | 30.20M (30.17M-31.46M) | 55.29M (47.63M-56.79M) | 25.20M (24.79M-25.88M) | 49.28M (46.98M-50.01M) |
| fastipc | 1 KiB | 8.15M (8.09M-8.22M) | 27.26M (27.23M-27.27M) | 6.95M (6.56M-7.16M) | 24.90M (24.65M-24.97M) |
| fastipc | 64 KiB | 646.5K (643.6K-650.2K) | 654.4K (644.0K-657.5K) | 598.2K (593.4K-652.4K) | 592.2K (576.9K-594.8K) |
| fastipc | 512 KiB | 86.3K (85.0K-86.7K) | 84.7K (83.6K-84.7K) | 80.2K (79.3K-83.0K) | 80.5K (76.0K-82.0K) |
| fastipc | 1.5 MiB | 18.0K (14.0K-18.4K) | 13.8K (13.2K-15.8K) | 17.9K (17.3K-17.9K) | 16.6K (15.5K-17.1K) |
| fastipc-zerocopy | 16 B | 53.98M (53.80M-54.30M) | 54.19M (54.02M-54.34M) | 53.14M (51.79M-53.48M) | 51.79M (49.14M-52.48M) |
| fastipc-zerocopy | 64 B | 59.17M (58.52M-67.32M) | 62.63M (60.04M-65.14M) | 38.58M (37.50M-40.42M) | 37.64M (36.10M-40.30M) |
| fastipc-zerocopy | 256 B | 27.21M (26.99M-28.01M) | 50.72M (48.84M-51.12M) | 26.22M (24.26M-29.29M) | 26.54M (25.91M-28.85M) |
| fastipc-zerocopy | 1 KiB | 7.85M (7.76M-7.87M) | 25.47M (25.44M-25.49M) | 19.63M (17.86M-20.27M) | 20.58M (20.14M-21.41M) |
| fastipc-zerocopy | 64 KiB | 437.6K (432.0K-441.0K) | 432.2K (431.6K-440.6K) | 534.8K (506.3K-545.7K) | 505.6K (497.6K-510.3K) |
| fastipc-zerocopy | 512 KiB | 54.8K (54.7K-55.0K) | 55.0K (51.9K-55.5K) | 65.2K (61.8K-69.7K) | 62.7K (58.4K-62.7K) |
| fastipc-zerocopy | 1.5 MiB | 14.8K (13.2K-18.4K) | 13.1K (12.8K-16.8K) | 15.2K (14.9K-17.7K) | 16.4K (14.4K-17.0K) |
| rpc | 16 B | 48.39M (48.20M-49.36M) | 49.71M (48.18M-50.98M) | 43.94M (16.93M-45.32M) | 43.39M (38.62M-44.01M) |
| rpc | 64 B | 43.96M (40.23M-45.49M) | 47.37M (45.27M-49.10M) | 38.05M (36.78M-43.23M) | 34.96M (31.69M-37.06M) |
| rpc | 256 B | 35.28M (33.29M-35.84M) | 53.22M (51.45M-54.52M) | 41.66M (40.96M-42.40M) | 46.35M (40.53M-46.50M) |
| rpc | 1 KiB | 7.92M (7.91M-7.96M) | 25.85M (25.71M-26.12M) | 6.55M (6.47M-6.72M) | 25.14M (24.98M-25.43M) |
| rpc | 64 KiB | 655.6K (646.6K-656.0K) | 635.2K (630.0K-643.9K) | 653.1K (637.5K-654.1K) | 630.8K (598.7K-638.3K) |
| rpc | 512 KiB | 86.1K (85.8K-87.4K) | 83.0K (81.7K-83.9K) | 81.7K (80.8K-83.7K) | 79.1K (77.3K-82.8K) |
| rpc | 1.5 MiB | 14.1K (13.4K-18.2K) | 16.3K (16.3K-17.5K) | 15.0K (14.9K-15.3K) | 15.1K (14.5K-17.2K) |
| latency-spin | 32 B p50 | 167 ns | 167 ns (167 ns-208 ns) | 208 ns | 208 ns |
| latency-spin | 32 B p99 | 250 ns | 250 ns | 250 ns | 250 ns |
| latency-sleep | 32 B p50 | 25.8 µs | 25.8 µs (25.8 µs-25.9 µs) | 34.8 µs (34.6 µs-34.8 µs) | 34.9 µs (34.6 µs-35.0 µs) |
| latency-sleep | 32 B p99 | 40.7 µs (40.2 µs-42.5 µs) | 40.5 µs (38.7 µs-41.0 µs) | 54.2 µs (52.5 µs-57.5 µs) | 64.3 µs (57.4 µs-65.4 µs) |
| wakeup | 32 B p50 | 26.2 µs (26.2 µs-26.3 µs) | 26.2 µs (26.2 µs-26.3 µs) | 35.3 µs (33.9 µs-40.0 µs) | 34.2 µs (33.9 µs-34.8 µs) |
| wakeup | 32 B p99 | 33.9 µs (33.5 µs-35.3 µs) | 33.3 µs (33.2 µs-33.4 µs) | 52.8 µs (50.3 µs-57.5 µs) | 50.5 µs (50.0 µs-52.5 µs) |
| wakeup-poll | 32 B p50 | 125 ns | 125 ns | 167 ns | 167 ns |
| wakeup-poll | 32 B p99 | 500 ns (208 ns-875 ns) | 292 ns (292 ns-500 ns) | 7.2 µs (6.8 µs-11.4 µs) | 6.1 µs (5.2 µs-7.5 µs) |
| setup | 1 MiB ring, cycles/s | 3.6K (3.4K-3.6K) | 3.6K (3.5K-3.6K) | 2.5K | 2.6K |
| latency-spin | 16 B p50 | 167 ns | 167 ns | 167 ns | 208 ns (167 ns-208 ns) |
| latency-spin | 16 B p99 | 208 ns (208 ns-250 ns) | 250 ns | 250 ns | 250 ns |

- **The stream pace:** 1 KiB runs 3.3x as fast on macOS (all three scenarios) and 3.6-3.8x in the VM (copy, RPC);
  256 B 1.5-2.0x. The VM's zero-copy 1 KiB ran at 19.6M/s without it (the receiver's copy keeps it behind its sender
  there), and runs at 20.6M/s with it. The round trip, the wake-ups and setup are unchanged, but for the VM's
  latency-sleep p99 (below).
- **The mechanism is measured, the cache-level account inferred.** Measured: with the old spin, a receiver at 1 KiB
  caught up with its sender at 0.75-0.9 waits per message, and with the pace at 0.003-0.03; the rates follow. Inferred,
  from the M1's 128-byte lines and the rates, not from a counter: each look takes the line of `head` and `tail` from
  the writer, and a reader that has caught up reads lines the writer has only just written from its core one at a
  time (about 9 GiB/s), where a reader that stays behind streams them (about 28 GiB/s).
- **1.5 MiB is bimodal on the M1**: a run lands near 13-14K or near 17-18K, for either library, in every scenario.
  Copy and zero-copy came out lower with the pace (13.8K and 13.1K against 18.0K and 14.8K) and RPC higher (16.3K
  against 14.1K). A re-check of copy and zero-copy (8 alternating rounds of 5 runs) gave 14.4K against 14.8K (3%
  lower) and 13.9K against 16.3K (15% lower), the rounds' medians overlapping (13.1-17.7K against 13.9-18.2K): a cost
  at 1.5 MiB is possible and not established (2026-10-07's A/B at the top: copy, zero-copy and RPC 0.96, 0.99 and
  0.96 of the unpaced library).
- **The VM's latency-sleep p99 is 15-24% higher in paced builds.** Two sessions with the Mac held awake (`caffeinate
  -i`), each 14 alternating rounds of 3 runs, pace 1, 2 and 256 paired: pace 1 66.2 µs, pace 2 83.2 µs (1.21, worse
  in 12 of 14 rounds), pace 256 78.9 µs (1.15, 12 of 14); then pace 1 73.7 µs, pace 2 88.7 µs (1.24, 13 of 14), pace
  256 88.3 µs (1.24, 13 of 14). The p50 37.4-37.7 µs in all, the p999 124-137 µs (paired 1.00-1.07). Neither side
  of latency-sleep is ever paced (each has just sent when it waits), and pace 2 costs as much as 256: it is the paced
  path compiled in (code placement, or the wait's `head_sent` load and store), not the pace; unexplained beyond that.
  macOS shows none (its latency-sleep p99 within noise in every A/B).
- **Linux in the VM against the Zenbook's WSL** (other hardware, a hypervisor, and WSL paced at 32 pauses: a rough
  comparison; the top table's columns): short messages about level (copy 16 B 49.6M against 47.7M, RPC 16 B 42.6M
  against 44.4M; zero-copy 64 B below, 41.3M against 52.5M), 1 KiB about level (copy 26.1M against 20.8M, zero-copy
  21.8M against 22.5M), 64 KiB about a third above (copy 644K against 469K), and a shorter round trip (212 ns against
  260 ns at 32 B). A sleeper wakes in 34 µs at the p50 (`wakeup`, above; WSL 52 µs, macOS 26 µs), and setup runs at
  2.3K/s (WSL 2.5K/s, macOS 3.5K/s).

### The stream pace on the M1

`devtool bench-pace --paces 1,32,64,128,256,512 --rounds 4 --runs 5`, on ReleaseFast builds (the sweep that chose 256
built Debug libraries, before c3a6fbe), macOS; each cell the median of all runs. The sweep (59 min) overlapped
about 36 min of host sleep between runs; its medians agree with the separate A/Bs (copy 1 KiB at 256: 26.94M against
26.99M), so they stand:

| Pace | copy 1 KiB | zero-copy 1 KiB | RPC 1 KiB |
|---|---|---|---|
| 1 (off) | 8.13M | 7.74M | 7.70M |
| 32 | 22.41M | 20.99M | 21.18M |
| 64 | 24.84M | 23.26M | 23.79M |
| 128 | 26.30M | 24.57M | 25.01M |
| 256 | 26.94M | 25.20M | 25.84M |
| 512 | 30.17M | 26.33M | 28.89M |

64 B and 1.5 MiB within their noise at every pace (zero-copy 1.5 MiB at 512: 12.5K, against 13-15K).

- **The round trip pays for the paced path, not for its length.** latency-spin 32 B in two sessions of 20
  alternating rounds of 5 runs of 1 s, paces 1, 32, 128, 256 and 512, the mean taken from each run's round trips
  (finer than the 42 ns tick): every paced build 0.4-0.8% slower than pace 1 (about 209.6 ns against 208.3 ns), the
  same at every pace (paired ratios 1.004-1.008, slower in 15-18 of 20 rounds); the p50 167 ns in 999 of the 1000
  runs. The cost is compiling the paced path at all; 256 stays (chosen over 512 on 2026-10-06, the more cautious for
  the p99).
- **The receive wait's `head_sent`.** Reading the endpoint's own `SendLine.head_sent` instead of the send ring's head
  in the segment (7268107; x86 above) keeps the M1's gains: against 4f50e8f, which loaded the segment's head, copy,
  zero-copy and RPC 1 KiB 1.00, 0.99 and 1.00, every case from 256 B up within its noise. But its store, placed
  after `publishHead`, waited behind that publish's swap of the sleep flag: zero-copy 16 B about 10% slower (0.88-0.91
  in three A/Bs against 4f50e8f) and RPC 64 B 0.87-0.90. Without the store, zero-copy 16 B was back (1.00); with it
  before the publish (f5a96c1), both are: zero-copy 16 B 1.03, RPC 64 B 1.04 (15 rounds).
- **Zero-copy 64 B is bimodal on the M1**: a run lands near 47-49M or near 55-65M, and the case moves 0.86-1.02
  between builds that don't change its path. The same tree at pace 1 ran it at 45M against cb60494's 54M, and pacing
  raises it (50.6M at 256): its 0.90 at the top is code placement, not the pace.

### Lowest latency: busy-polling

A receiver waiting in `fipc_recv` spins for 100-250 us (`spin_iters`, `src/zig/data/ring_wait.zig`), then sleeps, and
a message that comes later wakes it in tens of microseconds. For the lowest latency at any message rate, call
`fipc_recv` (or `fipc_rpc_recv`, `fipc_recv_acquire`) with timeout 0 in a loop and call again on `FIPC_TIMEOUT`. A call
that finds nothing doesn't spin, makes no system call and reads no clock: it loads the ring's indices, the connection's
`requests` and `status`, and returns; a loop of such calls runs at about 20 ns a call on both OSes. The polling thread
holds one core at 100%.

`wakeup-poll` measures it: the `wakeup` case's messages, one 32 B message per millisecond, its age on arrival, to a
receiver that polls. 2026-10-07 (`main` eeb6676's library), 7 rounds of 2 s each on WSL, 18 on Windows, whose p50 is bimodal: 12 rounds at
13-46 us, 6 at 92-109 us:

| Receiver | Windows p50 | Windows p99 | WSL p50 | WSL p99 |
|---|---|---|---|---|
| waits in `fipc_recv` (`wakeup`) | 42.3 us | 513 us | 55.4 us | 136 us |
| polls with timeout 0 (`wakeup-poll`) | 268 ns | 17.7 us | 257 ns | 70.8 us |

- **The p99 is the OS's.** A polling loop that timed its own calls lost 1.1-1.9% of its time in gaps over a
  microsecond (interrupts, the scheduler), and a message sent during one waits for the thread to run again. A sparse
  message lands in such a gap about as often: the p99. Keeping interrupts and other threads off the polling core is
  the usual remedy.

## The suite

`zig-out/bench/fipc_bench` (built by every `zig build`, always ReleaseFast) runs each case in two processes, itself
(the server: it listens and accepts, and measures) and a peer (the same executable, the client: it connects),
connected through the library's public C API, `include/fipc.h`. It loads the library at run time (`--lib`, by
default this tree's ReleaseFast build installed next to it), so one binary measures any build that speaks that API:
this tree's, or another revision's.

| Scenario | What | Cases (message size, ring) |
|---|---|---|
| `fastipc` | copy: `fipc_send` / `fipc_recv` | 16 B\*, 64 B\*, 256 B, 1 KiB\*, 64 KiB\* (512 KiB ring); 512 KiB (2 MiB ring); 1.5 MiB = 3 rings\* |
| `fastipc-zerocopy` | zero-copy: `fipc_send_acquire` + `fipc_send_commit` / `fipc_recv_acquire` + `fipc_recv_release`; a message of several pieces through the copying calls | as `fastipc` |
| `rpc` | RPC: `fipc_rpc_submit` / `fipc_rpc_recv` | as `fastipc` |
| `latency-spin` | round trip, the peer replies at once\* | 32 B (512 KiB ring) |
| `latency-sleep` | round trip, the peer thinks 500 us first, so this side is asleep when the reply comes\* | 32 B |
| `wakeup` | one message per millisecond to a receiver that sleeps in between\* | 32 B |
| `wakeup-poll` | the same messages to a receiver that calls `fipc_recv` with timeout 0 in a loop | 32 B |
| `setup` | a connection's life: listen, connect, accept, both close, stop listening\* | 1 MiB ring |

\* A case of the performance gates (`fipc_bench gates`, `devtool bench-compare --ab`'s default).

## Method

- **Throughput:** the sender warms up (0.2 s), measures its rate and announces a plan: 5 runs of as many messages as
  fill about 1 s each. The receiver times each run as one window; the windows follow each other, so every run
  measures steady traffic. Every message's length is checked; payloads are 'x' bytes; zero-copy receivers copy each
  message out (`--no-touch`: they don't).
- **Latency:** after the warm-up, 5 runs of 1 s of samples each; a ping-pong sample is the round trip minus the
  peer's hold time (the reply carries it), a wake-up sample the message's age on arrival (the sender stamps it).
  p50, p99 and p99.9 per run.
- **Setup:** after the warm-up, 5 runs of 1 s of cycles; a cycle is a connection's life (the server listens, tells the
  client to connect through a second, long-lived connection, accepts, closes it and stops listening; the client
  closes its end and confirms), from one confirmation to the next. Cycles per second, and the cycle time's p50 and
  p99.
- **Clocks:** run durations on the monotonic clock through `std.Io` (`CLOCK_MONOTONIC`,
  `QueryPerformanceCounter`); latency samples on the CPU's time-stamp counter (invariant and synchronized across
  cores on every supported CPU; on Apple Silicon the 24 MHz virtual counter), converted at the rate measured against
  the monotonic clock over the same run.
  QueryPerformanceCounter's 100 ns steps would quantize sub-microsecond round trips.
- **Reported:** each metric's median over the runs and its range; `--json` writes one object per case with every
  run's value.
- **Windows power throttling:** the suite turns it off for its processes (`SetProcessInformation`,
  `ProcessPowerThrottling`). Started from a background process (a terminal without focus, an automated run),
  Windows 11 otherwise runs it at low quality of service, on efficiency cores at lower clocks: 64 KiB stream messages
  at 210-280K/s instead of about 430K/s. For the other languages' benchmarks, devtool (`bench`, `bench-compare`)
  turns it off for every process they start (`devtool_lib/throttle.py`: a job object reports each new one).
- **macOS's background state:** a process started in it (`taskpolicy -b`, PRIO_DARWIN_BG), and every process it
  starts, runs on the M1's efficiency cores: copy 1 KiB at 7M/s instead of 27M/s, the round trip 550 ns instead of
  167 ns. The suite and devtool (`bench`, `bench-compare`, `bench-pace`) leave it before they start anything
  (`setpriority(PRIO_DARWIN_PROCESS, 0, 0)`). A QoS clamp of background (`taskpolicy -c background`) does the same and
  no process can lift its own: both warn of it, so start them from a foreground terminal. The macOS bindings tables
  of 2026-10-04/05 ran so (2.4-4.4 times slower at 64 KiB-1 MiB); every Zig-suite number here ran on the
  performance cores. Left alone, the Mac also goes to sleep, mid-benchmark, for about 15 minutes at a time, and a run's
  clock goes on (one copy 16 B run read 2.1K/s): devtool's bench commands hold `caffeinate -i` while they run
  (d11d083), and anything else run by hand for long needs it too.
- **Placement:** a 1 KiB copy's speed depends on where its buffers sit in their pages (4K aliasing, cache-line
  splits), and malloc places a buffer after everything allocated before it, the command line included: dlopen and
  the peer's argv copy the `--lib` path. On WSL, the same library ran 1 KiB stream messages about 10% apart from two
  directories. So the A/B gives both sides command lines of the same length (the libraries' copies in its scratch
  directory, `new/` and `ref/`), and the suite allocates its own hot buffers at the start of a page. Runs with other
  command lines can differ by that much at 1 KiB.
- **No CPU pinning** by default: `AUTO_AFFINITY=1` makes the suite pin the thread that accepts or connects a
  connection (`bench/zig/affinity.zig`); the library itself pins nothing.
- **latency-sleep's think time** is 500 us: before it sleeps, a waiting side spins through its spin phase, which
  takes 130-250 us on this machine under Windows and 100-150 us in WSL; with a shorter think time the receiver never
  sleeps.

## Machine

- ASUS Zenbook 14 (UX3404VC) laptop, on AC power, Windows power plan "High performance".
- Intel Core i9-13900H: 6 P-cores with Hyper-Threading and 8 E-cores, 20 logical CPUs; 32 GB RAM.
- Windows 11 Pro 10.0.26200; the suite driven from Git Bash with Python 3.13.5.
- WSL2 Ubuntu 24.04.3 LTS: kernel 6.18.33.2-microsoft-standard-WSL2, glibc 2.39, 20 vCPUs, 15 GiB; clock source
  `tsc` (`constant_tsc`, `nonstop_tsc`).
- Zig 0.17.0; every library ReleaseFast for x86-64-v3.
- The macOS column: a MacBook Air (M1: 4 performance and 4 efficiency cores, 8 GB RAM), macOS 26.5.1, on power; Zig
  0.17.0, ReleaseFast for `apple_m1`.
- The Linux VM column: the same MacBook Air, Lima 2.2.1 (Apple Virtualization framework): Ubuntu 24.04.5 arm64, kernel
  6.8.0-142-generic, glibc 2.39, 4 vCPUs, 4 GiB, clock source `arch_sys_counter`; Zig 0.17.0, ReleaseFast.
- Running alongside, idle: IDEs, a browser and a few background processes; 5-15% background CPU load on Windows when
  sampled. Nothing else runs benchmarks or tests; Windows and WSL run one after the other, never together.
- `AUTO_AFFINITY` unset (no CPU pinning); `LOG_LEVEL` unset.

## Commands

From the repository root (Windows: Git Bash; Linux: a shell in a Linux checkout), after `python devtool.py build`:

```bash
zig-out/bench/fipc_bench all --runs 5 --json run.jsonl       # every scenario, this tree's library
python devtool.py bench-compare --ab                          # the gate cases: this tree vs main (--ref REV)
python devtool.py bench-pace --paces 1,2,4,8,16,32,64         # the reader's spin pace: one build per pace
```

`bench-compare --ab` measures the library in zig-out/bench as it is: it doesn't build this tree, so build after every
change first (`zig build` or `devtool build`). It is by default a quick run of 3 alternating rounds of 0.3 s per case, a few minutes per OS,
because rough numbers (about ±5%) are enough. A scenario with a failing case is re-checked automatically, once, with
as many rounds again, and judged on all its rounds. Longer runs (`--runs`, `--duration`), A/A runs and 2x2
comparisons are for settling a disputed result, not a default.

## Reading the noise

From A/A runs (the same library on both sides, 7 alternating rounds) and the spread within a case's runs:

- **Throughput:** from 1 KiB up, two runs of the same library agree within ±2-3% on both OSes. At 16-64 B they
  differ by up to 4% on Windows and up to 9% on WSL. A gated failure at 16-64 B between 0.90 and 0.95 is inside that
  spread: repeat the A/B before acting on it (a real regression reproduces); from 1 KiB up, or below 0.90, it is
  likely real.
- **latency-spin:** p50 within ±1%, p99 within ±3%; on WSL a round's p50 lands near 240 ns or near 270 ns ("The
  stream pace on x86"), so compare the medians of many rounds there.
- **setup:** cycles per second within ±1%.
- **Wake-up latencies** (latency-sleep, wakeup) are bimodal on this machine: a run's p50 lands near 40 us or near 100
  us on Windows, 28-67 us on WSL, depending on where the sleeping thread wakes and how deeply its core slept; one A/A
  round-set gave latency-sleep a ratio of 0.76. `bench-compare --ab` shows them without gating them: watch for a
  shift of both p50 and p99 across repeated runs.
- **Within one case** (5 runs): throughput mostly within ±3%, a few cases up to ±8%; latency-spin's p50 within ±4%
  but for an occasional run; setup within ±2%.
- **The machine's state moves the numbers:** on a busy machine latency-sleep's p50 falls from about 30 us to 4-5 us
  on Windows (busy cores don't sleep deeply). Compare only runs of one session (the A/B alternates for that reason),
  and WSL numbers only with WSL numbers.
