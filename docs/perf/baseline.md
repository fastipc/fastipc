# Performance

What the benchmark suite (`bench/zig`, `fipc_bench`) measures, how it runs, the library's numbers on the baseline
machine, and how much two runs of the same library differ. The C, C++, C#, Java, Python, Rust, Lua, JavaScript and
Go benchmarks' reference medians are in [`bindings-baseline.md`](bindings-baseline.md).

## The numbers

The library on the baseline machine (below), ReleaseFast for x86-64-v3, with the stream pace (`stream_paces`: Windows 64,
Linux 32; "The stream pace on x86" below): the medians of `devtool bench-compare --ab --runs 5 --duration 1` (5
alternating rounds of 1 s per gate case against 4abedadc's library), the night of 2026-10-24 to 25 at main
2cafb052, Windows and then WSL, then macOS and the Linux VM. Messages per second, the round trip's p50, or
connection cycles per second.

| Scenario | Case | Windows | WSL | macOS\* | Linux VM on the M1† |
|---|---|---|---|---|---|
| fastipc | 16 B | 50.79M | 47.41M | 109.24M | 111.85M |
| fastipc | 64 B | 47.56M | 44.57M | 104.52M | 104.03M |
| fastipc | 1 KiB | 22.87M | 23.52M | 30.61M | 28.98M |
| fastipc | 64 KiB | 443.6K | 449.0K | 660.6K | 657.2K |
| fastipc | 1.5 MiB | 8.6K | 8.8K | 12.7K | 14.0K |
| fastipc-zerocopy | 16 B | 47.62M | 42.46M | 93.44M | 91.40M |
| fastipc-zerocopy | 64 B | 46.92M | 45.85M | 91.15M | 82.13M |
| fastipc-zerocopy | 1 KiB | 21.82M | 21.93M | 28.30M | 25.80M |
| fastipc-zerocopy | 64 KiB | 443.2K | 435.0K | 438.9K | 551.9K |
| fastipc-zerocopy | 1.5 MiB | 8.6K | 8.8K | 12.7K | 14.0K |
| latency-spin | 32 B p50 | 238 ns | 230 ns | 125 ns | 125 ns |
| latency-spin | 16 B p50\*\* | 212 ns | 209 ns | 125 ns | 125 ns |
| setup | 1 MiB ring | 7.8K | 2.5K | 4.1K | 2.2K |

Against 4abedadc in the same rounds, on Windows: copy 16 B 1.21 times as fast, 64 B 1.13, 1 KiB 1.40, zero-copy 16 B
1.12, 64 B 1.05 and 1 KiB 1.08, the round trip's p50 at 32 B 238 ns against 290 ns (its p99 421 ns against 408 ns),
the other cases within 3%; 12 of 12 gate cases passed. On WSL: copy 64 B 1.08, 1 KiB 1.22, zero-copy 64 B 1.05 and
1 KiB 1.09, the round trip 230 ns against 282 ns (p99 413 ns against 375 ns), copy 16 B 0.95 (47.41M against
49.76M), zero-copy 16 B 0.94 (42.46M against 45.22M, about the column's previous 41.67M; the gate's one failure, as
in the session before, so devtool ran 10 rounds of the zero-copy scenario, the WSL column's zero-copy medians), the
other cases within 3%.

\*\* At 16 B: from "Latency by message size" below (2026-10-25, rounds of their own), where 16 B is the quickest size
on both x86 columns (212 ns on Windows, 209 ns on WSL); the macOS and Linux VM columns' from
a session of the same kind the same night at the same commit (7 rounds, the sizes rotated), where 8, 16, 32 and 48 B
tie on both (125 ns, 3 ticks; 64 B 167 ns, 4 ticks; the p99 167 ns at every size). The session before (2026-10-24, the
pace 256 and before the changes below) read 125 ns at 8, 16 and 48 B on macOS and 169-172 ns at every size in the VM.

\* macOS: MacBook Air (M1, 8 GB RAM, 8 cores), macOS 26.5.1, on power; different hardware from the baseline machine,
so compare across columns with care. ReleaseFast for `apple_m1`, unpinned (macOS has no thread pinning), with the
stream pace (512 hints on arm64, "The stream pace on arm64" below): the medians of `devtool bench-compare --ab --runs 5 --duration 1` (5
alternating rounds of 1 s per gate case against 4abedadc's library, as the Windows and WSL columns), the night of
2026-10-24 to 25 at main 2cafb052, the Linux VM stopped. The rounds' medians spread most at zero-copy 1.5 MiB
(12.3K-15.5K, bimodal: "On the M1" below) and copy 16 B (107.92M-118.40M); copy 1 KiB within about 3%
(29.84M-30.74M). Against 4abedadc in the same rounds: copy 16 B 1.84 times as fast, 64 B 2.10 and 1 KiB 1.13,
zero-copy 16 B 1.64, 64 B 1.70 and 1 KiB 1.12, the round trip one tick shorter (p50 125 ns against 167 ns, p99 167 ns
against 250 ns), 64 KiB within 2%, 1.5 MiB 0.96; 12 of 12 gate cases passed. Against the column's previous session
(2026-10-24 at main 45b88f10, the pace 256): copy 16 and 64 B 1.33 and 1.81 times as fast (109.24M against 82.22M,
104.52M against 57.64M), zero-copy 16 and 64 B 1.76 and 1.80, 1 KiB 1.16-1.17 (the stream's arm64 changes since: the segment's ring lines and the endpoint's hot lines 128 bytes apart, each side reading its own index from its endpoint, the pace of 512), 64 KiB within 3%, copy
1.5 MiB 0.95 and zero-copy 1.5 MiB 0.80 (12.7K against 15.9K, the bimodal case), setup the same. The session of
2026-10-07 ran against a build without the pace; against it:

- 1 KiB 3.3 times as fast (copy 26.99M against 8.12M, zero-copy 25.32M against 7.72M);
- 16 B and 64 KiB within 5%, 1.5 MiB within its noise;
- copy 64 B 10% faster and zero-copy 64 B 10% slower (49.05M against 54.51M: code placement, below);
- the round trip's p50 167 ns and p99 250 ns the same.

Three things of the platform show in them:

- **The timer ticks at 24 MHz** (about 42 ns: the ARM generic timer, `cntvct_el0`, which the suite reads for latency
  samples), so latencies are quantized to that step: the round trip's 167 ns at 32 B is 4 ticks, and its 125 ns at
  16 B 3.
- **The M1's cache line is 128 bytes** for the lines that matter here (`hw.cachelinesize`): a ring's `head` and
  `tail` were 64 bytes apart in the segment and shared one line, until the segment put them 128 bytes apart
  (docs/protocol.md §5.1).
- **From 256 B to 1.5 KiB, without the stream pace, a receiver that reads each message runs at about half the rate of
  one that doesn't.** A sweep of the copy scenario without the pace (2026-10-05): 256 B 31.7M/s, 512 B 16.9M/s, 1 KiB
  9.0M/s, 1.5 KiB 6.9M/s, then 2 KiB 13.9M/s (27 GiB/s) and 16 KiB 2.4M/s (36 GiB/s). A zero-copy receiver that doesn't read the payload
  (`--no-touch`) runs 512 B at 35.4M/s and 1 KiB at 18.6M/s (8.0M/s when it copies each message out), and 2 KiB
  at 13.4M/s, about the copy scenario's 13.9M/s. The ring's size moved it (1 KiB with a 64 KiB ring: 10.7M/s). The
  cause is a receiver that has caught up looking at the head after every spin hint: each look takes the line of
  `head` and `tail` (one 128-byte line) from the sender, whose every publish must win it back, so the receiver stays
  caught up. So a receive that waits for a stream makes no look for 512 hints on arm64 (macOS and Linux:
  "The stream pace on arm64" below; x86 below, `stream_paces` in `core/wait.zig`), then one after every hint. At 256
  hints, the pace before (2026-10-06): copy 1 KiB about 27M/s, 256 B about 55M/s, 1.5 KiB about 19M/s, 2 KiB about
  10% more, the other cases within their noise (1.5 MiB: "On the M1" below); at 512, copy 1 KiB about 30M/s. The
  columns above are with it. `devtool bench-pace` measures a platform's paces.

† Linux in a VM on the macOS column's MacBook Air: Lima 2.2.1 (Apple Virtualization framework), Ubuntu 24.04.5 arm64,
kernel 6.8.0, glibc 2.39, 4 vCPUs and 4 GiB; Zig 0.17.0, ReleaseFast for generic ARMv8.0-A (the build's CPU floor
for Linux arm64). The macOS column's build and method, the same night at main 2cafb052, after the macOS column:
the first 5 rounds failed copy and zero-copy 1.5 MiB (0.95 and 0.93 of 4abedadc), so devtool ran 10 rounds of both
scenarios, the column's medians, which passed them (0.97 each); the rounds' medians spread most at copy 16 B
(103.48M-124.52M) and copy 1 KiB (25.63M-30.88M). Against 4abedadc in the same rounds: copy 16 B 2.31 times as fast,
64 B 2.12 and 1 KiB 1.11, zero-copy 16 B 1.73, 64 B 2.13 and 1 KiB 1.14, the round trip two ticks shorter (p50 125 ns
against 208 ns, p99 167 ns against 250 ns), 64 KiB within 3%; 12 of 12 gate cases passed. Against the column's
previous session (2026-10-24 at main 45b88f10, the pace 256): copy 16 and 64 B 1.56 and 1.88 times as fast (111.85M
against 71.78M, 104.03M against 55.41M), zero-copy 16 and 64 B 1.89 and 2.18 (82.13M against 37.76M at 64 B), 1 KiB
1.16-1.17 (the stream's arm64 changes since: the segment's ring lines and the endpoint's hot lines 128 bytes apart, each side reading its own index from its endpoint, the pace of 512), 64 KiB 1.04-1.05, 1.5 MiB the same; setup 2.2K against 2.4K (4abedadc's too, in the same
rounds). The session of 2026-10-07, against the unpaced build in the same rounds: copy
1 KiB 3.9 times as fast (26.09M against 6.66M), zero-copy 1 KiB 1.05 (21.81M
against 20.74M: its receiver's copy keeps it behind its sender either way); 16 and 64 B 1.06-1.10; 64 KiB within
1.5%, 1.5 MiB 0.95-0.97; the round trip (p50 212 ns, p99 255 ns) and setup the same; 17 of 17 gate cases passed. The
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
  message out and doesn't. Without the stream pace (`-Dstream_pace=1`), copy at 16-64 B runs at about half the
  zero-copy rate for that reason (the table below, pace 1). A consumer that does work on its messages runs in the fast
  regime. The swap is what keeps a sleeping receiver from missing a wake-up, so it stays; instead, a receiver that
  waits for a stream looks at the head less often (the stream pace, below). One that sent since its last wait, and so
  waits for an answer, looks after every pause: looking less often would slow the round trip.
- A message of three rings goes in pieces through the copying calls (zero-copy takes one piece), so its two
  scenarios run alike.

### The stream pace on arm64

`devtool bench-pace --paces 64,128,256,512,1024 --rounds 4`, 2026-10-24, on the library in which each side reads its own
ring index (the M1's 128-byte lines padded apart): each cell the median of all runs (4 rounds of 5 runs of 1 s), and
the range of the rounds' medians. The pace that is fastest at 1 KiB without slowing 1.5 MiB or the round trip is 512
on both; 1024 is a little faster at 1 KiB but slows 1.5 MiB.

| macOS (M1) | 64 | 128 | 256 | 512 | 1024 |
|---|---|---|---|---|---|
| copy 64 B | 90.57M | 99.17M | 103.81M | 105.94M | 104.90M |
| copy 1 KiB | 24.81M (23.62M-24.96M) | 26.50M (25.75M-27.37M) | 27.66M (26.72M-28.01M) | 30.73M (29.23M-31.48M) | 31.70M (31.04M-31.92M) |
| copy 1.5 MiB | 17.6K (14.1K-18.2K) | 17.2K (16.2K-17.4K) | 14.3K (13.0K-17.2K) | 15.0K (12.5K-17.0K) | 13.9K (11.6K-14.3K) |
| zero-copy 64 B | 86.55M | 86.47M | 85.58M | 85.51M | 85.94M |
| zero-copy 1 KiB | 24.62M | 26.29M | 26.74M | 28.32M | 28.30M |
| zero-copy 1.5 MiB | 18.1K (17.8K-18.3K) | 17.2K (16.4K-18.1K) | 13.8K (13.1K-16.6K) | 14.3K (12.4K-17.0K) | 14.0K (13.6K-14.3K) |
| round trip p50, p99 | 125, 167 ns | 125, 167 ns | 125, 167 ns | 125, 167 ns | 125, 167 ns |

| Linux ARM64 (the VM) | 64 | 128 | 256 | 512 | 1024 |
|---|---|---|---|---|---|
| copy 64 B | 98.02M | 96.33M | 98.96M | 100.56M | 100.97M |
| copy 1 KiB | 22.25M (17.35M-22.59M) | 25.70M (23.96M-26.36M) | 27.12M (26.47M-28.30M) | 28.71M (24.88M-29.52M) | 30.30M (30.24M-30.97M) |
| copy 1.5 MiB | 17.3K (14.9K-17.5K) | 17.1K (16.6K-17.4K) | 15.4K (14.4K-17.1K) | 15.9K (14.0K-16.4K) | 14.0K (13.4K-14.1K) |
| zero-copy 64 B | 77.15M | 75.81M | 75.80M | 77.33M | 76.57M |
| zero-copy 1 KiB | 22.12M | 23.63M | 24.11M | 25.06M | 24.95M |
| zero-copy 1.5 MiB | 17.5K (17.4K-17.8K) | 16.0K (14.7K-17.1K) | 14.7K (14.4K-16.4K) | 15.5K (14.3K-16.4K) | 14.0K (13.7K-14.2K) |
| round trip p50, p99 | 167, 167 ns | 146, 167 ns | 167, 167 ns | 125, 167 ns | 146, 167 ns |

The smaller paces are faster at 1.5 MiB (17-18K against 14-16K) but slower at 1 KiB and 64 B; 1.5 MiB is bimodal at
256 and 512 (the ranges).

### The stream pace on x86

A receiver that hasn't sent since its last wait (a stream's) looks at the ring once, then makes no look for its first
`stream_pace` pauses (`stream_paces`, `core/wait.zig`), then looks after every pause; the writer meanwhile
publishes without losing the index line to the reader. `devtool bench-pace` builds the library once per pace and runs
the copy stream against each, in alternating rounds. 2026-10-06, unpinned; each cell the median of all runs
(3 rounds of 5 runs of 1 s), and the range of the rounds' medians:

| Pace | Windows copy 64 B | WSL copy 64 B |
|---|---|---|
| 1 (off) | 18.59M (17.22M-19.06M) | 19.57M (18.25M-19.83M) |
| 2 | 21.25M (18.60M-22.38M) | 21.12M (19.40M-21.28M) |
| 4 | 23.86M (22.61M-24.71M) | 21.59M (20.95M-23.16M) |
| 8 | 23.80M (23.13M-25.08M) | 25.29M (24.33M-25.47M) |
| 16 | 28.50M (27.83M-29.06M) | 35.93M (35.14M-36.43M) |
| 32 | 34.98M (34.12M-35.85M) | 46.18M (43.96M-46.31M) |
| 64 | 40.12M (37.57M-41.15M) | 51.25M (50.95M-51.69M) |

Unpaced against paced, from A/B runs of 2026-10-07 (a build without the pace against the library with it,
alternating; the paced library of that day, before the table at the top was measured again):

| Case | Windows, no pace | Windows, 64 | WSL, no pace | WSL, 32 |
|---|---|---|---|---|
| fastipc 16 B | 36.68M | 48.25M | 30.75M | 47.89M |
| fastipc 64 B | 21.39M | 40.83M | 20.98M | 46.12M |
| fastipc-zerocopy 64 B | 42.33M | 47.94M | 48.85M | 51.95M |
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
- **The receiver must tell a stream from an answer without touching the segment.** A paced build that loads the send
  ring's head (the line the peer spins on) to see whether it sent runs the unpinned round trip 7-10% slower, in 18 of
  20 rounds, whatever the pace; the endpoint's own copy of that head (`SendLine.head_sent`) costs nothing.
- 64 pauses take about as long as 256 of the M1's `isb`s (some 2 µs; the M1's pace is 512). Other hardware may want another pace: `zig build
  -Dstream_pace=N` builds one (1 turns the pacing off), and `devtool bench-pace` measures them.

### Latency by message size

latency-spin at 8 to 64 B (`--sizes`), unpinned, 2026-10-25 at main 2cafb052:
7 rounds, each running every size once (1 s of samples after a 0.2 s warm-up) in a rotated order, so
drift reaches every size alike; the medians over the rounds.

| Size | Windows p50 | Windows p99 | WSL p50 | WSL p99 |
|---|---|---|---|---|
| 8 B | 216 ns | 272 ns | 210 ns | 267 ns |
| 16 B | 212 ns | 270 ns | 209 ns | 261 ns |
| 32 B | 242 ns | 437 ns | 229 ns | 413 ns |
| 48 B | 234 ns | 281 ns | 226 ns | 270 ns |
| 64 B | 278 ns | 425 ns | 263 ns | 383 ns |

- **The frame's fit in a cache line sets the order.** A frame is the message plus its 16-byte header, aligned to 16
  bytes: 8 and 16 B messages make 32-byte frames, which never straddle a 64-byte line, and 48 B makes 64-byte frames;
  32 B makes 48-byte frames, half of which straddle one, and 64 B 80-byte frames, which always span two. 16 B is the
  quickest; 8 B ties it on both (the rounds' ranges overlap: 204-225 ns against 205-218 ns on Windows, 200-213 ns
  against 204-212 ns on WSL), 48 B is 10% slower on Windows (225-238 ns) and 8% on WSL (211-234 ns); 32 B is 14%
  slower on Windows and 10% on WSL, 64 B 31% on Windows and 26% on WSL. The p99 of 32 and 64 B is 1.4-1.6 times that
  of 8, 16 and 48 B.
- **No pinned numbers.** The docs report the two sides on separate cores, as two processes that each do work run.
  Pinned to two separate P-cores, Windows measures as unpinned (16 B: 270-281 ns, neighbouring cores or not;
  2026-10-07); on WSL pinning changes nothing measurable (the VM's vCPUs aren't fixed to cores).
- The website and the README headline each OS's fastest size: 16 B on every column (on macOS and in the Linux VM it
  ties the quickest sizes: the note under the table at the top).

### On the M1: macOS and Linux, without and with the stream pace

Every scenario on macOS and in the Linux VM, a build without the pace and one with it (256 hints on arm64), 3 rounds
alternating between them, each round `fipc_bench all --runs 5` and latency-spin at 16 B; the median of the 15 runs,
and in brackets the range of the 3 rounds' medians. macOS first (2026-10-06, the VM stopped), then Linux in the VM
(the same day). Messages per second, ns or µs, cycles per second. The macOS and Linux VM columns at the top come from
another session (the night of 2026-10-24 to 25).

| Scenario | Case | macOS, no pace | macOS, pace 256 | Linux VM, no pace | Linux VM, pace 256 |
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

- **The stream pace:** 1 KiB runs 3.3x as fast on macOS (both scenarios) and 3.6x in the VM (copy);
  256 B 1.5-2.0x. The VM's zero-copy 1 KiB runs at 19.6M/s without it (the receiver's copy keeps it behind its sender
  there), and at 20.6M/s with it. The round trip, the wake-ups and setup are the same either way, but for the VM's
  latency-sleep p99 (below).
- **The mechanism is measured, the cache-level account inferred.** Measured: without the pace, a receiver at 1 KiB
  catches up with its sender at 0.75-0.9 waits per message, and with it at 0.003-0.03; the rates follow. Inferred,
  from the M1's 128-byte lines and the rates, not from a counter: each look takes the line of `head` and `tail` from
  the writer, and a reader that has caught up reads lines the writer has only just written from its core one at a
  time (about 9 GiB/s), where a reader that stays behind streams them (about 28 GiB/s).
- **1.5 MiB is bimodal on the M1**: a run lands near 13-14K or near 17-18K, for either build, in every scenario.
  Copy and zero-copy came out lower with the pace (13.8K and 13.1K against 18.0K and 14.8K). A re-check of copy and
  zero-copy (8 alternating rounds of 5 runs) gave 14.4K against 14.8K (3% lower) and 13.9K against 16.3K (15% lower),
  the rounds' medians overlapping (13.1-17.7K against 13.9-18.2K): a cost at 1.5 MiB is possible and not established
  (the A/B at the top: copy and zero-copy 0.96 and 0.99 of the unpaced build).
- **The VM's latency-sleep p99 is 15-24% higher in paced builds.** Two sessions with the Mac held awake (`caffeinate
  -i`), each 14 alternating rounds of 3 runs, pace 1, 2 and 256 paired: pace 1 66.2 µs, pace 2 83.2 µs (1.21, worse
  in 12 of 14 rounds), pace 256 78.9 µs (1.15, 12 of 14); then pace 1 73.7 µs, pace 2 88.7 µs (1.24, 13 of 14), pace
  256 88.3 µs (1.24, 13 of 14). The p50 37.4-37.7 µs in all, the p999 124-137 µs (paired 1.00-1.07). Neither side
  of latency-sleep is ever paced (each has just sent when it waits), and pace 2 costs as much as 256: it is the paced
  path compiled in (code placement, or the wait's `head_sent` load and store), not the pace; unexplained beyond that.
  macOS shows none (its latency-sleep p99 within noise in every A/B).
- **Linux in the VM against WSL** (other hardware, a hypervisor, and WSL paced at 32 pauses: a rough comparison;
  the top table's columns): short messages about twice as fast (copy 16 B 111.9M against 47.4M, zero-copy 64 B 82.1M
  against 45.9M), 1 KiB 1.2 times (copy 29.0M against 23.5M, zero-copy 25.8M against 21.9M), 64 KiB about 46% above
  (copy 657K against 449K), and a shorter round trip (125 ns against 230 ns at 32 B). A sleeper wakes in 34 µs at the
  p50 (`wakeup`, the top table's session; WSL 58 µs, macOS 26 µs), and setup runs at 2.2K/s (WSL 2.5K/s, macOS
  4.1K/s).

### The stream pace on the M1

`devtool bench-pace --paces 1,32,64,128,256,512 --rounds 4 --runs 5`, on ReleaseFast builds, macOS; each cell the
median of all runs. The sweep (59 min) overlapped
about 36 min of host sleep between runs; its medians agree with the separate A/Bs (copy 1 KiB at 256: 26.94M against
26.99M), so they stand:

| Pace | copy 1 KiB | zero-copy 1 KiB |
|---|---|---|
| 1 (off) | 8.13M | 7.74M |
| 32 | 22.41M | 20.99M |
| 64 | 24.84M | 23.26M |
| 128 | 26.30M | 24.57M |
| 256 | 26.94M | 25.20M |
| 512 | 30.17M | 26.33M |

64 B and 1.5 MiB within their noise at every pace (zero-copy 1.5 MiB at 512: 12.5K, against 13-15K).

- **The round trip pays for the paced path, not for its length.** latency-spin 32 B in two sessions of 20
  alternating rounds of 5 runs of 1 s, paces 1, 32, 128, 256 and 512, the mean taken from each run's round trips
  (finer than the 42 ns tick): every paced build 0.4-0.8% slower than pace 1 (about 209.6 ns against 208.3 ns), the
  same at every pace (paired ratios 1.004-1.008, slower in 15-18 of 20 rounds); the p50 167 ns in 999 of the 1000
  runs. The cost is compiling the paced path at all. The pace is 256 rather than 512, the more cautious choice for the
  p99.
- **The receive wait's `head_sent`.** The wait reads the endpoint's own `SendLine.head_sent`, not the send ring's head
  in the segment (x86 above), and keeps the pace's gains on the M1: copy and zero-copy 1 KiB 1.00 and 0.99 against a
  build that loads the segment's head, every case from 256 B up within its noise. Its store comes before
  `publishHead`: placed after it, the store waits behind the publish's swap of the sleep flag and makes zero-copy 16 B
  about 10% slower than that build (0.88-0.91 in three A/Bs); placed before it, 1.03 (15 rounds).
- **Zero-copy 64 B is bimodal on the M1**: a run lands near 47-49M or near 55-65M, and the case moves 0.86-1.02
  between builds that don't change its path. A pace-1 build (the paced code compiled in, pacing off) runs it at 45M,
  a build without the paced code at 54M, and pace 256 at 50.6M: its 0.90 at the top is code placement, not the pace.

### Lowest latency: busy-polling

A receiver waiting in `fipc_recv` spins for 100-250 us (`spin_iters`, `src/zig/core/wait.zig`), then sleeps, and
a message that comes later wakes it in tens of microseconds. For the lowest latency at any message rate, call
`fipc_recv` (or `fipc_recv_acquire`) with timeout 0 in a loop and call again on `FIPC_TIMEOUT`. A call
that finds nothing doesn't spin, makes no system call and reads no clock: it loads the ring's indices, pauses for one
spin hint (in a steady stream, up to the stream pace's hints) and looks once more, loads the connection's `requests`
and `phase`, and returns; a loop of such calls runs at about 50 ns a call on x86, on both OSes (14 ns without the
pause, which pulled the ring's head line from the writer on every call: a polling round trip ran 25-35% slower than a
waiting one at 16 B and 45-60% at 1 KiB). The polling thread holds one core at 100%.

`wakeup-poll` measures it: the `wakeup` case's messages, one 32 B message per millisecond, its age on arrival, to a
receiver that polls. 2026-10-25 at main 2cafb052, 7 rounds of 2 s each on WSL, 18 on Windows, where the sleeping
receiver's p50 was 28-44 us in every round (on WSL 57-60 us in every round); the medians over the rounds:

| Receiver | Windows p50 | Windows p99 | WSL p50 | WSL p99 |
|---|---|---|---|---|
| waits in `fipc_recv` (`wakeup`) | 30.3 us | 140 us | 58.5 us | 143 us |
| polls with timeout 0 (`wakeup-poll`) | 217 ns | 8.4 us | 222 ns | 18.7 us |

- **The p99 is the OS's.** A polling loop that timed its own calls lost 1.1-1.9% of its time in gaps over a
  microsecond (interrupts, the scheduler), and a message sent during one waits for the thread to run again. A sparse
  message lands in such a gap about as often: the p99. Keeping interrupts and other threads off the polling core is
  the usual remedy.

## The suite

`zig-out/bench/fipc_bench` (built by every `zig build`, always ReleaseFast) runs each case in two processes, itself
(the server: it creates the stream, accepts, and measures) and a peer (the same executable, the client: it connects),
connected through the library's public C API, `include/fipc.h`. It loads the library at run time (`--lib`, by
default this tree's ReleaseFast build installed next to it), so one binary measures any build that speaks that API:
this tree's, or another revision's.

| Scenario | What | Cases (message size, ring) |
|---|---|---|
| `fastipc` | copy: `fipc_send` / `fipc_recv` | 16 B\*, 64 B\*, 256 B, 1 KiB\*, 64 KiB\* (512 KiB ring); 512 KiB (2 MiB ring); 1.5 MiB = 3 rings\* |
| `fastipc-zerocopy` | zero-copy: `fipc_send_acquire` + `fipc_send_commit` / `fipc_recv_acquire` + `fipc_recv_release`; a message of several pieces through the copying calls | as `fastipc` |
| `latency-spin` | round trip, the peer replies at once\* | 32 B (512 KiB ring) |
| `latency-sleep` | round trip, the peer thinks 500 us first, so this side is asleep when the reply comes\* | 32 B |
| `wakeup` | one message per millisecond to a receiver that sleeps in between\* | 32 B |
| `wakeup-poll` | the same messages to a receiver that calls `fipc_recv` with timeout 0 in a loop | 32 B |
| `setup` | a connection's life: create the stream, connect, accept, both close, close the listener\* | 1 MiB ring |
| `broadcast-1`, `broadcast-4` | a broadcast's publisher (`fipc_send`) to 1 or 4 lossy subscriber processes (`fipc_recv`): the publisher's rate, the slowest subscriber's, the most any one missed | 16 B, 64 B, 1 KiB (512 KiB broadcast) |
| `broadcast-gated-1`, `broadcast-gated-4` | the same with gated subscribers (`fipc_recv_acquire`, each message copied out): the publisher waits for the slowest | as `broadcast-1` |
| `broadcast-latency` | round trip over two broadcasts, one each way, each with one lossy subscriber; the peer replies at once | 32 B |
| `queue-1x1`, `queue-4x4`, `queue-1x8`, `queue-8x8` | a work queue's producer threads (`fipc_send`) and consumer threads (`fipc_recv`), as many as the name says, each a joined member: the messages taken per second (to the last of each run) and that rate per producer | 16 B, 64 B, 1 KiB (512 KiB ring per producer) |
| `queue-1x1-zerocopy` | `queue-1x1` with `fipc_send_acquire` + `fipc_send_commit` / `fipc_recv_acquire` + `fipc_recv_release` (each message copied out) | as `queue-1x1` |
| `service-1x1`, `service-4x4`, `service-1x4`, `service-2x4` | a service's client threads (`fipc_send` a request, `fipc_recv` its reply) and server threads (`fipc_recv`, then `fipc_send` the request's bytes back), as many as the name says (clients x servers), each a joined member; one request in flight per client, as a binding's `call` makes it: the replies received per second (to the last of each run) and that rate per client | 16 B, 64 B, 1 KiB requests and replies (the service's max_message; 512 KiB ring per client) |
| `service-1x1-pipelined`, `service-8x1-pipelined` | the same with 64 requests in flight per client: a client sends while fewer than 64 await their replies, and receives otherwise | as `service-1x1` |
| `service-1x1-pipelined-zerocopy` | `service-1x1-pipelined` with `fipc_send_acquire` + `fipc_send_commit` / `fipc_recv_acquire` + `fipc_recv_release` (each reply copied out); the server replies from the request in place | as `service-1x1` |

\* A case of the performance gates (`fipc_bench gates`, `devtool bench-compare --ab`'s default).

## Method

- **Throughput:** the sender warms up (0.2 s), measures its rate and announces a plan: 5 runs of as many messages as
  fill about 1 s each. The receiver times each run as one window; the windows follow each other, so every run
  measures steady traffic. Every message's length is checked; payloads are 'x' bytes; zero-copy receivers copy each
  message out (`--no-touch`: they don't).
- **Latency:** after the warm-up, 5 runs of 1 s of samples each; a ping-pong sample is the round trip minus the
  peer's hold time (the reply carries it), a wake-up sample the message's age on arrival (the sender stamps it).
  p50, p99 and p99.9 per run.
- **Setup:** after the warm-up, 5 runs of 1 s of cycles; a cycle is a connection's life (the server creates the
  stream, tells the client to connect through a second, long-lived connection, accepts, closes it and closes the
  listener; the client closes its end and confirms), from one confirmation to the next. Cycles per second, and the
  cycle time's p50 and p99.
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
  no process can lift its own: both warn of it, so start them from a foreground terminal. Every Zig-suite number here
  ran on the performance cores. Left alone, the Mac also goes to sleep, mid-benchmark, for about 15 minutes at a time,
  and a run's clock goes on (one copy 16 B run read 2.1K/s): devtool's bench commands hold `caffeinate -i` while they
  run, and anything else run by hand for long needs it too.
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
- WSL2 Ubuntu 24.04.3 LTS: kernel 6.18.54.1-microsoft-standard-WSL2, glibc 2.39, 20 vCPUs, 15 GiB; clock source
  `tsc` (`constant_tsc`, `nonstop_tsc`).
- Zig 0.17.0; every library ReleaseFast for x86-64-v3.
- The macOS column: a MacBook Air (M1: 4 performance and 4 efficiency cores, 8 GB RAM), macOS 26.5.1, on power; Zig
  0.17.0, ReleaseFast for `apple_m1`.
- The Linux VM column: the same MacBook Air, Lima 2.2.1 (Apple Virtualization framework): Ubuntu 24.04.5 arm64, kernel
  6.8.0-147-generic, glibc 2.39, 4 vCPUs, 4 GiB, clock source `arch_sys_counter`; Zig 0.17.0, ReleaseFast.
- Running alongside, idle: IDEs, a browser and a few background processes; 5-15% background CPU load on Windows when
  sampled. Nothing else runs benchmarks or tests; Windows and WSL run one after the other, never together.
- `AUTO_AFFINITY` unset (no CPU pinning); `LOG_LEVEL` unset.

## Commands

From the repository root (Windows: Git Bash; Linux: a shell in a Linux checkout), after `python devtool.py build`:

```bash
zig-out/bench/fipc_bench all --runs 5 --json run.jsonl       # every scenario, this tree's library
python devtool.py bench-compare --ab                          # the gate cases: this tree vs main (--ref REV)
python devtool.py bench-compare --ab zig-queue-1x1 zig-fastipc # named scenarios, a broadcast's, a queue's or a service's too
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
