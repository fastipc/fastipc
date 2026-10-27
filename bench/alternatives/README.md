# fastipc against other IPC libraries

The same cases, run through fastipc and through other inter-process communication libraries. The website's
[performance page](../../website/performance.html) and its overview show the results; this folder makes them.

| Library | Version | Through | Has |
|---|---|---|---|
| fastipc | this checkout | its C API, `fipc.h` (copying calls; `fastipc-zerocopy`: the zero-copy ones) | stream, broadcast, queue, service |
| [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) | 0.10.0 | its Rust API (`rust/`); `iceoryx2-cxx`: its C++ binding | stream, broadcast, service with one server |
| [Aeron](https://github.com/aeron-io/aeron) | 1.53.3 | its C client over `aeron:ipc`, with the C media driver | stream, broadcast (lossless) |
| [ZeroMQ](https://github.com/zeromq/libzmq) (libzmq) | 4.3.5 | `ipc://` (`zeromq-tcp`: TCP on loopback) | stream, broadcast (lossy), queue, service |
| [NNG](https://github.com/nanomsg/nng) | 2.0.0 | `ipc://`, on Windows `winpipe://` (`nng-tcp`: TCP on loopback) | stream, broadcast (lossy), queue, service |
| [cpp-ipc](https://github.com/mutouyun/cpp-ipc) | 1.4.1 | `ipc::route` | stream, broadcast (lossless) |
| [Boost.Interprocess](https://www.boost.org/doc/libs/release/doc/html/interprocess.html) | 1.92.0 | `message_queue` | stream, queue |
| Unix domain socket, TCP on loopback | the OS's | a 4-byte length before each message | stream |

## The cases

Every role runs in a process of its own: a stream's sender and receiver, a broadcast's publisher and each of its
subscribers, a work queue's producers and consumers, a service's clients and servers.

- **stream-N**: one-way throughput, one sender to one receiver, N-byte messages (16 B, 1 KiB, 64 KiB).
- **rtt-N**: round trips over a stream, the peer replying to each message at once; the round trip's p50 and p99.
- **broadcast-S-lossless-N** / **-lossy-N**: one publisher to S subscribers (1, 4). Lossless: a subscriber that falls
  behind holds the publisher back. Lossy: it loses messages instead. The rate is the slowest subscriber's, and the loss
  is the largest share any subscriber lost.
- **queue-PxC-N**: P producers and C consumers (1x1, 4x4), each message to one consumer; the rate of all consumers.
- **service-CxS-N**: C clients and S servers (1x1, 4x1, 4x4), each client with one request in flight; calls per second
  of all clients, and the call's p50 and p99. A library whose requests go to every server (iceoryx2) has only S = 1.

Each run sends for 0.3 s of warm-up, then measures 1 s; a case's figure is the median of its runs (3 by default).

## The same work for every library

- A sender starts with the message in a buffer of its own, and a receiver ends with it in memory of its own: a
  library that copies (fastipc's copying calls, ZeroMQ, NNG, Boost, sockets) receives into the receiver's buffer, and
  one that lends shared memory (iceoryx2, Aeron, cpp-ipc, fastipc's zero-copy calls) has the receiver copy the message
  out of it. Every message's length is checked.
- **Each library runs at its fastest configuration on each platform.** A platform is an operating system on an
  architecture (Windows x86-64, Linux x86-64, macOS on Apple Silicon, Linux ARM64), so two platforms on one machine
  can differ. The configuration is each library's buffer per pattern, at most 32 MiB for every library, Aeron's
  conductor idle strategy (`spin`, or `backoff`, Aeron's default; one per platform, as the media driver serves every
  pattern). These are `run.py`'s `BUFFERS` and `AERON_CONDUCTOR`, keyed by platform, chosen by `sweep.py choose` from
  that platform's sweeps in `sweeps/` (64 KiB, 512 KiB, 4 MiB and 32 MiB per case, and Aeron's conductors and fragment
  limits, a sweep each; the best geometric mean over a pattern's cases, a tie going to the smaller buffer, then Aeron's
  default conductor). Every result records its library's buffer
  (`buffer_bytes`) and Aeron's its conductor and fragment limit (`fragment_limit`), and the page shows them. A library or pattern missing there runs at its
  usual size: 512 KiB of messages where a library counts them (8 to 1024 messages), fastipc's rings of 512 KiB,
  ZeroMQ's high-water marks of 1000 messages, Aeron's default term length (64 MiB: the publisher up to 32 MiB ahead).
  Every other option is the library's default. `run.py --buffer <bytes>` gives each library that takes a size that
  much buffering instead: fastipc's rings of that capacity; Aeron a term of twice it (its window is half a term), a
  power of two of 64 KiB to 1 GiB that holds at least 8 messages; iceoryx2's, Boost's and NNG's depth and ZeroMQ's
  high-water marks (`ZMQ_SNDHWM`, `ZMQ_RCVHWM`) the buffer's bytes of messages, at least 8 (NNG takes at most 8192).
  cpp-ipc's queue is fixed and the sockets' buffers are the OS's. A size a library refuses is skipped and recorded;
  every result records its library's buffer (`buffer_bytes`).
- A receiver either waits in the library's receive (with a timeout, to notice the end of the run), or polls it in a
  loop (`--poll`). iceoryx2's and Aeron's receives never block, so theirs always poll. The results show each library
  in the faster of the two.
- **Every receive returns one message.** Aeron's subscription poll can take several (its fragment limit: the most
  fragments one poll takes; a message up to the MTU is one fragment), which no other library's receive does, so the
  comparison runs it with a limit of 1 (`AERON_FRAGMENT_LIMIT`) and measures each library's synchronization per
  message; `--aeron-fragment-limit <n>` sets another limit. run.py passes the limit to alt_bench as
  `ALT_AERON_FRAGMENT_LIMIT`. Both libraries batching is a comparison of its own (below).
- Timing is on the CPU's counter (the TSC on x86-64, the generic timer on ARM64, on macOS through
  `mach_absolute_time`), read once per 64 messages; one round trip in 16 is sampled, chosen at random. The generic timer ticks
  at 24 MHz on Apple silicon, so a round trip there is measured in steps of 42 ns. No process is pinned to a core.

## The batched comparison

FastIPC and Aeron also have calls that move several messages at once, so `run.py --batch N` runs the two of them with
both ends batching N messages per call, beside the comparison (never in it):

- **FastIPC:** the sender calls `fipc_send_many` with N messages (published together, with at most one wake of the
  receiver), and the receiver `fipc_recv_many` with room for N: it waits for the first, then takes every further
  message already there, up to N, into one buffer.
- **Aeron:** the sender offers N messages as one block, `aeron_exclusive_publication_offer_block`, of frames it
  formats from the publication's constants (one copy into the term, one store of its tail; a block that doesn't fit
  the rest of the term goes as N single offers), and the receiver's subscription poll takes up to N fragments (its
  fragment limit is N).

The cases are those where both libraries batch at both ends: `stream-16`, `stream-1024`, `broadcast-1-lossless-16` and
`broadcast-4-lossless-16` (`BATCHED_CASES`; a round trip has one message at a time to send). Each library runs at its
configuration of the comparison (`BUFFERS`, `AERON_CONDUCTOR`), the receive waiting and polling as there (Aeron's
always polls), both libraries in the same run. The results are series of their own, `results/<platform>-batched.json`
and `-batched-poll.json` for N = 16 (the page's figures), `-batched-64.json` and `-batched-64-poll.json` for N = 64;
every result records its batch (`batch`), and a stream's receiver its sequence gaps (`lost`, always 0). Batch both
ends: a batching sender whose receiver takes one message per call runs slower than without batches, as the receiver
falls behind and the ring stays full.

## Running it

```bash
python bench/alternatives/deps.py          # fetches and builds the libraries into build/deps/<os> (git, CMake 3.30+,
                                           #   a C/C++ compiler, cargo); CMAKE=<path> selects a CMake
zig build -Doptimize=ReleaseFast           # fastipc, from the repository's root
cmake -S bench/alternatives -B bench/alternatives/build/cmake -DCMAKE_BUILD_TYPE=Release   # Windows: -G "Visual Studio 17 2022"
cmake --build bench/alternatives/build/cmake --config Release
python bench/alternatives/run.py --out results.json          # every case, every library
python bench/alternatives/run.py --poll --out results-poll.json
python bench/alternatives/run.py --libs fastipc,aeron --cases stream,rtt --runs 5
python bench/alternatives/run.py --buffer 4194304 --cases stream   # every library with 4 MiB of buffering
python bench/alternatives/sweep.py run      # the sweep: sizes alternating round by round, into sweeps/<platform>.json
python bench/alternatives/sweep.py run --libs aeron --aeron-conductor backoff --out bench/alternatives/sweeps/<platform>-aeron-backoff.json
python bench/alternatives/sweep.py run --libs aeron --aeron-conductor backoff --aeron-fragment-limit 1 --out bench/alternatives/sweeps/<platform>-aeron-backoff-f1.json
python bench/alternatives/sweep.py choose sweeps/<platform>.json sweeps/<platform>-aeron-*.json   # the platform's configuration
python bench/alternatives/merge.py windows run.json run.log   # a run of some cases into results/windows.*
python bench/alternatives/run.py --batch 16 --out batched.json | tee batched.log   # the batched comparison
python bench/alternatives/run.py --batch 16 --poll --out batched-poll.json | tee batched-poll.log
python bench/alternatives/merge.py windows-batched batched.json batched.log   # into results/windows-batched.*
python bench/alternatives/merge.py windows-batched-poll batched-poll.json batched-poll.log
python bench/alternatives/page.py          # the website's charts and tables, from results/
```

alt_bench loads fastipc from the checkout it was built in (its rpath; on Windows the copy next to it), and run.py
points it at its own checkout's `zig-out/lib` (`LD_LIBRARY_PATH`; on macOS `DYLD_LIBRARY_PATH`, which run.py sets
itself, as a system program such as `caffeinate` drops it from the environment it passes on). On Windows a Rust build in a deep folder can fail on paths over 260 characters: set `CARGO_TARGET_DIR` to a short
folder for `deps.py`. `alt_bench <lib> <pattern> <role> ...` runs one role by hand (`src/main.cpp` describes its
command line and control lines).
