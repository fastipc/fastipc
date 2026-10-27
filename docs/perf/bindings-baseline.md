# Bindings baseline

Reference medians of the C, C++, Zig, C#, Java, Python, Rust, Lua, JavaScript and Go benchmarks (`bench/c`, `bench/zig-api`,
`bench/csharp/FastIpc.Bench`, `bench/java`, `bench/python/fipc_bench.py`, `bench/rust`, `bench/lua/fipc_bench.lua`,
`bench/js/fipc_bench.mjs`, `bench/go`) on the baseline machine ([`baseline.md`](baseline.md), "Machine"): release
builds, 5 runs round-robin with the stream pace and the warm-up below, messages per second, median [min – max]. The
Windows and Linux columns: the night of 2026-10-24 to 25 at main 2cafb052, Windows and then WSL, each benchmark built
and run once untimed, then 5 runs round-robin with a rotating start (every benchmark loads `zig-out/bench`'s
ReleaseFast library: C# checked on Linux with `LD_DEBUG=libs`), JavaScript on Node.js 22, Go 1.27.1, no other
benchmark running. The runtimes, exactly: on Windows CPython 3.13.5, .NET 9.0.6, Temurin JDK 25.0.4.1, Node.js 22.16.0, Rust 1.99.0, Go 1.27.1, LuaJIT 2.1 (the v2.1 branch), C and C++ by Zig 0.17.0's Clang 22.1.8; on Linux CPython 3.12.3, .NET 9.0.9, Temurin JDK 25.0.4.1, Node.js 22.20.0, the same Rust, Go, LuaJIT and Zig. Against 2026-10-24 (main 54e8d460) 102 of 120 Windows medians and 114 of 120 Linux medians moved
within 10%. On Windows most rows read a few percent lower, as did the reference library of the Zig suite's A/B in
the same session ([`baseline.md`](baseline.md), "The numbers": 4abedadc's copy 64 KiB 435K against 449K the session
before); the Zig module's zero-copy 16 B fell most (0.79: 39.3M against 49.6M, its runs 38.2-41.0M), and Python's
zero-copy 512 KiB, a bimodal case, reads 8.6K against 6.6K (its runs 7.1K-15.5K). On Linux C's zero-copy 16 B reads
0.87 times the previous median and its 64 B 1.11, C#'s copied 256 B 0.86. The
macOS\* and Linux VM&dagger; tables further down: the same night at the same commit, after WSL, macOS and then the VM. `python devtool.py bench-compare <benchmark ...>` (without `--ab`) runs the benchmarks
and fails a case whose median falls below 95% of its row; the table's rows are what it reads.

The 16-256 B cases send 2,000,000 messages (1,000,000 at 256 B) so that a native case runs for tens of milliseconds,
several Windows scheduler quanta (about 16 ms each): a case that ends within one quantum measures the scheduler, not the
messages. The numbers are still rough, so a failing case is a hint, not a verdict: scheduling noise is large
(±20-30%). On Windows, devtool turns power throttling off for every process a benchmark starts (started from a
background process otherwise, a case can read half as fast). Compare WSL numbers only with WSL numbers. For a change
to the library, `bench-compare --ab` (the Zig suite) is the gate.

Every benchmark warms up: each case first sends messages untimed, for at least 0.2 s (0.5 s on the JITs: C#, Java,
LuaJIT and Node.js) and at least its count, with a start marker every 1,000 messages (so that the last one takes no
path a JIT hasn't seen), then a last start marker, its count and the end marker; the server times from the last start
marker to the end marker. A start marker is the 3-byte message `GO!`. Without the warm-up a benchmark times a fresh
process's first messages: on the M1 the first case of a newly written executable runs at about half speed (macOS's
first run of a new file; `cargo run` re-links the Rust benchmark on every run, so its 16 B rows read 31-34M against
C's 56-60M). Every row below is measured with the warm-up.

The benchmarks:

- **C and C++** (`bench/c`): one harness with two sets of the same loops, through the C API (`c_bench`, C11) and
  through the C++ wrapper `include/fipc.hpp` (`cpp_bench`, C++20), both built ReleaseFast by `zig build` against the
  release library and installed with it in `zig-out/bench`, whatever the build's mode. Their medians agree within the
  noise: the wrapper adds nothing measurable to a message's trip.
- **C#:** runs on .NET 9 through the binding's raw layer (`Fipc`).
- **Java:** runs on JDK 25 through the binding's API, receiving into a native segment it reuses.
- **Rust:** runs through the binding's safe API (`receive_into` a buffer it reuses, `send_acquire` /
  `receive_acquire` slots), built with Cargo's release profile.
- **Lua:** runs on LuaJIT 2.1 through the binding (`recv_into` a buffer it reuses, `send_acquire` / `recv_acquire`
  pointers into the ring).
- **JavaScript:** runs on Node.js 22 through the binding's async calls, each awaited (`receiveInto` a buffer it
  reuses, `sendAcquire` / `receiveAcquire` views copied out). A message that is there is taken on the JavaScript
  thread, so these measure the binding's per-call cost (a Promise, and for zero-copy the object it makes), not a
  hand-off to a thread; `--sync` measures the `Sync` calls (about twice the copy rates).
- **Go:** runs on Go 1.27.1 through the binding, built with `CGO_ENABLED=0` (`ReceiveInto` a buffer it reuses,
  `SendAcquire` / `ReceiveAcquire` slices into the ring, copied out).

  Each Go call goes into the library through purego and a mutex of its direction, which costs more than a short
  message's copy: copied 16 B messages run at about a quarter of Rust's rate on Windows and a fifth of it on Linux,
  and zero-copy, two calls per message on each side where a copy takes one, runs slower than copying up to 256 B.
  From 64 KiB on, Go runs as fast as the others.

| Bench | Test | Windows median [min – max] | Linux median [min – max] |
|---|---|---|---|
| c-fastipc | 1. Tiny messages (16B) | 50.14M [46.01M – 51.72M] | 50.04M [32.85M – 51.91M] |
| c-fastipc | 2. Small messages (64B) | 46.33M [43.19M – 47.49M] | 42.82M [34.04M – 45.07M] |
| c-fastipc | 3. Medium messages (256B) | 39.83M [36.41M – 41.41M] | 42.09M [32.54M – 42.95M] |
| c-fastipc | 4. Large messages (64KB) | 447.7K [407.9K – 459.6K] | 459.2K [434.8K – 464.1K] |
| c-fastipc | 5. Large messages (512KB) | 53.0K [51.2K – 61.9K] | 54.8K [52.8K – 58.7K] |
| c-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.8K [12.8K – 14.5K] | 14.6K [13.8K – 14.9K] |
| c-fastipc-zerocopy | 1. Tiny messages (16B) | 44.22M [37.56M – 46.94M] | 46.39M [44.25M – 48.58M] |
| c-fastipc-zerocopy | 2. Small messages (64B) | 45.74M [42.23M – 47.37M] | 46.77M [42.57M – 48.13M] |
| c-fastipc-zerocopy | 3. Medium messages (256B) | 42.79M [41.16M – 43.39M] | 44.69M [42.76M – 46.49M] |
| c-fastipc-zerocopy | 4. Large messages (64KB) | 446.2K [441.0K – 511.6K] | 394.8K [364.5K – 417.8K] |
| c-fastipc-zerocopy | 5. Large messages (512KB) | 56.6K [51.6K – 60.5K] | 53.4K [53.2K – 55.1K] |
| c-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.6K [11.3K – 14.6K] | 14.6K [12.2K – 14.6K] |
| cpp-fastipc | 1. Tiny messages (16B) | 44.55M [37.95M – 44.94M] | 41.41M [39.09M – 44.62M] |
| cpp-fastipc | 2. Small messages (64B) | 41.66M [38.61M – 42.69M] | 38.97M [35.57M – 42.79M] |
| cpp-fastipc | 3. Medium messages (256B) | 37.09M [36.66M – 38.55M] | 39.98M [36.79M – 40.73M] |
| cpp-fastipc | 4. Large messages (64KB) | 403.8K [367.5K – 447.8K] | 437.1K [424.6K – 465.9K] |
| cpp-fastipc | 5. Large messages (512KB) | 53.7K [50.0K – 62.4K] | 54.5K [53.3K – 56.0K] |
| cpp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.0K [12.8K – 14.4K] | 14.1K [13.5K – 14.7K] |
| cpp-fastipc-zerocopy | 1. Tiny messages (16B) | 41.32M [38.83M – 44.61M] | 42.33M [40.56M – 45.02M] |
| cpp-fastipc-zerocopy | 2. Small messages (64B) | 37.96M [36.52M – 40.75M] | 38.33M [36.32M – 41.30M] |
| cpp-fastipc-zerocopy | 3. Medium messages (256B) | 35.83M [30.56M – 39.55M] | 42.57M [37.44M – 43.52M] |
| cpp-fastipc-zerocopy | 4. Large messages (64KB) | 432.6K [394.4K – 479.5K] | 410.5K [388.8K – 433.1K] |
| cpp-fastipc-zerocopy | 5. Large messages (512KB) | 54.1K [45.2K – 60.1K] | 53.1K [46.2K – 54.6K] |
| cpp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.6K [11.7K – 14.7K] | 14.0K [13.2K – 14.4K] |
| zigapi-fastipc | 1. Tiny messages (16B) | 44.58M [42.47M – 46.40M] | 41.82M [41.07M – 44.26M] |
| zigapi-fastipc | 2. Small messages (64B) | 41.87M [39.98M – 43.78M] | 39.73M [37.42M – 40.74M] |
| zigapi-fastipc | 3. Medium messages (256B) | 38.73M [35.35M – 39.45M] | 39.63M [38.17M – 39.99M] |
| zigapi-fastipc | 4. Large messages (64KB) | 427.3K [412.1K – 456.7K] | 439.4K [382.6K – 462.8K] |
| zigapi-fastipc | 5. Large messages (512KB) | 51.3K [46.8K – 59.9K] | 55.6K [54.3K – 56.4K] |
| zigapi-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.8K [11.9K – 14.4K] | 14.5K [14.1K – 14.7K] |
| zigapi-fastipc-zerocopy | 1. Tiny messages (16B) | 39.34M [38.20M – 41.01M] | 47.15M [42.01M – 59.98M] |
| zigapi-fastipc-zerocopy | 2. Small messages (64B) | 47.99M [45.06M – 49.14M] | 47.56M [41.78M – 50.02M] |
| zigapi-fastipc-zerocopy | 3. Medium messages (256B) | 42.57M [38.89M – 46.22M] | 47.30M [40.78M – 48.32M] |
| zigapi-fastipc-zerocopy | 4. Large messages (64KB) | 441.3K [383.1K – 472.0K] | 460.5K [439.1K – 466.6K] |
| zigapi-fastipc-zerocopy | 5. Large messages (512KB) | 53.7K [49.9K – 60.3K] | 54.9K [51.0K – 57.1K] |
| zigapi-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.5K [12.6K – 14.6K] | 14.5K [12.6K – 14.6K] |
| csharp-fastipc | 1. Tiny messages (16B) | 30.85M [30.38M – 34.08M] | 37.42M [33.52M – 38.17M] |
| csharp-fastipc | 2. Small messages (64B) | 37.43M [36.06M – 39.09M] | 43.52M [29.73M – 45.73M] |
| csharp-fastipc | 3. Medium messages (256B) | 31.70M [30.71M – 32.20M] | 33.24M [32.20M – 38.24M] |
| csharp-fastipc | 4. Large messages (64KB) | 444.7K [422.2K – 472.9K] | 462.3K [433.9K – 467.9K] |
| csharp-fastipc | 5. Large messages (512KB) | 52.1K [48.0K – 58.3K] | 57.0K [56.7K – 60.2K] |
| csharp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.7K [11.9K – 14.5K] | 14.8K [10.2K – 15.3K] |
| csharp-fastipc-zerocopy | 1. Tiny messages (16B) | 37.48M [35.93M – 38.15M] | 31.76M [28.84M – 32.71M] |
| csharp-fastipc-zerocopy | 2. Small messages (64B) | 32.03M [29.92M – 33.44M] | 30.27M [29.14M – 30.75M] |
| csharp-fastipc-zerocopy | 3. Medium messages (256B) | 28.80M [26.84M – 29.50M] | 27.50M [26.55M – 27.68M] |
| csharp-fastipc-zerocopy | 4. Large messages (64KB) | 400.5K [378.1K – 422.5K] | 396.5K [391.0K – 420.2K] |
| csharp-fastipc-zerocopy | 5. Large messages (512KB) | 55.7K [38.8K – 58.9K] | 51.3K [48.3K – 56.2K] |
| csharp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.7K [12.4K – 16.0K] | 14.3K [14.1K – 15.5K] |
| java-fastipc | 1. Tiny messages (16B) | 14.90M [11.14M – 15.63M] | 13.70M [12.78M – 14.36M] |
| java-fastipc | 2. Small messages (64B) | 16.27M [12.09M – 16.62M] | 17.20M [15.14M – 17.55M] |
| java-fastipc | 3. Medium messages (256B) | 14.59M [13.92M – 16.12M] | 15.00M [14.07M – 16.73M] |
| java-fastipc | 4. Large messages (64KB) | 421.2K [394.8K – 450.8K] | 432.4K [386.9K – 451.8K] |
| java-fastipc | 5. Large messages (512KB) | 54.5K [48.4K – 58.1K] | 58.7K [50.6K – 63.5K] |
| java-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 12.8K [8.4K – 14.2K] | 14.3K [12.3K – 15.3K] |
| java-fastipc-zerocopy | 1. Tiny messages (16B) | 13.03M [12.09M – 13.79M] | 14.12M [12.45M – 14.58M] |
| java-fastipc-zerocopy | 2. Small messages (64B) | 11.06M [10.41M – 12.41M] | 12.65M [11.98M – 12.76M] |
| java-fastipc-zerocopy | 3. Medium messages (256B) | 9.96M [9.78M – 10.54M] | 13.75M [13.37M – 14.05M] |
| java-fastipc-zerocopy | 4. Large messages (64KB) | 395.4K [348.2K – 413.4K] | 436.1K [391.4K – 469.1K] |
| java-fastipc-zerocopy | 5. Large messages (512KB) | 50.4K [46.8K – 53.2K] | 54.7K [49.9K – 58.3K] |
| java-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 12.9K [11.8K – 14.2K] | 14.1K [12.7K – 15.0K] |
| lua-fastipc | 1. Tiny messages (16B) | 50.92M [39.22M – 52.57M] | 49.24M [46.54M – 50.55M] |
| lua-fastipc | 2. Small messages (64B) | 46.16M [44.60M – 48.16M] | 45.32M [43.55M – 45.74M] |
| lua-fastipc | 3. Medium messages (256B) | 40.56M [39.25M – 43.45M] | 42.67M [31.10M – 44.18M] |
| lua-fastipc | 4. Large messages (64KB) | 397.0K [329.8K – 416.4K] | 427.7K [389.4K – 437.1K] |
| lua-fastipc | 5. Large messages (512KB) | 57.1K [48.4K – 59.8K] | 53.7K [52.5K – 57.2K] |
| lua-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 12.8K [12.0K – 14.3K] | 13.8K [13.2K – 14.4K] |
| lua-fastipc-zerocopy | 1. Tiny messages (16B) | 51.32M [47.37M – 55.93M] | 57.01M [47.03M – 57.97M] |
| lua-fastipc-zerocopy | 2. Small messages (64B) | 46.51M [42.32M – 48.97M] | 50.50M [50.41M – 55.48M] |
| lua-fastipc-zerocopy | 3. Medium messages (256B) | 41.19M [37.77M – 42.02M] | 44.88M [38.25M – 46.61M] |
| lua-fastipc-zerocopy | 4. Large messages (64KB) | 398.1K [362.8K – 410.7K] | 382.9K [376.1K – 389.6K] |
| lua-fastipc-zerocopy | 5. Large messages (512KB) | 20.1K [19.6K – 23.1K] | 52.8K [51.0K – 54.5K] |
| lua-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.2K [10.4K – 14.3K] | 14.2K [13.1K – 14.5K] |
| js-fastipc | 1. Tiny messages (16B) | 2.10M [2.02M – 2.14M] | 2.45M [2.35M – 2.46M] |
| js-fastipc | 2. Small messages (64B) | 2.00M [1.93M – 2.15M] | 2.36M [2.28M – 2.39M] |
| js-fastipc | 3. Medium messages (256B) | 2.06M [1.97M – 2.17M] | 2.34M [2.17M – 2.43M] |
| js-fastipc | 4. Large messages (64KB) | 354.1K [323.6K – 368.0K] | 361.2K [352.1K – 392.5K] |
| js-fastipc | 5. Large messages (512KB) | 57.3K [53.0K – 59.4K] | 54.0K [52.2K – 57.1K] |
| js-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 10.3K [5.8K – 11.3K] | 9.0K [8.4K – 9.7K] |
| js-fastipc-zerocopy | 1. Tiny messages (16B) | 552.0K [491.9K – 569.2K] | 648.4K [629.2K – 659.6K] |
| js-fastipc-zerocopy | 2. Small messages (64B) | 549.1K [517.3K – 609.7K] | 656.3K [643.1K – 663.1K] |
| js-fastipc-zerocopy | 3. Medium messages (256B) | 558.4K [535.4K – 587.4K] | 661.4K [652.6K – 672.5K] |
| js-fastipc-zerocopy | 4. Large messages (64KB) | 244.1K [238.0K – 293.1K] | 223.0K [213.1K – 253.9K] |
| js-fastipc-zerocopy | 5. Large messages (512KB) | 49.6K [37.4K – 56.5K] | 44.7K [42.0K – 54.2K] |
| js-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 7.9K [5.7K – 10.5K] | 6.8K [5.5K – 7.3K] |
| go-fastipc | 1. Tiny messages (16B) | 10.49M [10.24M – 11.08M] | 8.76M [8.61M – 8.85M] |
| go-fastipc | 2. Small messages (64B) | 10.61M [10.38M – 11.45M] | 8.15M [7.98M – 8.36M] |
| go-fastipc | 3. Medium messages (256B) | 9.69M [9.10M – 9.89M] | 8.27M [7.58M – 8.57M] |
| go-fastipc | 4. Large messages (64KB) | 446.7K [382.0K – 598.5K] | 416.5K [397.9K – 470.9K] |
| go-fastipc | 5. Large messages (512KB) | 55.3K [50.0K – 60.3K] | 54.1K [49.9K – 54.7K] |
| go-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.7K [16.0K – 17.9K] | 13.4K [11.7K – 15.1K] |
| go-fastipc-zerocopy | 1. Tiny messages (16B) | 7.44M [6.80M – 7.90M] | 5.47M [5.42M – 5.49M] |
| go-fastipc-zerocopy | 2. Small messages (64B) | 6.86M [6.29M – 7.24M] | 5.17M [5.03M – 5.40M] |
| go-fastipc-zerocopy | 3. Medium messages (256B) | 7.20M [6.25M – 7.55M] | 5.29M [5.16M – 5.37M] |
| go-fastipc-zerocopy | 4. Large messages (64KB) | 426.7K [416.3K – 454.8K] | 387.2K [384.0K – 397.4K] |
| go-fastipc-zerocopy | 5. Large messages (512KB) | 52.0K [47.8K – 62.3K] | 51.7K [50.2K – 53.7K] |
| go-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.9K [16.5K – 19.3K] | 13.3K [12.4K – 15.0K] |
| python-fastipc | 1. Tiny messages (16B) | 1.47M [1.44M – 1.49M] | 1.23M [1.14M – 1.23M] |
| python-fastipc | 2. Small messages (64B) | 1.39M [1.32M – 1.47M] | 1.21M [1.17M – 1.22M] |
| python-fastipc | 3. Medium messages (256B) | 1.41M [1.36M – 1.48M] | 1.21M [1.13M – 1.22M] |
| python-fastipc | 4. Large messages (64KB) | 341.9K [314.0K – 356.5K] | 323.0K [248.1K – 329.4K] |
| python-fastipc | 5. Large messages (512KB) | 50.0K [41.0K – 56.2K] | 51.5K [49.5K – 55.1K] |
| python-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.5K [12.6K – 14.6K] | 13.6K [12.4K – 13.8K] |
| python-fastipc-zerocopy | 1. Tiny messages (16B) | 728.9K [711.0K – 758.2K] | 689.5K [656.0K – 697.9K] |
| python-fastipc-zerocopy | 2. Small messages (64B) | 722.9K [720.6K – 747.5K] | 685.8K [661.1K – 703.1K] |
| python-fastipc-zerocopy | 3. Medium messages (256B) | 695.6K [672.6K – 737.0K] | 671.1K [654.4K – 681.7K] |
| python-fastipc-zerocopy | 4. Large messages (64KB) | 198.3K [190.5K – 202.8K] | 198.0K [192.2K – 200.1K] |
| python-fastipc-zerocopy | 5. Large messages (512KB) | 8.6K [7.1K – 15.5K] | 28.5K [27.0K – 30.6K] |
| python-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.2K [11.5K – 13.7K] | 11.7K [11.2K – 12.5K] |
| rust-fastipc | 1. Tiny messages (16B) | 40.57M [40.03M – 42.84M] | 42.35M [40.69M – 44.12M] |
| rust-fastipc | 2. Small messages (64B) | 38.51M [37.30M – 40.79M] | 40.88M [31.83M – 43.80M] |
| rust-fastipc | 3. Medium messages (256B) | 37.09M [34.04M – 37.55M] | 39.32M [38.53M – 40.26M] |
| rust-fastipc | 4. Large messages (64KB) | 434.8K [395.2K – 463.1K] | 457.7K [376.0K – 483.4K] |
| rust-fastipc | 5. Large messages (512KB) | 57.9K [54.7K – 61.0K] | 52.4K [48.4K – 56.7K] |
| rust-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.3K [11.4K – 14.1K] | 13.8K [11.3K – 15.7K] |
| rust-fastipc-zerocopy | 1. Tiny messages (16B) | 27.25M [27.04M – 28.04M] | 38.30M [34.55M – 39.83M] |
| rust-fastipc-zerocopy | 2. Small messages (64B) | 28.53M [26.43M – 29.52M] | 38.96M [33.25M – 40.25M] |
| rust-fastipc-zerocopy | 3. Medium messages (256B) | 19.50M [17.83M – 19.82M] | 39.71M [21.84M – 42.73M] |
| rust-fastipc-zerocopy | 4. Large messages (64KB) | 402.9K [391.8K – 407.2K] | 411.2K [392.5K – 435.6K] |
| rust-fastipc-zerocopy | 5. Large messages (512KB) | 54.4K [53.0K – 57.3K] | 53.5K [49.7K – 56.4K] |
| rust-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.7K [13.1K – 14.4K] | 14.4K [12.4K – 15.4K] |

\* macOS: the MacBook Air M1 of [`baseline.md`](baseline.md) ("Machine"), macOS 26.5.1, on AC power; different
hardware from the baseline machine. The same benchmarks with the warm-up, the night of 2026-10-24 to 25 at main
2cafb052, each built and run once untimed, then 5 runs round-robin with a rotating start, with the stream pace (every
benchmark loads `zig-out/bench`'s ReleaseFast library: C# checked with `DYLD_PRINT_LIBRARIES`), JavaScript on
Node.js 22.23.3, Go 1.27.1 (also CPython 3.13.16, .NET 9.0.20, Temurin JDK 25.0.4.1, Rust 1.99.0, LuaJIT 2.1, Zig 0.17.0), at a foreground QoS (devtool leaves macOS's background state: [`baseline.md`](baseline.md),
"Method"), held awake (`caffeinate -i`), the Linux VM stopped, no other benchmark running. A few single runs read far
low (the minimums of a few rows: C#'s copied 64 KiB, C++'s copied 512 KiB, Lua's zero-copy 512 KiB and 1 MiB, C's
zero-copy 1 MiB). Against 2026-10-24 (main 45b88f10) 68 of 120 medians moved within 10%. The 16-256 B rows of C, C++,
the Zig module, Rust and Lua run up to 3.4 times as fast (the Zig module's zero-copy 64 B 99.43M against 29.39M, C's
copied 64 B 87.97M against 48.71M, Lua's zero-copy 16 B 101.95M against 37.42M), those of C# and Java up to 1.4
times: the stream's arm64 changes since (the segment's ring lines and the endpoint's hot lines 128 bytes apart, each
side reading its own index from its endpoint, the stream pace of 512; [`baseline.md`](baseline.md), "The numbers").
JavaScript's rows read 1-26% higher, on Node.js 22 against 26.10.0 then; Python's and Go's 16 B-64 KiB within 11%.
Three rows read 12-14% lower: C#'s copied 64 KiB (one run at 192.3K), Lua's zero-copy and Go's copied 1 MiB (10
messages timed). On 2026-10-07 (main 645ab67), against a run of the same day without the warm-up, every
1 MiB row was 1.1-2.8 times as fast (only 10 messages are timed there), Rust's 16 B rows 1.6-2.2 times (the first case
of a newly written executable: see above), C#'s 64 KiB-1 MiB rows 1.25-2.8 times (its JIT, warmed up). JavaScript's
copied 16-256 B messages ran 0.8 times as fast (3.2-3.3M against 3.9-4.1M, each set of runs within 4%): its steady
state, after the warm-up, is slower than a fresh process's first 2 million messages, most likely the garbage
collector (not measured). The other rows moved within the noise (±20-30% at 16-256 B). Not a gate: `bench-compare`
reads only the table above.

The Zig benchmark (`bench/zig-api`, `zigapi-*`: the same loops through the native module `fastipc`, `Conn.recv` into
a buffer it reuses, `Conn.acquire` / `Conn.recvAcquire` slices copied out, built ReleaseFast by
`zig build` into `zig-out/bench`). Its macOS and Linux VM rows are measured with the other benchmarks'.

The C, C++ and Zig zero-copy receivers copy each message out of the ring into a buffer they never read again. The
compiler drops a copy into a buffer that is freed unread, so an empty `asm` statement marks the buffer as used:
without it the zero-copy rows measure a consumer that doesn't look at its messages (1.5 to 1.8 times the copy rows
at 64 KiB-512 KiB). Every zero-copy row here is measured with the copy.

On the M1 a zero-copy consumer that copies its messages out with `memcpy` runs slower than the copying calls, whose
copy is the library's own (`ring.zig`'s `copyLong`): 64 KiB about 410K against 580K, 512 KiB 46K against 77K, also in
the Zig suite (64 KiB 434K against 653K, 2026-10-07) and in the Linux VM on the M1 (543K against 644K, 2026-10-07), but not on the baseline
machine's x86 (471K against 469K). The library's copy routine in the consumer's place closes the gap (64 KiB 612K):
measured 2026-10-07 with a scratch build of `bench/zig-api`, 1 KiB 25.3M against 18.8M, 16 KiB 2.20M against 1.67M,
4 KiB the same. Single-threaded, with the source in this core's cache, the two copies run at the same speed: what
differs is copying lines another core has just written. Where the buffers sit doesn't matter (every offset of either
side's buffer, 0 to 4 KiB, gave the same 64 KiB rate).

| Bench | Test | macOS\* median [min – max] |
|---|---|---|
| c-fastipc | 1. Tiny messages (16B) | 100.63M [94.42M – 101.69M] |
| c-fastipc | 2. Small messages (64B) | 87.97M [86.49M – 91.97M] |
| c-fastipc | 3. Medium messages (256B) | 66.94M [65.77M – 69.38M] |
| c-fastipc | 4. Large messages (64KB) | 623.4K [604.6K – 643.1K] |
| c-fastipc | 5. Large messages (512KB) | 84.0K [78.8K – 87.9K] |
| c-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.6K [19.0K – 24.6K] |
| c-fastipc-zerocopy | 1. Tiny messages (16B) | 100.14M [97.32M – 104.00M] |
| c-fastipc-zerocopy | 2. Small messages (64B) | 91.96M [85.27M – 98.07M] |
| c-fastipc-zerocopy | 3. Medium messages (256B) | 62.66M [62.12M – 63.46M] |
| c-fastipc-zerocopy | 4. Large messages (64KB) | 422.5K [409.8K – 458.7K] |
| c-fastipc-zerocopy | 5. Large messages (512KB) | 52.3K [48.5K – 54.3K] |
| c-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.8K [4.2K – 24.6K] |
| cpp-fastipc | 1. Tiny messages (16B) | 98.59M [86.39M – 101.03M] |
| cpp-fastipc | 2. Small messages (64B) | 85.04M [69.79M – 102.30M] |
| cpp-fastipc | 3. Medium messages (256B) | 63.87M [60.91M – 64.27M] |
| cpp-fastipc | 4. Large messages (64KB) | 618.0K [451.7K – 676.1K] |
| cpp-fastipc | 5. Large messages (512KB) | 82.4K [14.3K – 87.5K] |
| cpp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 22.3K [20.5K – 22.8K] |
| cpp-fastipc-zerocopy | 1. Tiny messages (16B) | 73.53M [32.23M – 75.50M] |
| cpp-fastipc-zerocopy | 2. Small messages (64B) | 74.44M [60.56M – 74.88M] |
| cpp-fastipc-zerocopy | 3. Medium messages (256B) | 59.99M [57.06M – 61.39M] |
| cpp-fastipc-zerocopy | 4. Large messages (64KB) | 423.4K [413.7K – 435.5K] |
| cpp-fastipc-zerocopy | 5. Large messages (512KB) | 52.8K [44.6K – 56.1K] |
| cpp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.1K [17.5K – 24.5K] |
| zigapi-fastipc | 1. Tiny messages (16B) | 76.20M [73.07M – 87.62M] |
| zigapi-fastipc | 2. Small messages (64B) | 72.48M [68.29M – 73.27M] |
| zigapi-fastipc | 3. Medium messages (256B) | 57.78M [56.00M – 60.63M] |
| zigapi-fastipc | 4. Large messages (64KB) | 615.1K [589.2K – 663.2K] |
| zigapi-fastipc | 5. Large messages (512KB) | 81.9K [79.0K – 82.2K] |
| zigapi-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 23.3K [20.3K – 24.5K] |
| zigapi-fastipc-zerocopy | 1. Tiny messages (16B) | 101.99M [97.02M – 106.01M] |
| zigapi-fastipc-zerocopy | 2. Small messages (64B) | 99.43M [97.77M – 101.09M] |
| zigapi-fastipc-zerocopy | 3. Medium messages (256B) | 74.92M [67.46M – 77.38M] |
| zigapi-fastipc-zerocopy | 4. Large messages (64KB) | 428.8K [394.0K – 440.7K] |
| zigapi-fastipc-zerocopy | 5. Large messages (512KB) | 52.0K [43.1K – 53.5K] |
| zigapi-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.9K [16.9K – 20.8K] |
| csharp-fastipc | 1. Tiny messages (16B) | 65.43M [61.64M – 68.80M] |
| csharp-fastipc | 2. Small messages (64B) | 47.85M [46.93M – 49.99M] |
| csharp-fastipc | 3. Medium messages (256B) | 47.57M [43.51M – 49.49M] |
| csharp-fastipc | 4. Large messages (64KB) | 545.6K [192.3K – 623.7K] |
| csharp-fastipc | 5. Large messages (512KB) | 85.7K [77.0K – 86.4K] |
| csharp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.1K [17.2K – 23.5K] |
| csharp-fastipc-zerocopy | 1. Tiny messages (16B) | 64.58M [56.60M – 68.42M] |
| csharp-fastipc-zerocopy | 2. Small messages (64B) | 60.95M [55.67M – 62.71M] |
| csharp-fastipc-zerocopy | 3. Medium messages (256B) | 37.96M [35.34M – 43.18M] |
| csharp-fastipc-zerocopy | 4. Large messages (64KB) | 622.2K [594.1K – 714.1K] |
| csharp-fastipc-zerocopy | 5. Large messages (512KB) | 86.0K [76.3K – 87.6K] |
| csharp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.3K [19.6K – 23.7K] |
| java-fastipc | 1. Tiny messages (16B) | 35.45M [32.33M – 37.37M] |
| java-fastipc | 2. Small messages (64B) | 34.47M [28.72M – 35.72M] |
| java-fastipc | 3. Medium messages (256B) | 29.77M [26.82M – 31.79M] |
| java-fastipc | 4. Large messages (64KB) | 598.7K [507.3K – 626.8K] |
| java-fastipc | 5. Large messages (512KB) | 84.1K [76.5K – 87.6K] |
| java-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 22.1K [17.6K – 24.5K] |
| java-fastipc-zerocopy | 1. Tiny messages (16B) | 24.32M [22.51M – 27.86M] |
| java-fastipc-zerocopy | 2. Small messages (64B) | 24.09M [21.99M – 26.29M] |
| java-fastipc-zerocopy | 3. Medium messages (256B) | 20.62M [19.25M – 21.63M] |
| java-fastipc-zerocopy | 4. Large messages (64KB) | 572.8K [494.1K – 590.3K] |
| java-fastipc-zerocopy | 5. Large messages (512KB) | 82.7K [77.7K – 87.0K] |
| java-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 24.0K [23.5K – 24.2K] |
| python-fastipc | 1. Tiny messages (16B) | 1.92M [1.86M – 1.95M] |
| python-fastipc | 2. Small messages (64B) | 1.79M [1.74M – 1.98M] |
| python-fastipc | 3. Medium messages (256B) | 1.82M [1.68M – 1.99M] |
| python-fastipc | 4. Large messages (64KB) | 458.9K [384.8K – 468.9K] |
| python-fastipc | 5. Large messages (512KB) | 77.5K [70.8K – 82.6K] |
| python-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.1K [20.1K – 21.5K] |
| python-fastipc-zerocopy | 1. Tiny messages (16B) | 1.02M [931.6K – 1.08M] |
| python-fastipc-zerocopy | 2. Small messages (64B) | 1.01M [948.2K – 1.08M] |
| python-fastipc-zerocopy | 3. Medium messages (256B) | 933.1K [917.7K – 997.2K] |
| python-fastipc-zerocopy | 4. Large messages (64KB) | 132.0K [122.8K – 138.8K] |
| python-fastipc-zerocopy | 5. Large messages (512KB) | 28.1K [23.9K – 31.0K] |
| python-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.5K [19.3K – 21.3K] |
| rust-fastipc | 1. Tiny messages (16B) | 98.58M [97.58M – 102.43M] |
| rust-fastipc | 2. Small messages (64B) | 102.93M [93.67M – 104.19M] |
| rust-fastipc | 3. Medium messages (256B) | 61.41M [45.01M – 64.16M] |
| rust-fastipc | 4. Large messages (64KB) | 639.6K [603.2K – 649.5K] |
| rust-fastipc | 5. Large messages (512KB) | 85.4K [61.4K – 87.0K] |
| rust-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.6K [18.4K – 24.3K] |
| rust-fastipc-zerocopy | 1. Tiny messages (16B) | 66.90M [65.22M – 68.89M] |
| rust-fastipc-zerocopy | 2. Small messages (64B) | 64.17M [59.24M – 67.51M] |
| rust-fastipc-zerocopy | 3. Medium messages (256B) | 51.88M [47.71M – 53.89M] |
| rust-fastipc-zerocopy | 4. Large messages (64KB) | 434.9K [417.0K – 441.2K] |
| rust-fastipc-zerocopy | 5. Large messages (512KB) | 54.3K [46.6K – 55.1K] |
| rust-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.6K [19.8K – 24.5K] |
| lua-fastipc | 1. Tiny messages (16B) | 122.02M [105.13M – 122.90M] |
| lua-fastipc | 2. Small messages (64B) | 105.04M [96.94M – 110.78M] |
| lua-fastipc | 3. Medium messages (256B) | 69.21M [68.37M – 69.58M] |
| lua-fastipc | 4. Large messages (64KB) | 596.7K [579.4K – 625.0K] |
| lua-fastipc | 5. Large messages (512KB) | 84.8K [74.2K – 85.6K] |
| lua-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.5K [18.1K – 22.5K] |
| lua-fastipc-zerocopy | 1. Tiny messages (16B) | 101.95M [91.02M – 104.94M] |
| lua-fastipc-zerocopy | 2. Small messages (64B) | 81.11M [48.26M – 84.05M] |
| lua-fastipc-zerocopy | 3. Medium messages (256B) | 64.22M [39.31M – 70.05M] |
| lua-fastipc-zerocopy | 4. Large messages (64KB) | 404.7K [359.3K – 426.8K] |
| lua-fastipc-zerocopy | 5. Large messages (512KB) | 50.9K [7.5K – 55.2K] |
| lua-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.2K [4.4K – 23.5K] |
| js-fastipc | 1. Tiny messages (16B) | 3.53M [3.39M – 3.77M] |
| js-fastipc | 2. Small messages (64B) | 3.25M [3.19M – 3.70M] |
| js-fastipc | 3. Medium messages (256B) | 3.22M [3.06M – 3.54M] |
| js-fastipc | 4. Large messages (64KB) | 543.8K [521.7K – 548.7K] |
| js-fastipc | 5. Large messages (512KB) | 82.6K [74.2K – 84.6K] |
| js-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.4K [16.2K – 18.2K] |
| js-fastipc-zerocopy | 1. Tiny messages (16B) | 1.08M [1.01M – 1.15M] |
| js-fastipc-zerocopy | 2. Small messages (64B) | 1.08M [841.3K – 1.15M] |
| js-fastipc-zerocopy | 3. Medium messages (256B) | 1.06M [979.5K – 1.16M] |
| js-fastipc-zerocopy | 4. Large messages (64KB) | 364.0K [331.8K – 386.2K] |
| js-fastipc-zerocopy | 5. Large messages (512KB) | 45.7K [44.8K – 49.4K] |
| js-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 16.7K [16.2K – 17.7K] |
| go-fastipc | 1. Tiny messages (16B) | 10.27M [9.80M – 10.76M] |
| go-fastipc | 2. Small messages (64B) | 9.24M [8.39M – 10.25M] |
| go-fastipc | 3. Medium messages (256B) | 9.29M [9.05M – 9.44M] |
| go-fastipc | 4. Large messages (64KB) | 602.3K [581.0K – 613.2K] |
| go-fastipc | 5. Large messages (512KB) | 85.8K [77.5K – 86.5K] |
| go-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.4K [19.3K – 21.9K] |
| go-fastipc-zerocopy | 1. Tiny messages (16B) | 5.46M [5.07M – 5.91M] |
| go-fastipc-zerocopy | 2. Small messages (64B) | 5.44M [5.05M – 5.68M] |
| go-fastipc-zerocopy | 3. Medium messages (256B) | 5.42M [5.12M – 5.54M] |
| go-fastipc-zerocopy | 4. Large messages (64KB) | 562.0K [516.5K – 564.2K] |
| go-fastipc-zerocopy | 5. Large messages (512KB) | 76.7K [73.4K – 81.8K] |
| go-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.5K [19.4K – 21.7K] |

&dagger; Linux VM: the same MacBook Air M1, Linux in a Lima 2.2.1 VM (Apple Virtualization framework): Ubuntu 24.04.5
arm64, kernel 6.8, glibc 2.39, 4 vCPUs, 4 GB; everything built for generic ARMv8.0-A. The same benchmarks with the
warm-up, the night of 2026-10-24 to 25 at main 2cafb052, after the macOS\* rows and run as they were (each built
and run once untimed, then 5 runs round-robin with a rotating start, every benchmark on `zig-out/bench`'s ReleaseFast
library: C# checked with `LD_DEBUG=libs`), JavaScript on Node.js 22.23.3, Go 1.27.1 (also CPython 3.12.3, .NET
9.0.20, Temurin JDK 25.0.4.1, Rust 1.99.0, LuaJIT 2.1, Zig 0.17.0), the Mac held awake and otherwise
idle, with no other benchmark running. Against 2026-10-24 (main 45b88f10) 82 of 120 medians moved within 10%. The
copied 16-256 B rows of C, C++, the Zig module, Rust and Lua run 1.1-2.3 times as fast (C's 64 B 111.97M against
53.70M, the Zig module's 103.89M against 44.48M), the Zig module's zero-copy 16-256 B 2.1-2.5 times (67.43M against
26.75M at 256 B) and C#'s zero-copy 16 and 64 B 1.3 and 1.7 times: the macOS\* rows' changes. The zero-copy 16-256 B
rows of C, C++, Rust and Lua stay where they were (C's 16 B 38.85M against 37.88M), where on macOS they doubled and
the Zig module's run at 92-99M here too (not investigated). Python, JavaScript and Go moved within 11% (Go's zero-copy 1 MiB 14% lower, 10 messages timed); C++'s copied
16-256 B and C#'s zero-copy 16-256 B rows each had one run at a quarter to a half of their median (the minimums). Against 2026-10-07 (main 645ab67), copied 16 B messages run about 1.5
times as fast in C, C++, the Zig module, C#, Rust and Lua (the copying send's prefetch, added since); Python's
zero-copy 16-256 B rows 0.71-0.73 times (576K against 812K at 16 B) and Java's zero-copy 16 B row 0.80 times (its
runs spread 14.4-19.1M); the other rows moved within the noise.
Against the macOS\* rows it changes the operating system on the same hardware;
against Linux above (measured at a later commit), the hardware under the same operating system. Not a gate.

| Bench | Test | Linux VM&dagger; median [min – max] |
|---|---|---|
| c-fastipc | 1. Tiny messages (16B) | 127.84M [117.33M – 129.89M] |
| c-fastipc | 2. Small messages (64B) | 111.97M [110.06M – 112.76M] |
| c-fastipc | 3. Medium messages (256B) | 69.97M [68.26M – 70.40M] |
| c-fastipc | 4. Large messages (64KB) | 653.5K [579.9K – 658.3K] |
| c-fastipc | 5. Large messages (512KB) | 77.3K [77.0K – 86.6K] |
| c-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.3K [19.8K – 21.9K] |
| c-fastipc-zerocopy | 1. Tiny messages (16B) | 38.85M [36.56M – 38.87M] |
| c-fastipc-zerocopy | 2. Small messages (64B) | 38.02M [37.51M – 38.06M] |
| c-fastipc-zerocopy | 3. Medium messages (256B) | 35.52M [33.86M – 35.58M] |
| c-fastipc-zerocopy | 4. Large messages (64KB) | 651.8K [600.3K – 675.3K] |
| c-fastipc-zerocopy | 5. Large messages (512KB) | 85.6K [77.0K – 86.2K] |
| c-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.2K [17.8K – 21.1K] |
| cpp-fastipc | 1. Tiny messages (16B) | 99.16M [26.95M – 103.92M] |
| cpp-fastipc | 2. Small messages (64B) | 94.51M [25.28M – 96.32M] |
| cpp-fastipc | 3. Medium messages (256B) | 62.80M [24.40M – 63.53M] |
| cpp-fastipc | 4. Large messages (64KB) | 663.0K [559.6K – 683.3K] |
| cpp-fastipc | 5. Large messages (512KB) | 85.1K [74.8K – 85.8K] |
| cpp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.3K [19.9K – 22.9K] |
| cpp-fastipc-zerocopy | 1. Tiny messages (16B) | 36.69M [32.00M – 36.88M] |
| cpp-fastipc-zerocopy | 2. Small messages (64B) | 35.88M [35.59M – 36.26M] |
| cpp-fastipc-zerocopy | 3. Medium messages (256B) | 32.01M [29.28M – 32.42M] |
| cpp-fastipc-zerocopy | 4. Large messages (64KB) | 651.1K [580.5K – 678.7K] |
| cpp-fastipc-zerocopy | 5. Large messages (512KB) | 77.4K [76.1K – 86.5K] |
| cpp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.5K [20.1K – 20.6K] |
| zigapi-fastipc | 1. Tiny messages (16B) | 106.33M [101.44M – 108.82M] |
| zigapi-fastipc | 2. Small messages (64B) | 103.89M [93.83M – 104.70M] |
| zigapi-fastipc | 3. Medium messages (256B) | 58.39M [58.04M – 59.55M] |
| zigapi-fastipc | 4. Large messages (64KB) | 643.7K [569.2K – 677.7K] |
| zigapi-fastipc | 5. Large messages (512KB) | 84.0K [77.4K – 86.4K] |
| zigapi-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.1K [19.7K – 24.0K] |
| zigapi-fastipc-zerocopy | 1. Tiny messages (16B) | 92.32M [90.28M – 95.91M] |
| zigapi-fastipc-zerocopy | 2. Small messages (64B) | 99.06M [92.92M – 100.01M] |
| zigapi-fastipc-zerocopy | 3. Medium messages (256B) | 67.43M [63.76M – 68.87M] |
| zigapi-fastipc-zerocopy | 4. Large messages (64KB) | 536.3K [506.5K – 567.8K] |
| zigapi-fastipc-zerocopy | 5. Large messages (512KB) | 66.8K [63.5K – 71.9K] |
| zigapi-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.1K [18.2K – 22.9K] |
| csharp-fastipc | 1. Tiny messages (16B) | 72.86M [61.09M – 73.77M] |
| csharp-fastipc | 2. Small messages (64B) | 48.36M [37.34M – 48.48M] |
| csharp-fastipc | 3. Medium messages (256B) | 47.10M [46.44M – 47.89M] |
| csharp-fastipc | 4. Large messages (64KB) | 606.9K [577.7K – 623.5K] |
| csharp-fastipc | 5. Large messages (512KB) | 74.6K [56.0K – 80.5K] |
| csharp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 18.3K [16.3K – 19.1K] |
| csharp-fastipc-zerocopy | 1. Tiny messages (16B) | 62.04M [31.61M – 68.05M] |
| csharp-fastipc-zerocopy | 2. Small messages (64B) | 58.23M [29.10M – 62.12M] |
| csharp-fastipc-zerocopy | 3. Medium messages (256B) | 42.61M [27.62M – 44.16M] |
| csharp-fastipc-zerocopy | 4. Large messages (64KB) | 666.2K [639.3K – 670.6K] |
| csharp-fastipc-zerocopy | 5. Large messages (512KB) | 82.5K [73.8K – 84.0K] |
| csharp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 18.8K [18.3K – 21.8K] |
| java-fastipc | 1. Tiny messages (16B) | 26.71M [23.78M – 34.46M] |
| java-fastipc | 2. Small messages (64B) | 34.01M [25.52M – 35.86M] |
| java-fastipc | 3. Medium messages (256B) | 29.14M [24.68M – 30.03M] |
| java-fastipc | 4. Large messages (64KB) | 632.9K [542.3K – 636.6K] |
| java-fastipc | 5. Large messages (512KB) | 84.4K [71.0K – 85.8K] |
| java-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.3K [18.2K – 20.5K] |
| java-fastipc-zerocopy | 1. Tiny messages (16B) | 20.99M [13.04M – 22.88M] |
| java-fastipc-zerocopy | 2. Small messages (64B) | 24.74M [23.74M – 24.83M] |
| java-fastipc-zerocopy | 3. Medium messages (256B) | 21.98M [21.00M – 23.54M] |
| java-fastipc-zerocopy | 4. Large messages (64KB) | 596.5K [516.1K – 600.1K] |
| java-fastipc-zerocopy | 5. Large messages (512KB) | 76.2K [72.9K – 80.9K] |
| java-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 22.8K [19.7K – 23.5K] |
| python-fastipc | 1. Tiny messages (16B) | 1.10M [1.09M – 1.12M] |
| python-fastipc | 2. Small messages (64B) | 1.07M [1.05M – 1.12M] |
| python-fastipc | 3. Medium messages (256B) | 1.07M [1.06M – 1.13M] |
| python-fastipc | 4. Large messages (64KB) | 375.0K [352.1K – 391.7K] |
| python-fastipc | 5. Large messages (512KB) | 70.6K [66.5K – 78.8K] |
| python-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.6K [17.9K – 19.8K] |
| python-fastipc-zerocopy | 1. Tiny messages (16B) | 599.3K [489.3K – 615.8K] |
| python-fastipc-zerocopy | 2. Small messages (64B) | 594.0K [561.2K – 614.0K] |
| python-fastipc-zerocopy | 3. Medium messages (256B) | 583.2K [544.3K – 596.1K] |
| python-fastipc-zerocopy | 4. Large messages (64KB) | 199.0K [188.5K – 207.2K] |
| python-fastipc-zerocopy | 5. Large messages (512KB) | 38.9K [36.0K – 39.7K] |
| python-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 18.8K [18.7K – 19.7K] |
| rust-fastipc | 1. Tiny messages (16B) | 99.63M [97.48M – 103.13M] |
| rust-fastipc | 2. Small messages (64B) | 95.33M [90.96M – 99.62M] |
| rust-fastipc | 3. Medium messages (256B) | 63.40M [60.37M – 64.21M] |
| rust-fastipc | 4. Large messages (64KB) | 664.9K [541.5K – 695.2K] |
| rust-fastipc | 5. Large messages (512KB) | 77.4K [77.0K – 86.4K] |
| rust-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.1K [19.7K – 21.5K] |
| rust-fastipc-zerocopy | 1. Tiny messages (16B) | 33.99M [25.22M – 34.11M] |
| rust-fastipc-zerocopy | 2. Small messages (64B) | 33.35M [31.37M – 33.64M] |
| rust-fastipc-zerocopy | 3. Medium messages (256B) | 29.61M [28.51M – 29.72M] |
| rust-fastipc-zerocopy | 4. Large messages (64KB) | 652.7K [561.8K – 669.7K] |
| rust-fastipc-zerocopy | 5. Large messages (512KB) | 84.5K [75.8K – 86.2K] |
| rust-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.2K [20.1K – 20.5K] |
| lua-fastipc | 1. Tiny messages (16B) | 127.46M [105.30M – 129.30M] |
| lua-fastipc | 2. Small messages (64B) | 107.19M [98.18M – 110.66M] |
| lua-fastipc | 3. Medium messages (256B) | 68.65M [59.27M – 68.92M] |
| lua-fastipc | 4. Large messages (64KB) | 546.3K [481.0K – 588.9K] |
| lua-fastipc | 5. Large messages (512KB) | 72.4K [67.8K – 81.7K] |
| lua-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.8K [14.0K – 18.4K] |
| lua-fastipc-zerocopy | 1. Tiny messages (16B) | 49.00M [47.49M – 49.42M] |
| lua-fastipc-zerocopy | 2. Small messages (64B) | 48.00M [45.09M – 48.27M] |
| lua-fastipc-zerocopy | 3. Medium messages (256B) | 43.09M [37.88M – 43.38M] |
| lua-fastipc-zerocopy | 4. Large messages (64KB) | 528.7K [470.8K – 610.2K] |
| lua-fastipc-zerocopy | 5. Large messages (512KB) | 75.9K [71.1K – 76.4K] |
| lua-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.9K [13.0K – 18.4K] |
| js-fastipc | 1. Tiny messages (16B) | 2.21M [2.15M – 2.34M] |
| js-fastipc | 2. Small messages (64B) | 2.05M [1.70M – 2.17M] |
| js-fastipc | 3. Medium messages (256B) | 2.18M [1.92M – 2.28M] |
| js-fastipc | 4. Large messages (64KB) | 249.7K [213.3K – 419.8K] |
| js-fastipc | 5. Large messages (512KB) | 74.7K [54.9K – 81.3K] |
| js-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 10.0K [9.9K – 10.3K] |
| js-fastipc-zerocopy | 1. Tiny messages (16B) | 603.9K [592.1K – 643.3K] |
| js-fastipc-zerocopy | 2. Small messages (64B) | 592.0K [587.5K – 605.0K] |
| js-fastipc-zerocopy | 3. Medium messages (256B) | 619.6K [599.6K – 689.1K] |
| js-fastipc-zerocopy | 4. Large messages (64KB) | 204.6K [193.0K – 287.3K] |
| js-fastipc-zerocopy | 5. Large messages (512KB) | 73.4K [55.8K – 74.0K] |
| js-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 5.6K [5.2K – 5.9K] |
| go-fastipc | 1. Tiny messages (16B) | 10.42M [10.16M – 11.04M] |
| go-fastipc | 2. Small messages (64B) | 9.70M [9.56M – 10.27M] |
| go-fastipc | 3. Medium messages (256B) | 9.54M [9.17M – 9.87M] |
| go-fastipc | 4. Large messages (64KB) | 594.5K [587.3K – 605.7K] |
| go-fastipc | 5. Large messages (512KB) | 84.2K [74.6K – 84.6K] |
| go-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.3K [19.1K – 20.4K] |
| go-fastipc-zerocopy | 1. Tiny messages (16B) | 5.61M [5.35M – 5.91M] |
| go-fastipc-zerocopy | 2. Small messages (64B) | 5.47M [5.24M – 5.58M] |
| go-fastipc-zerocopy | 3. Medium messages (256B) | 5.25M [5.11M – 5.71M] |
| go-fastipc-zerocopy | 4. Large messages (64KB) | 563.8K [529.2K – 602.9K] |
| go-fastipc-zerocopy | 5. Large messages (512KB) | 76.0K [74.7K – 84.8K] |
| go-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.2K [18.9K – 19.4K] |

## The C# binding's two layers

`python devtool.py bench csharp-layers` (`bench/csharp/FastIpc.Bench/Layers.cs`) compares the raw layer (`Fipc`,
IntPtr handles) with the object layer (`FipcConnection`, whose calls each hold a SafeHandle) in one process and one
thread: a batch of messages that fits the ring is sent and then received, so no call waits, and 21 rounds alternate
between the layers. ns per message, the median round: a send and a receive, 2 calls (zero-copy: acquire, commit,
acquire, release, 4 calls). Not gated; one run on each OS of the baseline machine, 2026-10-25 at main 2cafb052.

| Case | Windows raw | Windows object | Difference | Linux raw | Linux object | Difference |
|---|---|---|---|---|---|---|
| copy 16 B | 39.6 | 84.2 | +44.6 | 41.0 | 76.6 | +35.5 |
| copy 64 B | 35.0 | 59.4 | +24.4 | 40.8 | 73.1 | +32.3 |
| copy 1 KiB | 62.0 | 88.9 | +26.9 | 66.0 | 95.2 | +29.2 |
| zero-copy 16 B | 39.7 | 85.1 | +45.5 | 40.2 | 89.3 | +49.1 |
| zero-copy 64 B | 48.0 | 83.8 | +35.8 | 47.9 | 81.7 | +33.8 |
| zero-copy 1 KiB | 70.4 | 100.1 | +29.8 | 61.8 | 93.8 | +32.0 |

The object layer costs about 30 ns per message (24-45 ns copying, 30-49 ns zero-copy), 15 ns per copying call, on both OSes: nearly all of it the
SafeHandle's reference count, two atomic compare-and-swap loops per call (`DangerousAddRef` and `DangerousRelease`;
the pair alone measures 14.5 ns on Windows). A zero-copy message pays the same: its reservation holds one reference
from acquire to commit, and its received message one from acquire to release. Against a message's whole trip between
two processes (the table above: 31-37M messages/s on one side is 27-32 ns per message) the cost is real for tiny
messages, so the raw layer stays the escape hatch for the hottest loops; the object layer is the default for its
safety (no call ever runs on a closed handle).
