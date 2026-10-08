# Bindings baseline

Reference medians of the C, C++, Zig, C#, Java, Python, Rust, Lua, JavaScript and Go benchmarks (`bench/c`, `bench/zig-api`,
`bench/csharp/FastIpc.Bench`, `bench/java`, `bench/python/fipc_bench.py`, `bench/rust`, `bench/lua/fipc_bench.lua`,
`bench/js/fipc_bench.mjs`, `bench/go`) on the baseline machine ([`baseline.md`](baseline.md), "Machine"): release builds, 5 runs round-robin (2026-10-07,
`main` eeb6676, with the stream pace and the warm-up below), messages per second, median [min – max]. `python devtool.py bench-compare <benchmark ...>` (without `--ab`) runs the benchmarks
and fails a case whose median falls below 95% of its row; the table's rows are what it reads.

The 16-256 B cases send 2,000,000 messages (1,000,000 at 256 B) so that a native case runs for tens of milliseconds,
several Windows scheduler quanta (about 16 ms each): a case that ends within one quantum measures the scheduler, not the
messages. The numbers are still rough, so a failing case is a hint, not a verdict: scheduling noise is large
(±20-30%). On Windows, devtool turns power throttling off for every process a benchmark starts (started from a
background process otherwise, a case can read half as fast). Compare WSL numbers only with WSL numbers. For a change to the library, `bench-compare --ab`
(the Zig suite) is the gate. The C# benchmark runs on .NET 9 and uses the binding's raw layer (`Fipc`). The Java
benchmark runs on JDK 25 through the binding's API, receiving into a native segment it reuses.

Every benchmark warms up: each case first sends messages untimed, for at least 0.2 s (0.5 s on the JITs: C#, Java,
LuaJIT and Node.js) and at least its count, with a start marker every 1,000 messages (so that the last one takes no
path a JIT hasn't seen), then a last start marker, its count and the end marker; the server times from the last start
marker to the end marker. A start marker is the 3-byte message `GO!` (RPC: opcode `0xFB000002`, no payload). Until
2026-10-07 only Java warmed up, and the others timed a fresh process's first messages: on the M1 the first case of a
newly written executable ran at about half speed (macOS's first run of a new file; `cargo run` re-links the Rust
benchmark on every run, so its 16 B rows read 31-34M against C's 56-60M). Every row below is measured with the
warm-up.

The Rust benchmark runs through the binding's safe API (`receive_into` and
`rpc_receive_into` a buffer it reuses, `send_acquire` / `receive_acquire` slots), built with Cargo's release profile.
The Lua benchmark runs on LuaJIT 2.1 through the binding (`recv_into` and `rpc_recv_into` a buffer it reuses,
`send_acquire` / `recv_acquire` pointers into the ring). The JavaScript benchmark runs on Node.js 22 through the binding's async calls,
each awaited (`receiveInto` and `rpcReceiveInto` a buffer it reuses, `sendAcquire` / `receiveAcquire` views copied out):
a message that is there is taken on the JavaScript thread, so these measure the binding's per-call cost (a Promise,
and for zero-copy and RPC the object it makes), not a hand-off to a thread; `--sync` measures the `Sync` calls (about
twice the copy and RPC rates). The Go
benchmark runs on Go 1.27.1 through the binding, built with `CGO_ENABLED=0` (`ReceiveInto` and `RPCReceiveInto` a
buffer it reuses, `SendAcquire` / `ReceiveAcquire` slices into the ring, copied out). Each call goes into the library through purego and a mutex of its direction, which costs more than a
short message's copy: copied 16 B messages run at about a quarter of Rust's rate on Windows and a fifth of it on Linux, and
zero-copy, two calls per message on each side where a copy takes one, runs slower than copying up to 256 B. From
64 KiB on, Go runs as fast as the others. The C and C++
benchmark (`bench/c`) is one harness with two sets of the same loops: through the C API (`c_bench`,
C11) and through the C++ wrapper `include/fipc.hpp` (`cpp_bench`, C++20), both built ReleaseFast by `zig build`
against the release library and installed with it in `zig-out/bench`, whatever the build's mode. Their medians agree
within the noise: the wrapper adds nothing measurable to a message's trip.

| Bench | Test | Windows median [min – max] | Linux median [min – max] |
|---|---|---|---|
| c-fastipc | 1. Tiny messages (16B) | 47.52M [39.42M – 49.84M] | 48.79M [44.96M – 52.70M] |
| c-fastipc | 2. Small messages (64B) | 40.58M [32.23M – 45.67M] | 47.88M [42.68M – 50.14M] |
| c-fastipc | 3. Medium messages (256B) | 31.01M [25.70M – 34.70M] | 41.37M [41.01M – 41.81M] |
| c-fastipc | 4. Large messages (64KB) | 432.3K [370.5K – 455.2K] | 497.1K [464.0K – 526.2K] |
| c-fastipc | 5. Large messages (512KB) | 58.5K [51.5K – 59.2K] | 60.8K [60.3K – 61.5K] |
| c-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 14.1K [11.7K – 15.5K] | 16.2K [16.1K – 16.8K] |
| c-fastipc-zerocopy | 1. Tiny messages (16B) | 40.68M [40.40M – 42.02M] | 54.38M [49.97M – 55.74M] |
| c-fastipc-zerocopy | 2. Small messages (64B) | 43.84M [40.81M – 46.20M] | 52.50M [50.95M – 54.06M] |
| c-fastipc-zerocopy | 3. Medium messages (256B) | 40.99M [34.81M – 42.14M] | 45.54M [40.52M – 46.46M] |
| c-fastipc-zerocopy | 4. Large messages (64KB) | 468.7K [400.0K – 504.3K] | 460.1K [285.2K – 491.1K] |
| c-fastipc-zerocopy | 5. Large messages (512KB) | 52.7K [50.0K – 60.0K] | 57.6K [56.5K – 58.6K] |
| c-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.6K [10.0K – 14.2K] | 16.4K [15.5K – 16.8K] |
| c-rpc | 1. Tiny messages (16B) | 39.87M [32.64M – 43.05M] | 45.78M [43.43M – 47.48M] |
| c-rpc | 2. Small messages (64B) | 31.18M [28.69M – 36.83M] | 34.83M [31.79M – 37.00M] |
| c-rpc | 3. Medium messages (256B) | 28.41M [27.28M – 30.54M] | 29.78M [25.43M – 32.35M] |
| c-rpc | 4. Large messages (64KB) | 436.9K [426.9K – 476.2K] | 479.2K [472.7K – 511.8K] |
| c-rpc | 5. Large messages (512KB) | 57.6K [51.1K – 58.8K] | 60.5K [59.5K – 62.3K] |
| c-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.7K [13.1K – 15.3K] | 16.5K [15.9K – 16.8K] |
| cpp-fastipc | 1. Tiny messages (16B) | 43.07M [42.41M – 44.43M] | 43.49M [40.64M – 45.65M] |
| cpp-fastipc | 2. Small messages (64B) | 35.68M [33.29M – 40.08M] | 44.93M [44.49M – 46.72M] |
| cpp-fastipc | 3. Medium messages (256B) | 27.03M [22.49M – 28.08M] | 37.96M [36.30M – 38.46M] |
| cpp-fastipc | 4. Large messages (64KB) | 414.2K [360.6K – 457.8K] | 487.6K [477.8K – 531.2K] |
| cpp-fastipc | 5. Large messages (512KB) | 56.2K [50.1K – 60.0K] | 62.8K [58.4K – 63.8K] |
| cpp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.0K [12.0K – 14.4K] | 16.4K [16.0K – 16.9K] |
| cpp-fastipc-zerocopy | 1. Tiny messages (16B) | 47.24M [42.54M – 48.48M] | 57.91M [48.15M – 60.63M] |
| cpp-fastipc-zerocopy | 2. Small messages (64B) | 46.45M [46.01M – 49.36M] | 57.51M [49.96M – 59.54M] |
| cpp-fastipc-zerocopy | 3. Medium messages (256B) | 36.06M [33.50M – 36.66M] | 36.64M [32.19M – 37.91M] |
| cpp-fastipc-zerocopy | 4. Large messages (64KB) | 433.2K [380.9K – 469.4K] | 454.9K [435.3K – 495.0K] |
| cpp-fastipc-zerocopy | 5. Large messages (512KB) | 50.9K [50.3K – 58.3K] | 59.6K [55.9K – 61.3K] |
| cpp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 14.1K [12.2K – 14.3K] | 16.5K [15.5K – 16.9K] |
| cpp-rpc | 1. Tiny messages (16B) | 38.44M [36.30M – 39.11M] | 41.78M [36.84M – 42.31M] |
| cpp-rpc | 2. Small messages (64B) | 32.74M [25.18M – 32.88M] | 35.56M [34.34M – 36.92M] |
| cpp-rpc | 3. Medium messages (256B) | 27.06M [21.72M – 27.60M] | 29.09M [28.01M – 30.79M] |
| cpp-rpc | 4. Large messages (64KB) | 436.2K [392.9K – 455.8K] | 501.5K [477.1K – 518.2K] |
| cpp-rpc | 5. Large messages (512KB) | 59.0K [58.1K – 60.1K] | 61.8K [60.0K – 65.6K] |
| cpp-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 14.0K [12.5K – 15.4K] | 16.6K [15.5K – 17.0K] |
| zigapi-fastipc | 1. Tiny messages (16B) | 44.81M [42.22M – 45.89M] | 56.35M [52.09M – 58.37M] |
| zigapi-fastipc | 2. Small messages (64B) | 39.95M [35.62M – 42.52M] | 50.10M [44.95M – 52.66M] |
| zigapi-fastipc | 3. Medium messages (256B) | 27.39M [25.62M – 28.65M] | 40.36M [38.51M – 40.62M] |
| zigapi-fastipc | 4. Large messages (64KB) | 453.1K [391.5K – 476.6K] | 506.8K [500.9K – 508.6K] |
| zigapi-fastipc | 5. Large messages (512KB) | 53.5K [50.7K – 58.4K] | 61.7K [61.1K – 64.6K] |
| zigapi-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.2K [10.4K – 14.6K] | 16.4K [15.9K – 17.4K] |
| zigapi-fastipc-zerocopy | 1. Tiny messages (16B) | 51.29M [46.79M – 52.84M] | 49.77M [46.81M – 52.98M] |
| zigapi-fastipc-zerocopy | 2. Small messages (64B) | 48.65M [43.90M – 50.74M] | 53.99M [51.63M – 54.92M] |
| zigapi-fastipc-zerocopy | 3. Medium messages (256B) | 43.24M [41.17M – 44.60M] | 48.49M [47.61M – 50.05M] |
| zigapi-fastipc-zerocopy | 4. Large messages (64KB) | 435.4K [388.9K – 458.6K] | 507.1K [453.6K – 514.4K] |
| zigapi-fastipc-zerocopy | 5. Large messages (512KB) | 57.3K [48.6K – 58.8K] | 59.6K [54.2K – 63.1K] |
| zigapi-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 14.1K [12.4K – 14.8K] | 15.1K [14.4K – 16.5K] |
| zigapi-rpc | 1. Tiny messages (16B) | 40.34M [38.70M – 41.19M] | 39.81M [38.23M – 40.34M] |
| zigapi-rpc | 2. Small messages (64B) | 34.39M [32.82M – 35.41M] | 35.13M [30.23M – 38.20M] |
| zigapi-rpc | 3. Medium messages (256B) | 26.46M [22.09M – 27.79M] | 28.27M [25.53M – 29.93M] |
| zigapi-rpc | 4. Large messages (64KB) | 441.1K [380.1K – 457.4K] | 499.2K [483.1K – 510.1K] |
| zigapi-rpc | 5. Large messages (512KB) | 59.7K [52.4K – 60.3K] | 61.1K [54.4K – 64.5K] |
| zigapi-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 14.3K [11.2K – 15.0K] | 15.5K [14.8K – 16.4K] |
| csharp-fastipc | 1. Tiny messages (16B) | 27.24M [23.66M – 28.42M] | 7.50M [6.85M – 7.67M] |
| csharp-fastipc | 2. Small messages (64B) | 30.21M [28.88M – 31.94M] | 7.55M [7.45M – 7.77M] |
| csharp-fastipc | 3. Medium messages (256B) | 32.30M [30.03M – 35.16M] | 7.58M [7.32M – 7.88M] |
| csharp-fastipc | 4. Large messages (64KB) | 406.9K [376.9K – 436.3K] | 457.4K [448.5K – 487.3K] |
| csharp-fastipc | 5. Large messages (512KB) | 57.0K [44.2K – 58.8K] | 58.2K [50.5K – 61.5K] |
| csharp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 14.1K [11.5K – 14.4K] | 15.4K [12.7K – 16.4K] |
| csharp-fastipc-zerocopy | 1. Tiny messages (16B) | 28.48M [27.52M – 31.52M] | 7.47M [7.25M – 7.68M] |
| csharp-fastipc-zerocopy | 2. Small messages (64B) | 31.14M [27.35M – 34.24M] | 7.52M [7.18M – 7.61M] |
| csharp-fastipc-zerocopy | 3. Medium messages (256B) | 30.00M [27.57M – 32.00M] | 7.46M [7.05M – 7.55M] |
| csharp-fastipc-zerocopy | 4. Large messages (64KB) | 377.0K [354.9K – 385.0K] | 435.1K [420.2K – 441.6K] |
| csharp-fastipc-zerocopy | 5. Large messages (512KB) | 51.1K [43.3K – 57.2K] | 58.6K [50.2K – 62.3K] |
| csharp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 12.2K [7.0K – 13.9K] | 16.7K [15.9K – 16.8K] |
| csharp-rpc | 1. Tiny messages (16B) | 31.10M [27.98M – 31.97M] | 7.70M [7.35M – 7.95M] |
| csharp-rpc | 2. Small messages (64B) | 29.96M [29.50M – 31.80M] | 7.59M [7.27M – 7.84M] |
| csharp-rpc | 3. Medium messages (256B) | 18.44M [16.62M – 20.91M] | 7.33M [7.16M – 7.79M] |
| csharp-rpc | 4. Large messages (64KB) | 422.1K [379.3K – 462.5K] | 468.2K [437.8K – 471.9K] |
| csharp-rpc | 5. Large messages (512KB) | 56.1K [43.1K – 58.4K] | 61.0K [57.9K – 65.3K] |
| csharp-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 12.7K [9.5K – 14.8K] | 14.5K [11.2K – 16.6K] |
| java-fastipc | 1. Tiny messages (16B) | 15.47M [11.64M – 15.82M] | 16.30M [15.58M – 16.56M] |
| java-fastipc | 2. Small messages (64B) | 16.66M [13.39M – 16.75M] | 18.60M [17.97M – 18.64M] |
| java-fastipc | 3. Medium messages (256B) | 16.75M [14.06M – 16.93M] | 19.02M [18.78M – 19.60M] |
| java-fastipc | 4. Large messages (64KB) | 383.1K [375.0K – 404.6K] | 488.6K [411.0K – 528.3K] |
| java-fastipc | 5. Large messages (512KB) | 55.1K [44.0K – 58.5K] | 63.2K [52.3K – 66.6K] |
| java-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.7K [10.0K – 13.8K] | 13.1K [11.7K – 15.3K] |
| java-fastipc-zerocopy | 1. Tiny messages (16B) | 16.63M [14.48M – 17.44M] | 14.28M [13.84M – 18.50M] |
| java-fastipc-zerocopy | 2. Small messages (64B) | 13.55M [12.19M – 14.58M] | 15.86M [15.45M – 17.44M] |
| java-fastipc-zerocopy | 3. Medium messages (256B) | 12.11M [9.81M – 13.49M] | 15.08M [14.38M – 15.74M] |
| java-fastipc-zerocopy | 4. Large messages (64KB) | 374.9K [355.6K – 435.5K] | 442.1K [435.7K – 467.8K] |
| java-fastipc-zerocopy | 5. Large messages (512KB) | 52.8K [48.8K – 56.1K] | 58.2K [48.4K – 62.2K] |
| java-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 11.3K [9.7K – 14.1K] | 15.5K [15.0K – 15.9K] |
| java-rpc | 1. Tiny messages (16B) | 17.01M [16.16M – 17.49M] | 17.84M [16.72M – 19.55M] |
| java-rpc | 2. Small messages (64B) | 15.24M [14.68M – 16.54M] | 14.68M [14.32M – 15.39M] |
| java-rpc | 3. Medium messages (256B) | 14.59M [12.71M – 15.21M] | 13.56M [12.85M – 14.80M] |
| java-rpc | 4. Large messages (64KB) | 408.7K [362.4K – 419.9K] | 463.1K [429.2K – 488.8K] |
| java-rpc | 5. Large messages (512KB) | 54.9K [51.3K – 58.9K] | 60.8K [59.2K – 66.1K] |
| java-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 12.1K [10.2K – 13.8K] | 15.6K [11.3K – 16.0K] |
| lua-fastipc | 1. Tiny messages (16B) | 46.05M [41.89M – 52.18M] | 50.59M [49.28M – 57.54M] |
| lua-fastipc | 2. Small messages (64B) | 39.67M [34.21M – 41.39M] | 49.27M [45.62M – 51.31M] |
| lua-fastipc | 3. Medium messages (256B) | 30.30M [26.84M – 33.81M] | 40.04M [38.37M – 40.98M] |
| lua-fastipc | 4. Large messages (64KB) | 401.6K [308.7K – 461.2K] | 459.2K [406.4K – 481.1K] |
| lua-fastipc | 5. Large messages (512KB) | 56.8K [53.1K – 60.5K] | 57.8K [53.5K – 59.0K] |
| lua-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 12.4K [10.1K – 13.7K] | 15.8K [14.2K – 16.3K] |
| lua-fastipc-zerocopy | 1. Tiny messages (16B) | 40.41M [32.09M – 42.91M] | 41.31M [40.77M – 44.15M] |
| lua-fastipc-zerocopy | 2. Small messages (64B) | 39.62M [34.07M – 44.82M] | 40.51M [39.13M – 45.43M] |
| lua-fastipc-zerocopy | 3. Medium messages (256B) | 41.87M [38.36M – 44.33M] | 42.53M [41.92M – 47.51M] |
| lua-fastipc-zerocopy | 4. Large messages (64KB) | 406.8K [337.9K – 435.6K] | 416.0K [410.5K – 428.2K] |
| lua-fastipc-zerocopy | 5. Large messages (512KB) | 21.8K [21.1K – 22.7K] | 57.8K [53.3K – 59.1K] |
| lua-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.8K [11.1K – 15.1K] | 15.1K [12.1K – 16.1K] |
| lua-rpc | 1. Tiny messages (16B) | 42.53M [38.78M – 46.94M] | 44.63M [43.01M – 47.74M] |
| lua-rpc | 2. Small messages (64B) | 34.29M [29.78M – 38.09M] | 33.66M [28.48M – 37.63M] |
| lua-rpc | 3. Medium messages (256B) | 28.80M [25.06M – 31.75M] | 28.58M [27.26M – 29.94M] |
| lua-rpc | 4. Large messages (64KB) | 373.1K [312.1K – 392.7K] | 433.3K [373.6K – 459.6K] |
| lua-rpc | 5. Large messages (512KB) | 56.3K [49.8K – 57.9K] | 57.4K [56.6K – 59.4K] |
| lua-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 12.7K [11.3K – 15.2K] | 15.2K [12.8K – 15.9K] |
| js-fastipc | 1. Tiny messages (16B) | 2.13M [1.88M – 2.16M] | 2.74M [2.60M – 2.79M] |
| js-fastipc | 2. Small messages (64B) | 2.08M [1.87M – 2.12M] | 2.64M [2.58M – 2.74M] |
| js-fastipc | 3. Medium messages (256B) | 2.07M [1.92M – 2.09M] | 2.62M [2.51M – 2.76M] |
| js-fastipc | 4. Large messages (64KB) | 379.1K [282.2K – 408.0K] | 395.7K [356.4K – 431.1K] |
| js-fastipc | 5. Large messages (512KB) | 53.5K [46.2K – 57.3K] | 57.6K [55.9K – 62.3K] |
| js-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 11.3K [9.1K – 11.8K] | 9.2K [8.1K – 10.3K] |
| js-fastipc-zerocopy | 1. Tiny messages (16B) | 555.6K [489.4K – 609.5K] | 760.5K [750.5K – 767.1K] |
| js-fastipc-zerocopy | 2. Small messages (64B) | 589.4K [460.8K – 606.9K] | 762.6K [756.5K – 783.3K] |
| js-fastipc-zerocopy | 3. Medium messages (256B) | 598.1K [497.9K – 607.2K] | 776.4K [750.0K – 780.5K] |
| js-fastipc-zerocopy | 4. Large messages (64KB) | 244.4K [191.9K – 262.8K] | 281.0K [258.0K – 295.2K] |
| js-fastipc-zerocopy | 5. Large messages (512KB) | 51.5K [40.4K – 56.0K] | 53.9K [49.1K – 58.5K] |
| js-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 8.6K [6.1K – 10.6K] | 7.7K [6.9K – 8.1K] |
| js-rpc | 1. Tiny messages (16B) | 1.78M [1.48M – 1.80M] | 2.31M [2.23M – 2.35M] |
| js-rpc | 2. Small messages (64B) | 1.78M [1.34M – 1.82M] | 2.42M [2.39M – 2.43M] |
| js-rpc | 3. Medium messages (256B) | 1.78M [1.48M – 1.85M] | 2.40M [2.38M – 2.42M] |
| js-rpc | 4. Large messages (64KB) | 331.2K [294.3K – 377.6K] | 353.7K [124.8K – 400.9K] |
| js-rpc | 5. Large messages (512KB) | 56.0K [50.0K – 56.9K] | 57.2K [32.8K – 58.8K] |
| js-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 11.1K [8.8K – 11.4K] | 9.5K [8.2K – 10.4K] |
| go-fastipc | 1. Tiny messages (16B) | 10.87M [10.64M – 11.05M] | 9.96M [9.76M – 10.06M] |
| go-fastipc | 2. Small messages (64B) | 11.04M [10.85M – 11.15M] | 9.22M [9.14M – 9.29M] |
| go-fastipc | 3. Medium messages (256B) | 11.22M [10.76M – 11.45M] | 8.65M [8.41M – 8.68M] |
| go-fastipc | 4. Large messages (64KB) | 435.1K [398.5K – 473.3K] | 475.1K [470.2K – 488.7K] |
| go-fastipc | 5. Large messages (512KB) | 56.2K [47.9K – 57.6K] | 57.0K [56.0K – 61.0K] |
| go-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.9K [17.1K – 19.0K] | 15.0K [13.6K – 16.5K] |
| go-fastipc-zerocopy | 1. Tiny messages (16B) | 8.35M [8.18M – 8.50M] | 5.77M [5.71M – 5.78M] |
| go-fastipc-zerocopy | 2. Small messages (64B) | 7.90M [7.55M – 8.07M] | 5.46M [5.31M – 5.54M] |
| go-fastipc-zerocopy | 3. Medium messages (256B) | 7.77M [7.69M – 7.88M] | 5.39M [5.26M – 5.45M] |
| go-fastipc-zerocopy | 4. Large messages (64KB) | 404.0K [344.7K – 414.0K] | 434.0K [400.5K – 463.5K] |
| go-fastipc-zerocopy | 5. Large messages (512KB) | 59.1K [56.3K – 60.2K] | 56.4K [55.7K – 58.6K] |
| go-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.3K [15.7K – 19.4K] | 16.2K [14.6K – 16.4K] |
| go-rpc | 1. Tiny messages (16B) | 10.63M [9.91M – 10.73M] | 9.73M [9.25M – 9.81M] |
| go-rpc | 2. Small messages (64B) | 10.56M [6.96M – 11.01M] | 9.97M [9.73M – 10.01M] |
| go-rpc | 3. Medium messages (256B) | 9.84M [7.49M – 11.56M] | 9.49M [9.43M – 9.68M] |
| go-rpc | 4. Large messages (64KB) | 451.5K [403.5K – 552.5K] | 448.1K [388.5K – 485.5K] |
| go-rpc | 5. Large messages (512KB) | 56.8K [53.4K – 60.8K] | 62.9K [56.6K – 64.7K] |
| go-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.3K [15.5K – 18.9K] | 15.7K [12.4K – 16.7K] |
| python-fastipc | 1. Tiny messages (16B) | 1.82M [1.78M – 1.83M] | 1.57M [1.56M – 1.59M] |
| python-fastipc | 2. Small messages (64B) | 1.75M [1.71M – 1.77M] | 1.59M [1.58M – 1.60M] |
| python-fastipc | 3. Medium messages (256B) | 1.78M [1.65M – 1.81M] | 1.58M [1.57M – 1.60M] |
| python-fastipc | 4. Large messages (64KB) | 344.4K [305.7K – 359.7K] | 368.6K [359.1K – 379.7K] |
| python-fastipc | 5. Large messages (512KB) | 50.7K [49.7K – 58.3K] | 57.9K [50.7K – 59.4K] |
| python-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.6K [11.6K – 14.2K] | 15.7K [14.7K – 16.3K] |
| python-fastipc-zerocopy | 1. Tiny messages (16B) | 1.06M [1.04M – 1.08M] | 1.05M [1.04M – 1.07M] |
| python-fastipc-zerocopy | 2. Small messages (64B) | 1.05M [1.01M – 1.06M] | 1.06M [1.00M – 1.08M] |
| python-fastipc-zerocopy | 3. Medium messages (256B) | 1.02M [980.7K – 1.04M] | 1.05M [1.03M – 1.06M] |
| python-fastipc-zerocopy | 4. Large messages (64KB) | 208.6K [188.3K – 232.5K] | 237.9K [217.1K – 244.1K] |
| python-fastipc-zerocopy | 5. Large messages (512KB) | 29.0K [12.2K – 32.3K] | 31.9K [30.3K – 33.1K] |
| python-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 12.9K [11.3K – 13.6K] | 14.8K [14.1K – 16.2K] |
| python-rpc | 1. Tiny messages (16B) | 983.6K [907.0K – 993.2K] | 979.0K [966.6K – 988.2K] |
| python-rpc | 2. Small messages (64B) | 979.6K [931.1K – 995.5K] | 1.01M [969.8K – 1.01M] |
| python-rpc | 3. Medium messages (256B) | 955.2K [938.1K – 988.4K] | 993.5K [931.8K – 1.02M] |
| python-rpc | 4. Large messages (64KB) | 210.4K [191.5K – 233.1K] | 229.4K [225.0K – 256.0K] |
| python-rpc | 5. Large messages (512KB) | 27.9K [14.1K – 28.7K] | 29.3K [29.1K – 31.0K] |
| python-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 2.8K [2.7K – 2.9K] | 6.7K [5.4K – 7.5K] |
| rust-fastipc | 1. Tiny messages (16B) | 39.46M [36.43M – 45.20M] | 52.51M [42.67M – 54.53M] |
| rust-fastipc | 2. Small messages (64B) | 40.06M [33.20M – 40.69M] | 45.11M [41.92M – 48.13M] |
| rust-fastipc | 3. Medium messages (256B) | 26.49M [24.85M – 29.05M] | 38.04M [33.61M – 39.17M] |
| rust-fastipc | 4. Large messages (64KB) | 467.1K [425.3K – 471.7K] | 510.2K [423.2K – 528.6K] |
| rust-fastipc | 5. Large messages (512KB) | 56.1K [52.8K – 58.0K] | 60.5K [51.7K – 62.9K] |
| rust-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.3K [10.0K – 14.2K] | 16.1K [13.4K – 16.9K] |
| rust-fastipc-zerocopy | 1. Tiny messages (16B) | 33.01M [30.87M – 33.32M] | 60.32M [51.21M – 63.46M] |
| rust-fastipc-zerocopy | 2. Small messages (64B) | 32.47M [31.28M – 33.49M] | 57.13M [44.93M – 58.77M] |
| rust-fastipc-zerocopy | 3. Medium messages (256B) | 24.64M [20.58M – 25.42M] | 35.98M [32.27M – 38.61M] |
| rust-fastipc-zerocopy | 4. Large messages (64KB) | 406.5K [383.1K – 426.2K] | 441.2K [368.6K – 490.6K] |
| rust-fastipc-zerocopy | 5. Large messages (512KB) | 51.7K [47.6K – 54.0K] | 58.9K [53.7K – 62.3K] |
| rust-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 13.5K [11.3K – 14.4K] | 17.1K [11.8K – 17.8K] |
| rust-rpc | 1. Tiny messages (16B) | 32.07M [30.40M – 35.42M] | 42.52M [36.43M – 44.47M] |
| rust-rpc | 2. Small messages (64B) | 31.09M [29.96M – 34.46M] | 31.79M [26.38M – 34.78M] |
| rust-rpc | 3. Medium messages (256B) | 22.55M [21.55M – 22.96M] | 26.99M [25.17M – 28.27M] |
| rust-rpc | 4. Large messages (64KB) | 427.7K [365.4K – 470.8K] | 500.4K [449.1K – 539.5K] |
| rust-rpc | 5. Large messages (512KB) | 55.4K [53.3K – 60.6K] | 60.9K [59.0K – 61.5K] |
| rust-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 14.3K [12.5K – 14.6K] | 16.2K [15.9K – 16.9K] |

\* macOS: the MacBook Air M1 of [`baseline.md`](baseline.md) ("Machine"), macOS 26.5.1, on AC power; different
hardware from the baseline machine. The same benchmarks with the warm-up, 5 runs round-robin of all 30, 2026-10-07,
`main` eeb6676 (the library unchanged since f5a96c1, with the stream pace; every benchmark loads `zig-out/bench`'s
ReleaseFast library), JavaScript on Node.js 26.10.0, Go 1.27.1, at a foreground QoS (devtool leaves macOS's background
state: [`baseline.md`](baseline.md), "Method"), held awake (`caffeinate -i`; pmset's log shows no sleep), the Linux VM
stopped. They replace a table of the same day without the warm-up: against it, every 1 MiB row is 1.1-2.8 times as
fast (only 10 messages are timed there, and the start of the transfer no longer is), Rust's 16 B rows 1.6-2.2 times
(the first case of a newly written executable: see above), C#'s 64 KiB-1 MiB rows 1.25-2.8 times (its JIT, warmed
up). JavaScript's copied 16-256 B messages run 0.8 times as fast (3.2-3.3M against 3.9-4.1M, each set of runs within
4%): its steady state, after the warm-up, is slower than a fresh process's first 2 million messages, most likely the
garbage collector (not measured). The other rows move within the noise (±20-30% at 16-256 B; Lua's RPC at 64 B
lands near 29M or near 40M). An earlier macOS table (2026-10-04/05) ran on the M1's efficiency cores, its processes
in macOS's background state. Not a gate: `bench-compare` reads only the table above.

The Zig benchmark (`bench/zig-api`, `zigapi-*`: the same loops through the native module `fastipc`, `Conn.recv` and
`Conn.rpcRecv` into a buffer it reuses, `Conn.acquire` / `Conn.recvAcquire` slices copied out, built ReleaseFast by
`zig build` into `zig-out/bench`). Its macOS and Linux VM rows are measured with the other benchmarks'.

The C, C++ and Zig zero-copy receivers copy each message out of the ring into a buffer they never read again, and
the compiler drops a copy into a buffer that is freed unread: until 2026-10-07 the C and C++ zero-copy receivers
(and, at first, the Zig one) copied nothing, so their zero-copy rows measured a consumer that doesn't look at its
messages, 1.5 to 1.8 times their copy rows at 64 KiB-512 KiB. One build kept its copy: the C++ receiver for Windows,
whose `bench_serve` calls `memcpy` (`loops.cpp` before the fix, compiled to assembly for x86_64-windows-gnu; for
x86_64-linux-gnu, and `loops.c` for either, it calls none), so its 64 KiB row (434.9K) was never inflated. An empty
`asm` statement now marks the buffer as used.
The macOS rows of c-fastipc-zerocopy, cpp-fastipc-zerocopy and zigapi-fastipc-zerocopy are measured with the copy (5
runs round-robin, 2026-10-07); the Windows and Linux rows above are too.

On the M1 a zero-copy consumer that copies its messages out with `memcpy` runs slower than the copying calls, whose
copy is the library's own (`ring.zig`'s `copyLong`): 64 KiB about 420K against 605K, 512 KiB 52K against 85K, also in
the Zig suite (64 KiB 434K against 653K) and in the Linux VM on the M1 (543K against 644K), but not on the baseline
machine's x86 (471K against 469K). The library's copy routine in the consumer's place closes the gap (64 KiB 612K):
measured 2026-10-07 with a scratch build of `bench/zig-api`, 1 KiB 25.3M against 18.8M, 16 KiB 2.20M against 1.67M,
4 KiB unchanged. Single-threaded, with the source in this core's cache, the two copies run at the same speed: what
differs is copying lines another core has just written. Where the buffers sit doesn't matter (every offset of either
side's buffer, 0 to 4 KiB, gave the same 64 KiB rate).

| Bench | Test | macOS\* median [min – max] |
|---|---|---|
| c-fastipc | 1. Tiny messages (16B) | 45.96M [40.21M – 55.25M] |
| c-fastipc | 2. Small messages (64B) | 55.93M [52.55M – 56.12M] |
| c-fastipc | 3. Medium messages (256B) | 46.66M [37.80M – 51.86M] |
| c-fastipc | 4. Large messages (64KB) | 644.7K [589.6K – 650.2K] |
| c-fastipc | 5. Large messages (512KB) | 85.9K [82.3K – 86.8K] |
| c-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.5K [19.8K – 21.7K] |
| c-fastipc-zerocopy | 1. Tiny messages (16B) | 55.05M [54.20M – 56.96M] |
| c-fastipc-zerocopy | 2. Small messages (64B) | 29.85M [29.26M – 30.52M] |
| c-fastipc-zerocopy | 3. Medium messages (256B) | 51.70M [51.46M – 52.46M] |
| c-fastipc-zerocopy | 4. Large messages (64KB) | 429.4K [411.2K – 435.4K] |
| c-fastipc-zerocopy | 5. Large messages (512KB) | 54.6K [52.2K – 55.0K] |
| c-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.6K [21.1K – 26.0K] |
| c-rpc | 1. Tiny messages (16B) | 72.68M [68.01M – 72.82M] |
| c-rpc | 2. Small messages (64B) | 56.71M [53.60M – 58.12M] |
| c-rpc | 3. Medium messages (256B) | 54.18M [53.78M – 54.42M] |
| c-rpc | 4. Large messages (64KB) | 639.0K [625.4K – 680.7K] |
| c-rpc | 5. Large messages (512KB) | 86.2K [85.7K – 87.3K] |
| c-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.6K [18.7K – 22.5K] |
| cpp-fastipc | 1. Tiny messages (16B) | 52.21M [47.82M – 55.84M] |
| cpp-fastipc | 2. Small messages (64B) | 53.49M [43.57M – 56.23M] |
| cpp-fastipc | 3. Medium messages (256B) | 49.69M [31.65M – 59.13M] |
| cpp-fastipc | 4. Large messages (64KB) | 638.2K [619.6K – 655.3K] |
| cpp-fastipc | 5. Large messages (512KB) | 86.1K [82.7K – 87.2K] |
| cpp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.2K [19.8K – 22.6K] |
| cpp-fastipc-zerocopy | 1. Tiny messages (16B) | 56.88M [53.74M – 58.00M] |
| cpp-fastipc-zerocopy | 2. Small messages (64B) | 47.90M [46.80M – 50.39M] |
| cpp-fastipc-zerocopy | 3. Medium messages (256B) | 51.83M [51.63M – 52.26M] |
| cpp-fastipc-zerocopy | 4. Large messages (64KB) | 430.5K [404.2K – 447.4K] |
| cpp-fastipc-zerocopy | 5. Large messages (512KB) | 55.6K [49.4K – 55.7K] |
| cpp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.6K [21.0K – 21.6K] |
| cpp-rpc | 1. Tiny messages (16B) | 76.88M [76.41M – 77.15M] |
| cpp-rpc | 2. Small messages (64B) | 53.34M [51.95M – 54.20M] |
| cpp-rpc | 3. Medium messages (256B) | 53.23M [52.21M – 53.98M] |
| cpp-rpc | 4. Large messages (64KB) | 651.0K [635.7K – 664.5K] |
| cpp-rpc | 5. Large messages (512KB) | 86.9K [82.5K – 87.4K] |
| cpp-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.1K [20.4K – 23.5K] |
| zigapi-fastipc | 1. Tiny messages (16B) | 49.08M [42.99M – 49.99M] |
| zigapi-fastipc | 2. Small messages (64B) | 54.98M [53.82M – 55.91M] |
| zigapi-fastipc | 3. Medium messages (256B) | 37.52M [32.29M – 56.23M] |
| zigapi-fastipc | 4. Large messages (64KB) | 643.9K [571.8K – 648.2K] |
| zigapi-fastipc | 5. Large messages (512KB) | 86.3K [79.3K – 87.6K] |
| zigapi-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.4K [18.3K – 23.3K] |
| zigapi-fastipc-zerocopy | 1. Tiny messages (16B) | 44.22M [43.07M – 47.76M] |
| zigapi-fastipc-zerocopy | 2. Small messages (64B) | 43.01M [29.02M – 44.42M] |
| zigapi-fastipc-zerocopy | 3. Medium messages (256B) | 49.01M [47.28M – 54.09M] |
| zigapi-fastipc-zerocopy | 4. Large messages (64KB) | 437.8K [377.4K – 449.5K] |
| zigapi-fastipc-zerocopy | 5. Large messages (512KB) | 53.1K [48.4K – 54.6K] |
| zigapi-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.7K [21.3K – 22.8K] |
| zigapi-rpc | 1. Tiny messages (16B) | 65.86M [59.47M – 69.22M] |
| zigapi-rpc | 2. Small messages (64B) | 50.39M [45.42M – 60.04M] |
| zigapi-rpc | 3. Medium messages (256B) | 53.80M [52.71M – 54.64M] |
| zigapi-rpc | 4. Large messages (64KB) | 658.2K [582.6K – 675.4K] |
| zigapi-rpc | 5. Large messages (512KB) | 85.7K [80.1K – 87.4K] |
| zigapi-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.7K [21.5K – 26.1K] |
| csharp-fastipc | 1. Tiny messages (16B) | 47.58M [46.28M – 48.48M] |
| csharp-fastipc | 2. Small messages (64B) | 37.92M [33.35M – 39.09M] |
| csharp-fastipc | 3. Medium messages (256B) | 35.07M [26.04M – 42.06M] |
| csharp-fastipc | 4. Large messages (64KB) | 643.0K [530.8K – 655.2K] |
| csharp-fastipc | 5. Large messages (512KB) | 74.7K [44.1K – 85.1K] |
| csharp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 23.7K [18.4K – 25.2K] |
| csharp-fastipc-zerocopy | 1. Tiny messages (16B) | 48.12M [39.20M – 50.19M] |
| csharp-fastipc-zerocopy | 2. Small messages (64B) | 43.72M [40.36M – 46.00M] |
| csharp-fastipc-zerocopy | 3. Medium messages (256B) | 35.53M [34.50M – 38.54M] |
| csharp-fastipc-zerocopy | 4. Large messages (64KB) | 630.0K [606.7K – 646.0K] |
| csharp-fastipc-zerocopy | 5. Large messages (512KB) | 86.2K [79.5K – 87.4K] |
| csharp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.2K [18.1K – 23.4K] |
| csharp-rpc | 1. Tiny messages (16B) | 58.74M [55.88M – 59.65M] |
| csharp-rpc | 2. Small messages (64B) | 43.93M [39.66M – 47.32M] |
| csharp-rpc | 3. Medium messages (256B) | 38.65M [35.72M – 38.99M] |
| csharp-rpc | 4. Large messages (64KB) | 615.7K [544.5K – 647.3K] |
| csharp-rpc | 5. Large messages (512KB) | 82.8K [68.9K – 85.6K] |
| csharp-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.4K [18.1K – 24.1K] |
| java-fastipc | 1. Tiny messages (16B) | 32.56M [31.21M – 33.58M] |
| java-fastipc | 2. Small messages (64B) | 28.01M [27.79M – 29.17M] |
| java-fastipc | 3. Medium messages (256B) | 24.16M [23.55M – 27.68M] |
| java-fastipc | 4. Large messages (64KB) | 618.4K [573.2K – 633.3K] |
| java-fastipc | 5. Large messages (512KB) | 84.1K [67.2K – 87.2K] |
| java-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 22.7K [17.3K – 25.9K] |
| java-fastipc-zerocopy | 1. Tiny messages (16B) | 22.74M [21.20M – 24.15M] |
| java-fastipc-zerocopy | 2. Small messages (64B) | 22.42M [21.15M – 22.64M] |
| java-fastipc-zerocopy | 3. Medium messages (256B) | 19.23M [18.76M – 20.70M] |
| java-fastipc-zerocopy | 4. Large messages (64KB) | 587.3K [581.6K – 589.3K] |
| java-fastipc-zerocopy | 5. Large messages (512KB) | 84.3K [80.1K – 87.1K] |
| java-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 22.9K [21.2K – 25.4K] |
| java-rpc | 1. Tiny messages (16B) | 32.21M [30.67M – 32.83M] |
| java-rpc | 2. Small messages (64B) | 28.87M [28.29M – 29.94M] |
| java-rpc | 3. Medium messages (256B) | 25.01M [24.12M – 27.85M] |
| java-rpc | 4. Large messages (64KB) | 619.5K [604.2K – 631.7K] |
| java-rpc | 5. Large messages (512KB) | 87.1K [81.7K – 87.8K] |
| java-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.7K [16.8K – 23.8K] |
| python-fastipc | 1. Tiny messages (16B) | 2.24M [2.06M – 2.33M] |
| python-fastipc | 2. Small messages (64B) | 2.22M [2.17M – 2.29M] |
| python-fastipc | 3. Medium messages (256B) | 2.14M [2.03M – 2.25M] |
| python-fastipc | 4. Large messages (64KB) | 457.1K [431.1K – 480.3K] |
| python-fastipc | 5. Large messages (512KB) | 80.2K [78.5K – 82.8K] |
| python-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.1K [20.3K – 22.8K] |
| python-fastipc-zerocopy | 1. Tiny messages (16B) | 1.37M [1.35M – 1.45M] |
| python-fastipc-zerocopy | 2. Small messages (64B) | 1.34M [1.29M – 1.39M] |
| python-fastipc-zerocopy | 3. Medium messages (256B) | 1.31M [1.25M – 1.39M] |
| python-fastipc-zerocopy | 4. Large messages (64KB) | 140.3K [139.1K – 143.3K] |
| python-fastipc-zerocopy | 5. Large messages (512KB) | 30.7K [29.7K – 31.3K] |
| python-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.5K [20.1K – 21.3K] |
| python-rpc | 1. Tiny messages (16B) | 1.26M [1.25M – 1.31M] |
| python-rpc | 2. Small messages (64B) | 1.25M [1.22M – 1.31M] |
| python-rpc | 3. Medium messages (256B) | 1.24M [1.16M – 1.28M] |
| python-rpc | 4. Large messages (64KB) | 178.7K [141.4K – 183.4K] |
| python-rpc | 5. Large messages (512KB) | 46.6K [45.5K – 47.2K] |
| python-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 15.1K [14.8K – 15.4K] |
| rust-fastipc | 1. Tiny messages (16B) | 50.01M [41.57M – 56.69M] |
| rust-fastipc | 2. Small messages (64B) | 53.72M [51.71M – 55.65M] |
| rust-fastipc | 3. Medium messages (256B) | 58.53M [50.10M – 58.75M] |
| rust-fastipc | 4. Large messages (64KB) | 647.1K [625.6K – 665.2K] |
| rust-fastipc | 5. Large messages (512KB) | 84.5K [83.4K – 87.1K] |
| rust-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.1K [17.3K – 22.4K] |
| rust-fastipc-zerocopy | 1. Tiny messages (16B) | 50.63M [50.20M – 51.11M] |
| rust-fastipc-zerocopy | 2. Small messages (64B) | 46.09M [38.64M – 51.41M] |
| rust-fastipc-zerocopy | 3. Medium messages (256B) | 48.23M [47.98M – 49.27M] |
| rust-fastipc-zerocopy | 4. Large messages (64KB) | 431.7K [400.7K – 453.1K] |
| rust-fastipc-zerocopy | 5. Large messages (512KB) | 53.9K [52.7K – 55.3K] |
| rust-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.6K [21.0K – 23.8K] |
| rust-rpc | 1. Tiny messages (16B) | 70.72M [69.05M – 72.41M] |
| rust-rpc | 2. Small messages (64B) | 48.10M [41.83M – 50.19M] |
| rust-rpc | 3. Medium messages (256B) | 52.29M [46.95M – 55.45M] |
| rust-rpc | 4. Large messages (64KB) | 651.1K [625.0K – 664.3K] |
| rust-rpc | 5. Large messages (512KB) | 87.2K [85.5K – 87.4K] |
| rust-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.3K [20.8K – 21.7K] |
| lua-fastipc | 1. Tiny messages (16B) | 57.58M [52.25M – 59.04M] |
| lua-fastipc | 2. Small messages (64B) | 48.44M [40.41M – 49.42M] |
| lua-fastipc | 3. Medium messages (256B) | 54.08M [34.58M – 62.24M] |
| lua-fastipc | 4. Large messages (64KB) | 605.7K [563.7K – 607.2K] |
| lua-fastipc | 5. Large messages (512KB) | 85.4K [83.5K – 86.1K] |
| lua-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 18.0K [17.8K – 23.6K] |
| lua-fastipc-zerocopy | 1. Tiny messages (16B) | 39.05M [38.12M – 39.79M] |
| lua-fastipc-zerocopy | 2. Small messages (64B) | 29.01M [28.73M – 34.61M] |
| lua-fastipc-zerocopy | 3. Medium messages (256B) | 36.48M [35.51M – 36.81M] |
| lua-fastipc-zerocopy | 4. Large messages (64KB) | 413.4K [383.0K – 417.5K] |
| lua-fastipc-zerocopy | 5. Large messages (512KB) | 51.6K [49.9K – 55.4K] |
| lua-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 16.4K [13.2K – 24.8K] |
| lua-rpc | 1. Tiny messages (16B) | 52.09M [49.50M – 52.37M] |
| lua-rpc | 2. Small messages (64B) | 29.96M [28.22M – 40.06M] |
| lua-rpc | 3. Medium messages (256B) | 41.29M [39.50M – 45.59M] |
| lua-rpc | 4. Large messages (64KB) | 568.8K [560.2K – 589.6K] |
| lua-rpc | 5. Large messages (512KB) | 83.8K [70.8K – 85.1K] |
| lua-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.4K [17.4K – 22.5K] |
| js-fastipc | 1. Tiny messages (16B) | 3.33M [3.26M – 3.33M] |
| js-fastipc | 2. Small messages (64B) | 3.30M [3.26M – 3.37M] |
| js-fastipc | 3. Medium messages (256B) | 3.22M [3.11M – 3.23M] |
| js-fastipc | 4. Large messages (64KB) | 526.8K [519.6K – 537.4K] |
| js-fastipc | 5. Large messages (512KB) | 81.3K [77.7K – 84.3K] |
| js-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.4K [17.0K – 18.0K] |
| js-fastipc-zerocopy | 1. Tiny messages (16B) | 957.4K [950.6K – 1.03M] |
| js-fastipc-zerocopy | 2. Small messages (64B) | 982.5K [948.2K – 1.00M] |
| js-fastipc-zerocopy | 3. Medium messages (256B) | 941.6K [895.0K – 970.4K] |
| js-fastipc-zerocopy | 4. Large messages (64KB) | 339.8K [331.4K – 381.8K] |
| js-fastipc-zerocopy | 5. Large messages (512KB) | 43.5K [41.4K – 45.4K] |
| js-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 15.6K [15.3K – 16.0K] |
| js-rpc | 1. Tiny messages (16B) | 2.97M [2.90M – 3.02M] |
| js-rpc | 2. Small messages (64B) | 2.99M [2.93M – 3.07M] |
| js-rpc | 3. Medium messages (256B) | 2.80M [2.74M – 2.87M] |
| js-rpc | 4. Large messages (64KB) | 495.9K [486.4K – 503.7K] |
| js-rpc | 5. Large messages (512KB) | 82.3K [76.4K – 83.8K] |
| js-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 17.0K [15.4K – 18.2K] |
| go-fastipc | 1. Tiny messages (16B) | 10.58M [10.21M – 11.05M] |
| go-fastipc | 2. Small messages (64B) | 9.30M [9.02M – 9.37M] |
| go-fastipc | 3. Medium messages (256B) | 9.41M [8.74M – 9.45M] |
| go-fastipc | 4. Large messages (64KB) | 595.9K [565.4K – 614.1K] |
| go-fastipc | 5. Large messages (512KB) | 85.4K [83.7K – 86.8K] |
| go-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.3K [20.5K – 23.5K] |
| go-fastipc-zerocopy | 1. Tiny messages (16B) | 5.70M [5.49M – 5.77M] |
| go-fastipc-zerocopy | 2. Small messages (64B) | 5.73M [5.59M – 5.83M] |
| go-fastipc-zerocopy | 3. Medium messages (256B) | 5.52M [5.39M – 5.72M] |
| go-fastipc-zerocopy | 4. Large messages (64KB) | 574.1K [563.1K – 581.4K] |
| go-fastipc-zerocopy | 5. Large messages (512KB) | 84.7K [80.2K – 85.8K] |
| go-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.7K [20.4K – 22.7K] |
| go-rpc | 1. Tiny messages (16B) | 10.12M [9.96M – 10.25M] |
| go-rpc | 2. Small messages (64B) | 9.03M [8.73M – 9.33M] |
| go-rpc | 3. Medium messages (256B) | 9.20M [8.81M – 9.32M] |
| go-rpc | 4. Large messages (64KB) | 598.3K [579.6K – 611.9K] |
| go-rpc | 5. Large messages (512KB) | 85.3K [84.3K – 86.6K] |
| go-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.4K [20.3K – 22.0K] |

&dagger; Linux VM: the same MacBook Air M1, Linux in a Lima 2.2.1 VM (Apple Virtualization framework): Ubuntu 24.04.5
arm64, kernel 6.8, glibc 2.39, 4 vCPUs, 4 GB; everything built for generic ARMv8.0-A. The same benchmarks with the
warm-up, 5 runs round-robin of all 30, 2026-10-07, `main` eeb6676, JavaScript on Node.js 22.23.3, Go 1.27.1, the Mac held
awake and otherwise idle. Against the macOS\* rows it changes the operating system on the same hardware; against
Linux above, the hardware under the same operating system. Not a gate.

| Bench | Test | Linux VM&dagger; median [min – max] |
|---|---|---|
| c-fastipc | 1. Tiny messages (16B) | 48.99M [48.31M – 51.85M] |
| c-fastipc | 2. Small messages (64B) | 56.18M [53.01M – 57.15M] |
| c-fastipc | 3. Medium messages (256B) | 55.81M [50.11M – 56.22M] |
| c-fastipc | 4. Large messages (64KB) | 652.8K [508.8K – 664.3K] |
| c-fastipc | 5. Large messages (512KB) | 84.1K [82.7K – 85.8K] |
| c-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.4K [21.1K – 22.3K] |
| c-fastipc-zerocopy | 1. Tiny messages (16B) | 39.64M [37.76M – 39.76M] |
| c-fastipc-zerocopy | 2. Small messages (64B) | 38.36M [35.69M – 38.63M] |
| c-fastipc-zerocopy | 3. Medium messages (256B) | 34.14M [33.45M – 34.78M] |
| c-fastipc-zerocopy | 4. Large messages (64KB) | 656.3K [513.1K – 661.8K] |
| c-fastipc-zerocopy | 5. Large messages (512KB) | 85.5K [78.0K – 85.7K] |
| c-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.2K [21.1K – 21.9K] |
| c-rpc | 1. Tiny messages (16B) | 44.50M [42.64M – 45.92M] |
| c-rpc | 2. Small messages (64B) | 42.27M [38.62M – 44.64M] |
| c-rpc | 3. Medium messages (256B) | 43.34M [40.52M – 44.49M] |
| c-rpc | 4. Large messages (64KB) | 655.8K [620.3K – 665.8K] |
| c-rpc | 5. Large messages (512KB) | 83.9K [82.1K – 85.7K] |
| c-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.4K [21.2K – 25.9K] |
| cpp-fastipc | 1. Tiny messages (16B) | 49.52M [46.38M – 50.63M] |
| cpp-fastipc | 2. Small messages (64B) | 56.52M [32.87M – 59.06M] |
| cpp-fastipc | 3. Medium messages (256B) | 56.90M [54.92M – 57.13M] |
| cpp-fastipc | 4. Large messages (64KB) | 663.1K [657.3K – 673.8K] |
| cpp-fastipc | 5. Large messages (512KB) | 85.4K [84.5K – 86.0K] |
| cpp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.3K [21.2K – 21.5K] |
| cpp-fastipc-zerocopy | 1. Tiny messages (16B) | 37.55M [37.10M – 37.75M] |
| cpp-fastipc-zerocopy | 2. Small messages (64B) | 37.04M [35.54M – 37.26M] |
| cpp-fastipc-zerocopy | 3. Medium messages (256B) | 31.94M [31.07M – 32.19M] |
| cpp-fastipc-zerocopy | 4. Large messages (64KB) | 651.1K [642.5K – 671.9K] |
| cpp-fastipc-zerocopy | 5. Large messages (512KB) | 85.5K [84.4K – 85.7K] |
| cpp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.2K [19.7K – 22.6K] |
| cpp-rpc | 1. Tiny messages (16B) | 49.06M [45.50M – 52.16M] |
| cpp-rpc | 2. Small messages (64B) | 44.67M [38.20M – 49.10M] |
| cpp-rpc | 3. Medium messages (256B) | 54.66M [46.28M – 56.09M] |
| cpp-rpc | 4. Large messages (64KB) | 646.3K [542.7K – 657.4K] |
| cpp-rpc | 5. Large messages (512KB) | 85.7K [85.2K – 86.3K] |
| cpp-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.3K [20.9K – 22.3K] |
| zigapi-fastipc | 1. Tiny messages (16B) | 49.10M [42.91M – 50.22M] |
| zigapi-fastipc | 2. Small messages (64B) | 59.25M [58.95M – 59.85M] |
| zigapi-fastipc | 3. Medium messages (256B) | 57.05M [56.97M – 58.17M] |
| zigapi-fastipc | 4. Large messages (64KB) | 659.4K [634.5K – 672.5K] |
| zigapi-fastipc | 5. Large messages (512KB) | 85.2K [84.2K – 85.7K] |
| zigapi-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.2K [20.9K – 23.2K] |
| zigapi-fastipc-zerocopy | 1. Tiny messages (16B) | 43.20M [35.28M – 43.72M] |
| zigapi-fastipc-zerocopy | 2. Small messages (64B) | 31.53M [30.98M – 35.84M] |
| zigapi-fastipc-zerocopy | 3. Medium messages (256B) | 25.04M [23.23M – 28.00M] |
| zigapi-fastipc-zerocopy | 4. Large messages (64KB) | 553.9K [529.8K – 561.3K] |
| zigapi-fastipc-zerocopy | 5. Large messages (512KB) | 64.8K [60.0K – 69.4K] |
| zigapi-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.6K [21.0K – 23.1K] |
| zigapi-rpc | 1. Tiny messages (16B) | 54.11M [32.66M – 56.11M] |
| zigapi-rpc | 2. Small messages (64B) | 51.21M [35.96M – 54.91M] |
| zigapi-rpc | 3. Medium messages (256B) | 53.39M [44.53M – 53.94M] |
| zigapi-rpc | 4. Large messages (64KB) | 650.3K [633.2K – 665.6K] |
| zigapi-rpc | 5. Large messages (512KB) | 85.8K [85.0K – 86.4K] |
| zigapi-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.4K [21.1K – 21.4K] |
| csharp-fastipc | 1. Tiny messages (16B) | 44.94M [43.70M – 49.03M] |
| csharp-fastipc | 2. Small messages (64B) | 45.81M [43.97M – 48.99M] |
| csharp-fastipc | 3. Medium messages (256B) | 41.23M [40.54M – 46.15M] |
| csharp-fastipc | 4. Large messages (64KB) | 595.7K [189.3K – 659.8K] |
| csharp-fastipc | 5. Large messages (512KB) | 79.0K [77.3K – 81.1K] |
| csharp-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.3K [19.0K – 20.0K] |
| csharp-fastipc-zerocopy | 1. Tiny messages (16B) | 47.95M [30.14M – 48.25M] |
| csharp-fastipc-zerocopy | 2. Small messages (64B) | 34.92M [27.51M – 36.40M] |
| csharp-fastipc-zerocopy | 3. Medium messages (256B) | 38.74M [25.17M – 41.46M] |
| csharp-fastipc-zerocopy | 4. Large messages (64KB) | 664.7K [603.6K – 676.6K] |
| csharp-fastipc-zerocopy | 5. Large messages (512KB) | 82.2K [81.7K – 82.8K] |
| csharp-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.3K [18.0K – 20.3K] |
| csharp-rpc | 1. Tiny messages (16B) | 63.81M [61.18M – 63.98M] |
| csharp-rpc | 2. Small messages (64B) | 45.22M [43.67M – 45.39M] |
| csharp-rpc | 3. Medium messages (256B) | 48.37M [46.62M – 50.60M] |
| csharp-rpc | 4. Large messages (64KB) | 652.8K [601.4K – 665.8K] |
| csharp-rpc | 5. Large messages (512KB) | 80.4K [79.0K – 81.3K] |
| csharp-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 18.8K [17.9K – 19.7K] |
| java-fastipc | 1. Tiny messages (16B) | 27.69M [25.15M – 34.68M] |
| java-fastipc | 2. Small messages (64B) | 33.00M [31.43M – 33.67M] |
| java-fastipc | 3. Medium messages (256B) | 25.99M [25.71M – 26.71M] |
| java-fastipc | 4. Large messages (64KB) | 625.4K [587.3K – 635.2K] |
| java-fastipc | 5. Large messages (512KB) | 85.4K [83.4K – 86.0K] |
| java-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.4K [21.1K – 23.7K] |
| java-fastipc-zerocopy | 1. Tiny messages (16B) | 21.42M [18.26M – 23.29M] |
| java-fastipc-zerocopy | 2. Small messages (64B) | 23.88M [23.02M – 25.05M] |
| java-fastipc-zerocopy | 3. Medium messages (256B) | 22.58M [22.41M – 23.89M] |
| java-fastipc-zerocopy | 4. Large messages (64KB) | 590.0K [586.2K – 602.7K] |
| java-fastipc-zerocopy | 5. Large messages (512KB) | 79.6K [73.8K – 85.4K] |
| java-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.8K [20.2K – 24.5K] |
| java-rpc | 1. Tiny messages (16B) | 33.78M [33.31M – 35.06M] |
| java-rpc | 2. Small messages (64B) | 24.45M [22.47M – 28.10M] |
| java-rpc | 3. Medium messages (256B) | 25.63M [24.79M – 27.18M] |
| java-rpc | 4. Large messages (64KB) | 630.3K [606.0K – 632.4K] |
| java-rpc | 5. Large messages (512KB) | 84.7K [82.0K – 85.7K] |
| java-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.8K [19.8K – 22.9K] |
| python-fastipc | 1. Tiny messages (16B) | 1.25M [1.21M – 1.27M] |
| python-fastipc | 2. Small messages (64B) | 1.24M [1.21M – 1.27M] |
| python-fastipc | 3. Medium messages (256B) | 1.23M [1.20M – 1.26M] |
| python-fastipc | 4. Large messages (64KB) | 400.3K [385.7K – 414.9K] |
| python-fastipc | 5. Large messages (512KB) | 76.7K [72.1K – 78.2K] |
| python-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.3K [20.2K – 20.8K] |
| python-fastipc-zerocopy | 1. Tiny messages (16B) | 811.9K [803.4K – 821.0K] |
| python-fastipc-zerocopy | 2. Small messages (64B) | 803.0K [793.1K – 831.5K] |
| python-fastipc-zerocopy | 3. Medium messages (256B) | 804.6K [775.7K – 826.6K] |
| python-fastipc-zerocopy | 4. Large messages (64KB) | 234.0K [220.3K – 235.1K] |
| python-fastipc-zerocopy | 5. Large messages (512KB) | 40.3K [40.0K – 40.5K] |
| python-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.9K [17.6K – 22.7K] |
| python-rpc | 1. Tiny messages (16B) | 785.1K [758.6K – 799.8K] |
| python-rpc | 2. Small messages (64B) | 799.4K [770.0K – 809.6K] |
| python-rpc | 3. Medium messages (256B) | 793.7K [774.8K – 806.2K] |
| python-rpc | 4. Large messages (64KB) | 234.9K [218.0K – 238.1K] |
| python-rpc | 5. Large messages (512KB) | 39.4K [37.6K – 39.7K] |
| python-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 8.4K [8.2K – 8.6K] |
| rust-fastipc | 1. Tiny messages (16B) | 49.66M [46.73M – 50.74M] |
| rust-fastipc | 2. Small messages (64B) | 57.13M [55.12M – 57.79M] |
| rust-fastipc | 3. Medium messages (256B) | 56.40M [39.35M – 57.55M] |
| rust-fastipc | 4. Large messages (64KB) | 672.5K [656.2K – 675.2K] |
| rust-fastipc | 5. Large messages (512KB) | 85.9K [82.2K – 86.1K] |
| rust-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 22.4K [21.0K – 22.9K] |
| rust-fastipc-zerocopy | 1. Tiny messages (16B) | 37.28M [33.85M – 37.31M] |
| rust-fastipc-zerocopy | 2. Small messages (64B) | 35.71M [35.03M – 36.10M] |
| rust-fastipc-zerocopy | 3. Medium messages (256B) | 31.60M [29.87M – 32.13M] |
| rust-fastipc-zerocopy | 4. Large messages (64KB) | 664.3K [647.1K – 683.7K] |
| rust-fastipc-zerocopy | 5. Large messages (512KB) | 83.0K [82.3K – 85.5K] |
| rust-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.1K [20.7K – 21.3K] |
| rust-rpc | 1. Tiny messages (16B) | 55.14M [54.61M – 55.67M] |
| rust-rpc | 2. Small messages (64B) | 46.57M [44.62M – 48.83M] |
| rust-rpc | 3. Medium messages (256B) | 53.14M [52.51M – 54.58M] |
| rust-rpc | 4. Large messages (64KB) | 659.6K [597.5K – 676.3K] |
| rust-rpc | 5. Large messages (512KB) | 85.8K [80.7K – 86.3K] |
| rust-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 21.2K [20.4K – 22.9K] |
| lua-fastipc | 1. Tiny messages (16B) | 51.08M [47.81M – 54.34M] |
| lua-fastipc | 2. Small messages (64B) | 53.89M [52.66M – 56.20M] |
| lua-fastipc | 3. Medium messages (256B) | 48.05M [46.67M – 49.13M] |
| lua-fastipc | 4. Large messages (64KB) | 560.6K [549.4K – 601.0K] |
| lua-fastipc | 5. Large messages (512KB) | 80.8K [79.2K – 81.4K] |
| lua-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 19.8K [17.5K – 21.2K] |
| lua-fastipc-zerocopy | 1. Tiny messages (16B) | 47.83M [46.47M – 48.13M] |
| lua-fastipc-zerocopy | 2. Small messages (64B) | 48.03M [44.80M – 48.31M] |
| lua-fastipc-zerocopy | 3. Medium messages (256B) | 43.57M [41.93M – 44.04M] |
| lua-fastipc-zerocopy | 4. Large messages (64KB) | 585.6K [500.0K – 600.2K] |
| lua-fastipc-zerocopy | 5. Large messages (512KB) | 73.2K [71.4K – 75.3K] |
| lua-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 16.8K [15.6K – 18.8K] |
| lua-rpc | 1. Tiny messages (16B) | 42.27M [40.32M – 47.47M] |
| lua-rpc | 2. Small messages (64B) | 31.03M [30.30M – 42.79M] |
| lua-rpc | 3. Medium messages (256B) | 43.32M [41.34M – 44.80M] |
| lua-rpc | 4. Large messages (64KB) | 533.3K [463.8K – 604.7K] |
| lua-rpc | 5. Large messages (512KB) | 79.7K [74.1K – 81.2K] |
| lua-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 18.4K [15.1K – 19.4K] |
| js-fastipc | 1. Tiny messages (16B) | 2.17M [2.12M – 2.24M] |
| js-fastipc | 2. Small messages (64B) | 1.98M [1.94M – 2.21M] |
| js-fastipc | 3. Medium messages (256B) | 2.06M [1.89M – 2.30M] |
| js-fastipc | 4. Large messages (64KB) | 250.5K [229.0K – 273.1K] |
| js-fastipc | 5. Large messages (512KB) | 74.5K [53.4K – 80.5K] |
| js-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 10.2K [9.3K – 10.4K] |
| js-fastipc-zerocopy | 1. Tiny messages (16B) | 636.1K [622.3K – 654.7K] |
| js-fastipc-zerocopy | 2. Small messages (64B) | 623.2K [588.2K – 683.4K] |
| js-fastipc-zerocopy | 3. Medium messages (256B) | 639.9K [635.8K – 655.4K] |
| js-fastipc-zerocopy | 4. Large messages (64KB) | 222.5K [208.4K – 229.4K] |
| js-fastipc-zerocopy | 5. Large messages (512KB) | 72.5K [47.7K – 74.2K] |
| js-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 5.6K [5.4K – 6.0K] |
| js-rpc | 1. Tiny messages (16B) | 1.81M [1.61M – 1.94M] |
| js-rpc | 2. Small messages (64B) | 1.87M [1.65M – 1.93M] |
| js-rpc | 3. Medium messages (256B) | 1.86M [1.72M – 1.92M] |
| js-rpc | 4. Large messages (64KB) | 203.9K [116.7K – 225.3K] |
| js-rpc | 5. Large messages (512KB) | 77.7K [60.1K – 78.9K] |
| js-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 10.2K [10.1K – 10.5K] |
| go-fastipc | 1. Tiny messages (16B) | 10.94M [10.81M – 11.01M] |
| go-fastipc | 2. Small messages (64B) | 9.53M [9.34M – 9.57M] |
| go-fastipc | 3. Medium messages (256B) | 9.67M [9.63M – 9.76M] |
| go-fastipc | 4. Large messages (64KB) | 614.1K [593.4K – 633.4K] |
| go-fastipc | 5. Large messages (512KB) | 82.1K [81.0K – 83.9K] |
| go-fastipc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.2K [20.0K – 20.4K] |
| go-fastipc-zerocopy | 1. Tiny messages (16B) | 5.86M [5.78M – 5.93M] |
| go-fastipc-zerocopy | 2. Small messages (64B) | 5.86M [5.71M – 5.88M] |
| go-fastipc-zerocopy | 3. Medium messages (256B) | 5.68M [4.43M – 5.76M] |
| go-fastipc-zerocopy | 4. Large messages (64KB) | 586.5K [554.7K – 587.5K] |
| go-fastipc-zerocopy | 5. Large messages (512KB) | 83.9K [83.6K – 84.3K] |
| go-fastipc-zerocopy | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.7K [20.0K – 21.5K] |
| go-rpc | 1. Tiny messages (16B) | 10.29M [10.14M – 10.59M] |
| go-rpc | 2. Small messages (64B) | 9.35M [9.11M – 9.46M] |
| go-rpc | 3. Medium messages (256B) | 9.36M [9.33M – 9.55M] |
| go-rpc | 4. Large messages (64KB) | 591.2K [562.6K – 616.5K] |
| go-rpc | 5. Large messages (512KB) | 81.7K [81.1K – 83.9K] |
| go-rpc | 6. Exceeds buffer (1MB msg, 512KB buffer) | 20.4K [19.8K – 21.4K] |

## The C# binding's two layers

`python devtool.py bench csharp-layers` (`bench/csharp/FastIpc.Bench/Layers.cs`) compares the raw layer (`Fipc`,
IntPtr handles) with the object layer (`FipcConnection`, whose calls each hold a SafeHandle) in one process and one
thread: a batch of messages that fits the ring is sent and then received, so no call waits, and 21 rounds alternate
between the layers. ns per message, the median round: a send and a receive, 2 calls (zero-copy: acquire, commit,
acquire, release, 4 calls). Not gated; one run on each OS of the baseline machine.

| Case | Windows raw | Windows object | Difference | Linux raw | Linux object | Difference |
|---|---|---|---|---|---|---|
| copy 16 B | 42.1 | 80.2 | +38.1 | 38.4 | 71.8 | +33.3 |
| copy 64 B | 43.3 | 77.9 | +34.6 | 38.6 | 70.0 | +31.4 |
| copy 1 KiB | 60.1 | 94.9 | +34.8 | 56.3 | 82.0 | +25.7 |
| zero-copy 16 B | 45.0 | 91.6 | +46.6 | 40.4 | 82.9 | +42.5 |
| zero-copy 64 B | 49.5 | 82.1 | +32.6 | 43.4 | 77.0 | +33.6 |
| zero-copy 1 KiB | 68.2 | 101.2 | +33.0 | 61.9 | 91.3 | +29.4 |
| rpc 16 B | 56.4 | 89.4 | +33.0 | 48.8 | 80.0 | +31.2 |
| rpc 64 B | 56.7 | 88.8 | +32.0 | 50.7 | 80.8 | +30.1 |
| rpc 1 KiB | 77.3 | 109.8 | +32.5 | 73.3 | 99.6 | +26.3 |

The object layer costs about 30 ns per message, 15 ns per copying call, on both OSes: nearly all of it the
SafeHandle's reference count, two atomic compare-and-swap loops per call (`DangerousAddRef` and `DangerousRelease`;
the pair alone measures 14.5 ns on Windows). A zero-copy message pays the same: its reservation holds one reference
from acquire to commit, and its received message one from acquire to release. Against a message's whole trip between
two processes (the table above: 32-40M messages/s on one side is 25-31 ns per message) the cost is real for tiny
messages, so the raw layer stays the escape hatch for the hottest loops; the object layer is the default for its
safety (no call ever runs on a closed handle).
