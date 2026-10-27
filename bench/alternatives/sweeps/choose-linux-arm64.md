## linux-arm64

| Library | Pattern | Chosen | 64 KiB | 512 KiB | 4 MiB | 32 MiB |
|---|---|---|---|---|---|---|
| fastipc | stream | 512 KiB | 0.53 | 1.00 | 1.00 | 0.88 |
| fastipc | broadcast | 32 MiB | 0.15 | 0.46 | 0.54 | 1.00 |
| fastipc | queue | 512 KiB | 0.70 | 0.97 | 1.00 | 1.00 |
| fastipc | service | 64 KiB | 1.00 | 0.96 | 0.95 | 0.86 |
| fastipc-zerocopy | stream | 512 KiB | - | 0.99 | 1.00 | 0.88 |
| fastipc-zerocopy | broadcast | 4 MiB | 0.16 | 0.58 | 0.98 | 1.00 |
| fastipc-zerocopy | queue | 512 KiB | 0.94 | 0.99 | 0.98 | 1.00 |
| fastipc-zerocopy | service | 64 KiB | 0.97 | 0.95 | 1.00 | 0.96 |
| iceoryx2 | stream | 4 MiB | 0.87 | 0.88 | 1.00 | 0.90 |
| iceoryx2 | broadcast | 32 MiB | 0.23 | 0.56 | 0.84 | 1.00 |
| iceoryx2 | service | 64 KiB | 1.00 | 0.89 | 0.93 | 0.90 |
| iceoryx2-cxx | stream | 64 KiB | 0.97 | 0.95 | 1.00 | 0.98 |
| iceoryx2-cxx | broadcast | 32 MiB | 0.26 | 0.53 | 0.82 | 1.00 |
| iceoryx2-cxx | service | 64 KiB | 0.96 | 1.00 | 0.88 | 0.77 |
| aeron (backoff conductor, 16 fragments) | stream |  | - | 0.66 | 0.84 | 0.74 |
| aeron (backoff conductor, 1 fragment) | stream | 512 KiB | - | 0.96 | 1.00 | 0.92 |
| aeron (backoff conductor, 4 fragments) | stream |  | - | 0.77 | 0.86 | 0.76 |
| aeron (backoff conductor, 64 fragments) | stream |  | - | 0.64 | 0.87 | 0.78 |
| aeron (spin conductor, 16 fragments) | stream |  | - | 0.50 | 0.49 | 0.44 |
| aeron (backoff conductor, 16 fragments) | broadcast |  | 0.00 | 0.04 | 0.27 | 0.94 |
| aeron (backoff conductor, 1 fragment) | broadcast |  | 0.00 | 0.04 | 0.28 | 0.77 |
| aeron (backoff conductor, 4 fragments) | broadcast | 32 MiB | 0.00 | 0.04 | 0.27 | 1.00 |
| aeron (backoff conductor, 64 fragments) | broadcast |  | 0.01 | 0.04 | 0.30 | 0.90 |
| aeron (spin conductor, 16 fragments) | broadcast |  | 0.00 | 0.02 | 0.18 | 0.77 |
| zeromq | stream | 512 KiB | 0.90 | 1.00 | 0.94 | 0.97 |
| zeromq | broadcast | 512 KiB | 0.46 | 1.00 | 0.94 | 0.98 |
| zeromq | queue | 512 KiB | 0.87 | 0.96 | 0.90 | 1.00 |
| zeromq | service | 64 KiB | 0.99 | 1.00 | 0.99 | 1.00 |
| zeromq-tcp | stream | 512 KiB | 0.91 | 1.00 | 1.00 | 0.95 |
| zeromq-tcp | broadcast | 32 MiB | 0.42 | 0.93 | 0.94 | 1.00 |
| zeromq-tcp | queue | 512 KiB | 0.87 | 1.00 | 0.98 | 0.98 |
| zeromq-tcp | service | 64 KiB | 0.98 | 1.00 | 1.00 | 1.00 |
| nng | stream | 64 KiB | 1.00 | - | - | - |
| nng | broadcast | 64 KiB | 1.00 | - | - | - |
| nng | queue | 64 KiB | 1.00 | - | - | - |
| nng | service | 64 KiB | 1.00 | - | - | - |
| nng-tcp | stream | 64 KiB | 1.00 | - | - | - |
| nng-tcp | broadcast | 64 KiB | 1.00 | - | - | - |
| nng-tcp | queue | 64 KiB | 1.00 | - | - | - |
| nng-tcp | service | 64 KiB | 1.00 | - | - | - |
| cpp-ipc | stream | (not settable) | 0.97 | 1.00 | 0.97 | 0.97 |
| cpp-ipc | broadcast | (not settable) | 1.00 | 0.99 | 0.99 | 0.99 |
| boost | stream | 4 MiB | 0.78 | 0.91 | 0.96 | 1.00 |
| boost | queue | 32 MiB | 0.61 | 0.82 | 0.85 | 1.00 |
| unix-socket | stream | (not settable) | 1.00 | 1.00 | 1.00 | 0.99 |
| tcp-loopback | stream | (not settable) | 0.99 | 1.00 | 0.99 | 0.99 |

"linux-arm64": {
    "fastipc": {"stream": 512 * KiB, "broadcast": 32 * MiB, "queue": 512 * KiB, "service": 64 * KiB},
    "fastipc-zerocopy": {"stream": 512 * KiB, "broadcast": 4 * MiB, "queue": 512 * KiB, "service": 64 * KiB},
    "iceoryx2": {"stream": 4 * MiB, "broadcast": 32 * MiB, "service": 64 * KiB},
    "iceoryx2-cxx": {"stream": 64 * KiB, "broadcast": 32 * MiB, "service": 64 * KiB},
    "aeron": {"stream": 512 * KiB, "broadcast": 32 * MiB},
    "zeromq": {"stream": 512 * KiB, "broadcast": 512 * KiB, "queue": 512 * KiB, "service": 64 * KiB},
    "zeromq-tcp": {"stream": 512 * KiB, "broadcast": 32 * MiB, "queue": 512 * KiB, "service": 64 * KiB},
    "nng": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
    "nng-tcp": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
    "boost": {"stream": 4 * MiB, "queue": 32 * MiB},
},
AERON_CONDUCTOR["linux-arm64"] = "backoff"
AERON_FRAGMENT_LIMIT["linux-arm64"] = {"stream": 1, "broadcast": 4}

Aeron's broadcast fragment limit stays 16, not the 4 printed above: the sweep's broadcast cases are the 4-subscriber
ones only, and a direct check (`run.py --libs aeron --cases broadcast`, the lossless cases, 3 alternating rounds of
limits 16, 4 and 1) gave 16 the best geometric mean over the four published lossless broadcasts (16: 21.8M, 4: 20.3M,
1: 20.1M msg/s), ahead on the 4-subscriber cases too (16 B: 43.1M vs 38.9M; 1 KiB: 6.93M vs 6.61M).
