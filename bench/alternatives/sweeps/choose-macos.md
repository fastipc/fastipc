## macos

| Library | Pattern | Chosen | 64 KiB | 512 KiB | 4 MiB | 32 MiB |
|---|---|---|---|---|---|---|
| fastipc | stream | 512 KiB | 0.63 | 0.99 | 1.00 | 0.95 |
| fastipc | broadcast | 32 MiB | 0.37 | 0.54 | 0.68 | 1.00 |
| fastipc | queue | 4 MiB | 0.74 | 0.91 | 1.00 | 0.90 |
| fastipc | service | 64 KiB | 1.00 | 0.86 | 0.97 | 0.94 |
| fastipc-zerocopy | stream | 512 KiB | - | 1.00 | 0.96 | 0.93 |
| fastipc-zerocopy | broadcast | 32 MiB | 0.49 | 0.55 | 0.67 | 1.00 |
| fastipc-zerocopy | queue | 4 MiB | 0.72 | 0.90 | 1.00 | 0.92 |
| fastipc-zerocopy | service | 64 KiB | 1.00 | 0.87 | 0.95 | 0.94 |
| iceoryx2 | stream | 64 KiB | 0.98 | 1.00 | 0.96 | 0.92 |
| iceoryx2 | broadcast | 64 KiB | 0.97 | 0.92 | 1.00 | 0.98 |
| iceoryx2-cxx | stream | 64 KiB | 1.00 | 1.00 | 0.93 | 0.93 |
| iceoryx2-cxx | broadcast | 64 KiB | 1.00 | 0.93 | 0.96 | 0.94 |
| aeron (backoff conductor, 16 fragments) | stream |  | - | 0.81 | 0.81 | 0.75 |
| aeron (backoff conductor, 1 fragment) | stream |  | - | 0.85 | 0.87 | 0.77 |
| aeron (backoff conductor, 4 fragments) | stream | 512 KiB | - | 1.00 | 0.78 | 0.72 |
| aeron (backoff conductor, 64 fragments) | stream |  | - | 0.63 | 0.85 | 0.75 |
| aeron (spin conductor, 16 fragments) | stream |  | - | 0.44 | 0.41 | 0.39 |
| aeron (backoff conductor, 16 fragments) | broadcast |  | 0.22 | 0.28 | 0.45 | 0.70 |
| aeron (backoff conductor, 1 fragment) | broadcast | 32 MiB | 0.31 | 0.45 | 0.70 | 1.00 |
| aeron (backoff conductor, 4 fragments) | broadcast |  | 0.24 | 0.28 | 0.45 | 0.64 |
| aeron (backoff conductor, 64 fragments) | broadcast |  | 0.22 | 0.25 | 0.42 | 0.60 |
| aeron (spin conductor, 16 fragments) | broadcast |  | 0.20 | 0.17 | 0.21 | 0.20 |
| zeromq | stream | 512 KiB | 0.89 | 1.00 | 0.99 | 0.99 |
| zeromq | broadcast | 4 MiB | 0.48 | 0.95 | 0.96 | 1.00 |
| zeromq | queue | 32 MiB | 0.82 | 0.94 | 0.92 | 1.00 |
| zeromq | service | 64 KiB | 0.97 | 1.00 | 0.97 | 0.95 |
| zeromq-tcp | stream | 512 KiB | 0.82 | 0.98 | 1.00 | 0.98 |
| zeromq-tcp | broadcast | 32 MiB | 0.45 | 0.89 | 0.85 | 1.00 |
| zeromq-tcp | queue | 512 KiB | 0.94 | 1.00 | 0.92 | 0.85 |
| zeromq-tcp | service | 64 KiB | 0.96 | 1.00 | 0.98 | 0.96 |
| nng | stream | 64 KiB | 1.00 | - | - | - |
| nng | broadcast | 64 KiB | 1.00 | - | - | - |
| nng | queue | 64 KiB | 1.00 | - | - | - |
| nng | service | 64 KiB | 1.00 | - | - | - |
| nng-tcp | stream | 64 KiB | 1.00 | - | - | - |
| nng-tcp | broadcast | 64 KiB | 1.00 | - | - | - |
| nng-tcp | queue | 64 KiB | 1.00 | - | - | - |
| nng-tcp | service | 64 KiB | 1.00 | - | - | - |
| boost | stream | 32 MiB | 0.84 | 0.86 | 0.82 | 1.00 |
| boost | queue | 32 MiB | 0.29 | 0.28 | 0.40 | 1.00 |
| unix-socket | stream | (not settable) | 1.00 | 0.97 | 0.93 | 0.97 |
| tcp-loopback | stream | (not settable) | 1.00 | 0.95 | 0.98 | 0.99 |

"macos": {
    "fastipc": {"stream": 512 * KiB, "broadcast": 32 * MiB, "queue": 4 * MiB, "service": 64 * KiB},
    "fastipc-zerocopy": {"stream": 512 * KiB, "broadcast": 32 * MiB, "queue": 4 * MiB, "service": 64 * KiB},
    "iceoryx2": {"stream": 64 * KiB, "broadcast": 64 * KiB},
    "iceoryx2-cxx": {"stream": 64 * KiB, "broadcast": 64 * KiB},
    "aeron": {"stream": 512 * KiB, "broadcast": 32 * MiB},
    "zeromq": {"stream": 512 * KiB, "broadcast": 4 * MiB, "queue": 32 * MiB, "service": 64 * KiB},
    "zeromq-tcp": {"stream": 512 * KiB, "broadcast": 32 * MiB, "queue": 512 * KiB, "service": 64 * KiB},
    "nng": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
    "nng-tcp": {"stream": 64 * KiB, "broadcast": 64 * KiB, "queue": 64 * KiB, "service": 64 * KiB},
    "boost": {"stream": 32 * MiB, "queue": 32 * MiB},
},
AERON_CONDUCTOR["macos"] = "backoff"
AERON_FRAGMENT_LIMIT["macos"] = {"stream": 4, "broadcast": 1}
