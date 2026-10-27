// alt_bench: one role of one case (run.py starts each role in a process of its own and reads its results).
//
//   alt_bench <lib> <pattern> <role> [--size N] [--name S] [--dir D] [--index I] [--subscribers N] [--producers N]
//             [--consumers N] [--clients N] [--servers N] [--lossy] [--port P] [--warmup-ms T] [--duration-ms T]
//             [--poll] [--buffer B] [--batch N]
//   alt_bench <lib> supports <pattern> [options]   exit 0 if the library has the case, 1 if not (also for a --buffer it
//                                                  doesn't take, or a --batch without batch calls)
//
// On stdin and stdout: the role prints "ready" once its endpoint is open (a library may wait there for its peers),
// waits for "go", runs, and prints one "result {...}" line. A sending role (tx, pub, producer) and a calling role
// (ping, client) time themselves: they send for --warmup-ms, then for --duration-ms, print their result and wait for
// "quit" (so their endpoint stays open while the receivers drain it). A receiving role (rx, sub, consumer) and a
// replying role (echo, server) run until "stop" and then until nothing has come for 50 ms. Every message starts
// with a 16-byte header: its sequence number and its phase (0 warm-up, 1 measured). --poll makes every receive look
// once (timeout 0) in a loop instead of waiting in the library's receive. --buffer gives a library that takes a buffer
// size B bytes of it (harness.hpp `depth_for`, each adapter's comment); without it, each library has its usual size.
// --batch N (a divisor of 64, for a library with batch calls: fastipc's copying calls, Aeron) makes a sender hand over
// N messages per call and a receiver take up to N per call (each adapter's comment).
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "harness.hpp"

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <intrin.h>
#include <windows.h>
#elif defined(__x86_64__)
#include <x86intrin.h>
#endif
#if defined(__linux__)
#include <sys/prctl.h>
#endif
#if defined(__APPLE__)
#include <mach/mach_time.h>
#endif

namespace alt
{

#define ALT_LIB(name) extern const Lib name;
#include "libs.inc"
#undef ALT_LIB

const Lib* find_lib(const std::string& name)
{
    static const Lib* const libs[] = {
#define ALT_LIB(name) &name,
#include "libs.inc"
#undef ALT_LIB
    };
    for (const Lib* lib : libs)
    {
        if (name == lib->name)
            return lib;
    }
    return nullptr;
}

uint64_t ticks()
{
#if defined(_M_X64) || defined(__x86_64__)
    return __rdtsc();
#elif defined(__APPLE__)
    // macOS doesn't give a process the generic timer's own register (cntvct_el0 reads a stale value that differs
    // between cores): mach_absolute_time reads the same 24 MHz counter.
    return mach_absolute_time();
#elif defined(__aarch64__)
    uint64_t value;
    asm volatile("isb; mrs %0, cntvct_el0" : "=r"(value));
    return value;
#else
#error "x86-64 or ARM64 only"
#endif
}

double ticks_per_second()
{
    static const double rate = []
    {
#if defined(__APPLE__)
        mach_timebase_info_data_t timebase;
        mach_timebase_info(&timebase);
        return 1e9 * timebase.denom / timebase.numer;
#elif defined(__aarch64__)
        uint64_t frequency;
        asm volatile("mrs %0, cntfrq_el0" : "=r"(frequency));
        return static_cast<double>(frequency);
#else
        auto t0 = std::chrono::steady_clock::now();
        uint64_t c0 = ticks();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        auto t1 = std::chrono::steady_clock::now();
        uint64_t c1 = ticks();
        return static_cast<double>(c1 - c0) / std::chrono::duration<double>(t1 - t0).count();
#endif
    }();
    return rate;
}

void fail(const char* what, const std::string& detail)
{
    std::fprintf(stderr, "alt_bench: %s%s%s\n", what, detail.empty() ? "" : ": ", detail.c_str());
    std::fflush(stderr);
    // Not exit(): the thread that reads stdin holds its lock, which exit's flush of every stream would wait for.
    std::_Exit(2);
}

namespace
{

struct Options
{
    Params params;
    int warmup_ms = 300;
    int duration_ms = 1000;
    bool poll = false;
};

// The control lines from run.py, read on a thread of their own by the roles that run until "stop".
struct Control
{
    std::atomic<bool> go{false}, stop{false}, quit{false};

    void read_lines()
    {
        std::string line;
        while (std::getline(std::cin, line))
        {
            if (line == "go")
                go = true;
            if (line == "stop")
                stop = true;
            if (line == "quit")
                quit = true;
        }
        stop = true;
        quit = true;
    }

    static void wait_for(const std::atomic<bool>& flag)
    {
        while (!flag.load())
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
};

void say(const std::string& line)
{
    std::fputs((line + "\n").c_str(), stdout);
    std::fflush(stdout);
}

struct Header
{
    uint64_t seq;
    uint64_t phase;
};

Header header_of(const uint8_t* data)
{
    Header header;
    std::memcpy(&header, data, sizeof header);
    return header;
}

void write_header(uint8_t* message, uint64_t seq, uint64_t phase)
{
    Header header{seq, phase};
    std::memcpy(message, &header, sizeof header);
}

void check_length(size_t len, size_t expected)
{
    if (len != expected)
        fail("a message of the wrong length", std::to_string(len) + " for " + std::to_string(expected));
}

// The measured phase's span on the sender's or caller's side: phase 0 for warmup_ms, phase 1 for duration_ms, the
// clock read once per 64 messages.
struct Phases
{
    uint64_t warmup, duration, start, measured_start = 0, end = 0;
    uint64_t phase = 0;

    explicit Phases(const Options& options)
    {
        double hz = ticks_per_second();
        warmup = static_cast<uint64_t>(options.warmup_ms * hz / 1000);
        duration = static_cast<uint64_t>(options.duration_ms * hz / 1000);
        start = ticks();
    }

    // After a batch: moves on to the measured phase, or reports its end.
    bool done()
    {
        uint64_t now = ticks();
        if (phase == 0 && now - start >= warmup)
        {
            phase = 1;
            measured_start = now;
        }
        else if (phase == 1 && now - measured_start >= duration)
        {
            end = now;
            return true;
        }
        return false;
    }
};

std::string span_json(uint64_t t0, uint64_t t1)
{
    char text[160];
    std::snprintf(
        text,
        sizeof text,
        "\"t0\":%llu,\"t1\":%llu,\"hz\":%.0f",
        static_cast<unsigned long long>(t0),
        static_cast<unsigned long long>(t1),
        ticks_per_second()
    );
    return text;
}

// Sends 64 messages between looks at the clock: one per call, or with --batch N 64 / N calls of N messages each, laid
// out back to back.
void run_sender(Endpoint& endpoint, const Options& options, Control& control)
{
    const size_t size = options.params.size;
    const int batch = options.params.batch;
    std::vector<uint8_t> messages(size * batch, 0xA5);
    Control::wait_for(control.go);
    Phases phases(options);
    uint64_t seq = 0, sent = 0;
    do
    {
        for (int k = 0; k < 64; k += batch)
        {
            for (int j = 0; j < batch; j++)
                write_header(messages.data() + j * size, seq++, phases.phase);
            if (batch == 1)
                endpoint.send(messages.data(), size);
            else
                endpoint.send_batch(messages.data(), size, batch);
        }
        if (phases.phase == 1)
            sent += 64;
    }
    while (!phases.done());
    say("result {\"sent\":" + std::to_string(sent) + "," + span_json(phases.measured_start, phases.end) + "}");
    Control::wait_for(control.quit);
}

// Counts the measured messages, reading the clock at the first and after every 64th; a broadcast's subscriber also
// counts the sequence numbers it never saw.
struct Counter : Sink
{
    size_t expected;
    uint64_t count = 0, count_at_stamp = 0, first = 0, stamp = 0, last_seq = 0, lost = 0, last_message = 0;

    explicit Counter(size_t expected) : expected(expected)
    {
    }

    void on(const uint8_t* data, size_t len) override
    {
        check_length(len, expected);
        Header header = header_of(data);
        if (header.phase != 1)
            return;
        if (count == 0)
        {
            first = stamp = ticks();
            count = count_at_stamp = 1;
            last_seq = header.seq;
            return;
        }
        if (header.seq > last_seq + 1)
            lost += header.seq - last_seq - 1;
        if (header.seq > last_seq)
            last_seq = header.seq;
        if ((++count & 63) == 0)
        {
            stamp = ticks();
            count_at_stamp = count;
        }
    }
};

// Receives (and, for echo and server, replies to each message) until "stop" and 50 ms without a message.
void run_receiver(Endpoint& endpoint, const Options& options, Control& control, bool replies)
{
    Counter counter(options.params.size);
    std::vector<uint8_t> reply(options.params.size, 0x5A);
    Control::wait_for(control.go);
    int timeout = options.poll ? 0 : 100;
    uint64_t idle_limit = static_cast<uint64_t>(ticks_per_second() * 0.05);
    uint64_t last_message = ticks();
    uint64_t received = 0;
    for (;;)
    {
        int n = endpoint.recv(counter, timeout);
        if (n > 0)
        {
            received += n;
            if (replies)
            {
                for (int k = 0; k < n; k++)
                    endpoint.send(reply.data(), reply.size());
            }
            if ((received & 1023) < static_cast<uint64_t>(n))
                last_message = ticks();
            continue;
        }
        if (control.stop.load())
        {
            uint64_t now = ticks();
            if (now - last_message > idle_limit)
                break;
        }
        else if (options.poll)
        {
            last_message = ticks();
        }
    }
    say("result {\"count\":" + std::to_string(counter.count_at_stamp) + ",\"lost\":" + std::to_string(counter.lost)
        + ",\"received\":" + std::to_string(received) + "," + span_json(counter.first, counter.stamp) + "}");
}

struct One : Sink
{
    size_t expected;
    int got = 0;

    explicit One(size_t expected) : expected(expected)
    {
    }

    void on(const uint8_t*, size_t len) override
    {
        check_length(len, expected);
        got++;
    }
};

// Round trips: sends a message, waits for the reply; one round trip in 16 is sampled, chosen at random, so that the
// samples don't fall into step with a period of the library's own (a ring's lap, a batch of messages).
void run_caller(Endpoint& endpoint, const Options& options, Control& control)
{
    std::vector<uint8_t> message(options.params.size, 0xA5);
    One one(options.params.size);
    std::vector<uint64_t> samples;
    samples.reserve(1 << 22);
    Control::wait_for(control.go);
    int timeout = options.poll ? 0 : 100;
    Phases phases(options);
    uint64_t seq = 0, calls = 0;
    uint64_t random = 0x9E3779B97F4A7C15;  // xorshift64
    do
    {
        for (int k = 0; k < 64; k++)
        {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            bool sampled = (random & 15) == 0 && phases.phase == 1;
            uint64_t t0 = sampled ? ticks() : 0;
            write_header(message.data(), seq++, phases.phase);
            endpoint.send(message.data(), message.size());
            one.got = 0;
            while (one.got == 0)
                endpoint.recv(one, timeout);
            if (one.got != 1)
                fail("more than one reply to a call");
            if (sampled && samples.size() < samples.capacity())
                samples.push_back(ticks() - t0);
        }
        if (phases.phase == 1)
            calls += 64;
    }
    while (!phases.done());
    std::sort(samples.begin(), samples.end());
    double ns = 1e9 / ticks_per_second();
    auto percentile = [&](double p)
    { return samples.empty() ? 0.0 : samples[static_cast<size_t>(p * (samples.size() - 1))] * ns; };
    char text[200];
    std::snprintf(
        text,
        sizeof text,
        "\"calls\":%llu,\"p50_ns\":%.1f,\"p99_ns\":%.1f,\"p999_ns\":%.1f,",
        static_cast<unsigned long long>(calls),
        percentile(0.5),
        percentile(0.99),
        percentile(0.999)
    );
    say(std::string("result {") + text + span_json(phases.measured_start, phases.end) + "}");
    Control::wait_for(control.quit);
}

Pattern parse_pattern(const std::string& text)
{
    if (text == "stream")
        return Pattern::stream;
    if (text == "broadcast")
        return Pattern::broadcast;
    if (text == "queue")
        return Pattern::queue;
    if (text == "service")
        return Pattern::service;
    fail("unknown pattern", text);
}

Role parse_role(const std::string& text)
{
    static const std::pair<const char*, Role> roles[] = {
        {      "tx",       Role::tx},
        {      "rx",       Role::rx},
        {    "ping",     Role::ping},
        {    "echo",     Role::echo},
        {     "pub",      Role::pub},
        {     "sub",      Role::sub},
        {"producer", Role::producer},
        {"consumer", Role::consumer},
        {  "client",   Role::client},
        {  "server",   Role::server},
    };
    for (auto& [name, role] : roles)
    {
        if (text == name)
            return role;
    }
    fail("unknown role", text);
}

void lower_power_throttling()
{
#if defined(_WIN32)
    // Windows runs a process started from the background on efficiency cores unless its throttling is off.
    PROCESS_POWER_THROTTLING_STATE state{};
    state.Version = PROCESS_POWER_THROTTLING_CURRENT_VERSION;
    state.ControlMask = PROCESS_POWER_THROTTLING_EXECUTION_SPEED;
    state.StateMask = 0;
    SetProcessInformation(GetCurrentProcess(), ProcessPowerThrottling, &state, sizeof state);
#endif
}

}  // namespace
}  // namespace alt

int main(int argc, char** argv)
{
    using namespace alt;
    if (argc < 4)
        fail("usage: alt_bench <lib> <pattern> <role> [options]");
    lower_power_throttling();
#if defined(__linux__)
    // A debugger may attach to a role that hangs (Ubuntu lets a process trace only its children by default).
    prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
#endif
    const Lib* lib = find_lib(argv[1]);
    if (!lib)
        fail("unknown library (or not built)", argv[1]);
    bool supports_only = std::string(argv[2]) == "supports";
    Pattern pattern = parse_pattern(argv[supports_only ? 3 : 2]);
    Options options;
    Params& p = options.params;
    for (int i = 4; i < argc; i++)
    {
        std::string arg = argv[i];
        auto value = [&]() -> std::string
        {
            if (i + 1 >= argc)
                fail("a value is missing", arg);
            return argv[++i];
        };
        if (arg == "--size")
            p.size = std::stoull(value());
        else if (arg == "--name")
            p.name = value();
        else if (arg == "--dir")
            p.dir = value();
        else if (arg == "--index")
            p.index = std::stoi(value());
        else if (arg == "--subscribers")
            p.subscribers = std::stoi(value());
        else if (arg == "--producers")
            p.producers = std::stoi(value());
        else if (arg == "--consumers")
            p.consumers = std::stoi(value());
        else if (arg == "--clients")
            p.clients = std::stoi(value());
        else if (arg == "--servers")
            p.servers = std::stoi(value());
        else if (arg == "--lossy")
            p.lossy = true;
        else if (arg == "--port")
            p.port = std::stoi(value());
        else if (arg == "--warmup-ms")
            options.warmup_ms = std::stoi(value());
        else if (arg == "--duration-ms")
            options.duration_ms = std::stoi(value());
        else if (arg == "--poll")
            options.poll = true;
        else if (arg == "--buffer")
            p.buffer = std::stoull(value());
        else if (arg == "--batch")
            p.batch = std::stoi(value());
        else
            fail("unknown option", arg);
    }
    if (p.batch < 1 || p.batch > 64 || 64 % p.batch != 0)
        fail("--batch is a divisor of 64");
    bool takes_batch = p.batch == 1 || lib->batches;
    if (supports_only)
        return takes_batch && lib->supports(pattern, p) ? 0 : 1;
    if (p.size < sizeof(Header))
        fail("a message holds at least 16 bytes");
    if (!takes_batch || !lib->supports(pattern, p))
        fail("the library lacks this case");
    Role role = parse_role(argv[3]);
    ticks_per_second();

    std::unique_ptr<Endpoint> endpoint = lib->open(pattern, role, p);
    Control control;
    std::thread reader([&] { control.read_lines(); });
    reader.detach();
    say("ready");
    switch (role)
    {
        case Role::tx:
        case Role::pub:
        case Role::producer:
            run_sender(*endpoint, options, control);
            break;
        case Role::rx:
        case Role::sub:
        case Role::consumer:
            run_receiver(*endpoint, options, control, false);
            break;
        case Role::echo:
        case Role::server:
            run_receiver(*endpoint, options, control, true);
            break;
        case Role::ping:
        case Role::client:
            run_caller(*endpoint, options, control);
            break;
    }
    endpoint.reset();
    std::fflush(stdout);
    // Some libraries' background threads outlive their handles: end the process without waiting for them.
    std::_Exit(0);
}
