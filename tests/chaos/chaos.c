/*
 * The chaos harness peer: `python devtool.py test chaos` (devtool_lib/chaos.py starts, kills, suspends and restarts
 * these peers and reads their events). One self-contained program, built through the public header include/fipc.h
 * against this tree's static library (the variant with the test hooks, for FASTIPC_TEST_CRASH_AT).
 *
 * A chaos peer is one side of a typical application's connection: copied messages, 1 MiB rings, a producer thread on
 * blocking fipc_send and a consumer thread on fipc_recv(100 ms) into a buffer of its own. Both sides send and receive.
 * Recovery is a new connection. Every message is verified: sender and session, sequence (nothing lost, repeated or
 * stale), length, contents and checksum. A peer's end is reported when its process exits (EOF on the control
 * connection), so DISCONNECTED arrives in milliseconds. A send reports the peer's end at once, and only stops the
 * producer; the consumer's FIPC_DISCONNECTED ends the connection, after it has received every message the peer
 * completed. So the harness also checks fipc.h's promise that receives deliver those first: a peer that leaves right
 * after its verified exchange may leave its last messages unread in the ring. A publisher or subscriber peer is one
 * side of a broadcast instead, an owner, producer or consumer peer a member of a work queue, and a service-owner,
 * service-client or service-server peer a member of a service (the roles below): every message checked too.
 *
 * Usage: chaos_peer <server|client|publisher|subscriber|owner|producer|consumer|service-owner|service-client|
 *                    service-server> <patient|bounded> <name> [key=value ...]
 *   role    server: fipc_stream_create, then fipc_stream_accept(ready_ms); the listener lives until the connection
 *                   ends, and every new connection listens again;
 *           client: fipc_stream_connect(ready_ms);
 *           publisher: a broadcast's: fipc_broadcast_create (its process admits the subscribers by itself), and
 *                   checked messages of 56-295 bytes sent as fast as a millisecond's sleep after every 32 allows,
 *                   until STOP;
 *           subscriber: fipc_broadcast_join(ready_ms), then every message received and checked; it joins again after
 *                   DISCONNECTED or UNSUBSCRIBED. Both always wait again after a timeout (the policy is ignored);
 *           owner: a work queue's: fipc_queue_create, and its own producer joined with fipc_queue_join (its process
 *                   admits every member by itself), and checked messages sent on its producer as a publisher sends
 *                   them, until QUIET or STOP;
 *           producer: fipc_queue_join(ready_ms) as a queue's producer, then checked messages as the owner sends them;
 *                   it joins again after DISCONNECTED;
 *           consumer: fipc_queue_join(ready_ms) as a queue's consumer, then every message received and checked
 *                   (whole, and each producer's in order: later than the last this consumer took of it); it joins
 *                   again after DISCONNECTED. The queue roles ignore the policy too;
 *           service-owner: a service's: fipc_service_create, and its own client joined with fipc_service_join (its
 *                   process admits every member by itself), on which it calls as a service-client does, until QUIET
 *                   or STOP;
 *           service-client: fipc_service_join(ready_ms) as a service's client, then requests of 56-295 bytes, up to a
 *                   depth of 1-12 in flight (from the seed), each reply checked as it comes: whole, of the request it
 *                   answers (the n-th reply received answers the n-th request sent, so none is lost, repeated or out
 *                   of order), or FIPC_ABANDONED (ABANDONED: that request counts as answered). After QUIET it sends
 *                   no more and receives every reply it is owed (QUIETED, or UNDRAINED if they don't come within
 *                   SERVICE_DRAIN_MS). It joins again after DISCONNECTED;
 *           service-server: fipc_service_join(ready_ms) as a service's server, then every request received, checked
 *                   (whole, and each client's in order) and replied to with a reply made from it; it joins again after
 *                   DISCONNECTED. The service roles ignore the policy too.
 *   policy  patient: after a TIMEOUT, wait again (accept on the same listener, or connect again);
 *           bounded: 3 attempts, then GAVE_UP and exit status 4. Both make a new connection after a disconnect, so
 *                    that the harness can reconnect them.
 *   seed=N          salt of the message sizes and contents
 *   ring=BYTES      ring size (server; default 1 MiB): the client learns it, and needs it for the message sizes
 *   ready_ms=MS     accept / connect timeout (default 5000)
 *   gate=1          after a disconnect, wait for GO on stdin before the next connection
 *   traffic=mixed   (default) bursts of mostly small messages, 3% of 1-3.5 rings;
 *           stream  back-to-back messages of 1.5-3.5 rings (every one in several pieces);
 *           drain   small messages every 10 ms, and a pause of up to 2 ms after each message received;
 *           sink    small messages every 10 ms, and no pause after a message received
 *   kill=PHASE:US   end this process US microseconds after PHASE of its first connection starts:
 *                   init (server: fipc_stream_create; client: fipc_stream_connect), handshake (server:
 *                   fipc_stream_accept; client: fipc_stream_connect), traffic (the verified exchange is done),
 *                   cleanup (fipc_close after leaving; ended right after it if it returns first); or, on a
 *                   service-server, held: at once, holding its US-th request (after its TOOK, before its reply)
 *   crash=segv|abort  the kill lands as a real crash (a null dereference or abort()) rather than a hard kill, so
 *                   the OS's default crash handling decides how soon the peer sees EOF (measured, not gated)
 *   leave=MS        leave the first connection MS ms after the verified exchange (cancel, join, close)
 *   rejoin=MS       ... and connect (or listen) again MS ms later in this process (a graceful in-process reconnect)
 *   subs=N          publisher: max_subscribers (default 8)
 *   stall=MS        publisher: max_stall_ms (default FIPC_FOREVER)
 *   mode=lossy|gated|inplace  subscriber: lossy (fipc_recv), gated (fipc_recv), or gated receiving in place
 *                   (fipc_recv_acquire); default lossy. consumer: inplace (fipc_recv_acquire), else fipc_recv
 *   producers=N, consumers=N  owner: max_producers (default 2) and max_consumers (default 3)
 *   clients=N, servers=N, message=BYTES  service-owner: max_clients (default 2), max_servers (default 3) and
 *                   max_message (default 512)
 *   hold_ms=MS      consumer in place: holds each message MS ms before its release; service-server: holds each
 *                   request MS ms before its reply
 *   took=1          consumer: prints TOOK for every message it takes (a victim's: the driver's count of what it took);
 *                   service-server: TOOK for every request it takes, REPLIED once its reply returned FIPC_OK
 *   again=1         consumer: a message earlier in its producer's stream than the last it took of it is one that went
 *                   again (a killed consumer's), not out of order: counted, its order not checked
 *
 * The crash-at-step scenario needs no option here: the driver sets FASTIPC_TEST_CRASH_AT in the peer's
 * environment, and the library (static build) ends the process at that lifecycle step (exit 99).
 * stdin:  GO (continue at the gate), STOP (leave gracefully: BYE, exit status 0), MARK <id> (answered at once by
 *         MARKED id=<id> with the traffic counters: the driver's probe of progress); a queue's: QUIET (a producer or
 *         owner stops sending and reports QUIETED with the messages it sent), TALLY <id> (a consumer answers at once
 *         with TALLIED: per producer's stream, what it took); a service's: QUIET (a client or owner stops sending,
 *         receives what it is owed and reports QUIETED with its counts).
 * stdout: one event per line (START, INIT, INIT_FAILED, READY_TIMEOUT, READY_FAILED, CONNECTED, VERIFIED,
 *         DISCONNECTED, SESSION_ERROR, BAD_MESSAGE, CLOSED, LEFT, GATE, KILL_ARMED, GAVE_UP, BYE; a broadcast's:
 *         SEND_TIMEOUT, SEND_RESUMED, UNSUBSCRIBED; a queue's: QUIETED, TALLIED, TOOK; a service's: ABANDONED,
 *         QUIETED, UNDRAINED, TOOK, REPLIED) and a STATUS line
 *         every 500 ms: see devtool_lib/chaos.py.
 */

#define _GNU_SOURCE

#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fipc.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <dirent.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>

#include <sys/resource.h>
#endif

#define VERIFY_COUNT 6               /* messages each way that make the verified exchange of a connection */
#define RECV_MS 100                  /* the consumer thread's receive timeout: 100 ms */
#define BOUNDED_ATTEMPTS 3           /* the bounded policy's attempts */
#define EXIT_GAVE_UP 4               /* not 3: Windows' abort() exits with 3 */
#define EXIT_KILLED 137              /* Windows exit code of a self-kill (Linux: SIGKILL) */
#define MSG_MAGIC 0x53414843u        /* "CHAS" */
#define GOLDEN 0x9E3779B97F4A7C15ull /* odd: consecutive words of a body differ */
#define LOAD(p) __atomic_load_n((p), __ATOMIC_ACQUIRE)
#define STORE(p, v) __atomic_store_n((p), (v), __ATOMIC_RELEASE)
#define ADD(p, v) __atomic_add_fetch((p), (v), __ATOMIC_ACQ_REL)

/* ---------- reporting ---------- */

static const char* g_scenario = "chaos";
static const char* g_role = "?";

/* Prints "chaos_peer <role> (<file>:<line>): <message>" to stderr and exits with status 1. */
static void fail_at(const char* file, int line, const char* fmt, ...) __attribute__((noreturn, format(printf, 3, 4)));

static void fail_at(const char* file, int line, const char* fmt, ...)
{
    const char* base = file;
    for (const char* p = file; *p; p++)
        if (*p == '/' || *p == '\\')
            base = p + 1;
    va_list args;
    fprintf(stderr, "chaos_peer %s/%s (%s:%d): ", g_scenario, g_role, base, line);
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fputc('\n', stderr);
    _Exit(1); /* not exit: the stdin thread holds the stdin lock that exit's flush would wait for */
}

#define FAIL(...) fail_at(__FILE__, __LINE__, __VA_ARGS__)

/* ---------- platform: threads, locks, clocks, process ---------- */

#if defined(_WIN32)
typedef HANDLE thread_t;
#define THREAD_FN(name) static DWORD WINAPI name(LPVOID arg)
#define THREAD_RETURN return 0

static void thread_start(thread_t* t, LPTHREAD_START_ROUTINE fn, void* arg)
{
    *t = CreateThread(NULL, 0, fn, arg, 0, NULL);
    if (!*t)
        FAIL("CreateThread failed");
}

static void thread_join(thread_t t)
{
    WaitForSingleObject(t, INFINITE);
    CloseHandle(t);
}

static void thread_detach(thread_t t)
{
    CloseHandle(t);
}

/* The monotonic clock of the STATUS lines, in ms */
static uint64_t now_ms(void)
{
    ULONGLONG ticks = 0;
    QueryUnbiasedInterruptTime(&ticks);
    return (uint64_t) ticks / 10000;
}

static void sleep_us(uint64_t us)
{
    LARGE_INTEGER freq, start, now;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);
    const int64_t ticks = (int64_t) (us * (uint64_t) freq.QuadPart / 1000000u);
    if (us > 20000)
        Sleep((DWORD) (us / 1000 - 16)); /* the scheduler tick is ~15.6 ms: spin the rest */
    do
    {
        QueryPerformanceCounter(&now);
        if (now.QuadPart - start.QuadPart < ticks)
            SwitchToThread();
    }
    while (now.QuadPart - start.QuadPart < ticks);
}

static uint32_t current_pid(void)
{
    return (uint32_t) GetCurrentProcessId();
}

static void hard_kill(void)
{
    TerminateProcess(GetCurrentProcess(), EXIT_KILLED);
    for (;;)
        Sleep(1000);
}

static uint64_t cpu_ms(void)
{
    FILETIME created, exited, kernel, user;
    if (!GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user))
        return 0;
    const uint64_t k = ((uint64_t) kernel.dwHighDateTime << 32) | kernel.dwLowDateTime;
    const uint64_t u = ((uint64_t) user.dwHighDateTime << 32) | user.dwLowDateTime;
    return (k + u) / 10000;
}

/* Open handles (GetProcessHandleCount): must stay flat across reconnects */
static long resource_count(void)
{
    DWORD count = 0;
    return GetProcessHandleCount(GetCurrentProcess(), &count) ? (long) count : -1;
}
#else
typedef pthread_t thread_t;
#define THREAD_FN(name) static void* name(void* arg)
#define THREAD_RETURN return NULL

static void thread_start(thread_t* t, void* (*fn)(void*), void* arg)
{
    if (pthread_create(t, NULL, fn, arg) != 0)
        FAIL("pthread_create failed");
}

static void thread_join(thread_t t)
{
    pthread_join(t, NULL);
}

static void thread_detach(thread_t t)
{
    pthread_detach(t);
}

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000u + (uint64_t) ts.tv_nsec / 1000000u;
}

static void sleep_us(uint64_t us)
{
    struct timespec ts = {(time_t) (us / 1000000u), (long) (us % 1000000u) * 1000L};
    while (nanosleep(&ts, &ts) != 0)
    {
    }
}

static uint32_t current_pid(void)
{
    return (uint32_t) getpid();
}

static void hard_kill(void)
{
    kill(getpid(), SIGKILL);
    for (;;)
        pause();
}

static uint64_t cpu_ms(void)
{
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) != 0)
        return 0;
    return (uint64_t) (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000u
         + (uint64_t) (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) / 1000u;
}

/* Open file descriptors: must stay flat across reconnects */
static long resource_count(void)
{
#if defined(__APPLE__)
    DIR* dir = opendir("/dev/fd");
#else
    DIR* dir = opendir("/proc/self/fd");
#endif
    if (!dir)
        return -1;
    long count = 0;
    for (struct dirent* entry; (entry = readdir(dir)) != NULL;)
        if (entry->d_name[0] != '.')
            count++;
    closedir(dir);
    return count - 1; /* the directory's own descriptor */
}
#endif

static void sleep_ms(unsigned ms)
{
#if defined(_WIN32)
    Sleep(ms);
#else
    struct timespec ts = {(time_t) (ms / 1000), (long) (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
#endif
}

/* ---------- options ---------- */

enum role
{
    ROLE_SERVER,
    ROLE_CLIENT,
    ROLE_PUBLISHER,
    ROLE_SUBSCRIBER,
    ROLE_OWNER,
    ROLE_PRODUCER,
    ROLE_CONSUMER,
    ROLE_SERVICE_OWNER,
    ROLE_SERVICE_CLIENT,
    ROLE_SERVICE_SERVER
};

/* A subscriber's mode (mode=) */
enum mode
{
    MODE_LOSSY,
    MODE_GATED,
    MODE_INPLACE
};

enum policy
{
    POLICY_PATIENT,
    POLICY_BOUNDED
};

enum traffic
{
    TRAFFIC_MIXED,
    TRAFFIC_STREAM,
    TRAFFIC_DRAIN,
    TRAFFIC_SINK
};

enum phase
{
    PHASE_NONE,
    PHASE_INIT,
    PHASE_HANDSHAKE,
    PHASE_TRAFFIC,
    PHASE_CLEANUP,
    PHASE_HELD /* a service-server's: holding its kill_us-th request */
};

/* How the killer thread ends this process: a hard kill (as an outside SIGKILL/TerminateProcess), or a real crash
 * with the OS's default handling left in place: a null dereference or abort(). */
enum crash_method
{
    CRASH_KILL,
    CRASH_SEGV,
    CRASH_ABORT
};

static const char* const PHASE_NAMES[] = {"none", "init", "handshake", "traffic", "cleanup", "held"};

static struct
{
    enum role role;
    enum policy policy;
    const char* name;
    uint32_t salt;
    uint32_t ring;
    int ready_ms;
    bool gate;
    enum traffic traffic;
    enum phase kill_phase;
    enum crash_method crash; /* how the kill lands: hard kill (default), or a real crash */
    uint64_t kill_us;
    long leave_ms;  /* -1: stay */
    long rejoin_ms; /* -1: exit after leaving */
    enum mode mode;
    uint32_t subscribers;
    int stall_ms;
    uint32_t producers;
    uint32_t consumers;
    uint32_t clients;
    uint32_t servers;
    uint32_t message;
    unsigned hold_ms;
    bool took;
    bool again;
} g_opt;

static void parse_options(int argc, char** argv)
{
    if (argc < 4)
        FAIL("usage: chaos_peer <server|client|publisher|subscriber|owner|producer|consumer|service-owner|"
             "service-client|service-server> <patient|bounded> <name> [key=value ...]");
    if (strcmp(argv[1], "server") == 0)
        g_opt.role = ROLE_SERVER;
    else if (strcmp(argv[1], "client") == 0)
        g_opt.role = ROLE_CLIENT;
    else if (strcmp(argv[1], "publisher") == 0)
        g_opt.role = ROLE_PUBLISHER;
    else if (strcmp(argv[1], "subscriber") == 0)
        g_opt.role = ROLE_SUBSCRIBER;
    else if (strcmp(argv[1], "owner") == 0)
        g_opt.role = ROLE_OWNER;
    else if (strcmp(argv[1], "producer") == 0)
        g_opt.role = ROLE_PRODUCER;
    else if (strcmp(argv[1], "consumer") == 0)
        g_opt.role = ROLE_CONSUMER;
    else if (strcmp(argv[1], "service-owner") == 0)
        g_opt.role = ROLE_SERVICE_OWNER;
    else if (strcmp(argv[1], "service-client") == 0)
        g_opt.role = ROLE_SERVICE_CLIENT;
    else if (strcmp(argv[1], "service-server") == 0)
        g_opt.role = ROLE_SERVICE_SERVER;
    else
        FAIL("unknown role (expected server, client, publisher, subscriber, owner, producer, consumer, service-owner, "
             "service-client or service-server)");
    if (strcmp(argv[2], "patient") == 0)
        g_opt.policy = POLICY_PATIENT;
    else if (strcmp(argv[2], "bounded") == 0)
        g_opt.policy = POLICY_BOUNDED;
    else
        FAIL("unknown policy (expected patient or bounded)");
    g_opt.name = argv[3];
    g_opt.salt = 1;
    g_opt.ring = 1u << 20;
    g_opt.ready_ms = 5000;
    g_opt.leave_ms = -1;
    g_opt.rejoin_ms = -1;
    g_opt.subscribers = 8;
    g_opt.stall_ms = FIPC_FOREVER;
    g_opt.producers = 2;
    g_opt.consumers = 3;
    g_opt.clients = 2;
    g_opt.servers = 3;
    g_opt.message = 512;
    for (int k = 4; k < argc; k++)
    {
        const char* arg = argv[k];
        const char* value = strchr(arg, '=');
        if (!value)
            FAIL("option without a value: %s", arg);
        value++;
        const size_t key_len = (size_t) (value - 1 - arg);
#define KEY(name) (key_len == strlen(name) && strncmp(arg, name, key_len) == 0)
        if (KEY("seed"))
            g_opt.salt = (uint32_t) strtoul(value, NULL, 10);
        else if (KEY("ring"))
            g_opt.ring = (uint32_t) strtoul(value, NULL, 10);
        else if (KEY("ready_ms"))
            g_opt.ready_ms = atoi(value);
        else if (KEY("gate"))
            g_opt.gate = atoi(value) != 0;
        else if (KEY("leave"))
            g_opt.leave_ms = atol(value);
        else if (KEY("rejoin"))
            g_opt.rejoin_ms = atol(value);
        else if (KEY("subs"))
            g_opt.subscribers = (uint32_t) strtoul(value, NULL, 10);
        else if (KEY("stall"))
            g_opt.stall_ms = atoi(value);
        else if (KEY("producers"))
            g_opt.producers = (uint32_t) strtoul(value, NULL, 10);
        else if (KEY("consumers"))
            g_opt.consumers = (uint32_t) strtoul(value, NULL, 10);
        else if (KEY("clients"))
            g_opt.clients = (uint32_t) strtoul(value, NULL, 10);
        else if (KEY("servers"))
            g_opt.servers = (uint32_t) strtoul(value, NULL, 10);
        else if (KEY("message"))
            g_opt.message = (uint32_t) strtoul(value, NULL, 10);
        else if (KEY("hold_ms"))
            g_opt.hold_ms = (unsigned) strtoul(value, NULL, 10);
        else if (KEY("took"))
            g_opt.took = atoi(value) != 0;
        else if (KEY("again"))
            g_opt.again = atoi(value) != 0;
        else if (KEY("mode"))
        {
            if (strcmp(value, "lossy") == 0)
                g_opt.mode = MODE_LOSSY;
            else if (strcmp(value, "gated") == 0)
                g_opt.mode = MODE_GATED;
            else if (strcmp(value, "inplace") == 0)
                g_opt.mode = MODE_INPLACE;
            else
                FAIL("mode=lossy|gated|inplace expected: %s", arg);
        }
        else if (KEY("crash"))
        {
            if (strcmp(value, "segv") == 0)
                g_opt.crash = CRASH_SEGV;
            else if (strcmp(value, "abort") == 0)
                g_opt.crash = CRASH_ABORT;
            else
                FAIL("crash=segv|abort expected: %s", arg);
        }
        else if (KEY("traffic"))
        {
            if (strcmp(value, "mixed") == 0)
                g_opt.traffic = TRAFFIC_MIXED;
            else if (strcmp(value, "stream") == 0)
                g_opt.traffic = TRAFFIC_STREAM;
            else if (strcmp(value, "drain") == 0)
                g_opt.traffic = TRAFFIC_DRAIN;
            else if (strcmp(value, "sink") == 0)
                g_opt.traffic = TRAFFIC_SINK;
            else
                FAIL("unknown traffic: %s", value);
        }
        else if (KEY("kill"))
        {
            const char* colon = strchr(value, ':');
            if (!colon)
                FAIL("kill=PHASE:US expected: %s", arg);
            g_opt.kill_phase = PHASE_NONE;
            for (int p = PHASE_INIT; p <= PHASE_HELD; p++)
                if (strlen(PHASE_NAMES[p]) == (size_t) (colon - value)
                    && strncmp(value, PHASE_NAMES[p], (size_t) (colon - value)) == 0)
                    g_opt.kill_phase = (enum phase) p;
            if (g_opt.kill_phase == PHASE_NONE)
                FAIL("unknown kill phase: %s", arg);
            g_opt.kill_us = strtoull(colon + 1, NULL, 10);
        }
        else
            FAIL("unknown option: %s", arg);
#undef KEY
    }
    if (g_opt.ring < 4096 || (g_opt.ring & (g_opt.ring - 1)) != 0)
        FAIL("ring=%u: a power of two of at least 4096 expected", g_opt.ring);
}

static const char* role_name(void)
{
    static const char* const names[] = {
        "server",
        "client",
        "publisher",
        "subscriber",
        "owner",
        "producer",
        "consumer",
        "service-owner",
        "service-client",
        "service-server"
    };
    return names[g_opt.role];
}

/* ---------- process state ---------- */

static uint32_t g_pid;
static int g_stop;          /* STOP received */
static int g_go;            /* GO lines received */
static int g_connected;     /* this side has a connection (for STATUS) */
static uint64_t g_tx_all;   /* messages sent, all connections */
static uint64_t g_rx_all;   /* messages received and verified, all connections */
static uint64_t g_overruns; /* a lossy subscriber's FIPC_OVERRUN results, all joins */
static int g_quiet;         /* QUIET received: a queue's producers stop sending */
static const char* volatile g_phase = "start";

static void set_phase(const char* phase)
{
    g_phase = phase;
}

/* "FIPC_TIMEOUT" -> "TIMEOUT" */
static const char* rc_name(int rc)
{
    if (rc < 0)
        return "BAD_MESSAGE";
    const char* s = fipc_result_str((fipc_result_t) rc);
    return strncmp(s, "FIPC_", 5) == 0 ? s + 5 : s;
}

/* ---------- self-kill ---------- */

static const char* g_kill_phase_name;

/* Ends the process at the armed point: a hard kill (the whole process, no cleanup, as an outside kill), or a real
 * crash whose default OS handling (a core dump on Linux, a crash report on macOS, WER on Windows, whose dialog
 * SetErrorMode suppresses) governs how long the kernel takes to close the process's handles and so how long the peer
 * takes to see EOF. */
static void do_crash(void)
{
    switch (g_opt.crash)
    {
        case CRASH_SEGV:
            *(volatile int*) 0 = 0;
            break; /* unreachable */
        case CRASH_ABORT:
            abort();
        case CRASH_KILL:
        default:
            hard_kill();
    }
    for (;;)
        sleep_ms(1000);
}

THREAD_FN(killer_main)
{
    (void) arg;
    sleep_us(g_opt.kill_us);
    do_crash();
    THREAD_RETURN;
}

/* Starts the killer if PHASE of the first connection is the one of kill=PHASE:US */
static void arm_kill(enum phase phase, bool first_connection)
{
    if (!first_connection || g_opt.kill_phase != phase)
        return;
    g_kill_phase_name = PHASE_NAMES[phase];
    printf("KILL_ARMED phase=%s us=%" PRIu64 "\n", g_kill_phase_name, g_opt.kill_us);
    thread_t t;
    thread_start(&t, killer_main, NULL);
    thread_detach(t);
}

/* ---------- stdin commands and status ---------- */

static void print_tally(int id);

THREAD_FN(stdin_main)
{
    (void) arg;
    char line[64];
    while (fgets(line, sizeof(line), stdin))
    {
        if (strncmp(line, "GO", 2) == 0)
            ADD(&g_go, 1);
        else if (strncmp(line, "STOP", 4) == 0)
            STORE(&g_stop, 1);
        else if (strncmp(line, "QUIET", 5) == 0)
            STORE(&g_quiet, 1);
        else if (strncmp(line, "TALLY", 5) == 0)
            print_tally(atoi(line + 5));
        else if (strncmp(line, "MARK", 4) == 0)
            printf(
                "MARKED id=%d tx=%" PRIu64 " rx=%" PRIu64 " ovr=%" PRIu64 "\n",
                atoi(line + 4),
                LOAD(&g_tx_all),
                LOAD(&g_rx_all),
                LOAD(&g_overruns)
            );
    }
    THREAD_RETURN; /* EOF: nobody to listen to any more */
}

/* One line every 500 ms: this side's phase and role, whether it has a connection, its traffic counters, CPU time and
 * open handles/descriptors, for the driver's watchdog (spins, wedges) and failure reports. */
THREAD_FN(status_main)
{
    (void) arg;
    for (;;)
    {
        sleep_ms(500);
        printf(
            "STATUS t=%" PRIu64 " phase=%s role=%s connected=%d tx=%" PRIu64 " rx=%" PRIu64 " cpu=%" PRIu64
            " res=%ld ovr=%" PRIu64 "\n",
            now_ms(),
            g_phase,
            role_name(),
            LOAD(&g_connected),
            LOAD(&g_tx_all),
            LOAD(&g_rx_all),
            cpu_ms(),
            resource_count(),
            LOAD(&g_overruns)
        );
    }
    THREAD_RETURN;
}

/* ---------- messages ---------- */

/* In front of every payload; the payload bytes follow from (salt, session, seq). */
typedef struct
{
    uint32_t magic;
    uint32_t sender_pid;
    uint32_t salt;
    uint32_t session;  /* the sender's connection number */
    uint64_t seq;      /* 0, 1, ... per connection */
    uint32_t body_len; /* bytes after this header */
    uint32_t prev_len; /* body_len of message seq - 1: the size of a message found missing */
    uint64_t checksum; /* Fletcher-style sums over the body's 64-bit words */
} msg_hdr_t;

static uint64_t mix64(uint64_t x)
{
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    return x ^ (x >> 31);
}

static uint64_t msg_base(uint32_t salt, uint32_t session, uint64_t seq)
{
    return mix64((((uint64_t) salt << 32) | session) ^ mix64(seq));
}

/* The body's 64-bit words are base + i * GOLDEN (the last one cut short): cheap to make and check, so the peers
 * spend their time in the library rather than in their own loops, and data from anywhere else doesn't match. */
static uint64_t word_at(uint64_t base, size_t i)
{
    return base + (uint64_t) i * GOLDEN;
}

static uint64_t fold(uint64_t s1, uint64_t s2)
{
    return s1 ^ (s2 * GOLDEN);
}

/* Fills body[0..len) and returns its checksum. */
static uint64_t fill_body(uint8_t* body, size_t len, uint64_t base)
{
    uint64_t s1 = 0, s2 = 0;
    const size_t words = len / 8;
    for (size_t i = 0; i < words; i++)
    {
        const uint64_t word = word_at(base, i);
        memcpy(body + i * 8, &word, 8);
        s1 += word;
        s2 += s1;
    }
    if (len % 8)
    {
        uint64_t word = word_at(base, words), tail = 0;
        memcpy(body + words * 8, &word, len % 8);
        memcpy(&tail, &word, len % 8);
        s1 += tail;
        s2 += s1;
    }
    return fold(s1, s2);
}

/* Offset of the first byte that differs from the pattern (len if none); *sum = checksum of what arrived */
static size_t check_body(const uint8_t* body, size_t len, uint64_t base, uint64_t* sum)
{
    uint64_t s1 = 0, s2 = 0;
    size_t first_bad_word = SIZE_MAX;
    const size_t words = (len + 7) / 8;
    for (size_t i = 0; i < words; i++)
    {
        const size_t n = i * 8 + 8 <= len ? 8 : len - i * 8;
        uint64_t got = 0, expected = 0, pattern = word_at(base, i);
        memcpy(&got, body + i * 8, n);
        memcpy(&expected, &pattern, n);
        if (got != expected && first_bad_word == SIZE_MAX)
            first_bad_word = i;
        s1 += got;
        s2 += s1;
    }
    *sum = fold(s1, s2);
    if (first_bad_word == SIZE_MAX)
        return len;
    const uint64_t pattern = word_at(base, first_bad_word);
    for (size_t b = 0; b < 8 && first_bad_word * 8 + b < len; b++)
        if (body[first_bad_word * 8 + b] != ((const uint8_t*) &pattern)[b])
            return first_bad_word * 8 + b;
    return len;
}

static bool several_pieces(const fipc_conn_t* conn, size_t body_len)
{
    /* our header + body in one message of more than one piece */
    return sizeof(msg_hdr_t) + body_len > fipc_max_piece(conn);
}

/* Body sizes: the first VERIFY_COUNT messages of a connection are fixed (one of 2.5 rings), then by traffic kind */
static size_t body_len_for(uint32_t session, uint64_t seq)
{
    const size_t ring = g_opt.ring;
    const size_t first[VERIFY_COUNT] = {16, 300, 5000, 70000 % (ring / 2), ring * 5 / 2, 64};
    if (seq < VERIFY_COUNT)
        return first[seq];
    uint64_t r = mix64(msg_base(g_opt.salt, session, seq) ^ 0x51ull);
    switch (g_opt.traffic)
    {
        case TRAFFIC_STREAM:
            return ring * 3 / 2 + (size_t) (r % (ring * 2));
        case TRAFFIC_DRAIN:
        case TRAFFIC_SINK:
            return 16 + (size_t) (r % 240);
        case TRAFFIC_MIXED:
        default:
        {
            const unsigned p = (unsigned) (r % 1000);
            r >>= 10;
            if (p < 700)
                return 1 + (size_t) (r % 512);
            if (p < 900)
                return 512 + (size_t) (r % (16384 - 512));
            if (p < 970)
                return 16384 + (size_t) (r % (262144 - 16384));
            return ring + (size_t) (r % (ring * 5 / 2));
        }
    }
}

/* Room for the largest message: a header and a body of under 3.5 rings, so a header and 4 rings */
static size_t max_message(void)
{
    return sizeof(msg_hdr_t) + (size_t) g_opt.ring * 4u;
}

/* ---------- one connection's traffic ---------- */

typedef struct
{
    fipc_conn_t* conn;
    uint32_t session;
    int end;            /* set by the thread that ends the session (not the producer on the peer's end), or main */
    int end_rc;         /* the fipc_result_t that ended it (-1: a bad message) */
    const char* end_by; /* "producer" or "consumer" */
    uint64_t tx, rx;    /* messages sent, and received and verified, on this connection */
    uint8_t* send_buf;
    uint8_t* recv_buf;
    size_t recv_len;
    /* receiver: the sender and session this connection receives from */
    bool have_peer;
    uint32_t peer_pid, peer_salt, peer_session;
    uint64_t expected_seq;
    bool reported_bad;
    uint64_t drain_rng;
} session_t;

static void end_session(session_t* s, int rc, const char* by)
{
    int expected = 0;
    if (__atomic_compare_exchange_n(&s->end, &expected, 1, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
    {
        s->end_rc = rc;
        s->end_by = by;
    }
}

static bool bad_message(session_t* s, const char* fmt, ...) __attribute__((format(printf, 2, 3)));

static bool bad_message(session_t* s, const char* fmt, ...)
{
    if (!s->reported_bad)
    {
        char detail[400];
        va_list args;
        va_start(args, fmt);
        vsnprintf(detail, sizeof(detail), fmt, args);
        va_end(args);
        printf("BAD_MESSAGE session=%u %s\n", s->session, detail);
        s->reported_bad = true;
    }
    return false;
}

static bool verify(session_t* s, const uint8_t* message, size_t len)
{
    msg_hdr_t h;
    if (len < sizeof(h))
        return bad_message(s, "message of %zu bytes, shorter than a message header", len);
    memcpy(&h, message, sizeof(h));
    if (h.magic != MSG_MAGIC)
        return bad_message(s, "magic 0x%08x (%zu bytes)", h.magic, len);
    if (h.body_len != len - sizeof(h))
        return bad_message(s, "seq %" PRIu64 " claims %u body bytes, the message has %zu", h.seq, h.body_len, len);
    if (!s->have_peer)
    {
        if (h.seq != 0)
            return bad_message(
                s,
                "stale: the first message of this connection is seq %" PRIu64 " of pid %u session %u",
                h.seq,
                h.sender_pid,
                h.session
            );
        s->have_peer = true;
        s->peer_pid = h.sender_pid;
        s->peer_salt = h.salt;
        s->peer_session = h.session;
        s->expected_seq = 0;
    }
    else if (h.sender_pid != s->peer_pid || h.session != s->peer_session || h.salt != s->peer_salt)
        return bad_message(
            s,
            "mixed: seq %" PRIu64 " of pid %u session %u while receiving pid %u session %u",
            h.seq,
            h.sender_pid,
            h.session,
            s->peer_pid,
            s->peer_session
        );
    if (h.seq > s->expected_seq)
        return bad_message(
            s,
            "gap: seq %" PRIu64 ", expected %" PRIu64 " (%" PRIu64 " missing, the last one %u bytes, %s)",
            h.seq,
            s->expected_seq,
            h.seq - s->expected_seq,
            h.prev_len,
            several_pieces(s->conn, h.prev_len) ? "several pieces" : "one piece"
        );
    if (h.seq < s->expected_seq)
        return bad_message(s, "repeat: seq %" PRIu64 ", expected %" PRIu64, h.seq, s->expected_seq);
    uint64_t sum = 0;
    const size_t at = check_body(message + sizeof(h), h.body_len, msg_base(h.salt, h.session, h.seq), &sum);
    if (at != h.body_len)
        return bad_message(s, "corrupt: seq %" PRIu64 " byte %zu of %u differs", h.seq, at, h.body_len);
    if (sum != h.checksum)
        return bad_message(s, "corrupt: seq %" PRIu64 " checksum differs", h.seq);
    s->expected_seq++;
    return true;
}

THREAD_FN(producer_main)
{
    session_t* s = (session_t*) arg;
    uint32_t prev_len = 0;
    for (uint64_t seq = 0; !LOAD(&s->end); seq++)
    {
        const size_t body = body_len_for(s->session, seq);
        msg_hdr_t h = {MSG_MAGIC, g_pid, g_opt.salt, s->session, seq, (uint32_t) body, prev_len, 0};
        h.checksum = fill_body(s->send_buf + sizeof(h), body, msg_base(g_opt.salt, s->session, seq));
        memcpy(s->send_buf, &h, sizeof(h));
        const fipc_result_t rc = fipc_send(s->conn, s->send_buf, sizeof(h) + body, FIPC_FOREVER);
        if (rc != FIPC_OK)
        {
            /* The peer's end only stops the sending: a send reports it at once, while messages the peer completed
             * may still wait in this side's ring. The consumer receives them, then gets FIPC_DISCONNECTED itself
             * and ends the session (fipc.h: receives deliver every completed message first). Any other error ends
             * the session here. */
            if (rc != FIPC_DISCONNECTED)
                end_session(s, rc, "producer");
            break;
        }
        prev_len = (uint32_t) body;
        ADD(&s->tx, 1);
        ADD(&g_tx_all, 1);
        if (g_opt.traffic == TRAFFIC_DRAIN || g_opt.traffic == TRAFFIC_SINK)
            sleep_ms(10);
        else if (g_opt.traffic == TRAFFIC_MIXED && seq % 4 == 3)
            sleep_ms(1);
    }
    THREAD_RETURN;
}

THREAD_FN(consumer_main)
{
    session_t* s = (session_t*) arg;
    while (!LOAD(&s->end))
    {
        size_t len = 0;
        fipc_result_t rc = fipc_recv(s->conn, s->recv_buf, s->recv_len, &len, RECV_MS);
        if (rc == FIPC_TOO_LARGE)
        {
            /* Longer than any message this harness sends: take it into a buffer of its size, then report it */
            uint8_t* bigger = (uint8_t*) realloc(s->recv_buf, len);
            if (!bigger)
                FAIL("out of memory for a message of %zu bytes", len);
            s->recv_buf = bigger;
            s->recv_len = len;
            rc = fipc_recv(s->conn, s->recv_buf, s->recv_len, &len, RECV_MS);
        }
        if (rc == FIPC_TIMEOUT)
            continue;
        if (rc != FIPC_OK)
        {
            end_session(s, rc, "consumer");
            break;
        }
        if (!verify(s, s->recv_buf, len))
        {
            end_session(s, -1, "consumer");
            break;
        }
        ADD(&g_rx_all, 1);
        if (ADD(&s->rx, 1) == VERIFY_COUNT)
            printf("VERIFIED session=%u peer=%u peer_session=%u\n", s->session, s->peer_pid, s->peer_session);
        if (g_opt.traffic == TRAFFIC_DRAIN)
        {
            s->drain_rng = mix64(s->drain_rng);
            sleep_us(s->drain_rng % 2000);
        }
    }
    THREAD_RETURN;
}

enum session_end
{
    END_THREAD, /* a thread stopped: disconnect, error or bad message */
    END_STOP,
    END_LEAVE
};

/* Runs the producer and consumer until one of them ends the session (the consumer on the peer's end or a bad message,
 * either one on another error), or STOP or the planned leave. */
static enum session_end run_session(session_t* s, bool first_connection)
{
    thread_t producer, consumer;
    thread_start(&producer, producer_main, s);
    thread_start(&consumer, consumer_main, s);
    set_phase("session");

    enum session_end why;
    uint64_t verified_at = 0;
    for (;;)
    {
        sleep_ms(2);
        if (LOAD(&s->end))
        {
            why = END_THREAD;
            break;
        }
        if (LOAD(&g_stop))
        {
            why = END_STOP;
            break;
        }
        if (!verified_at && LOAD(&s->rx) >= VERIFY_COUNT && LOAD(&s->tx) >= VERIFY_COUNT)
        {
            verified_at = now_ms();
            arm_kill(PHASE_TRAFFIC, first_connection);
        }
        if (verified_at && first_connection && g_opt.leave_ms >= 0
            && now_ms() - verified_at >= (uint64_t) g_opt.leave_ms)
        {
            why = END_LEAVE;
            break;
        }
    }
    end_session(s, FIPC_CANCELLED, "main");
    fipc_cancel(s->conn);
    thread_join(producer);
    thread_join(consumer);
    return why;
}

/* ---------- the connection loop ---------- */

/* Closes a connection (NULL: none) and the server's listener (NULL: none). */
static void close_all(fipc_conn_t* conn, fipc_listener_t* listener)
{
    STORE(&g_connected, 0);
    set_phase("cleanup");
    fipc_close(conn);
    if (listener)
        fipc_listener_close(listener);
    printf("CLOSED\n");
}

/* _Exit, not exit: glibc's exit flushes every stream and would wait for the stdin lock that the stdin thread holds
 * while it blocks in fgets. stdout and stderr are unbuffered, and the connection is already closed. */
static void finish(void)
{
    printf("BYE\n");
    _Exit(0);
}

/* Waits for the driver's GO (or STOP) before the next connection, when gate=1. */
static void pass_gate(int* go_used)
{
    if (!g_opt.gate)
        return;
    set_phase("gate");
    printf("GATE\n");
    while (LOAD(&g_go) <= *go_used)
    {
        if (LOAD(&g_stop))
            finish();
        sleep_ms(2);
    }
    (*go_used)++;
}

/* The accept or connect as the policy does it: OK and *conn, or the result that ends this attempt. The patient policy
 * waits again after a TIMEOUT (on the same listener). */
static fipc_result_t handshake(fipc_listener_t* listener, fipc_conn_t** conn, int attempt)
{
    for (;;)
    {
        const fipc_result_t rc = listener ? fipc_stream_accept(listener, conn, g_opt.ready_ms)
                                          : fipc_stream_connect(g_opt.name, conn, g_opt.ready_ms);
        if (rc == FIPC_OK)
            return rc;
        if (rc != FIPC_TIMEOUT)
        {
            printf("READY_FAILED rc=%s attempt=%d\n", rc_name(rc), attempt);
            return rc;
        }
        printf("READY_TIMEOUT attempt=%d\n", attempt);
        if (g_opt.policy == POLICY_BOUNDED || LOAD(&g_stop))
            return rc;
    }
}

/* ---------- broadcasts: the publisher and its subscribers ---------- */

#define BROADCAST_VERIFY 50 /* messages a subscriber verifies before it reports VERIFIED */
#define BROADCAST_SEND_MS 100
#define BROADCAST_BURST 32 /* the publisher sleeps a millisecond after every BROADCAST_BURST messages */
#define JOIN_SLICE_MS 250

/* A broadcast message's body: 16-255 bytes, from the message's own hash */
static size_t broadcast_body_len(uint32_t channel, uint64_t seq)
{
    return 16 + (size_t) (mix64(msg_base(g_opt.salt, channel, seq) ^ 0xB7ull) % 240);
}

/* Whether a subscriber is joined: the publisher's count, without waiting. */
static bool subscribed(fipc_conn_t* publisher)
{
    uint32_t count = 0;
    return fipc_broadcast_wait_for_members(publisher, 1, &count, 0) == FIPC_OK;
}

/* The publisher: claims the name (max_subscribers=subs, max_stall_ms=stall) and sends checked messages, as fast as
 * BROADCAST_BURST lets it, until STOP (or an error: then a new broadcast). A send that waits longer than
 * BROADCAST_SEND_MS for a gated subscriber reports SEND_TIMEOUT once and is tried again; SEND_RESUMED says when it went
 * through. Its STATUS rx counts the messages sent (its progress). */
static void run_publisher(void)
{
    uint8_t* buf = (uint8_t*) malloc(sizeof(msg_hdr_t) + 256);
    if (!buf)
        FAIL("out of memory");
    bool first_connection = true;
    uint32_t channels = 0;
    for (int attempt = 1;; attempt++)
    {
        if (LOAD(&g_stop))
            finish();
        set_phase("init");
        arm_kill(PHASE_INIT, first_connection);
        fipc_conn_t* writer = NULL;
        const fipc_result_t rc =
            fipc_broadcast_create(g_opt.name, g_opt.ring, g_opt.subscribers, g_opt.stall_ms, &writer);
        if (rc != FIPC_OK)
        {
            printf("INIT_FAILED rc=%s attempt=%d\n", rc_name(rc), attempt);
            sleep_ms(100);
            continue;
        }
        channels++;
        printf("INIT role=publisher attempt=%d channel=%u\n", attempt, channels);
        STORE(&g_connected, 1);
        set_phase("ready");
        arm_kill(PHASE_HANDSHAKE, first_connection);
        set_phase("traffic");
        uint64_t held_since = 0;
        bool armed = false;
        for (uint64_t seq = 0; !LOAD(&g_stop);)
        {
            const size_t body = broadcast_body_len(channels, seq);
            msg_hdr_t h = {MSG_MAGIC, g_pid, g_opt.salt, channels, seq, (uint32_t) body, 0, 0};
            h.checksum = fill_body(buf + sizeof(h), body, msg_base(g_opt.salt, channels, seq));
            memcpy(buf, &h, sizeof(h));
            const fipc_result_t sent = fipc_send(writer, buf, sizeof(h) + body, BROADCAST_SEND_MS);
            if (sent == FIPC_TIMEOUT)
            {
                if (!held_since)
                {
                    held_since = now_ms() - BROADCAST_SEND_MS;
                    printf("SEND_TIMEOUT seq=%" PRIu64 "\n", seq);
                }
                continue;
            }
            if (sent != FIPC_OK)
            {
                printf("SESSION_ERROR rc=%s by=writer channel=%u\n", rc_name(sent), channels);
                break;
            }
            if (held_since)
            {
                printf("SEND_RESUMED seq=%" PRIu64 " waited_ms=%" PRIu64 "\n", seq, now_ms() - held_since);
                held_since = 0;
            }
            seq++;
            ADD(&g_tx_all, 1);
            ADD(&g_rx_all, 1);
            if (!armed && seq >= 200 && subscribed(writer))
            {
                armed = true;
                arm_kill(PHASE_TRAFFIC, first_connection);
            }
            if (seq % BROADCAST_BURST == 0)
                sleep_ms(1);
        }
        STORE(&g_connected, 0);
        set_phase("cleanup");
        fipc_close(writer); /* ends the broadcast */
        printf("CLOSED\n");
        first_connection = false;
    }
}

/* fipc_broadcast_join (a subscriber's role), fipc_queue_join (a producer's or a consumer's) or fipc_service_join (a
 * client's or a server's) in slices of JOIN_SLICE_MS up to ready_ms, so that STOP is seen soon: OK, TIMEOUT or the
 * failure. */
static fipc_result_t join_sliced(int role, fipc_conn_t** conn)
{
    const uint64_t deadline = now_ms() + (uint64_t) g_opt.ready_ms;
    const bool queue = role == FIPC_PRODUCER || role == FIPC_CONSUMER;
    const bool service = role == FIPC_CLIENT || role == FIPC_SERVER;
    for (;;)
    {
        const fipc_result_t rc = service ? fipc_service_join(g_opt.name, role, conn, JOIN_SLICE_MS)
                               : queue   ? fipc_queue_join(g_opt.name, role, conn, JOIN_SLICE_MS)
                                         : fipc_broadcast_join(g_opt.name, role, conn, JOIN_SLICE_MS);
        if (rc != FIPC_TIMEOUT || LOAD(&g_stop) || now_ms() >= deadline)
            return rc;
    }
}

typedef struct
{
    uint32_t session; /* this subscriber's join number */
    bool have_pub;
    uint32_t pub_pid, pub_salt, pub_channel;
    uint64_t expected_seq;
    bool lapped; /* an OVERRUN came last: the next message may skip ahead */
    bool reported_bad;
    uint64_t rx;
} subscription_t;

static bool sub_bad(subscription_t* s, const char* fmt, ...) __attribute__((format(printf, 2, 3)));

static bool sub_bad(subscription_t* s, const char* fmt, ...)
{
    if (!s->reported_bad)
    {
        char detail[400];
        va_list args;
        va_start(args, fmt);
        vsnprintf(detail, sizeof(detail), fmt, args);
        va_end(args);
        printf("BAD_MESSAGE session=%u %s\n", s->session, detail);
        s->reported_bad = true;
    }
    return false;
}

/* A broadcast message: whole (header, length, contents, checksum), of the publisher this subscriber joined, and the
 * next in order. The first after a join may be any (a subscriber receives what is sent after it joined); a lossy one
 * may skip ahead right after an OVERRUN, never back. */
static bool sub_verify(subscription_t* s, const uint8_t* m, size_t len)
{
    msg_hdr_t h;
    if (len < sizeof(h))
        return sub_bad(s, "a message of %zu bytes, shorter than its header", len);
    memcpy(&h, m, sizeof(h));
    if (h.magic != MSG_MAGIC)
        return sub_bad(s, "magic 0x%08x (%zu bytes)", h.magic, len);
    if (h.body_len != len - sizeof(h))
        return sub_bad(s, "seq %" PRIu64 " claims %u body bytes, the message has %zu", h.seq, h.body_len, len);
    if (!s->have_pub)
    {
        s->have_pub = true;
        s->pub_pid = h.sender_pid;
        s->pub_salt = h.salt;
        s->pub_channel = h.session;
        s->expected_seq = h.seq;
    }
    else if (h.sender_pid != s->pub_pid || h.session != s->pub_channel || h.salt != s->pub_salt)
        return sub_bad(
            s,
            "mixed: seq %" PRIu64 " of pid %u channel %u while receiving pid %u channel %u",
            h.seq,
            h.sender_pid,
            h.session,
            s->pub_pid,
            s->pub_channel
        );
    if (h.seq < s->expected_seq)
        return sub_bad(s, "repeat: seq %" PRIu64 ", expected %" PRIu64, h.seq, s->expected_seq);
    if (h.seq > s->expected_seq && !s->lapped)
        return sub_bad(
            s,
            "gap: seq %" PRIu64 ", expected %" PRIu64 " (%" PRIu64 " missing, no OVERRUN)",
            h.seq,
            s->expected_seq,
            h.seq - s->expected_seq
        );
    uint64_t sum = 0;
    const size_t at = check_body(m + sizeof(h), h.body_len, msg_base(h.salt, h.session, h.seq), &sum);
    if (at != h.body_len)
        return sub_bad(s, "corrupt: seq %" PRIu64 " byte %zu of %u differs", h.seq, at, h.body_len);
    if (sum != h.checksum)
        return sub_bad(s, "corrupt: seq %" PRIu64 " checksum differs", h.seq);
    s->lapped = false;
    s->expected_seq = h.seq + 1;
    return true;
}

/* A subscriber (mode=lossy: fipc_recv; gated: fipc_recv; inplace: gated, fipc_recv_acquire): joins, verifies every
 * message, and joins again after the broadcast's end (DISCONNECTED) or its own (UNSUBSCRIBED), until STOP. */
static void run_subscriber(void)
{
    const int role = g_opt.mode == MODE_LOSSY ? FIPC_SUBSCRIBER_LOSSY : FIPC_SUBSCRIBER_GATED;
    const size_t cap = g_opt.ring; /* more than a broadcast's longest message, a quarter of it */
    uint8_t* buf = (uint8_t*) malloc(cap);
    if (!buf)
        FAIL("out of memory");
    bool first_connection = true;
    uint32_t joins = 0;
    for (int attempt = 1;; attempt++)
    {
        if (LOAD(&g_stop))
            finish();
        set_phase("init");
        arm_kill(PHASE_INIT, first_connection);
        printf("INIT role=subscriber attempt=%d\n", attempt);
        set_phase("ready");
        arm_kill(PHASE_HANDSHAKE, first_connection);
        fipc_conn_t* conn = NULL;
        const fipc_result_t joined = join_sliced(role, &conn);
        if (joined != FIPC_OK)
        {
            if (joined == FIPC_TIMEOUT)
                printf("READY_TIMEOUT attempt=%d\n", attempt);
            else
                printf("READY_FAILED rc=%s attempt=%d\n", rc_name(joined), attempt);
            printf("CLOSED\n");
            if (joined != FIPC_TIMEOUT)
                sleep_ms(50);
            continue;
        }
        joins++;
        attempt = 0;
        STORE(&g_connected, 1);
        printf("CONNECTED role=subscriber session=%u\n", joins);
        set_phase("traffic");
        subscription_t s;
        memset(&s, 0, sizeof(s));
        s.session = joins;
        for (;;)
        {
            fipc_result_t rc;
            if (g_opt.mode == MODE_INPLACE)
            {
                const void* at = NULL;
                size_t len = 0;
                rc = fipc_recv_acquire(conn, &at, &len, RECV_MS);
                if (rc == FIPC_OK)
                {
                    sub_verify(&s, (const uint8_t*) at, len);
                    fipc_recv_release(conn);
                }
            }
            else
            {
                size_t len = 0;
                rc = fipc_recv(conn, buf, cap, &len, RECV_MS);
                if (rc == FIPC_OK)
                    sub_verify(&s, buf, len);
            }
            if (rc == FIPC_OK)
            {
                ADD(&g_rx_all, 1);
                if (++s.rx == BROADCAST_VERIFY)
                {
                    printf("VERIFIED session=%u peer=%u peer_session=%u\n", joins, s.pub_pid, s.pub_channel);
                    arm_kill(PHASE_TRAFFIC, first_connection);
                }
                continue;
            }
            if (rc == FIPC_TIMEOUT)
            {
                if (LOAD(&g_stop))
                    break;
                continue;
            }
            if (rc == FIPC_OVERRUN && g_opt.mode == MODE_LOSSY)
            {
                s.lapped = true;
                ADD(&g_overruns, 1);
                continue;
            }
            if (rc == FIPC_DISCONNECTED || rc == FIPC_UNSUBSCRIBED)
                printf("%s session=%u rx=%" PRIu64 "\n", rc_name(rc), joins, s.rx);
            else
                printf("SESSION_ERROR rc=%s by=subscriber session=%u\n", rc_name(rc), joins);
            break;
        }
        STORE(&g_connected, 0);
        set_phase("cleanup");
        fipc_close(conn);
        printf("CLOSED\n");
        first_connection = false;
    }
}

/* ---------- work queues: the owner, its producers and its consumers ---------- */

#define TALLY_MAX 64 /* the producers' streams a consumer keeps count of: a cycle's are a handful */

/* What this consumer took of one producer's stream (its pid and session): written by the consumer's thread, read by
 * the stdin thread for TALLY, so the counts are atomic. A new stream's entry is filled before the count includes it. */
typedef struct
{
    uint32_t pid, session;
    uint64_t taken; /* messages */
    uint64_t sum;   /* of their sequence numbers, wrapping */
    uint64_t last;  /* the sequence number of the last */
} tally_t;

static tally_t g_tally[TALLY_MAX];
static int g_tally_count;

/* TALLIED id=<id> t=<pid>.<session>.<taken>.<sum>.<last>,... : the driver adds every consumer's up per stream and
 * compares them with what each producer sent (devtool_lib/chaos.py). */
static void print_tally(int id)
{
    char text[TALLY_MAX * 72 + 64];
    int at = snprintf(text, sizeof(text), "TALLIED id=%d t=", id);
    const int count = LOAD(&g_tally_count);
    for (int k = 0; k < count && at > 0 && (size_t) at < sizeof(text); k++)
    {
        tally_t* e = &g_tally[k];
        at += snprintf(
            text + at,
            sizeof(text) - (size_t) at,
            "%s%u.%u.%" PRIu64 ".%" PRIu64 ".%" PRIu64,
            k ? "," : "",
            e->pid,
            e->session,
            LOAD(&e->taken),
            LOAD(&e->sum),
            LOAD(&e->last)
        );
    }
    printf("%s\n", text);
}

/* The stream's entry, made on its first message */
static tally_t* tally_of(uint32_t pid, uint32_t session)
{
    const int count = g_tally_count; /* only this thread adds */
    for (int k = 0; k < count; k++)
        if (g_tally[k].pid == pid && g_tally[k].session == session)
            return &g_tally[k];
    if (count == TALLY_MAX)
        FAIL("more than %d producer streams", TALLY_MAX);
    g_tally[count].pid = pid;
    g_tally[count].session = session;
    STORE(&g_tally_count, count + 1);
    return &g_tally[count];
}

/* A queue message: whole (header, length, contents, checksum), and later in its producer's stream than the last this
 * consumer took of it (one producer's messages are taken in its order; the other consumers take the gaps), or, with
 * again=1, earlier (a killed consumer's message that went again, after its producer's later ones). Counted in its
 * stream's tally, whose `last` stays the latest. */
static bool queue_verify(subscription_t* s, const uint8_t* m, size_t len)
{
    msg_hdr_t h;
    if (len < sizeof(h))
        return sub_bad(s, "a message of %zu bytes, shorter than its header", len);
    memcpy(&h, m, sizeof(h));
    if (h.magic != MSG_MAGIC)
        return sub_bad(s, "magic 0x%08x (%zu bytes)", h.magic, len);
    if (h.body_len != len - sizeof(h))
        return sub_bad(s, "seq %" PRIu64 " claims %u body bytes, the message has %zu", h.seq, h.body_len, len);
    uint64_t sum = 0;
    const size_t at = check_body(m + sizeof(h), h.body_len, msg_base(h.salt, h.session, h.seq), &sum);
    if (at != h.body_len)
        return sub_bad(
            s, "corrupt: pid %u seq %" PRIu64 " byte %zu of %u differs", h.sender_pid, h.seq, at, h.body_len
        );
    if (sum != h.checksum)
        return sub_bad(s, "corrupt: pid %u seq %" PRIu64 " checksum differs", h.sender_pid, h.seq);
    tally_t* e = tally_of(h.sender_pid, h.session);
    const bool earlier = LOAD(&e->taken) > 0 && h.seq <= LOAD(&e->last);
    if (earlier && !(g_opt.again && h.seq < LOAD(&e->last)))
        return sub_bad(
            s,
            "out of order: pid %u session %u seq %" PRIu64 " after seq %" PRIu64,
            h.sender_pid,
            h.session,
            h.seq,
            LOAD(&e->last)
        );
    if (!earlier)
        STORE(&e->last, h.seq);
    ADD(&e->sum, h.seq);
    ADD(&e->taken, 1);
    if (g_opt.took)
        printf("TOOK pid=%u session=%u seq=%" PRIu64 "\n", h.sender_pid, h.session, h.seq);
    return true;
}

/* A producer's stream on `producer` (its session: the owner's queue number, or a joined producer's join number):
 * checked messages as the publisher sends them, until QUIET or STOP (FIPC_OK) or a send's failure (its result). A
 * send that waits longer than BROADCAST_SEND_MS for room reports SEND_TIMEOUT once and is tried again. *sent: the
 * messages sent. */
static fipc_result_t queue_produce(fipc_conn_t* producer, uint32_t session, bool first_connection, uint64_t* sent)
{
    uint8_t buf[sizeof(msg_hdr_t) + 256];
    uint64_t held_since = 0;
    uint64_t seq = 0;
    fipc_result_t rc = FIPC_OK;
    while (!LOAD(&g_stop) && !LOAD(&g_quiet))
    {
        const size_t body = broadcast_body_len(session, seq);
        msg_hdr_t h = {MSG_MAGIC, g_pid, g_opt.salt, session, seq, (uint32_t) body, 0, 0};
        h.checksum = fill_body(buf + sizeof(h), body, msg_base(g_opt.salt, session, seq));
        memcpy(buf, &h, sizeof(h));
        const fipc_result_t done = fipc_send(producer, buf, sizeof(h) + body, BROADCAST_SEND_MS);
        if (done == FIPC_TIMEOUT)
        {
            if (!held_since)
            {
                held_since = now_ms() - BROADCAST_SEND_MS;
                printf("SEND_TIMEOUT seq=%" PRIu64 "\n", seq);
            }
            continue;
        }
        if (done != FIPC_OK)
        {
            rc = done;
            break;
        }
        if (held_since)
        {
            printf("SEND_RESUMED seq=%" PRIu64 " waited_ms=%" PRIu64 "\n", seq, now_ms() - held_since);
            held_since = 0;
        }
        seq++;
        ADD(&g_tx_all, 1);
        ADD(&g_rx_all, 1);
        if (seq == BROADCAST_VERIFY)
        {
            printf("VERIFIED session=%u sent=%" PRIu64 "\n", session, seq);
            arm_kill(PHASE_TRAFFIC, first_connection);
        }
        if (seq % BROADCAST_BURST == 0)
            sleep_ms(1);
    }
    *sent = seq;
    return rc;
}

/* After QUIET: QUIETED with the stream's count (session 0: none), then nothing more until STOP. */
static void quieted(uint32_t session, uint64_t sent)
{
    printf("QUIETED session=%u sent=%" PRIu64 "\n", session, sent);
    while (!LOAD(&g_stop))
        sleep_ms(2);
}

/* The owner: claims the name (max_producers=producers, max_consumers=consumers), joins it as a producer (its process
 * admits it, as every member) and sends on that producer until QUIET (then it keeps the queue until STOP) or STOP;
 * fipc_queue_close ends the queue. */
static void run_owner(void)
{
    bool first_connection = true;
    uint32_t queues = 0;
    for (int attempt = 1;; attempt++)
    {
        if (LOAD(&g_stop))
            finish();
        set_phase("init");
        arm_kill(PHASE_INIT, first_connection);
        fipc_queue_t* queue = NULL;
        fipc_conn_t* producer = NULL;
        fipc_result_t rc = fipc_queue_create(g_opt.name, g_opt.ring, g_opt.producers, g_opt.consumers, &queue);
        if (rc == FIPC_OK)
        {
            rc = join_sliced(FIPC_PRODUCER, &producer);
            if (rc != FIPC_OK)
                fipc_queue_close(queue);
        }
        if (rc != FIPC_OK)
        {
            printf("INIT_FAILED rc=%s attempt=%d\n", rc_name(rc), attempt);
            sleep_ms(100);
            continue;
        }
        queues++;
        printf("INIT role=owner attempt=%d queue=%u\n", attempt, queues);
        STORE(&g_connected, 1);
        set_phase("ready");
        arm_kill(PHASE_HANDSHAKE, first_connection);
        set_phase("traffic");
        uint64_t sent = 0;
        const fipc_result_t ended = queue_produce(producer, queues, first_connection, &sent);
        if (ended != FIPC_OK)
            printf("SESSION_ERROR rc=%s by=producer queue=%u\n", rc_name(ended), queues);
        else if (!LOAD(&g_stop))
            quieted(queues, sent);
        STORE(&g_connected, 0);
        set_phase("cleanup");
        fipc_close(producer);
        fipc_queue_close(queue); /* ends the queue */
        printf("CLOSED\n");
        first_connection = false;
    }
}

/* A joined producer or consumer: joins, sends (or receives and checks), and joins again after the queue's end
 * (DISCONNECTED), until STOP. A consumer in place (mode=inplace) holds each message hold_ms. */
static void run_queue_member(void)
{
    const bool producing = g_opt.role == ROLE_PRODUCER;
    const size_t cap = g_opt.ring; /* more than a queue's longest message, a quarter of it */
    uint8_t* buf = (uint8_t*) malloc(cap);
    if (!buf)
        FAIL("out of memory");
    bool first_connection = true;
    uint32_t joins = 0;
    for (int attempt = 1;; attempt++)
    {
        if (LOAD(&g_stop))
            finish();
        if (producing && LOAD(&g_quiet))
            quieted(0, 0);
        set_phase("init");
        arm_kill(PHASE_INIT, first_connection);
        printf("INIT role=%s attempt=%d\n", role_name(), attempt);
        set_phase("ready");
        arm_kill(PHASE_HANDSHAKE, first_connection);
        fipc_conn_t* conn = NULL;
        const fipc_result_t joined = join_sliced(producing ? FIPC_PRODUCER : FIPC_CONSUMER, &conn);
        if (joined != FIPC_OK)
        {
            if (joined == FIPC_TIMEOUT)
                printf("READY_TIMEOUT attempt=%d\n", attempt);
            else
                printf("READY_FAILED rc=%s attempt=%d\n", rc_name(joined), attempt);
            printf("CLOSED\n");
            if (joined != FIPC_TIMEOUT)
                sleep_ms(50);
            continue;
        }
        joins++;
        attempt = 0;
        STORE(&g_connected, 1);
        printf("CONNECTED role=%s session=%u\n", role_name(), joins);
        set_phase("traffic");
        if (producing)
        {
            uint64_t sent = 0;
            const fipc_result_t rc = queue_produce(conn, joins, first_connection, &sent);
            if (rc == FIPC_DISCONNECTED)
                printf("DISCONNECTED session=%u sent=%" PRIu64 "\n", joins, sent);
            else if (rc != FIPC_OK)
                printf("SESSION_ERROR rc=%s by=producer session=%u\n", rc_name(rc), joins);
            else if (!LOAD(&g_stop))
                quieted(joins, sent);
        }
        else
        {
            subscription_t s;
            memset(&s, 0, sizeof(s));
            s.session = joins;
            for (;;)
            {
                fipc_result_t rc;
                if (g_opt.mode == MODE_INPLACE)
                {
                    const void* at = NULL;
                    size_t len = 0;
                    rc = fipc_recv_acquire(conn, &at, &len, RECV_MS);
                    if (rc == FIPC_OK)
                    {
                        queue_verify(&s, (const uint8_t*) at, len);
                        if (g_opt.hold_ms)
                            sleep_ms(g_opt.hold_ms);
                        fipc_recv_release(conn);
                    }
                }
                else
                {
                    size_t len = 0;
                    rc = fipc_recv(conn, buf, cap, &len, RECV_MS);
                    if (rc == FIPC_OK)
                        queue_verify(&s, buf, len);
                }
                if (rc == FIPC_OK)
                {
                    ADD(&g_rx_all, 1);
                    if (++s.rx == BROADCAST_VERIFY)
                    {
                        printf("VERIFIED session=%u\n", joins);
                        arm_kill(PHASE_TRAFFIC, first_connection);
                    }
                    continue;
                }
                if (rc == FIPC_TIMEOUT)
                {
                    if (LOAD(&g_stop))
                        break;
                    continue;
                }
                if (rc == FIPC_DISCONNECTED)
                    printf("DISCONNECTED session=%u rx=%" PRIu64 "\n", joins, s.rx);
                else
                    printf("SESSION_ERROR rc=%s by=consumer session=%u\n", rc_name(rc), joins);
                break;
            }
        }
        STORE(&g_connected, 0);
        set_phase("cleanup");
        fipc_close(conn);
        printf("CLOSED\n");
        first_connection = false;
    }
}

/* ---------- services: the owner, its clients and its servers ---------- */

#define SERVICE_DEPTH_MAX 12    /* a client keeps 1 to this many requests in flight (its depth, from the seed) */
#define SERVICE_DRAIN_MS 5000   /* after QUIET, how long a client may wait for the replies it is owed */
#define REPLY_MAGIC 0x4C505253u /* "SRPL" */

/* In front of every reply; its body follows from (the client's salt, session, seq). */
typedef struct
{
    uint32_t magic;
    uint32_t server_pid;
    uint32_t client_pid;     /* the request's sender */
    uint32_t client_session; /* its join number */
    uint64_t seq;            /* the request's */
    uint32_t body_len;
    uint32_t client_salt;
    uint64_t checksum;
} reply_hdr_t;

/* A reply's body: 16-255 bytes, from the request's own hash, and its words' base */
static size_t reply_body_len(uint32_t salt, uint32_t session, uint64_t seq)
{
    return 16 + (size_t) (mix64(msg_base(salt, session, seq) ^ 0x5Eull) % 240);
}

static uint64_t reply_base(uint32_t salt, uint32_t session, uint64_t seq)
{
    return mix64(msg_base(salt, session, seq) ^ 0x5E5Eull);
}

/* The reply to the request `request` (its header), made in out[0..returned) */
static size_t make_reply(uint8_t* out, const msg_hdr_t* request)
{
    const size_t body = reply_body_len(request->salt, request->session, request->seq);
    reply_hdr_t r = {
        REPLY_MAGIC, g_pid, request->sender_pid, request->session, request->seq, (uint32_t) body, request->salt, 0
    };
    r.checksum = fill_body(out + sizeof(r), body, reply_base(request->salt, request->session, request->seq));
    memcpy(out, &r, sizeof(r));
    return sizeof(r) + body;
}

/* A reply: whole (header, length, contents, checksum), to this client, its session, and request `seq` (the oldest
 * request it hasn't received an answer to: the n-th reply answers the n-th request). */
static bool reply_verify(subscription_t* s, const uint8_t* m, size_t len, uint32_t session, uint64_t seq)
{
    reply_hdr_t r;
    if (len < sizeof(r))
        return sub_bad(s, "a reply of %zu bytes, shorter than its header (to seq %" PRIu64 ")", len, seq);
    memcpy(&r, m, sizeof(r));
    if (r.magic != REPLY_MAGIC)
        return sub_bad(s, "a reply's magic 0x%08x (%zu bytes, to seq %" PRIu64 ")", r.magic, len, seq);
    if (r.client_pid != g_pid || r.client_session != session || r.client_salt != g_opt.salt)
        return sub_bad(
            s,
            "a reply to pid %u session %u salt %u, not to this client (pid %u session %u salt %u), from server %u",
            r.client_pid,
            r.client_session,
            r.client_salt,
            g_pid,
            session,
            g_opt.salt,
            r.server_pid
        );
    if (r.seq != seq)
        return sub_bad(
            s,
            "out of order: the reply to seq %" PRIu64 " came for seq %" PRIu64 " (server %u)",
            r.seq,
            seq,
            r.server_pid
        );
    const size_t body = reply_body_len(g_opt.salt, session, seq);
    if (r.body_len != body || len != sizeof(r) + body)
        return sub_bad(
            s,
            "the reply to seq %" PRIu64 " has %zu bytes, claims %u body bytes, %zu expected",
            seq,
            len,
            r.body_len,
            body
        );
    uint64_t sum = 0;
    const size_t at = check_body(m + sizeof(r), body, reply_base(g_opt.salt, session, seq), &sum);
    if (at != body)
        return sub_bad(s, "corrupt: the reply to seq %" PRIu64 " differs at byte %zu of %zu", seq, at, body);
    if (sum != r.checksum)
        return sub_bad(s, "corrupt: the reply to seq %" PRIu64 ": its checksum differs", seq);
    return true;
}

/* What a client's calls of one session did */
typedef struct
{
    uint64_t sent;      /* requests sent */
    uint64_t answered;  /* answers received: replies and FIPC_ABANDONED */
    uint64_t abandoned; /* of which FIPC_ABANDONED */
} calls_t;

#define CALLS_UNDRAINED (-2) /* service_calls: after QUIET, the replies owed didn't come within SERVICE_DRAIN_MS */

/* A client's calls on `client` (its session: the owner's service number, or a joined client's join number): requests
 * sent while fewer than its depth are in flight, every answer received in order and checked, until STOP (FIPC_OK), or
 * after QUIET until every request is answered (FIPC_OK; CALLS_UNDRAINED past SERVICE_DRAIN_MS), or a call's failure
 * (its result). mode=inplace sends and receives in place. A send that waits longer than BROADCAST_SEND_MS for room
 * reports SEND_TIMEOUT once and is tried again after a receive. It sleeps a millisecond after every BROADCAST_BURST
 * requests, as a producer does. */
static int service_calls(fipc_conn_t* client, uint32_t session, bool first_connection, calls_t* c)
{
    const size_t piece = fipc_max_piece(client);
    uint8_t request[sizeof(msg_hdr_t) + 256];
    uint8_t* reply = (uint8_t*) malloc(piece);
    if (!reply)
        FAIL("out of memory");
    const bool inplace = g_opt.mode == MODE_INPLACE;
    const uint64_t depth = 1 + mix64(((uint64_t) g_opt.salt << 32) ^ session ^ 0xD0ull) % SERVICE_DEPTH_MAX;
    subscription_t s;
    memset(&s, 0, sizeof(s));
    s.session = session;
    memset(c, 0, sizeof(*c));
    uint64_t held_since = 0, drain_deadline = 0;
    int rc = FIPC_OK;
    for (;;)
    {
        if (LOAD(&g_stop))
            break;
        const bool quiet = LOAD(&g_quiet) != 0;
        if (quiet && !drain_deadline)
            drain_deadline = now_ms() + SERVICE_DRAIN_MS;
        if (!quiet && c->sent - c->answered < depth)
        {
            const uint64_t seq = c->sent;
            const size_t body = broadcast_body_len(session, seq);
            msg_hdr_t h = {MSG_MAGIC, g_pid, g_opt.salt, session, seq, (uint32_t) body, 0, 0};
            h.checksum = fill_body(request + sizeof(h), body, msg_base(g_opt.salt, session, seq));
            memcpy(request, &h, sizeof(h));
            const size_t len = sizeof(h) + body;
            fipc_result_t sent;
            if (inplace)
            {
                void* at = NULL;
                sent = fipc_send_acquire(client, len, &at, BROADCAST_SEND_MS);
                if (sent == FIPC_OK)
                {
                    memcpy(at, request, len);
                    sent = fipc_send_commit(client, len);
                }
            }
            else
                sent = fipc_send(client, request, len, BROADCAST_SEND_MS);
            if (sent == FIPC_OK)
            {
                if (held_since)
                {
                    printf("SEND_RESUMED seq=%" PRIu64 " waited_ms=%" PRIu64 "\n", seq, now_ms() - held_since);
                    held_since = 0;
                }
                c->sent++;
                ADD(&g_tx_all, 1);
                if (c->sent % BROADCAST_BURST == 0)
                    sleep_ms(1);
                continue;
            }
            if (sent != FIPC_TIMEOUT)
            {
                rc = sent;
                break;
            }
            if (!held_since)
            {
                held_since = now_ms() - BROADCAST_SEND_MS;
                printf("SEND_TIMEOUT seq=%" PRIu64 "\n", seq);
            }
        }
        if (c->sent == c->answered)
        {
            if (quiet)
                break; /* drained */
            sleep_ms(1);
            continue;
        }
        const void* at = NULL;
        size_t len = 0;
        const fipc_result_t got =
            inplace ? fipc_recv_acquire(client, &at, &len, RECV_MS) : fipc_recv(client, reply, piece, &len, RECV_MS);
        if (got == FIPC_OK || got == FIPC_ABANDONED)
        {
            if (got == FIPC_OK)
            {
                reply_verify(&s, inplace ? (const uint8_t*) at : reply, len, session, c->answered);
                if (inplace)
                    fipc_recv_release(client);
            }
            else
            {
                if (len != 0)
                    sub_bad(&s, "FIPC_ABANDONED for seq %" PRIu64 " with *out_len %zu, not 0", c->answered, len);
                printf("ABANDONED session=%u seq=%" PRIu64 "\n", session, c->answered);
                c->abandoned++;
            }
            c->answered++;
            ADD(&g_rx_all, 1);
            if (c->answered == BROADCAST_VERIFY)
            {
                printf("VERIFIED session=%u sent=%" PRIu64 "\n", session, c->sent);
                arm_kill(PHASE_TRAFFIC, first_connection);
            }
            continue;
        }
        if (got == FIPC_TIMEOUT)
        {
            if (quiet && now_ms() >= drain_deadline)
            {
                rc = CALLS_UNDRAINED;
                break;
            }
            continue;
        }
        rc = got;
        break;
    }
    free(reply);
    return rc;
}

/* How a client's session ended: DISCONNECTED (then it joins again; for the owner's own client, by=owner, a
 * SESSION_ERROR), SESSION_ERROR, or after QUIET its counts (QUIETED, or UNDRAINED) and nothing more until STOP. */
static void calls_ended(int rc, uint32_t session, const calls_t* c, const char* by)
{
    if (rc == FIPC_DISCONNECTED && strcmp(by, "owner") != 0)
        printf(
            "DISCONNECTED session=%u sent=%" PRIu64 " answered=%" PRIu64 " abandoned=%" PRIu64 "\n",
            session,
            c->sent,
            c->answered,
            c->abandoned
        );
    else if (rc == CALLS_UNDRAINED || (rc == FIPC_OK && !LOAD(&g_stop)))
    {
        printf(
            "%s session=%u sent=%" PRIu64 " answered=%" PRIu64 " abandoned=%" PRIu64 "\n",
            rc == FIPC_OK ? "QUIETED" : "UNDRAINED",
            session,
            c->sent,
            c->answered,
            c->abandoned
        );
        while (!LOAD(&g_stop))
            sleep_ms(2);
    }
    else if (rc != FIPC_OK)
        printf("SESSION_ERROR rc=%s by=%s session=%u\n", rc_name(rc), by, session);
}

/* The service's owner: claims the name (max_clients=clients, max_servers=servers, max_message=message), joins it as a
 * client (its process admits it, as every member) and calls on that client until QUIET (then it keeps the service
 * until STOP) or STOP; fipc_service_close ends the service. */
static void run_service_owner(void)
{
    bool first_connection = true;
    uint32_t services = 0;
    for (int attempt = 1;; attempt++)
    {
        if (LOAD(&g_stop))
            finish();
        set_phase("init");
        arm_kill(PHASE_INIT, first_connection);
        fipc_service_t* service = NULL;
        fipc_conn_t* client = NULL;
        fipc_result_t rc =
            fipc_service_create(g_opt.name, g_opt.ring, g_opt.message, g_opt.clients, g_opt.servers, &service);
        if (rc == FIPC_OK)
        {
            rc = join_sliced(FIPC_CLIENT, &client);
            if (rc != FIPC_OK)
                fipc_service_close(service);
        }
        if (rc != FIPC_OK)
        {
            printf("INIT_FAILED rc=%s attempt=%d\n", rc_name(rc), attempt);
            sleep_ms(100);
            continue;
        }
        services++;
        printf("INIT role=service-owner attempt=%d service=%u\n", attempt, services);
        STORE(&g_connected, 1);
        set_phase("ready");
        arm_kill(PHASE_HANDSHAKE, first_connection);
        set_phase("traffic");
        calls_t c;
        const int ended = service_calls(client, services, first_connection, &c);
        calls_ended(ended, services, &c, "owner"); /* its client never sees the service end while it keeps it */
        STORE(&g_connected, 0);
        set_phase("cleanup");
        fipc_close(client);
        fipc_service_close(service); /* ends the service */
        printf("CLOSED\n");
        first_connection = false;
    }
}

/* A service's server on `server` (its session: its join number): every request received, checked and replied to,
 * until STOP (FIPC_OK) or a call's failure (its result). mode=inplace receives and replies in place; hold_ms waits
 * before each reply; took=1 prints TOOK and REPLIED; kill=held:N ends the process holding its N-th request. */
static fipc_result_t service_serve(fipc_conn_t* server, uint32_t session, bool first_connection, uint64_t* taken)
{
    const size_t piece = fipc_max_piece(server);
    uint8_t* buf = (uint8_t*) malloc(piece);
    uint8_t* out = (uint8_t*) malloc(sizeof(reply_hdr_t) + 256);
    if (!buf || !out)
        FAIL("out of memory");
    const bool inplace = g_opt.mode == MODE_INPLACE;
    subscription_t s;
    memset(&s, 0, sizeof(s));
    s.session = session;
    uint64_t replied = 0;
    *taken = 0;
    fipc_result_t rc = FIPC_OK;
    while (!LOAD(&g_stop))
    {
        const void* at = NULL;
        size_t len = 0;
        const fipc_result_t got =
            inplace ? fipc_recv_acquire(server, &at, &len, RECV_MS) : fipc_recv(server, buf, piece, &len, RECV_MS);
        if (got == FIPC_TIMEOUT)
            continue;
        if (got != FIPC_OK)
        {
            rc = got;
            break;
        }
        const uint8_t* m = inplace ? (const uint8_t*) at : buf;
        queue_verify(&s, m, len); /* prints TOOK with took=1 */
        msg_hdr_t h;
        memset(&h, 0, sizeof(h));
        memcpy(&h, m, len < sizeof(h) ? len : sizeof(h));
        (*taken)++;
        ADD(&g_rx_all, 1);
        if (first_connection && g_opt.kill_phase == PHASE_HELD && *taken == g_opt.kill_us)
            do_crash(); /* holding the request: its client must get FIPC_ABANDONED */
        if (g_opt.hold_ms)
            sleep_ms(g_opt.hold_ms);
        const size_t reply_len = sizeof(reply_hdr_t) + reply_body_len(h.salt, h.session, h.seq);
        fipc_result_t answered;
        if (inplace)
        {
            void* room = NULL;
            answered = fipc_send_acquire(server, reply_len, &room, 0);
            if (answered == FIPC_OK)
            {
                make_reply((uint8_t*) room, &h);
                answered = fipc_send_commit(server, reply_len);
            }
        }
        else
        {
            make_reply(out, &h);
            answered = fipc_send(server, out, reply_len, 0);
        }
        if (answered != FIPC_OK)
        {
            rc = answered;
            break;
        }
        replied++;
        ADD(&g_tx_all, 1);
        if (g_opt.took)
            printf("REPLIED pid=%u session=%u seq=%" PRIu64 "\n", h.sender_pid, h.session, h.seq);
        if (replied == BROADCAST_VERIFY)
        {
            printf("VERIFIED session=%u\n", session);
            arm_kill(PHASE_TRAFFIC, first_connection);
        }
    }
    free(buf);
    free(out);
    return rc;
}

/* A joined client or server: joins, calls (or serves), and joins again after the service's end (DISCONNECTED), until
 * STOP. */
static void run_service_member(void)
{
    const bool calling = g_opt.role == ROLE_SERVICE_CLIENT;
    bool first_connection = true;
    uint32_t joins = 0;
    for (int attempt = 1;; attempt++)
    {
        if (LOAD(&g_stop))
            finish();
        if (calling && LOAD(&g_quiet))
        {
            const calls_t none = {0, 0, 0};
            calls_ended(FIPC_OK, 0, &none, "client");
        }
        set_phase("init");
        arm_kill(PHASE_INIT, first_connection);
        printf("INIT role=%s attempt=%d\n", role_name(), attempt);
        set_phase("ready");
        arm_kill(PHASE_HANDSHAKE, first_connection);
        fipc_conn_t* conn = NULL;
        const fipc_result_t joined = join_sliced(calling ? FIPC_CLIENT : FIPC_SERVER, &conn);
        if (joined != FIPC_OK)
        {
            if (joined == FIPC_TIMEOUT)
                printf("READY_TIMEOUT attempt=%d\n", attempt);
            else
                printf("READY_FAILED rc=%s attempt=%d\n", rc_name(joined), attempt);
            printf("CLOSED\n");
            if (joined != FIPC_TIMEOUT)
                sleep_ms(50);
            continue;
        }
        joins++;
        attempt = 0;
        STORE(&g_connected, 1);
        printf("CONNECTED role=%s session=%u\n", role_name(), joins);
        set_phase("traffic");
        if (calling)
        {
            calls_t c;
            const int rc = service_calls(conn, joins, first_connection, &c);
            calls_ended(rc, joins, &c, "client");
        }
        else
        {
            uint64_t taken = 0;
            const fipc_result_t rc = service_serve(conn, joins, first_connection, &taken);
            if (rc == FIPC_DISCONNECTED)
                printf("DISCONNECTED session=%u rx=%" PRIu64 "\n", joins, taken);
            else if (rc != FIPC_OK)
                printf("SESSION_ERROR rc=%s by=server session=%u\n", rc_name(rc), joins);
        }
        STORE(&g_connected, 0);
        set_phase("cleanup");
        fipc_close(conn);
        printf("CLOSED\n");
        first_connection = false;
    }
}

int main(int argc, char** argv)
{
    setvbuf(stdout, NULL, _IONBF, 0); /* the driver reads events line by line off the pipe */
    g_role = argc > 1 ? argv[1] : "?";
    parse_options(argc, argv);
    g_pid = current_pid();
#if defined(_WIN32)
    /* A crash or abort must end the process, not wait in an error dialog (so a null deref or abort() terminates
     * quickly rather than triggering the JIT-debugger prompt) */
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX);
#endif
    static session_t session;
    session.send_buf = (uint8_t*) malloc(max_message());
    session.recv_len = max_message();
    session.recv_buf = (uint8_t*) malloc(session.recv_len);
    if (!session.send_buf || !session.recv_buf)
        FAIL("out of memory");
    printf("START pid=%u\n", g_pid);

    thread_t t;
    thread_start(&t, stdin_main, NULL);
    thread_detach(t);
    thread_start(&t, status_main, NULL);
    thread_detach(t);
    if (g_opt.role == ROLE_PUBLISHER)
        run_publisher();
    if (g_opt.role == ROLE_SUBSCRIBER)
        run_subscriber();
    if (g_opt.role == ROLE_OWNER)
        run_owner();
    if (g_opt.role == ROLE_PRODUCER || g_opt.role == ROLE_CONSUMER)
        run_queue_member();
    if (g_opt.role == ROLE_SERVICE_OWNER)
        run_service_owner();
    if (g_opt.role == ROLE_SERVICE_CLIENT || g_opt.role == ROLE_SERVICE_SERVER)
        run_service_member();

    bool first_connection = true;
    uint32_t connections = 0;
    int attempt = 0, go_used = 0;
    for (;;)
    {
        if (LOAD(&g_stop))
            finish();
        attempt++;
        if (g_opt.policy == POLICY_BOUNDED && attempt > BOUNDED_ATTEMPTS)
        {
            printf("GAVE_UP attempts=%d\n", attempt - 1);
            _Exit(EXIT_GAVE_UP);
        }

        set_phase("init");
        arm_kill(PHASE_INIT, first_connection);
        fipc_listener_t* listener = NULL;
        if (g_opt.role == ROLE_SERVER)
        {
            const fipc_result_t rc = fipc_stream_create(g_opt.name, g_opt.ring, &listener);
            if (rc != FIPC_OK)
            {
                printf("INIT_FAILED rc=%s attempt=%d\n", rc_name(rc), attempt);
                sleep_ms(100);
                continue;
            }
        }
        printf("INIT role=%s attempt=%d\n", role_name(), attempt);

        set_phase("ready");
        arm_kill(PHASE_HANDSHAKE, first_connection);
        fipc_conn_t* conn = NULL;
        if (handshake(listener, &conn, attempt) != FIPC_OK)
        {
            close_all(NULL, listener);
            if (LOAD(&g_stop))
                finish();
            continue;
        }

        STORE(&g_connected, 1);
        connections++;
        printf("CONNECTED role=%s session=%u attempt=%d\n", role_name(), connections, attempt);
        attempt = 0;

        uint8_t* send_buf = session.send_buf;
        uint8_t* recv_buf = session.recv_buf;
        const size_t recv_len = session.recv_len;
        memset(&session, 0, sizeof(session));
        session.send_buf = send_buf;
        session.recv_buf = recv_buf;
        session.recv_len = recv_len;
        session.conn = conn;
        session.session = connections;
        session.drain_rng = mix64(g_opt.salt ^ connections);
        const enum session_end why = run_session(&session, first_connection);
        const bool leaving = why == END_LEAVE;
        if (why == END_THREAD)
        {
            if (session.end_rc == FIPC_DISCONNECTED)
                printf("DISCONNECTED rc=%s by=%s session=%u\n", rc_name(session.end_rc), session.end_by, connections);
            else
                printf("SESSION_ERROR rc=%s by=%s session=%u\n", rc_name(session.end_rc), session.end_by, connections);
        }
        if (leaving)
        {
            STORE(&g_connected, 0);
            set_phase("cleanup");
            arm_kill(PHASE_CLEANUP, first_connection);
            fipc_close(conn);
            if (listener)
                fipc_listener_close(listener);
            if (g_opt.kill_phase == PHASE_CLEANUP)
                do_crash(); /* killed while quitting: never a clean exit */
            printf("CLOSED\n");
        }
        else
            close_all(conn, listener);
        first_connection = false;

        if (why == END_STOP)
            finish();
        if (leaving)
        {
            printf("LEFT session=%u\n", connections);
            if (g_opt.rejoin_ms < 0)
                finish();
            set_phase("rejoin");
            sleep_ms((unsigned) g_opt.rejoin_ms);
            continue;
        }
        pass_gate(&go_used);
    }
}
