/*
 * The chaos harness peer: `python devtool.py test chaos` (devtool_lib/chaos.py starts, kills, suspends and restarts
 * these peers and reads their events). One self-contained program, built against this tree's static library through
 * the public header, include/fipc.h: the one consumer of the library that goes through it.
 *
 * A chaos peer is one side of a game platform's connection, a typical user: RPC only, 1 MiB rings, a producer
 * thread on blocking fipc_rpc_submit and a consumer thread on fipc_rpc_recv(100 ms) into a buffer of its own. Both
 * sides submit and receive. Recovery is a new connection. Every message is verified: sender and session, sequence
 * (nothing lost, repeated or stale), the library's request id, length, contents and checksum. A peer's end is reported
 * when its process exits (EOF on the control connection), so DISCONNECTED arrives in milliseconds. A send reports the
 * peer's end at once, and only stops the producer; the consumer's FIPC_DISCONNECTED ends the connection, after it has
 * received every message the peer completed. So the harness also checks fipc.h's promise that receives deliver those
 * first: a peer that leaves right after its verified exchange may leave its last messages unread in the ring.
 *
 * Usage: chaos_peer <server|client> <unity|game> <name> [key=value ...]
 *   role    server: fipc_listen, then fipc_accept(ready_ms); the listener lives until the connection ends, and every
 *                   new connection listens again;
 *           client: fipc_connect(ready_ms).
 *   policy  unity: after a TIMEOUT, wait again (accept on the same listener, or connect again): the Unity backend;
 *           game:  3 attempts, then GAVE_UP and exit status 4 (the game). Both make a new connection after a
 *                  disconnect; a game wouldn't, the harness needs it to reconnect.
 *   seed=N          salt of the message sizes and contents
 *   ring=BYTES      ring size (server; default 1 MiB): the client learns it, and needs it for the message sizes
 *   ready_ms=MS     accept / connect timeout (default 5000)
 *   gate=1          after a disconnect, wait for GO on stdin before the next connection
 *   traffic=mixed   (default) bursts of mostly small messages, 3% of 1-3.5 rings;
 *           stream  back-to-back messages of 1.5-3.5 rings (every one in several pieces);
 *           drain   small messages every 10 ms, and a pause of up to 2 ms after each message received;
 *           sink    small messages every 10 ms, and no pause after a message received
 *   kill=PHASE:US   end this process US microseconds after PHASE of its first connection starts:
 *                   init (server: fipc_listen; client: fipc_connect), handshake (server: fipc_accept; client:
 *                   fipc_connect), traffic (the verified exchange is done), cleanup (fipc_close after leaving; ended
 *                   right after it if it returns first)
 *   crash=segv|abort  the kill lands as a real crash (a null dereference or abort()) rather than a hard kill, so
 *                   the OS's default crash handling decides how soon the peer sees EOF (measured, not gated)
 *   leave=MS        leave the first connection MS ms after the verified exchange (cancel, join, close)
 *   rejoin=MS       ... and connect (or listen) again MS ms later in this process (a graceful in-process reconnect)
 *
 * The crash-at-step scenario needs no option here: the driver sets FASTIPC_TEST_CRASH_AT in the peer's
 * environment, and the library (static build) ends the process at that lifecycle step (exit 99).
 * stdin:  GO (continue at the gate), STOP (leave gracefully: BYE, exit status 0).
 * stdout: one event per line (START, INIT, INIT_FAILED, READY_TIMEOUT, READY_FAILED, CONNECTED, VERIFIED,
 *         DISCONNECTED, SESSION_ERROR, BAD_MESSAGE, CLOSED, LEFT, GATE, KILL_ARMED, GAVE_UP, BYE) and a STATUS
 *         line every 500 ms: see devtool_lib/chaos.py.
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
#define GAME_ATTEMPTS 3              /* the game policy's attempts */
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
    ROLE_CLIENT
};

enum policy
{
    POLICY_UNITY,
    POLICY_GAME
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
    PHASE_CLEANUP
};

/* How the killer thread ends this process: a hard kill (as an outside SIGKILL/TerminateProcess), or a real crash
 * with the OS's default handling left in place: a null dereference or abort(). */
enum crash_method
{
    CRASH_KILL,
    CRASH_SEGV,
    CRASH_ABORT
};

static const char* const PHASE_NAMES[] = {"none", "init", "handshake", "traffic", "cleanup"};

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
} g_opt;

static void parse_options(int argc, char** argv)
{
    if (argc < 4)
        FAIL("usage: chaos_peer <server|client> <unity|game> <name> [key=value ...]");
    if (strcmp(argv[1], "server") == 0)
        g_opt.role = ROLE_SERVER;
    else if (strcmp(argv[1], "client") == 0)
        g_opt.role = ROLE_CLIENT;
    else
        FAIL("unknown role (expected server or client)");
    if (strcmp(argv[2], "unity") == 0)
        g_opt.policy = POLICY_UNITY;
    else if (strcmp(argv[2], "game") == 0)
        g_opt.policy = POLICY_GAME;
    else
        FAIL("unknown policy (expected unity or game)");
    g_opt.name = argv[3];
    g_opt.salt = 1;
    g_opt.ring = 1u << 20;
    g_opt.ready_ms = 5000;
    g_opt.leave_ms = -1;
    g_opt.rejoin_ms = -1;
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
            for (int p = PHASE_INIT; p <= PHASE_CLEANUP; p++)
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
    return g_opt.role == ROLE_SERVER ? "server" : "client";
}

/* ---------- process state ---------- */

static uint32_t g_pid;
static int g_stop;        /* STOP received */
static int g_go;          /* GO lines received */
static int g_connected;   /* this side has a connection (for STATUS) */
static uint64_t g_tx_all; /* messages sent, all connections */
static uint64_t g_rx_all; /* messages received and verified, all connections */
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
 * crash whose default OS handling (a core dump on Linux, WER on Windows; the dialog is suppressed by SetErrorMode)
 * governs how long the kernel takes to close the process's handles and so how long the peer takes to see EOF. */
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
            " res=%ld\n",
            now_ms(),
            g_phase,
            role_name(),
            LOAD(&g_connected),
            LOAD(&g_tx_all),
            LOAD(&g_rx_all),
            cpu_ms(),
            resource_count()
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
    /* RPC's header (the size of fipc_rpc_msg_t) + our header + body in one message of more than one piece */
    return sizeof(fipc_rpc_msg_t) + sizeof(msg_hdr_t) + body_len > fipc_max_piece(conn);
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

/* The largest message: a header and a body of 3.5 rings */
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

static bool verify(session_t* s, const fipc_rpc_msg_t* msg, const uint8_t* payload)
{
    const uint64_t len = msg->len;
    msg_hdr_t h;
    if (msg->kind != FIPC_RPC_REQUEST)
        return bad_message(s, "kind %u (id %" PRIu64 "), not a request", msg->kind, msg->id);
    if (len < sizeof(h))
        return bad_message(
            s, "payload of %" PRIu64 " bytes (id %" PRIu64 "), shorter than a message header", len, msg->id
        );
    memcpy(&h, payload, sizeof(h));
    if (h.magic != MSG_MAGIC)
        return bad_message(s, "magic 0x%08x (id %" PRIu64 ", %" PRIu64 " bytes)", h.magic, msg->id, len);
    if (h.body_len != len - sizeof(h))
        return bad_message(s, "seq %" PRIu64 " claims %u body bytes, the payload has %" PRIu64, h.seq, h.body_len, len);
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
    if (msg->id != h.seq + 1)
        return bad_message(s, "id %" PRIu64 " on seq %" PRIu64 " (expected seq + 1)", msg->id, h.seq);
    uint64_t sum = 0;
    const size_t at = check_body(payload + sizeof(h), h.body_len, msg_base(h.salt, h.session, h.seq), &sum);
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
        uint64_t id = 0;
        const fipc_result_t rc =
            fipc_rpc_submit(s->conn, (uint32_t) (seq & 0xFFFFu), s->send_buf, sizeof(h) + body, &id, FIPC_FOREVER);
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
        if (id != seq + 1)
            printf("BAD_MESSAGE session=%u submit assigned id %" PRIu64 " to seq %" PRIu64 "\n", s->session, id, seq);
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
        fipc_rpc_msg_t msg;
        fipc_result_t rc = fipc_rpc_recv(s->conn, s->recv_buf, s->recv_len, &msg, RECV_MS);
        if (rc == FIPC_TOO_LARGE)
        {
            /* Longer than any message this harness sends: take it into a buffer of its size, then report it */
            uint8_t* bigger = (uint8_t*) realloc(s->recv_buf, (size_t) msg.len);
            if (!bigger)
                FAIL("out of memory for a message of %" PRIu64 " bytes", msg.len);
            s->recv_buf = bigger;
            s->recv_len = (size_t) msg.len;
            rc = fipc_rpc_recv(s->conn, s->recv_buf, s->recv_len, &msg, RECV_MS);
        }
        if (rc == FIPC_TIMEOUT)
            continue;
        if (rc != FIPC_OK)
        {
            end_session(s, rc, "consumer");
            break;
        }
        if (!verify(s, &msg, s->recv_buf))
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

/* The accept or connect as the policy does it: OK and *conn, or the result that ends this attempt. The unity policy
 * waits again after a TIMEOUT (on the same listener). */
static fipc_result_t handshake(fipc_listener_t* listener, fipc_conn_t** conn, int attempt)
{
    for (;;)
    {
        const fipc_result_t rc =
            listener ? fipc_accept(listener, conn, g_opt.ready_ms) : fipc_connect(g_opt.name, conn, g_opt.ready_ms);
        if (rc == FIPC_OK)
            return rc;
        if (rc != FIPC_TIMEOUT)
        {
            printf("READY_FAILED rc=%s attempt=%d\n", rc_name(rc), attempt);
            return rc;
        }
        printf("READY_TIMEOUT attempt=%d\n", attempt);
        if (g_opt.policy == POLICY_GAME || LOAD(&g_stop))
            return rc;
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

    bool first_connection = true;
    uint32_t connections = 0;
    int attempt = 0, go_used = 0;
    for (;;)
    {
        if (LOAD(&g_stop))
            finish();
        attempt++;
        if (g_opt.policy == POLICY_GAME && attempt > GAME_ATTEMPTS)
        {
            printf("GAVE_UP attempts=%d\n", attempt - 1);
            _Exit(EXIT_GAVE_UP);
        }

        set_phase("init");
        arm_kill(PHASE_INIT, first_connection);
        fipc_listener_t* listener = NULL;
        if (g_opt.role == ROLE_SERVER)
        {
            const fipc_result_t rc = fipc_listen(g_opt.name, g_opt.ring, &listener);
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
