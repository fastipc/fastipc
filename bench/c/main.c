/*
 * FastIPC's C and C++ benchmark: one-way throughput between this process, a server that receives and times, and a
 * client process (this program again) that sends. The same cases, output and checks as the Python, C#, Java, Rust, Lua,
 * JavaScript and Go benchmarks.
 *
 *   c_bench copy|zerocopy|rpc       the loops of loops.c, through the C API (include/fipc.h)
 *   cpp_bench copy|zerocopy|rpc     the loops of loops.cpp, through the C++ wrapper (include/fipc.hpp)
 *
 *   copy      fipc_send / fipc_recv into a buffer the server reuses
 *   zerocopy  fipc_send_acquire + fipc_send_commit / fipc_recv_acquire, copied out, + fipc_recv_release (a message of
 *             several pieces through the copying calls)
 *   rpc       fipc_rpc_submit / fipc_rpc_recv into a buffer the server reuses
 *
 * Each case first sends messages untimed for 0.2 s (the warm-up; at least its count), then a start marker, then its
 * count, and the server times from the last start marker to the end marker (bench.h). Each case prints "Test i/n:
 * name" and "Throughput: n messages/sec" (devtool bench-compare reads them).
 */

#ifndef _WIN32
#define _POSIX_C_SOURCE 200809L /* clock_gettime, posix_spawn, readlink */
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bench.h"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <time.h>
#include <unistd.h>

#include <sys/wait.h>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

extern char** environ;
#endif

#define CONNECT_MS 15000

typedef struct
{
    size_t count;
    size_t ring;
    size_t size;
    const char* name;
} bench_case;

/* The cases of the other benchmarks */
static const bench_case cases[] = {
    {2000000,      512 * 1024,          16,                    "Tiny messages (16B)"},
    {2000000,      512 * 1024,          64,                   "Small messages (64B)"},
    {1000000,      512 * 1024,         256,                 "Medium messages (256B)"},
    {   1000,      512 * 1024,   64 * 1024,                  "Large messages (64KB)"},
    {   1000, 2 * 1024 * 1024,  512 * 1024,                 "Large messages (512KB)"},
    {     10,      512 * 1024, 1024 * 1024, "Exceeds buffer (1MB msg, 512KB buffer)"},
};

#define CASE_COUNT (sizeof cases / sizeof cases[0])

/* === The platform: a monotonic clock, this program's path, the client process === */

#ifdef _WIN32

double bench_now(void)
{
    LARGE_INTEGER count, frequency;
    QueryPerformanceCounter(&count);
    QueryPerformanceFrequency(&frequency);
    return (double) count.QuadPart / (double) frequency.QuadPart;
}

typedef HANDLE process_t;

static unsigned long process_id(void)
{
    return GetCurrentProcessId();
}

/* Starts this program as the client of a case (its output discarded); 0 if it couldn't */
static process_t spawn_client(const char* mode, const char* name, size_t size, size_t count)
{
    char path[MAX_PATH];
    if (GetModuleFileNameA(NULL, path, sizeof path) == 0)
        return 0;
    char line[MAX_PATH + 256];
    snprintf(line, sizeof line, "\"%s\" client %s %s %zu %zu", path, mode, name, size, count);
    SECURITY_ATTRIBUTES inherit = {.nLength = sizeof inherit, .bInheritHandle = TRUE};
    HANDLE nul = CreateFileA("NUL", GENERIC_WRITE, FILE_SHARE_WRITE, &inherit, OPEN_EXISTING, 0, NULL);
    STARTUPINFOA startup = {.cb = sizeof startup, .dwFlags = STARTF_USESTDHANDLES};
    startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
    startup.hStdOutput = nul;
    startup.hStdError = GetStdHandle(STD_ERROR_HANDLE);
    PROCESS_INFORMATION info;
    const BOOL ok = CreateProcessA(NULL, line, NULL, NULL, TRUE, 0, NULL, NULL, &startup, &info);
    CloseHandle(nul);
    if (!ok)
        return 0;
    CloseHandle(info.hThread);
    return info.hProcess;
}

/* Waits for the process (killing it first if `kill`); its exit code */
static int wait_process(process_t process, int kill)
{
    if (kill)
        TerminateProcess(process, 1);
    WaitForSingleObject(process, INFINITE);
    DWORD code = 1;
    GetExitCodeProcess(process, &code);
    CloseHandle(process);
    return (int) code;
}

#else

double bench_now(void)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (double) now.tv_sec + (double) now.tv_nsec / 1e9;
}

typedef pid_t process_t;

static unsigned long process_id(void)
{
    return (unsigned long) getpid();
}

static process_t spawn_client(const char* mode, const char* name, size_t size, size_t count)
{
    char path[4096];
#ifdef __APPLE__
    uint32_t path_size = sizeof path;
    if (_NSGetExecutablePath(path, &path_size) != 0)
        return 0;
#else
    const ssize_t n = readlink("/proc/self/exe", path, sizeof path - 1);
    if (n <= 0)
        return 0;
    path[n] = '\0';
#endif
    char size_arg[32], count_arg[32];
    snprintf(size_arg, sizeof size_arg, "%zu", size);
    snprintf(count_arg, sizeof count_arg, "%zu", count);
    char* argv[] = {path, "client", (char*) mode, (char*) name, size_arg, count_arg, NULL};
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, "/dev/null", O_WRONLY, 0);
    pid_t pid = 0;
    const int failed = posix_spawn(&pid, path, &actions, NULL, argv, environ);
    posix_spawn_file_actions_destroy(&actions);
    return failed ? 0 : pid;
}

static int wait_process(process_t process, int kill_first)
{
    if (kill_first)
        kill(process, SIGKILL);
    int status = 0;
    if (waitpid(process, &status, 0) != process)
        return 1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

#endif

/* === The cases === */

static const char* mode_name(enum bench_mode mode)
{
    return mode == BENCH_COPY ? "copy" : mode == BENCH_ZEROCOPY ? "zerocopy" : "rpc";
}

static int parse_mode(const char* name, enum bench_mode* mode)
{
    if (strcmp(name, "copy") == 0)
        *mode = BENCH_COPY;
    else if (strcmp(name, "zerocopy") == 0)
        *mode = BENCH_ZEROCOPY;
    else if (strcmp(name, "rpc") == 0)
        *mode = BENCH_RPC;
    else
        return 0;
    return 1;
}

/* Runs one case: listens, starts the client, serves it, checks what arrived and prints the rate. 1 if it passed. */
static int run_case(enum bench_mode mode, const bench_case* c, size_t index)
{
    char name[64];
    snprintf(name, sizeof name, "cbench_%lu_%zu", process_id(), index);
    fipc_listener_t* listener;
    fipc_result_t result = fipc_listen(name, c->ring, &listener);
    if (result != FIPC_OK)
    {
        fprintf(stderr, "listen: %s\n", fipc_result_str(result));
        return 0;
    }
    process_t client = spawn_client(mode_name(mode), name, c->size, c->count);
    if (!client)
    {
        fprintf(stderr, "couldn't start the client\n");
        fipc_listener_close(listener);
        return 0;
    }
    fipc_conn_t* conn;
    result = fipc_accept(listener, &conn, CONNECT_MS);
    bench_served served = {0, 0, 0.0, result};
    if (result == FIPC_OK)
        served = bench_serve(conn, mode, c->size);
    fipc_listener_close(listener);
    if (served.result != FIPC_OK)
    {
        fprintf(stderr, "server: %s\n", fipc_result_str(served.result));
        wait_process(client, 1);
        return 0;
    }
    const int status = wait_process(client, 0);
    if (status != 0)
    {
        fprintf(stderr, "the client failed: exit code %d\n", status);
        return 0;
    }
    if (served.messages != c->count || served.bytes != c->count * c->size)
    {
        fprintf(
            stderr,
            "expected %zu messages of %zu B, got %zu (%zu B)\n",
            c->count,
            c->size,
            served.messages,
            served.bytes
        );
        return 0;
    }
    printf("Messages: %zu | Ring: %zuKB | Size: %zuB\n", c->count, c->ring / 1024, c->size);
    printf("Duration: %.3fs\n", served.seconds);
    printf(
        "Throughput: %.0f messages/sec, %.1f MB/sec\n",
        (double) served.messages / served.seconds,
        (double) served.bytes / served.seconds / (1024.0 * 1024.0)
    );
    return 1;
}

/* The client process: `client <mode> <name> <size> <count>` */
static int client(int argc, char** argv)
{
    enum bench_mode mode;
    if (argc != 6 || !parse_mode(argv[2], &mode))
        return 2;
    const size_t size = (size_t) strtoull(argv[4], NULL, 10);
    const size_t count = (size_t) strtoull(argv[5], NULL, 10);
    fipc_conn_t* conn;
    fipc_result_t result = fipc_connect(argv[3], &conn, CONNECT_MS);
    if (result == FIPC_OK)
        result = bench_send(conn, mode, size, count);
    if (result != FIPC_OK)
    {
        fprintf(stderr, "client: %s\n", fipc_result_str(result));
        return 1;
    }
    return 0;
}

int main(int argc, char** argv)
{
    if (argc >= 2 && strcmp(argv[1], "client") == 0)
        return client(argc, argv);
    enum bench_mode mode = BENCH_COPY;
    if (argc > 2 || (argc == 2 && !parse_mode(argv[1], &mode)))
    {
        fprintf(stderr, "usage: %s copy|zerocopy|rpc\n", argv[0]);
        return 2;
    }
    printf("FastIPC %s benchmark: %s\n\n", bench_api, mode_name(mode));
    size_t passed = 0;
    for (size_t i = 0; i < CASE_COUNT; i++)
    {
        printf("Test %zu/%zu: %s\n", i + 1, CASE_COUNT, cases[i].name);
        fflush(stdout);
        if (run_case(mode, &cases[i], i))
            passed++;
        else
            printf("FAILED\n");
        printf("\n");
        fflush(stdout);
    }
    printf("Summary: %zu/%zu tests passed\n", passed, CASE_COUNT);
    return passed == CASE_COUNT ? 0 : 1;
}
