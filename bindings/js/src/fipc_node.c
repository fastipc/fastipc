/*
 * fipc_node.c: the Node-API addon of FastIPC's JavaScript binding (bindings/js), one file for Node.js, Bun and Deno.
 *
 * A thin layer over include/fipc.h for index.cjs, which wraps it in the classes Listener and Connection. It loads the
 * FastIPC library at run time, from the path index.cjs gives init(), and it links nothing of the JavaScript runtime:
 * on Linux and macOS the Node-API functions are the host process's, resolved when the addon loads; on Windows, where a
 * DLL can't leave them undefined and the host may be node.exe, bun.exe or deno.exe, the addon looks them up in the
 * host executable when it loads (napi_windows.h). So `zig build` cross-compiles the addon for every platform.
 *
 * Every call that may wait has two forms. The Sync form calls the library on the JavaScript thread, with the caller's
 * timeout. The async form returns a Promise: it first tries the call without waiting, on the JavaScript thread, and
 * only if it would have to wait hands it to a thread of the handle's own, a lane (a connection has one for sending
 * and one for receiving, a listener one for accepting; each connect gets a thread of its own), which makes the call
 * with the rest of the timeout; the promise is settled on the JavaScript thread, through a thread-safe function. A
 * message of several pieces always goes to the lane: once its first piece has moved, the call goes on until the
 * whole message has, and the JavaScript thread mustn't wait for that. A lane's calls run in the order they were
 * made; a Sync call, a commit or a release on a lane with an async call pending fails (ERR_FIPC_BUSY): it would call
 * the library on a second thread at once.
 *
 * Zero-copy: an acquire returns a Uint8Array over an external ArrayBuffer in the ring. The addon detaches that
 * ArrayBuffer (its length becomes 0) when its bytes stop being valid: at the commit or the release, at the lane's
 * next call, and at close. An async acquire runs alone on its lane (ERR_FIPC_BUSY otherwise), so that no queued
 * call can end its bytes before the caller has seen them. The ArrayBuffer holds the connection's handle (a property
 * keyed by a private symbol), so a view that can still be reached keeps the connection from being finalized.
 *
 * A listener or connection the program drops is closed when the garbage collector finalizes its handle; the
 * environment's teardown (the process's end, a worker thread's) closes those still open.
 */

#ifndef _WIN32
#define _GNU_SOURCE /* RTLD_NODELETE */
#endif

#define NAPI_VERSION 8
#include <fipc.h>
#include <limits.h>
#include <node_api.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include "napi_windows.h"
#else
#include <dlfcn.h>
#include <pthread.h>
#include <time.h>
#endif

/* === Threads, locks and the clock === */

#ifdef _WIN32
typedef SRWLOCK mutex_t;
typedef CONDITION_VARIABLE cond_t;
typedef HANDLE thread_t;
#define MUTEX_INITIALIZER SRWLOCK_INIT
#define THREAD_FN(name) static DWORD WINAPI name(void* arg)
#define THREAD_RETURN return 0
typedef LPTHREAD_START_ROUTINE thread_fn_t;

static void mutex_init(mutex_t* m)
{
    InitializeSRWLock(m);
}

static void mutex_destroy(mutex_t* m)
{
    (void) m;
}

static void mutex_lock(mutex_t* m)
{
    AcquireSRWLockExclusive(m);
}

static void mutex_unlock(mutex_t* m)
{
    ReleaseSRWLockExclusive(m);
}

static void cond_init(cond_t* c)
{
    InitializeConditionVariable(c);
}

static void cond_destroy(cond_t* c)
{
    (void) c;
}

static void cond_wait(cond_t* c, mutex_t* m)
{
    SleepConditionVariableSRW(c, m, INFINITE, 0);
}

static void cond_signal(cond_t* c)
{
    WakeConditionVariable(c);
}

static bool thread_start(thread_t* thread, thread_fn_t fn, void* arg)
{
    *thread = CreateThread(NULL, 0, fn, arg, 0, NULL);
    return *thread != NULL;
}

static void thread_join(thread_t thread)
{
    WaitForSingleObject(thread, INFINITE);
    CloseHandle(thread);
}

static void thread_detach(thread_t thread)
{
    CloseHandle(thread);
}

static int64_t now_us(void)
{
    LARGE_INTEGER now, frequency;
    QueryPerformanceCounter(&now);
    QueryPerformanceFrequency(&frequency);
    return (int64_t) (now.QuadPart / frequency.QuadPart * 1000000
                      + now.QuadPart % frequency.QuadPart * 1000000 / frequency.QuadPart);
}

/* Not GetTickCount64, which steps by about 15.6 ms: a deadline would come up to a step early */
static int64_t now_ms(void)
{
    return now_us() / 1000;
}

static const char anchor = 0; /* an address in the addon */

/* Keeps the addon loaded for the rest of the process: Node.js unloads an addon with the last environment that loaded
 * it, a worker's, and a connect's thread, which nothing joins, may still run in it then */
static void pin_addon(void)
{
    HMODULE module;
    GetModuleHandleExW(
        GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_PIN, (LPCWSTR) (const void*) &anchor, &module
    );
}
#else
typedef pthread_mutex_t mutex_t;
typedef pthread_cond_t cond_t;
typedef pthread_t thread_t;
#define MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER
#define THREAD_FN(name) static void* name(void* arg)
#define THREAD_RETURN return NULL
typedef void* (*thread_fn_t)(void*);

static void mutex_init(mutex_t* m)
{
    pthread_mutex_init(m, NULL);
}

static void mutex_destroy(mutex_t* m)
{
    pthread_mutex_destroy(m);
}

static void mutex_lock(mutex_t* m)
{
    pthread_mutex_lock(m);
}

static void mutex_unlock(mutex_t* m)
{
    pthread_mutex_unlock(m);
}

static void cond_init(cond_t* c)
{
    pthread_cond_init(c, NULL);
}

static void cond_destroy(cond_t* c)
{
    pthread_cond_destroy(c);
}

static void cond_wait(cond_t* c, mutex_t* m)
{
    pthread_cond_wait(c, m);
}

static void cond_signal(cond_t* c)
{
    pthread_cond_signal(c);
}

static bool thread_start(thread_t* thread, thread_fn_t fn, void* arg)
{
    return pthread_create(thread, NULL, fn, arg) == 0;
}

static void thread_join(thread_t thread)
{
    pthread_join(thread, NULL);
}

static void thread_detach(thread_t thread)
{
    pthread_detach(thread);
}

static int64_t now_ms(void)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (int64_t) now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

static int64_t now_us(void)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (int64_t) now.tv_sec * 1000000 + now.tv_nsec / 1000;
}

static const char anchor = 0; /* an address in the addon */

/* Keeps the addon loaded for the rest of the process: Node.js unloads an addon with the last environment that loaded
 * it, a worker's, and a connect's thread, which nothing joins, may still run in it then */
static void pin_addon(void)
{
    Dl_info info;
    if (dladdr(&anchor, &info) && info.dli_fname)
        dlopen(info.dli_fname, RTLD_NOW | RTLD_NOLOAD | RTLD_NODELETE);
}
#endif

static void spin_hint(void)
{
#if defined(__x86_64__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield");
#endif
}

/* How long an async receive that finds nothing keeps trying on the JavaScript thread before it hands the call to its
 * lane: a reply often comes within microseconds, and the hand-off and the way back to the event loop cost more (tens
 * of microseconds on Node.js). As the library spins before it sleeps, and bounded the same way. */
#define RECEIVE_SPIN_US 20

/* Guards loading the library (and, on Windows, the Node-API imports): module init may run on worker threads at once */
static mutex_t load_lock = MUTEX_INITIALIZER;

/* Guards every environment's state's refs and closing: a connect's thread, which nothing joins, uses them too */
static mutex_t state_lock = MUTEX_INITIALIZER;

/* === The library, loaded at run time (init) === */

#define FIPC_FUNCTIONS(X)                                                                                              \
    X(fipc_result_str)                                                                                                 \
    X(fipc_listen)                                                                                                     \
    X(fipc_accept)                                                                                                     \
    X(fipc_listener_cancel)                                                                                            \
    X(fipc_listener_close)                                                                                             \
    X(fipc_connect)                                                                                                    \
    X(fipc_max_piece)                                                                                                  \
    X(fipc_cancel)                                                                                                     \
    X(fipc_close)                                                                                                      \
    X(fipc_send)                                                                                                       \
    X(fipc_recv)                                                                                                       \
    X(fipc_send_acquire)                                                                                               \
    X(fipc_send_commit)                                                                                                \
    X(fipc_recv_acquire)                                                                                               \
    X(fipc_recv_release)                                                                                               \
    X(fipc_rpc_submit)                                                                                                 \
    X(fipc_rpc_respond)                                                                                                \
    X(fipc_rpc_recv)

/* The library's functions, by their names in include/fipc.h: lib.fipc_send(...) */
static struct
{
#define X(name) __typeof__(&name) name;
    FIPC_FUNCTIONS(X)
#undef X
} lib;

/* The path the library was loaded from (the first init's), or NULL before */
static char* lib_path;

/* An RPC message is a plain message with this header in front */
#define RPC_HEADER sizeof(fipc_rpc_msg_t)

/* Loads the library at `path` and keeps it loaded until the process ends (the C API allows unloading only once every
 * handle is closed, and never on Linux or macOS). NULL, or a static description of what failed. Under load_lock. */
static const char* load_library(const char* path)
{
#ifdef _WIN32
    int length = MultiByteToWideChar(CP_UTF8, 0, path, -1, NULL, 0);
    wchar_t* wide = length > 0 ? malloc((size_t) length * sizeof(wchar_t)) : NULL;
    if (!wide)
        return "can't convert the library's path";
    MultiByteToWideChar(CP_UTF8, 0, path, -1, wide, length);
    /* A path: its folder is searched for the library's dependencies; a bare name: the system's search */
    bool bare = !wcschr(wide, L'\\') && !wcschr(wide, L'/');
    HMODULE module = bare ? LoadLibraryW(wide) : LoadLibraryExW(wide, NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
    free(wide);
    if (!module)
        return "LoadLibrary failed";
#define X(name)                                                                                                        \
    lib.name = (__typeof__(lib.name)) (void*) GetProcAddress(module, #name);                                           \
    if (!lib.name)                                                                                                     \
        return "the library lacks " #name;
    FIPC_FUNCTIONS(X)
#undef X
    HMODULE pinned;
    if (!GetModuleHandleExW(
            GET_MODULE_HANDLE_EX_FLAG_PIN | GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS,
            (LPCWSTR) (void*) lib.fipc_close,
            &pinned
        ))
        return "can't keep the library loaded";
#else
    void* module = dlopen(path, RTLD_NOW | RTLD_NODELETE);
    if (!module)
        return dlerror();
#define X(name)                                                                                                        \
    lib.name = (__typeof__(lib.name)) dlsym(module, #name);                                                            \
    if (!lib.name)                                                                                                     \
        return "the library lacks " #name;
    FIPC_FUNCTIONS(X)
#undef X
#endif
    return NULL;
}

/* === The environment's state, handles, lanes and calls === */

typedef struct handle handle_t;
typedef struct op op_t;

/* One per environment (the main thread's, each worker thread's): every function's callback data */
typedef struct state
{
    napi_env env;
    napi_threadsafe_function tsfn; /* settles the calls the threads made (on_done) */
    napi_ref make_error;           /* index.cjs's (result, call, length) => FipcError */
    napi_ref make_rpc;             /* index.cjs's (id, kind, opcode, status, payload, length) => an RPC message */
    napi_ref make_view;            /* index.cjs's (arrayBuffer, handle) => a Uint8Array over it, which holds handle */
    unsigned pending;              /* calls handed to threads and not settled: the tsfn keeps the loop alive */
    unsigned refs;                 /* state_lock's: the environment, each handle not yet freed, each connect */
    bool closing;                  /* state_lock's: the environment is torn down, its tsfn ended */
    handle_t* handles;             /* the listeners and connections not yet freed */
} state_t;

/* A thread that makes one handle's calls that wait, one at a time, in order */
typedef struct lane
{
    mutex_t mutex;
    cond_t cond;
    op_t* head; /* the calls queued for the thread (under mutex) */
    op_t* tail;
    bool quit; /* under mutex: the thread fails what is queued (FIPC_CANCELLED) and ends */
    thread_t thread;
    bool started;     /* the JavaScript thread's: the thread runs */
    unsigned pending; /* the JavaScript thread's: the lane's async calls not settled yet */
    bool exclusive;   /* the JavaScript thread's: the pending call is an acquire, which runs alone */
} lane_t;

#define HANDLE_MAGIC 0x66697063u /* "fipc" */

struct handle
{
    uint32_t magic;
    bool is_conn;
    state_t* state;
    handle_t* prev;
    handle_t* next;
};

typedef struct listener
{
    handle_t base;
    fipc_listener_t* handle; /* NULL once closed */
    lane_t lane;
} listener_t;

enum
{
    SEND = 0,
    RECV = 1
};

typedef struct conn
{
    handle_t base;
    fipc_conn_t* handle; /* NULL once closed */
    size_t max_piece;
    bool ended;        /* a call returned FIPC_DISCONNECTED */
    lane_t lanes[2];   /* SEND, RECV */
    napi_ref views[2]; /* weak: the ArrayBuffer of the lane's last zero-copy view, until it is detached */
} conn_t;

typedef enum
{
    OP_ACCEPT,
    OP_CONNECT,
    OP_SEND,
    OP_RECV,
    OP_RECV_INTO,
    OP_SEND_ACQUIRE,
    OP_RECV_ACQUIRE,
    OP_RPC_SUBMIT,
    OP_RPC_RESPOND,
    OP_RPC_RECV,
    OP_RPC_RECV_INTO,
} op_kind_t;

/* The methods' names, for the errors: async, Sync */
static const char* const op_names[][2] = {
    [OP_ACCEPT] = {        "accept",         "acceptSync"},
    [OP_CONNECT] = {       "connect",        "connectSync"},
    [OP_SEND] = {          "send",           "sendSync"},
    [OP_RECV] = {       "receive",        "receiveSync"},
    [OP_RECV_INTO] = {   "receiveInto",    "receiveIntoSync"},
    [OP_SEND_ACQUIRE] = {   "sendAcquire",    "sendAcquireSync"},
    [OP_RECV_ACQUIRE] = {"receiveAcquire", "receiveAcquireSync"},
    [OP_RPC_SUBMIT] = {     "rpcSubmit",      "rpcSubmitSync"},
    [OP_RPC_RESPOND] = {    "rpcRespond",     "rpcRespondSync"},
    [OP_RPC_RECV] = {    "rpcReceive",     "rpcReceiveSync"},
    [OP_RPC_RECV_INTO] = {"rpcReceiveInto", "rpcReceiveIntoSync"},
};

typedef enum
{
    ASYNC = 0,
    SYNC = 1
} call_mode_t;

/* One call */
struct op
{
    op_t* next; /* in the lane's queue */
    op_kind_t kind;
    call_mode_t mode;
    state_t* state;
    listener_t* listener;
    conn_t* conn;
    lane_t* lane;
    napi_threadsafe_function tsfn;
    int timeout;
    int64_t deadline; /* now_ms() + timeout when the call was made; -1: no limit */
    /* What the call takes */
    const uint8_t* data; /* the payload, or the caller's receive buffer */
    size_t len;          /* its length; an acquire's length */
    uint32_t opcode;
    int32_t status;
    uint64_t id;
    char* name; /* connect's */
    /* What it gives */
    fipc_result_t result;
    size_t out_len;
    void* out_ptr;
    fipc_rpc_msg_t msg;
    fipc_conn_t* out_conn;
    uint8_t* owned;     /* allocated here: a string payload's UTF-8, or a message a thread received */
    napi_value payload; /* a message received into a new Buffer on the JavaScript thread (this callback's only) */
    /* JavaScript */
    napi_deferred deferred;
    napi_ref handle_ref; /* the handle's external, while a thread has the call */
    napi_ref data_ref;   /* the caller's buffer, while a thread has the call */
};

/* === Values and errors === */

static napi_value undefined(napi_env env)
{
    napi_value value;
    napi_get_undefined(env, &value);
    return value;
}

/* The pending exception, cleared (the value a failed call rejects with) */
static napi_value take_exception(napi_env env)
{
    napi_value error = NULL;
    bool pending = false;
    napi_is_exception_pending(env, &pending);
    if (pending)
        napi_get_and_clear_last_exception(env, &error);
    if (!error)
        napi_create_error(env, NULL, NULL, &error);
    return error;
}

/* A promise rejected with the pending exception */
static napi_value rejected(napi_env env)
{
    napi_value error = take_exception(env), promise;
    napi_deferred deferred;
    napi_create_promise(env, &deferred, &promise);
    napi_reject_deferred(env, deferred, error);
    return promise;
}

static napi_value error_with_code(napi_env env, const char* code, const char* message)
{
    napi_value code_value, message_value, error;
    napi_create_string_utf8(env, code, NAPI_AUTO_LENGTH, &code_value);
    napi_create_string_utf8(env, message, NAPI_AUTO_LENGTH, &message_value);
    napi_create_error(env, code_value, message_value, &error);
    return error;
}

/* A FipcError (index.cjs) for `result` of the method `call`; `length` >= 0: the message's length (FIPC_TOO_LARGE) */
static napi_value fipc_error(napi_env env, state_t* state, fipc_result_t result, const char* call, int64_t length)
{
    napi_value make, args[3], error = NULL;
    napi_get_reference_value(env, state->make_error, &make);
    napi_create_int32(env, (int32_t) result, &args[0]);
    napi_create_string_utf8(env, call, NAPI_AUTO_LENGTH, &args[1]);
    if (length >= 0)
        napi_create_double(env, (double) length, &args[2]);
    else
        args[2] = undefined(env);
    if (!make || napi_call_function(env, undefined(env), make, 3, args, &error) != napi_ok || !error)
        error = take_exception(env);
    return error;
}

static bool throw_closed(napi_env env, const char* what)
{
    napi_throw(env, error_with_code(env, "ERR_FIPC_CLOSED", what));
    return false;
}

/* === Arguments === */

static bool type_error(napi_env env, const char* message)
{
    napi_throw_type_error(env, "ERR_INVALID_ARG_TYPE", message);
    return false;
}

/* A timeout: undefined, null or a negative number waits for ever; a fraction of a millisecond counts as a whole one */
static bool arg_timeout(napi_env env, napi_value value, int* timeout)
{
    napi_valuetype type;
    napi_typeof(env, value, &type);
    if (type == napi_undefined || type == napi_null)
    {
        *timeout = FIPC_FOREVER;
        return true;
    }
    double ms;
    if (type != napi_number || napi_get_value_double(env, value, &ms) != napi_ok || ms != ms)
        return type_error(env, "fipc: a timeout is a number of milliseconds, or undefined (no limit)");
    if (ms < 0)
        *timeout = FIPC_FOREVER;
    else if (ms >= (double) INT_MAX)
        *timeout = INT_MAX;
    else
        *timeout = (int) ms + ((double) (int) ms < ms);
    return true;
}

/* A whole number from `min` to `max` */
static bool arg_integer(napi_env env, napi_value value, double min, double max, double* out, const char* what)
{
    napi_valuetype type;
    napi_typeof(env, value, &type);
    double number;
    if (type != napi_number || napi_get_value_double(env, value, &number) != napi_ok)
    {
        char message[96];
        snprintf(message, sizeof message, "fipc: %s is a number", what);
        return type_error(env, message);
    }
    if (number != (double) (int64_t) number || number < min || number > max)
    {
        char message[128];
        snprintf(message, sizeof message, "fipc: %s is a whole number from %.0f to %.0f", what, min, max);
        napi_throw_range_error(env, "ERR_OUT_OF_RANGE", message);
        return false;
    }
    *out = number;
    return true;
}

/* The bytes of a TypedArray, a DataView or an ArrayBuffer (in place) */
static bool arg_view(napi_env env, napi_value value, void** data, size_t* len, const char* what)
{
    bool is = false;
    napi_value buffer;
    size_t offset;
    if (napi_is_typedarray(env, value, &is) == napi_ok && is)
    {
        napi_typedarray_type type;
        size_t length;
        napi_get_typedarray_info(env, value, &type, &length, data, &buffer, &offset);
        size_t size = 1;
        switch (type)
        {
            case napi_int16_array:
            case napi_uint16_array:
            case napi_float16_array:
                size = 2;
                break;
            case napi_int32_array:
            case napi_uint32_array:
            case napi_float32_array:
                size = 4;
                break;
            case napi_float64_array:
            case napi_bigint64_array:
            case napi_biguint64_array:
                size = 8;
                break;
            default:
                break;
        }
        *len = length * size;
        return true;
    }
    if (napi_is_dataview(env, value, &is) == napi_ok && is)
    {
        napi_get_dataview_info(env, value, len, data, &buffer, &offset);
        return true;
    }
    if (napi_is_arraybuffer(env, value, &is) == napi_ok && is)
    {
        napi_get_arraybuffer_info(env, value, data, len);
        return true;
    }
    char message[128];
    snprintf(
        message,
        sizeof message,
        "fipc: %s is a TypedArray (a Buffer, a Uint8Array, ...), a DataView or an "
        "ArrayBuffer",
        what
    );
    return type_error(env, message);
}

/* A payload: a string (its UTF-8, copied into op->owned) or bytes in place; undefined or null is empty if `optional` */
static bool arg_bytes(napi_env env, napi_value value, op_t* op, bool optional)
{
    napi_valuetype type;
    napi_typeof(env, value, &type);
    if (optional && (type == napi_undefined || type == napi_null))
    {
        op->data = NULL;
        op->len = 0;
        return true;
    }
    if (type == napi_string)
    {
        size_t length = 0;
        napi_get_value_string_utf8(env, value, NULL, 0, &length);
        op->owned = malloc(length + 1);
        if (!op->owned)
        {
            napi_throw(env, error_with_code(env, "ERR_FIPC_NO_MEMORY", "fipc: out of memory for a string's UTF-8"));
            return false;
        }
        napi_get_value_string_utf8(env, value, (char*) op->owned, length + 1, &length);
        op->data = op->owned;
        op->len = length;
        return true;
    }
    void* data = NULL;
    if (!arg_view(env, value, &data, &op->len, "a message"))
        return false;
    op->data = data;
    return true;
}

/* A string, as UTF-8 allocated with malloc; *length its bytes, if given */
static char* arg_string(napi_env env, napi_value value, const char* what, size_t* length_out)
{
    napi_valuetype type;
    napi_typeof(env, value, &type);
    if (type != napi_string)
    {
        char message[96];
        snprintf(message, sizeof message, "fipc: %s is a string", what);
        type_error(env, message);
        return NULL;
    }
    size_t length = 0;
    napi_get_value_string_utf8(env, value, NULL, 0, &length);
    char* text = malloc(length + 1);
    if (text)
        napi_get_value_string_utf8(env, value, text, length + 1, &length);
    else
        napi_throw(env, error_with_code(env, "ERR_FIPC_NO_MEMORY", "fipc: out of memory"));
    if (length_out)
        *length_out = length;
    return text;
}

/* A name, as arg_string gives it. One that holds a NUL character, which no name may, is emptied: the library rejects
 * it (FIPC_INVALID) instead of taking the part before the NUL. */
static char* arg_name(napi_env env, napi_value value)
{
    size_t length = 0;
    char* name = arg_string(env, value, "a name", &length);
    if (name && strlen(name) != length)
        name[0] = '\0';
    return name;
}

static handle_t* arg_handle(napi_env env, napi_value value, bool conn)
{
    napi_valuetype type;
    handle_t* handle = NULL;
    napi_typeof(env, value, &type);
    if (type == napi_external)
        napi_get_value_external(env, value, (void**) &handle);
    if (!handle || handle->magic != HANDLE_MAGIC || handle->is_conn != conn)
    {
        type_error(env, conn ? "fipc: not a connection" : "fipc: not a listener");
        return NULL;
    }
    return handle;
}

/* An open connection (else an exception) */
static conn_t* arg_conn(napi_env env, napi_value value)
{
    conn_t* conn = (conn_t*) arg_handle(env, value, true);
    if (conn && !conn->handle)
    {
        throw_closed(env, "fipc: the connection is closed");
        return NULL;
    }
    return conn;
}

static listener_t* arg_listener(napi_env env, napi_value value)
{
    listener_t* listener = (listener_t*) arg_handle(env, value, false);
    if (listener && !listener->handle)
    {
        throw_closed(env, "fipc: the listener is closed");
        return NULL;
    }
    return listener;
}

/* === Lanes === */

static void lane_init(lane_t* lane)
{
    mutex_init(&lane->mutex);
    cond_init(&lane->cond);
}

static void lane_destroy(lane_t* lane)
{
    mutex_destroy(&lane->mutex);
    cond_destroy(&lane->cond);
}

static void op_run(op_t* op);
static void op_discard(op_t* op);

/* The lane's thread: makes the queued calls in order, each with the rest of its timeout */
THREAD_FN(lane_main)
{
    lane_t* lane = arg;
    mutex_lock(&lane->mutex);
    for (;;)
    {
        while (!lane->head && !lane->quit)
            cond_wait(&lane->cond, &lane->mutex);
        op_t* op = lane->head;
        if (!op)
            break;
        lane->head = op->next;
        if (!lane->head)
            lane->tail = NULL;
        bool quit = lane->quit;
        mutex_unlock(&lane->mutex);
        if (quit)
            op->result = FIPC_CANCELLED;
        else
            op_run(op);
        /* The tsfn outlives the lane: a close and the environment's teardown join the thread first */
        if (napi_call_threadsafe_function(op->tsfn, op, napi_tsfn_nonblocking) != napi_ok)
            op_discard(op);
        mutex_lock(&lane->mutex);
    }
    mutex_unlock(&lane->mutex);
    THREAD_RETURN;
}

/* Queues `op` for the lane's thread, starting it the first time; false if the thread can't start */
static bool lane_push(lane_t* lane, op_t* op)
{
    bool ok = true;
    mutex_lock(&lane->mutex);
    if (!lane->started)
        ok = lane->started = thread_start(&lane->thread, lane_main, lane);
    if (ok)
    {
        op->next = NULL;
        if (lane->tail)
            lane->tail->next = op;
        else
            lane->head = op;
        lane->tail = op;
        cond_signal(&lane->cond);
    }
    mutex_unlock(&lane->mutex);
    return ok;
}

/* Ends the lane's thread: what is queued fails with FIPC_CANCELLED (cancel the handle first, to end the call it is
 * in). The thread can't be restarted. */
static void lane_stop(lane_t* lane)
{
    mutex_lock(&lane->mutex);
    lane->quit = true;
    cond_signal(&lane->cond);
    mutex_unlock(&lane->mutex);
    if (lane->started)
    {
        thread_join(lane->thread);
        lane->started = false;
    }
}

/* === Handles === */

static void state_retain(state_t* state)
{
    mutex_lock(&state_lock);
    state->refs++;
    mutex_unlock(&state_lock);
}

static void state_release(state_t* state)
{
    mutex_lock(&state_lock);
    bool last = --state->refs == 0;
    mutex_unlock(&state_lock);
    if (last)
        free(state);
}

static void handle_link(state_t* state, handle_t* handle, bool is_conn)
{
    handle->magic = HANDLE_MAGIC;
    handle->is_conn = is_conn;
    handle->state = state;
    handle->next = state->handles;
    if (state->handles)
        state->handles->prev = handle;
    state->handles = handle;
    state_retain(state);
}

static void handle_unlink(handle_t* handle)
{
    state_t* state = handle->state;
    if (handle->prev)
        handle->prev->next = handle->next;
    else
        state->handles = handle->next;
    if (handle->next)
        handle->next->prev = handle->prev;
    handle->magic = 0;
    state_release(state);
}

/* Detaches the lane's zero-copy view, if it has one: its bytes are no longer valid */
static void drop_view(napi_env env, conn_t* conn, int lane, bool detach)
{
    napi_ref ref = conn->views[lane];
    if (!ref)
        return;
    conn->views[lane] = NULL;
    napi_value buffer = NULL;
    if (detach && napi_get_reference_value(env, ref, &buffer) == napi_ok && buffer)
        napi_detach_arraybuffer(env, buffer);
    napi_delete_reference(env, ref);
}

/* Closes the connection: its views are detached (if `js`: not in a finalizer or the environment's teardown), its
 * lanes' calls cancelled and their threads joined, then fipc_close. Pending promises are settled later, with
 * FIPC_CANCELLED. */
static void conn_close(napi_env env, conn_t* conn, bool js)
{
    if (!conn->handle)
        return;
    drop_view(env, conn, SEND, js);
    drop_view(env, conn, RECV, js);
    if (conn->lanes[SEND].started || conn->lanes[RECV].started)
        lib.fipc_cancel(conn->handle);
    lane_stop(&conn->lanes[SEND]);
    lane_stop(&conn->lanes[RECV]);
    lib.fipc_close(conn->handle);
    conn->handle = NULL;
}

static void listener_close(listener_t* listener)
{
    if (!listener->handle)
        return;
    if (listener->lane.started)
        lib.fipc_listener_cancel(listener->handle);
    lane_stop(&listener->lane);
    lib.fipc_listener_close(listener->handle);
    listener->handle = NULL;
}

static void conn_finalize(napi_env env, void* data, void* hint)
{
    (void) hint;
    conn_t* conn = data;
    conn_close(env, conn, false);
    drop_view(env, conn, SEND, false);
    drop_view(env, conn, RECV, false);
    lane_destroy(&conn->lanes[SEND]);
    lane_destroy(&conn->lanes[RECV]);
    handle_unlink(&conn->base);
    free(conn);
}

static void listener_finalize(napi_env env, void* data, void* hint)
{
    (void) env;
    (void) hint;
    listener_t* listener = data;
    listener_close(listener);
    lane_destroy(&listener->lane);
    handle_unlink(&listener->base);
    free(listener);
}

/* A connection's external, for index.cjs's Connection; NULL (and the handle closed) if it can't be made */
static napi_value new_conn(napi_env env, state_t* state, fipc_conn_t* handle)
{
    conn_t* conn = calloc(1, sizeof *conn);
    napi_value external;
    if (!conn)
    {
        lib.fipc_close(handle);
        napi_throw(env, error_with_code(env, "ERR_FIPC_NO_MEMORY", "fipc: out of memory"));
        return NULL;
    }
    conn->handle = handle;
    conn->max_piece = lib.fipc_max_piece(handle);
    lane_init(&conn->lanes[SEND]);
    lane_init(&conn->lanes[RECV]);
    handle_link(state, &conn->base, true);
    if (napi_create_external(env, conn, conn_finalize, NULL, &external) != napi_ok)
    {
        conn_finalize(env, conn, NULL);
        return NULL;
    }
    return external;
}

/* === Calls === */

static int remaining(int64_t deadline)
{
    if (deadline < 0)
        return FIPC_FOREVER;
    int64_t left = deadline - now_ms();
    return left <= 0 ? 0 : left >= INT_MAX ? INT_MAX : (int) left;
}

static op_t* op_new(state_t* state, op_kind_t kind, call_mode_t mode)
{
    op_t* op = calloc(1, sizeof *op);
    if (op)
    {
        op->kind = kind;
        op->mode = mode;
        op->state = state;
        op->deadline = -1;
    }
    return op;
}

/* Frees what the call holds (a thread may have it: no Node-API) */
static void op_discard(op_t* op)
{
    if (op->out_conn)
        lib.fipc_close(op->out_conn);
    free(op->owned);
    free(op->name);
    free(op);
}

static void op_free(napi_env env, op_t* op)
{
    if (op->handle_ref)
        napi_delete_reference(env, op->handle_ref);
    if (op->data_ref)
        napi_delete_reference(env, op->data_ref);
    op_discard(op);
}

/* The call, waiting up to the rest of its timeout: on a lane's thread, or for a Sync call. No Node-API. */
static void op_run(op_t* op)
{
    int timeout = remaining(op->deadline);
    fipc_conn_t* conn = op->conn ? op->conn->handle : NULL;
    fipc_result_t result = FIPC_INVALID;
    size_t length = 0;
    switch (op->kind)
    {
        case OP_ACCEPT:
            result = lib.fipc_accept(op->listener->handle, &op->out_conn, timeout);
            break;
        case OP_CONNECT:
            result = lib.fipc_connect(op->name, &op->out_conn, timeout);
            break;
        case OP_SEND:
            result = lib.fipc_send(conn, op->data, op->len, timeout);
            break;
        case OP_RECV:
            /* The message's length (it stays queued), then the message into a buffer of that length */
            result = lib.fipc_recv(conn, NULL, 0, &length, timeout);
            if (result == FIPC_TOO_LARGE)
            {
                op->owned = malloc(length);
                /* Its first piece is there: the call waits only for the others */
                result = op->owned ? lib.fipc_recv(conn, op->owned, length, &length, FIPC_NO_WAIT) : FIPC_NO_MEMORY;
            }
            op->out_len = length;
            break;
        case OP_RECV_INTO:
            result = lib.fipc_recv(conn, (void*) op->data, op->len, &op->out_len, timeout);
            break;
        case OP_SEND_ACQUIRE:
            result = lib.fipc_send_acquire(conn, op->len, &op->out_ptr, timeout);
            break;
        case OP_RECV_ACQUIRE:
        {
            const void* data = NULL;
            result = lib.fipc_recv_acquire(conn, &data, &op->out_len, timeout);
            op->out_ptr = (void*) data;
            break;
        }
        case OP_RPC_SUBMIT:
            result = lib.fipc_rpc_submit(conn, op->opcode, op->data, op->len, &op->id, timeout);
            break;
        case OP_RPC_RESPOND:
            result = lib.fipc_rpc_respond(conn, op->id, op->opcode, op->status, op->data, op->len, timeout);
            break;
        case OP_RPC_RECV:
            /* An empty payload is taken at once; else the header (the payload stays queued), then the payload */
            result = lib.fipc_rpc_recv(conn, NULL, 0, &op->msg, timeout);
            if (result == FIPC_TOO_LARGE)
            {
                length = (size_t) op->msg.len;
                op->owned = malloc(length);
                result =
                    op->owned ? lib.fipc_rpc_recv(conn, op->owned, length, &op->msg, FIPC_NO_WAIT) : FIPC_NO_MEMORY;
            }
            break;
        case OP_RPC_RECV_INTO:
            result = lib.fipc_rpc_recv(conn, (void*) op->data, op->len, &op->msg, timeout);
            break;
    }
    op->result = result;
}

/* A receive into a new Buffer on the JavaScript thread: the message's length (it stays queued), a Buffer of that
 * length, then the message straight into it, waiting up to `timeout` for the first piece. The other pieces, if any,
 * are waited for whatever the timeout; with `pieces` (an async call's try), a message of several pieces is left
 * queued for the lane instead: *pieces is set, and the result is FIPC_TIMEOUT. */
static fipc_result_t receive_into_new_buffer(napi_env env, op_t* op, int timeout, bool* pieces)
{
    fipc_conn_t* conn = op->conn->handle;
    void* data = NULL;
    size_t length = 0;
    if (op->kind == OP_RECV)
    {
        fipc_result_t result = lib.fipc_recv(conn, NULL, 0, &length, timeout);
        if (result != FIPC_TOO_LARGE)
            return result;
        if (pieces && length > op->conn->max_piece)
        {
            *pieces = true; /* the lane receives it */
            return FIPC_TIMEOUT;
        }
        if (napi_create_buffer(env, length, &data, &op->payload) != napi_ok)
            return FIPC_NO_MEMORY;
        result = lib.fipc_recv(conn, data, length, &op->out_len, FIPC_NO_WAIT);
        return result;
    }
    fipc_result_t result = lib.fipc_rpc_recv(conn, NULL, 0, &op->msg, timeout);
    if (result != FIPC_TOO_LARGE)
        return result;
    length = (size_t) op->msg.len;
    if (pieces && length + RPC_HEADER > op->conn->max_piece)
    {
        *pieces = true;
        return FIPC_TIMEOUT;
    }
    if (napi_create_buffer(env, length, &data, &op->payload) != napi_ok)
        return FIPC_NO_MEMORY;
    return lib.fipc_rpc_recv(conn, data, length, &op->msg, FIPC_NO_WAIT);
}

/* What an async call's attempt on the JavaScript thread came to */
typedef enum
{
    TRIED_DONE,  /* op->result is the call's */
    TRIED_WAIT,  /* it would have to wait */
    TRIED_PIECES /* a message of several pieces: the lane makes the call, even with timeout 0 */
} tried_t;

/* An async call's attempt on the JavaScript thread, without waiting */
static tried_t op_try(napi_env env, op_t* op)
{
    conn_t* conn = op->conn;
    fipc_conn_t* handle = conn->handle;
    size_t max = conn->max_piece;
    fipc_result_t result;
    size_t length = 0;
    bool pieces = false;
    switch (op->kind)
    {
        case OP_SEND:
            if (op->len > max)
                return TRIED_PIECES;
            result = lib.fipc_send(handle, op->data, op->len, FIPC_NO_WAIT);
            break;
        case OP_RPC_SUBMIT:
            if (op->len > max - RPC_HEADER)
                return TRIED_PIECES;
            result = lib.fipc_rpc_submit(handle, op->opcode, op->data, op->len, &op->id, FIPC_NO_WAIT);
            break;
        case OP_RPC_RESPOND:
            if (op->len > max - RPC_HEADER)
                return TRIED_PIECES;
            result = lib.fipc_rpc_respond(handle, op->id, op->opcode, op->status, op->data, op->len, FIPC_NO_WAIT);
            break;
        case OP_SEND_ACQUIRE:
            result = lib.fipc_send_acquire(handle, op->len, &op->out_ptr, FIPC_NO_WAIT);
            break;
        case OP_RECV_ACQUIRE:
        {
            const void* data = NULL;
            result = lib.fipc_recv_acquire(handle, &data, &op->out_len, FIPC_NO_WAIT);
            op->out_ptr = (void*) data;
            break;
        }
        case OP_RECV:
        case OP_RPC_RECV:
            result = receive_into_new_buffer(env, op, FIPC_NO_WAIT, &pieces);
            if (pieces)
                return TRIED_PIECES;
            break;
        case OP_RECV_INTO:
            result = lib.fipc_recv(handle, NULL, 0, &length, FIPC_NO_WAIT);
            if (result == FIPC_TOO_LARGE)
            {
                op->out_len = length;
                if (length > op->len)
                    break; /* FIPC_TOO_LARGE, with the length: the message stays queued */
                if (length > max)
                    return TRIED_PIECES;
                result = lib.fipc_recv(handle, (void*) op->data, op->len, &op->out_len, FIPC_NO_WAIT);
            }
            break;
        case OP_RPC_RECV_INTO:
            result = lib.fipc_rpc_recv(handle, NULL, 0, &op->msg, FIPC_NO_WAIT);
            if (result == FIPC_TOO_LARGE)
            {
                if (op->msg.len > op->len)
                    break;
                if (op->msg.len + RPC_HEADER > max)
                    return TRIED_PIECES;
                result = lib.fipc_rpc_recv(handle, (void*) op->data, op->len, &op->msg, FIPC_NO_WAIT);
            }
            break;
        default:
            return TRIED_WAIT; /* accept, connect: always on a thread */
    }
    if (result == FIPC_TIMEOUT && op->timeout != FIPC_NO_WAIT)
        return TRIED_WAIT;
    op->result = result;
    return TRIED_DONE;
}

static bool receives(op_kind_t kind)
{
    return kind == OP_RECV || kind == OP_RECV_INTO || kind == OP_RECV_ACQUIRE || kind == OP_RPC_RECV
        || kind == OP_RPC_RECV_INTO;
}

/* A Uint8Array over `length` bytes of the ring at `data`, the lane's view until it is detached. Its ArrayBuffer holds
 * the connection's handle (index.cjs sets it, under a private symbol): a view that can be reached keeps the ring
 * mapped. */
static napi_value make_view(napi_env env, op_t* op, napi_value handle, int lane, void* data, size_t length)
{
    napi_value buffer, make, args[2], view = NULL;
    if (napi_create_external_arraybuffer(env, data, length, NULL, NULL, &buffer) != napi_ok
        || napi_get_reference_value(env, op->state->make_view, &make) != napi_ok)
        return NULL;
    args[0] = buffer;
    args[1] = handle ? handle : undefined(env);
    if (napi_call_function(env, undefined(env), make, 2, args, &view) != napi_ok)
        return NULL;
    napi_create_reference(env, buffer, 0, &op->conn->views[lane]);
    return view;
}

static void free_owned(napi_env env, void* data, void* hint)
{
    (void) env;
    (void) hint;
    free(data);
}

/* A Buffer of the message a thread received into op->owned (which it takes) */
static napi_value owned_buffer(napi_env env, op_t* op, size_t length)
{
    napi_value buffer = NULL;
    if (length > 65536 && napi_create_external_buffer(env, length, op->owned, free_owned, NULL, &buffer) == napi_ok
        && buffer)
    {
        op->owned = NULL;
        return buffer;
    }
    void* copy;
    if (napi_create_buffer_copy(env, length, op->owned ? (void*) op->owned : (void*) "", &copy, &buffer) != napi_ok)
        return NULL;
    return buffer;
}

/* { id, kind, opcode, status } and the payload if given, else its length: made by index.cjs, in one call */
static napi_value rpc_object(napi_env env, state_t* state, const fipc_rpc_msg_t* msg, napi_value payload)
{
    napi_value make, args[6], object = NULL;
    napi_get_reference_value(env, state->make_rpc, &make);
    napi_create_double(env, (double) msg->id, &args[0]);
    napi_create_double(env, (double) msg->kind, &args[1]);
    napi_create_double(env, (double) msg->opcode, &args[2]);
    napi_create_double(env, (double) msg->status, &args[3]);
    if (payload)
    {
        args[4] = payload;
        args[5] = undefined(env);
    }
    else
    {
        args[4] = undefined(env);
        napi_create_double(env, (double) msg->len, &args[5]);
    }
    if (napi_call_function(env, undefined(env), make, 6, args, &object) != napi_ok)
        return NULL;
    return object;
}

/* The call's value, or (*ok false) its error. `handle` is the handle's external. */
static napi_value finish(napi_env env, op_t* op, napi_value handle, bool* ok)
{
    fipc_result_t result = op->result;
    if (result == FIPC_DISCONNECTED && op->conn)
        op->conn->ended = true;
    /* No view into a connection closed meanwhile */
    if (result == FIPC_OK && (op->kind == OP_SEND_ACQUIRE || op->kind == OP_RECV_ACQUIRE) && !op->conn->handle)
        result = FIPC_CANCELLED;
    *ok = result == FIPC_OK;
    if (!*ok)
    {
        int64_t length = -1;
        if (result == FIPC_TOO_LARGE)
        {
            if (op->kind == OP_RPC_RECV || op->kind == OP_RPC_RECV_INTO)
                length = (int64_t) op->msg.len;
            else if (op->kind == OP_RECV || op->kind == OP_RECV_INTO || op->kind == OP_RECV_ACQUIRE)
                length = (int64_t) op->out_len;
        }
        return fipc_error(env, op->state, result, op_names[op->kind][op->mode], length);
    }
    napi_value value = NULL;
    switch (op->kind)
    {
        case OP_ACCEPT:
        case OP_CONNECT:
        {
            fipc_conn_t* conn = op->out_conn;
            op->out_conn = NULL;
            value = new_conn(env, op->state, conn);
            break;
        }
        case OP_SEND:
        case OP_RPC_RESPOND:
            value = undefined(env);
            break;
        case OP_RECV:
            value = op->payload ? op->payload : owned_buffer(env, op, op->out_len);
            break;
        case OP_RECV_INTO:
            napi_create_double(env, (double) op->out_len, &value);
            break;
        case OP_SEND_ACQUIRE:
            value = make_view(env, op, handle, SEND, op->out_ptr, op->len);
            break;
        case OP_RECV_ACQUIRE:
            value = make_view(env, op, handle, RECV, op->out_ptr, op->out_len);
            break;
        case OP_RPC_SUBMIT:
            napi_create_double(env, (double) op->id, &value);
            break;
        case OP_RPC_RECV:
        {
            napi_value payload = op->payload ? op->payload : owned_buffer(env, op, (size_t) op->msg.len);
            value = payload ? rpc_object(env, op->state, &op->msg, payload) : NULL;
            break;
        }
        case OP_RPC_RECV_INTO:
            value = rpc_object(env, op->state, &op->msg, NULL);
            break;
    }
    if (!value)
    {
        *ok = false;
        return take_exception(env);
    }
    return value;
}

/* Settles an async call's promise and frees the call */
static void settle(napi_env env, op_t* op, napi_value handle)
{
    bool ok;
    napi_value value = finish(env, op, handle, &ok);
    if (ok)
        napi_resolve_deferred(env, op->deferred, value);
    else
        napi_reject_deferred(env, op->deferred, value);
    op_free(env, op);
}

/* The tsfn's callback, on the JavaScript thread: a thread made its call. env is NULL when the environment is torn
 * down: the call is freed, nothing settled. */
static void on_done(napi_env env, napi_value callback, void* context, void* data)
{
    (void) callback;
    (void) context;
    op_t* op = data;
    if (!env)
    {
        op_discard(op);
        return;
    }
    state_t* state = op->state;
    if (op->lane)
    {
        op->lane->pending--;
        op->lane->exclusive = false;
    }
    if (--state->pending == 0)
        napi_unref_threadsafe_function(env, state->tsfn);
    napi_value handle = NULL;
    if (op->handle_ref)
        napi_get_reference_value(env, op->handle_ref, &handle);
    settle(env, op, handle);
}

THREAD_FN(connect_main)
{
    op_t* op = arg;
    state_t* state = op->state;
    napi_threadsafe_function tsfn = op->tsfn;
    op_run(op);
    /* Nothing joins this thread: once the environment's teardown ended the tsfn, it is freed, and the call with it */
    mutex_lock(&state_lock);
    if (state->closing || napi_call_threadsafe_function(tsfn, op, napi_tsfn_nonblocking) != napi_ok)
        op_discard(op);
    if (!state->closing)
        napi_release_threadsafe_function(tsfn, napi_tsfn_release);
    mutex_unlock(&state_lock);
    state_release(state);
    THREAD_RETURN;
}

/* Hands an async call to a thread: the handle's lane, or a thread of its own for a connect. False if no thread could
 * start. */
static bool hand_off(napi_env env, op_t* op, napi_value handle, napi_value data)
{
    state_t* state = op->state;
    op->tsfn = state->tsfn;
    op->payload = NULL;
    if (handle && napi_create_reference(env, handle, 1, &op->handle_ref) != napi_ok)
        return false;
    /* Only bytes in place need holding: an empty payload may be undefined or null, which takes no reference */
    if (data && op->data && !op->owned && napi_create_reference(env, data, 1, &op->data_ref) != napi_ok)
        return false;
    bool started;
    if (op->kind == OP_CONNECT)
    {
        thread_t thread;
        started = napi_acquire_threadsafe_function(state->tsfn) == napi_ok;
        if (started)
            state_retain(state); /* the thread's: it may outlive the environment */
        if (started && !(started = thread_start(&thread, connect_main, op)))
        {
            napi_release_threadsafe_function(state->tsfn, napi_tsfn_release);
            state_release(state);
        }
        if (started)
            thread_detach(thread);
    }
    else
    {
        started = lane_push(op->lane, op);
        if (started)
        {
            op->lane->pending++;
            if (op->kind == OP_SEND_ACQUIRE || op->kind == OP_RECV_ACQUIRE)
                op->lane->exclusive = true;
        }
    }
    if (started && state->pending++ == 0)
        napi_ref_threadsafe_function(env, state->tsfn);
    return started;
}

/* An async call: tried at once if its lane is idle, else (or if it would wait) handed to a thread */
static napi_value submit(napi_env env, op_t* op, napi_value handle, napi_value data)
{
    napi_value promise;
    if (napi_create_promise(env, &op->deferred, &promise) != napi_ok)
    {
        op_free(env, op);
        return rejected(env);
    }
    if (op->conn && op->lane->pending == 0)
    {
        tried_t tried = op_try(env, op);
        if (tried == TRIED_WAIT && receives(op->kind))
        {
            int64_t until = now_us() + RECEIVE_SPIN_US;
            while (tried == TRIED_WAIT && now_us() < until)
            {
                spin_hint();
                tried = op_try(env, op);
            }
        }
        if (tried == TRIED_DONE)
        {
            settle(env, op, handle);
            return promise;
        }
    }
    if (!hand_off(env, op, handle, data))
    {
        op->result = FIPC_NO_MEMORY;
        settle(env, op, handle);
    }
    return promise;
}

/* A Sync call, on this thread: its value, or (thrown) its error */
static napi_value call_sync(napi_env env, op_t* op, napi_value handle)
{
    op->deadline = op->timeout < 0 ? -1 : now_ms() + op->timeout;
    if (op->kind == OP_RECV || op->kind == OP_RPC_RECV)
        op->result = receive_into_new_buffer(env, op, op->timeout, NULL);
    else
        op_run(op);
    bool ok;
    napi_value value = finish(env, op, handle, &ok);
    op_free(env, op);
    if (!ok)
    {
        napi_throw(env, value);
        return NULL;
    }
    return value;
}

/* A call that would use the lane while an async call of it is pending: ERR_FIPC_BUSY */
static bool lane_busy(napi_env env, lane_t* lane, op_kind_t kind, call_mode_t mode, const char* call)
{
    bool acquire = kind == OP_SEND_ACQUIRE || kind == OP_RECV_ACQUIRE;
    if (mode == ASYNC ? !(lane->exclusive || (acquire && lane->pending)) : !lane->pending)
        return false;
    char message[256];
    snprintf(
        message,
        sizeof message,
        "fipc: %s while an asynchronous call of this %s is pending (%s): await it first",
        call,
        kind == OP_ACCEPT ? "listener" : "connection's direction",
        lane->exclusive ? "an acquire runs alone" : "a Sync call can't run beside it"
    );
    napi_throw(env, error_with_code(env, "ERR_FIPC_BUSY", message));
    return true;
}

static bool sends(op_kind_t kind)
{
    return kind == OP_SEND || kind == OP_SEND_ACQUIRE || kind == OP_RPC_SUBMIT || kind == OP_RPC_RESPOND;
}

/* A connection's call: (handle, arguments..., timeout) */
static napi_value conn_call(napi_env env, napi_callback_info info, op_kind_t kind, call_mode_t mode)
{
    size_t argc = 6;
    napi_value argv[6], data = NULL;
    state_t* state;
    napi_get_cb_info(env, info, &argc, argv, NULL, (void**) &state);
    op_t* op = op_new(state, kind, mode);
    if (!op)
    {
        napi_throw(env, error_with_code(env, "ERR_FIPC_NO_MEMORY", "fipc: out of memory"));
        return mode == ASYNC ? rejected(env) : NULL;
    }
    conn_t* conn = arg_conn(env, argv[0]);
    bool ok = conn != NULL;
    size_t timeout_at = 1;
    double number;
    if (ok)
    {
        switch (kind)
        {
            case OP_SEND:
                ok = arg_bytes(env, argv[1], op, false);
                data = argv[1];
                timeout_at = 2;
                break;
            case OP_RECV_INTO:
            case OP_RPC_RECV_INTO:
            {
                void* buffer = NULL;
                ok = arg_view(env, argv[1], &buffer, &op->len, "a receive buffer");
                op->data = buffer;
                data = argv[1];
                timeout_at = 2;
                break;
            }
            case OP_SEND_ACQUIRE:
                ok = arg_integer(env, argv[1], 0, 9007199254740991.0, &number, "a length");
                op->len = (size_t) number;
                timeout_at = 2;
                break;
            case OP_RPC_SUBMIT:
                ok = arg_integer(env, argv[1], 0, 4294967295.0, &number, "an opcode")
                  && arg_bytes(env, argv[2], op, true);
                op->opcode = (uint32_t) number;
                data = argv[2];
                timeout_at = 3;
                break;
            case OP_RPC_RESPOND:
            {
                napi_valuetype type;
                napi_typeof(env, argv[1], &type);
                if (type == napi_bigint)
                {
                    bool lossless;
                    ok = napi_get_value_bigint_uint64(env, argv[1], &op->id, &lossless) == napi_ok && lossless;
                    if (!ok)
                        napi_throw_range_error(env, "ERR_OUT_OF_RANGE", "fipc: a request id is a uint64");
                }
                else
                {
                    ok = arg_integer(env, argv[1], 0, 9007199254740991.0, &number, "a request id");
                    op->id = (uint64_t) number;
                }
                ok = ok && arg_integer(env, argv[2], 0, 4294967295.0, &number, "an opcode");
                op->opcode = (uint32_t) number;
                ok = ok && arg_integer(env, argv[3], -2147483648.0, 2147483647.0, &number, "a status");
                op->status = (int32_t) number;
                ok = ok && arg_bytes(env, argv[4], op, true);
                data = argv[4];
                timeout_at = 5;
                break;
            }
            default:
                break;
        }
    }
    if (ok)
        ok = arg_timeout(env, argv[timeout_at], &op->timeout);
    int lane = sends(kind) ? SEND : RECV;
    if (ok)
        ok = !lane_busy(env, &conn->lanes[lane], kind, mode, op_names[kind][mode]);
    if (!ok)
    {
        op_free(env, op);
        return mode == ASYNC ? rejected(env) : NULL;
    }
    op->conn = conn;
    op->lane = &conn->lanes[lane];
    /* The lane's next call ends its view: a receive releases the message, a send or an acquire drops the reservation */
    drop_view(env, conn, lane, true);
    if (mode != ASYNC)
        return call_sync(env, op, argv[0]);
    op->deadline = op->timeout < 0 ? -1 : now_ms() + op->timeout;
    return submit(env, op, argv[0], data);
}

#define CONN_CALL(fn, kind, mode)                                                                                      \
    static napi_value fn(napi_env env, napi_callback_info info)                                                        \
    {                                                                                                                  \
        return conn_call(env, info, kind, mode);                                                                       \
    }

CONN_CALL(js_send, OP_SEND, ASYNC)
CONN_CALL(js_send_sync, OP_SEND, SYNC)
CONN_CALL(js_receive, OP_RECV, ASYNC)
CONN_CALL(js_receive_sync, OP_RECV, SYNC)
CONN_CALL(js_receive_into, OP_RECV_INTO, ASYNC)
CONN_CALL(js_receive_into_sync, OP_RECV_INTO, SYNC)
CONN_CALL(js_send_acquire, OP_SEND_ACQUIRE, ASYNC)
CONN_CALL(js_send_acquire_sync, OP_SEND_ACQUIRE, SYNC)
CONN_CALL(js_receive_acquire, OP_RECV_ACQUIRE, ASYNC)
CONN_CALL(js_receive_acquire_sync, OP_RECV_ACQUIRE, SYNC)
CONN_CALL(js_rpc_submit, OP_RPC_SUBMIT, ASYNC)
CONN_CALL(js_rpc_submit_sync, OP_RPC_SUBMIT, SYNC)
CONN_CALL(js_rpc_respond, OP_RPC_RESPOND, ASYNC)
CONN_CALL(js_rpc_respond_sync, OP_RPC_RESPOND, SYNC)
CONN_CALL(js_rpc_receive, OP_RPC_RECV, ASYNC)
CONN_CALL(js_rpc_receive_sync, OP_RPC_RECV, SYNC)
CONN_CALL(js_rpc_receive_into, OP_RPC_RECV_INTO, ASYNC)
CONN_CALL(js_rpc_receive_into_sync, OP_RPC_RECV_INTO, SYNC)

/* sendCommit(handle, length) */
static napi_value js_send_commit(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value argv[2];
    state_t* state;
    double length;
    napi_get_cb_info(env, info, &argc, argv, NULL, (void**) &state);
    conn_t* conn = arg_conn(env, argv[0]);
    if (!conn || !arg_integer(env, argv[1], 0, 9007199254740991.0, &length, "a length")
        || lane_busy(env, &conn->lanes[SEND], OP_SEND, SYNC, "sendCommit"))
        return NULL;
    fipc_result_t result = lib.fipc_send_commit(conn->handle, (size_t) length);
    if (result != FIPC_OK)
    {
        napi_throw(env, fipc_error(env, state, result, "sendCommit", -1));
        return NULL;
    }
    drop_view(env, conn, SEND, true);
    return undefined(env);
}

/* receiveRelease(handle) */
static napi_value js_receive_release(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    conn_t* conn = arg_conn(env, argv[0]);
    if (!conn || lane_busy(env, &conn->lanes[RECV], OP_RECV, SYNC, "receiveRelease"))
        return NULL;
    drop_view(env, conn, RECV, true);
    lib.fipc_recv_release(conn->handle);
    return undefined(env);
}

/* maxPiece(handle): the connection's, also once it is closed */
static napi_value js_max_piece(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value argv[1], value;
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    conn_t* conn = (conn_t*) arg_handle(env, argv[0], true);
    if (!conn)
        return NULL;
    napi_create_double(env, (double) conn->max_piece, &value);
    return value;
}

/* ended(handle): whether a call returned FIPC_DISCONNECTED */
static napi_value js_ended(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value argv[1], value;
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    conn_t* conn = (conn_t*) arg_handle(env, argv[0], true);
    if (!conn)
        return NULL;
    napi_get_boolean(env, conn->ended, &value);
    return value;
}

static napi_value js_cancel(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    conn_t* conn = arg_conn(env, argv[0]);
    if (!conn)
        return NULL;
    lib.fipc_cancel(conn->handle);
    return undefined(env);
}

/* close(handle): closing a closed connection does nothing */
static napi_value js_close(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    conn_t* conn = (conn_t*) arg_handle(env, argv[0], true);
    if (!conn)
        return NULL;
    conn_close(env, conn, true);
    return undefined(env);
}

static napi_value js_closed(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value argv[1], value;
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    handle_t* handle = NULL;
    napi_valuetype type;
    napi_typeof(env, argv[0], &type);
    if (type == napi_external)
        napi_get_value_external(env, argv[0], (void**) &handle);
    if (!handle || handle->magic != HANDLE_MAGIC)
    {
        type_error(env, "fipc: not a listener or a connection");
        return NULL;
    }
    bool closed = handle->is_conn ? ((conn_t*) handle)->handle == NULL : ((listener_t*) handle)->handle == NULL;
    napi_get_boolean(env, closed, &value);
    return value;
}

/* connect(name, timeout) and connectSync(name, timeout) */
static napi_value connect_call(napi_env env, napi_callback_info info, call_mode_t mode)
{
    size_t argc = 2;
    napi_value argv[2];
    state_t* state;
    napi_get_cb_info(env, info, &argc, argv, NULL, (void**) &state);
    op_t* op = op_new(state, OP_CONNECT, mode);
    bool ok = op && (op->name = arg_name(env, argv[0])) != NULL && arg_timeout(env, argv[1], &op->timeout);
    if (!ok)
    {
        if (op)
            op_free(env, op);
        else
            napi_throw(env, error_with_code(env, "ERR_FIPC_NO_MEMORY", "fipc: out of memory"));
        return mode == ASYNC ? rejected(env) : NULL;
    }
    if (mode != ASYNC)
        return call_sync(env, op, NULL);
    op->deadline = op->timeout < 0 ? -1 : now_ms() + op->timeout;
    return submit(env, op, NULL, NULL);
}

static napi_value js_connect(napi_env env, napi_callback_info info)
{
    return connect_call(env, info, ASYNC);
}

static napi_value js_connect_sync(napi_env env, napi_callback_info info)
{
    return connect_call(env, info, SYNC);
}

/* listen(name, capacity): the listener's external */
static napi_value js_listen(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value argv[2], external;
    state_t* state;
    double capacity;
    napi_get_cb_info(env, info, &argc, argv, NULL, (void**) &state);
    char* name = arg_name(env, argv[0]);
    if (!name)
        return NULL;
    if (!arg_integer(env, argv[1], 0, 9007199254740991.0, &capacity, "a capacity"))
    {
        free(name);
        return NULL;
    }
    fipc_listener_t* handle = NULL;
    fipc_result_t result = lib.fipc_listen(name, (size_t) capacity, &handle);
    free(name);
    if (result != FIPC_OK)
    {
        napi_throw(env, fipc_error(env, state, result, "listen", -1));
        return NULL;
    }
    listener_t* listener = calloc(1, sizeof *listener);
    if (!listener)
    {
        lib.fipc_listener_close(handle);
        napi_throw(env, error_with_code(env, "ERR_FIPC_NO_MEMORY", "fipc: out of memory"));
        return NULL;
    }
    listener->handle = handle;
    lane_init(&listener->lane);
    handle_link(state, &listener->base, false);
    if (napi_create_external(env, listener, listener_finalize, NULL, &external) != napi_ok)
    {
        listener_finalize(env, listener, NULL);
        return NULL;
    }
    return external;
}

/* accept(handle, timeout), acceptSync(handle, timeout) */
static napi_value accept_call(napi_env env, napi_callback_info info, call_mode_t mode)
{
    size_t argc = 2;
    napi_value argv[2];
    state_t* state;
    napi_get_cb_info(env, info, &argc, argv, NULL, (void**) &state);
    op_t* op = op_new(state, OP_ACCEPT, mode);
    listener_t* listener = op ? arg_listener(env, argv[0]) : NULL;
    bool ok = listener != NULL;
    if (ok)
        ok = arg_timeout(env, argv[1], &op->timeout);
    ok = ok && !lane_busy(env, &listener->lane, OP_ACCEPT, mode, op_names[OP_ACCEPT][mode]);
    if (!ok)
    {
        if (op)
            op_free(env, op);
        else
            napi_throw(env, error_with_code(env, "ERR_FIPC_NO_MEMORY", "fipc: out of memory"));
        return mode == ASYNC ? rejected(env) : NULL;
    }
    op->listener = listener;
    op->lane = &listener->lane;
    if (mode != ASYNC)
        return call_sync(env, op, argv[0]);
    op->deadline = op->timeout < 0 ? -1 : now_ms() + op->timeout;
    return submit(env, op, argv[0], NULL);
}

static napi_value js_accept(napi_env env, napi_callback_info info)
{
    return accept_call(env, info, ASYNC);
}

static napi_value js_accept_sync(napi_env env, napi_callback_info info)
{
    return accept_call(env, info, SYNC);
}

static napi_value js_listener_cancel(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    listener_t* listener = arg_listener(env, argv[0]);
    if (!listener)
        return NULL;
    lib.fipc_listener_cancel(listener->handle);
    return undefined(env);
}

static napi_value js_listener_close(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    listener_t* listener = (listener_t*) arg_handle(env, argv[0], false);
    if (!listener)
        return NULL;
    listener_close(listener);
    return undefined(env);
}

/* init(makeError, makeRpc, makeView, libraryPath): loads the library (the first init in the process; later ones keep
 * it) and returns the path it was loaded from */
static napi_value js_init(napi_env env, napi_callback_info info)
{
    size_t argc = 4;
    napi_value argv[4], value;
    state_t* state;
    napi_get_cb_info(env, info, &argc, argv, NULL, (void**) &state);
    for (int i = 0; i < 3; i++)
    {
        napi_valuetype type;
        napi_typeof(env, argv[i], &type);
        if (type != napi_function)
        {
            type_error(env, "fipc: init takes the error, RPC message and view factories, then the library's path");
            return NULL;
        }
    }
    char* path = arg_string(env, argv[3], "the library's path", NULL);
    if (!path)
        return NULL;
    const char* failure = NULL;
    mutex_lock(&load_lock);
    if (!lib_path)
    {
        failure = load_library(path);
        if (!failure)
        {
            lib_path = path;
            path = NULL;
        }
    }
    mutex_unlock(&load_lock);
    if (failure)
    {
        char message[1024];
        snprintf(
            message,
            sizeof message,
            "fipc: can't load %s (%s); set FASTIPC_LIB_DIR to the folder that holds it",
            path,
            failure
        );
        free(path);
        napi_throw(env, error_with_code(env, "ERR_FIPC_LOAD", message));
        return NULL;
    }
    free(path);
    napi_ref* factories[] = {&state->make_error, &state->make_rpc, &state->make_view};
    for (int i = 0; i < 3; i++)
    {
        if (*factories[i])
            napi_delete_reference(env, *factories[i]);
        napi_create_reference(env, argv[i], 1, factories[i]);
    }
    napi_create_string_utf8(env, lib_path, NAPI_AUTO_LENGTH, &value);
    return value;
}

/* The environment's teardown: closes what is still open, then ends the tsfn (queued calls are freed unsettled) */
static void state_cleanup(void* data)
{
    state_t* state = data;
    for (handle_t* handle = state->handles; handle; handle = handle->next)
    {
        if (handle->is_conn)
            conn_close(state->env, (conn_t*) handle, false);
        else
            listener_close((listener_t*) handle);
    }
    mutex_lock(&state_lock);
    state->closing = true;
    napi_release_threadsafe_function(state->tsfn, napi_tsfn_abort);
    mutex_unlock(&state_lock);
    state_release(state);
}

static napi_value noop(napi_env env, napi_callback_info info)
{
    (void) info;
    return undefined(env);
}

/* The addon's functions, as index.cjs calls them */
static const struct
{
    const char* name;
    napi_callback fn;
} methods[] = {
    {              "init",                  js_init},
    {            "listen",                js_listen},
    {            "accept",                js_accept},
    {        "acceptSync",           js_accept_sync},
    {    "listenerCancel",       js_listener_cancel},
    {     "listenerClose",        js_listener_close},
    {            "closed",                js_closed},
    {           "connect",               js_connect},
    {       "connectSync",          js_connect_sync},
    {          "maxPiece",             js_max_piece},
    {             "ended",                 js_ended},
    {            "cancel",                js_cancel},
    {             "close",                 js_close},
    {              "send",                  js_send},
    {          "sendSync",             js_send_sync},
    {           "receive",               js_receive},
    {       "receiveSync",          js_receive_sync},
    {       "receiveInto",          js_receive_into},
    {   "receiveIntoSync",     js_receive_into_sync},
    {       "sendAcquire",          js_send_acquire},
    {   "sendAcquireSync",     js_send_acquire_sync},
    {        "sendCommit",           js_send_commit},
    {    "receiveAcquire",       js_receive_acquire},
    {"receiveAcquireSync",  js_receive_acquire_sync},
    {    "receiveRelease",       js_receive_release},
    {         "rpcSubmit",            js_rpc_submit},
    {     "rpcSubmitSync",       js_rpc_submit_sync},
    {        "rpcRespond",           js_rpc_respond},
    {    "rpcRespondSync",      js_rpc_respond_sync},
    {        "rpcReceive",           js_rpc_receive},
    {    "rpcReceiveSync",      js_rpc_receive_sync},
    {    "rpcReceiveInto",      js_rpc_receive_into},
    {"rpcReceiveIntoSync", js_rpc_receive_into_sync},
};

NAPI_MODULE_INIT()
{
#ifdef _WIN32
    mutex_lock(&load_lock);
    bool imported = napi_windows_import();
    mutex_unlock(&load_lock);
    if (!imported)
        return NULL;
#endif
    pin_addon();
    state_t* state = calloc(1, sizeof *state);
    if (!state)
        return NULL;
    state->env = env;
    state->refs = 1;

    napi_value name, callback;
    napi_create_string_utf8(env, "fipc", NAPI_AUTO_LENGTH, &name);
    napi_create_function(env, "fipc", NAPI_AUTO_LENGTH, noop, NULL, &callback);
    if (napi_create_threadsafe_function(env, callback, NULL, name, 0, 1, NULL, NULL, NULL, on_done, &state->tsfn)
        != napi_ok)
    {
        free(state);
        return NULL;
    }
    napi_unref_threadsafe_function(env, state->tsfn); /* only pending calls keep the loop alive */
    napi_add_env_cleanup_hook(env, state_cleanup, state);

    napi_property_descriptor descriptors[sizeof methods / sizeof methods[0]];
    memset(descriptors, 0, sizeof descriptors);
    for (size_t i = 0; i < sizeof methods / sizeof methods[0]; i++)
    {
        descriptors[i].utf8name = methods[i].name;
        descriptors[i].method = methods[i].fn;
        descriptors[i].attributes = napi_default;
        descriptors[i].data = state;
    }
    napi_define_properties(env, exports, sizeof descriptors / sizeof descriptors[0], descriptors);
    return exports;
}
