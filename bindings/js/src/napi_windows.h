/*
 * napi_windows.h: the Node-API functions fipc_node.c calls, on Windows, looked up in the host process when the addon
 * loads.
 *
 * On Linux and macOS a shared library may leave symbols undefined, and the addon's Node-API functions are the host
 * process's (Node.js, Bun and Deno export them). A Windows DLL can't: it imports each function from a named DLL or
 * executable, and the host may be node.exe, bun.exe or deno.exe. So on Windows the addon declares no imports of
 * them: napi_windows_import() looks each one up in the host executable (GetModuleHandle(NULL)), in a table with the
 * functions' own types, and every call in fipc_node.c goes through the table (the defines below). The same addon
 * then loads in all three, with no import library and no delay-load hook.
 *
 * Every function fipc_node.c calls is in NAPI_IMPORTS, and nothing else: a function missing here doesn't link.
 */

#ifndef FIPC_NAPI_WINDOWS_H
#define FIPC_NAPI_WINDOWS_H

#include <node_api.h>
#include <stdbool.h>
#include <stdio.h>
#include <windows.h>

#define NAPI_IMPORTS(X)                                                                                                \
    X(napi_acquire_threadsafe_function)                                                                                \
    X(napi_add_env_cleanup_hook)                                                                                       \
    X(napi_call_function)                                                                                              \
    X(napi_call_threadsafe_function)                                                                                   \
    X(napi_create_buffer)                                                                                              \
    X(napi_create_buffer_copy)                                                                                         \
    X(napi_create_double)                                                                                              \
    X(napi_create_error)                                                                                               \
    X(napi_create_external)                                                                                            \
    X(napi_create_external_arraybuffer)                                                                                \
    X(napi_create_external_buffer)                                                                                     \
    X(napi_create_function)                                                                                            \
    X(napi_create_int32)                                                                                               \
    X(napi_create_promise)                                                                                             \
    X(napi_create_reference)                                                                                           \
    X(napi_create_string_utf8)                                                                                         \
    X(napi_create_threadsafe_function)                                                                                 \
    X(napi_define_properties)                                                                                          \
    X(napi_delete_reference)                                                                                           \
    X(napi_detach_arraybuffer)                                                                                         \
    X(napi_get_and_clear_last_exception)                                                                               \
    X(napi_get_arraybuffer_info)                                                                                       \
    X(napi_get_boolean)                                                                                                \
    X(napi_get_cb_info)                                                                                                \
    X(napi_get_dataview_info)                                                                                          \
    X(napi_get_reference_value)                                                                                        \
    X(napi_get_typedarray_info)                                                                                        \
    X(napi_get_undefined)                                                                                              \
    X(napi_get_value_bigint_uint64)                                                                                    \
    X(napi_get_value_double)                                                                                           \
    X(napi_get_value_external)                                                                                         \
    X(napi_get_value_string_utf8)                                                                                      \
    X(napi_is_arraybuffer)                                                                                             \
    X(napi_is_dataview)                                                                                                \
    X(napi_is_exception_pending)                                                                                       \
    X(napi_is_typedarray)                                                                                              \
    X(napi_ref_threadsafe_function)                                                                                    \
    X(napi_reject_deferred)                                                                                            \
    X(napi_release_threadsafe_function)                                                                                \
    X(napi_resolve_deferred)                                                                                           \
    X(napi_throw)                                                                                                      \
    X(napi_throw_range_error)                                                                                          \
    X(napi_throw_type_error)                                                                                           \
    X(napi_typeof)                                                                                                     \
    X(napi_unref_threadsafe_function)

/* The host's functions, with the types node_api.h declares */
static struct
{
#define X(name) __typeof__(&name) name;
    NAPI_IMPORTS(X)
#undef X
} napi_imports;

/* Fills the table from the host executable (a module named node.dll or libnode.dll, for an embedder that links Node.js
 * as a DLL). False, with a message on stderr, if a function is missing: the runtime lacks Node-API 8. */
static bool napi_windows_import(void)
{
    if (napi_imports.napi_typeof)
        return true;
    HMODULE hosts[] = {GetModuleHandleW(NULL), GetModuleHandleW(L"node.dll"), GetModuleHandleW(L"libnode.dll")};
    HMODULE host = NULL;
    for (size_t i = 0; i < sizeof hosts / sizeof hosts[0] && !host; i++)
        if (hosts[i] && GetProcAddress(hosts[i], "napi_typeof"))
            host = hosts[i];
    if (!host)
    {
        fprintf(stderr, "fipc: the process exports no Node-API functions (napi_typeof)\n");
        return false;
    }
#define X(name)                                                                                                        \
    if (!(napi_imports.name = (__typeof__(napi_imports.name)) (void*) GetProcAddress(host, #name)))                    \
    {                                                                                                                  \
        fprintf(stderr, "fipc: the process lacks the Node-API function " #name "\n");                                  \
        return false;                                                                                                  \
    }
    NAPI_IMPORTS(X)
#undef X
    return true;
}

/* Every call below goes through the table */
#define napi_acquire_threadsafe_function napi_imports.napi_acquire_threadsafe_function
#define napi_add_env_cleanup_hook napi_imports.napi_add_env_cleanup_hook
#define napi_call_function napi_imports.napi_call_function
#define napi_call_threadsafe_function napi_imports.napi_call_threadsafe_function
#define napi_create_buffer napi_imports.napi_create_buffer
#define napi_create_buffer_copy napi_imports.napi_create_buffer_copy
#define napi_create_double napi_imports.napi_create_double
#define napi_create_error napi_imports.napi_create_error
#define napi_create_external napi_imports.napi_create_external
#define napi_create_external_arraybuffer napi_imports.napi_create_external_arraybuffer
#define napi_create_external_buffer napi_imports.napi_create_external_buffer
#define napi_create_function napi_imports.napi_create_function
#define napi_create_int32 napi_imports.napi_create_int32
#define napi_create_promise napi_imports.napi_create_promise
#define napi_create_reference napi_imports.napi_create_reference
#define napi_create_string_utf8 napi_imports.napi_create_string_utf8
#define napi_create_threadsafe_function napi_imports.napi_create_threadsafe_function
#define napi_define_properties napi_imports.napi_define_properties
#define napi_delete_reference napi_imports.napi_delete_reference
#define napi_detach_arraybuffer napi_imports.napi_detach_arraybuffer
#define napi_get_and_clear_last_exception napi_imports.napi_get_and_clear_last_exception
#define napi_get_arraybuffer_info napi_imports.napi_get_arraybuffer_info
#define napi_get_boolean napi_imports.napi_get_boolean
#define napi_get_cb_info napi_imports.napi_get_cb_info
#define napi_get_dataview_info napi_imports.napi_get_dataview_info
#define napi_get_reference_value napi_imports.napi_get_reference_value
#define napi_get_typedarray_info napi_imports.napi_get_typedarray_info
#define napi_get_undefined napi_imports.napi_get_undefined
#define napi_get_value_bigint_uint64 napi_imports.napi_get_value_bigint_uint64
#define napi_get_value_double napi_imports.napi_get_value_double
#define napi_get_value_external napi_imports.napi_get_value_external
#define napi_get_value_string_utf8 napi_imports.napi_get_value_string_utf8
#define napi_is_arraybuffer napi_imports.napi_is_arraybuffer
#define napi_is_dataview napi_imports.napi_is_dataview
#define napi_is_exception_pending napi_imports.napi_is_exception_pending
#define napi_is_typedarray napi_imports.napi_is_typedarray
#define napi_ref_threadsafe_function napi_imports.napi_ref_threadsafe_function
#define napi_reject_deferred napi_imports.napi_reject_deferred
#define napi_release_threadsafe_function napi_imports.napi_release_threadsafe_function
#define napi_resolve_deferred napi_imports.napi_resolve_deferred
#define napi_throw napi_imports.napi_throw
#define napi_throw_range_error napi_imports.napi_throw_range_error
#define napi_throw_type_error napi_imports.napi_throw_type_error
#define napi_typeof napi_imports.napi_typeof
#define napi_unref_threadsafe_function napi_imports.napi_unref_threadsafe_function

#endif
