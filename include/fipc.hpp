// fipc.hpp: FastIPC for C++20, a header-only layer over the C API of fipc.h.
//
// It wraps the 30 functions of fipc.h and nothing else: no ABI of its own, no exceptions (it works with
// -fno-exceptions and /EHs-c-), no RTTI. fipc.h documents every call's contract; the comments here say how the C++
// types map onto it. One call is the wrapper's own: a service client's call(), a request and its reply, which keeps
// count of the replies owed so that it never returns another request's reply.
//
//   stream_create, stream_connect        fipc_stream_create (a listener), fipc_stream_connect (a connection)
//   broadcast_create, broadcast_join     fipc_broadcast_create (the publisher's connection), fipc_broadcast_join
//   queue_create, queue_join             fipc_queue_create (a queue), fipc_queue_join (a member's connection)
//   service_create, service_join         fipc_service_create (a service), fipc_service_join (a member's connection)
//   listener      a stream's: accept (fipc_stream_accept), fipc_listener_cancel; closes in its destructor
//                 (fipc_listener_close)
//   connection    a stream's, a publisher, or a member: the message calls (with the batches send_many and
//                 receive_many: fipc_send_many, fipc_recv_many), fipc_cancel, fipc_max_piece, a publisher's
//                 wait_for_members (fipc_broadcast_wait_for_members) and a service client's call (a fipc_send, then
//                 fipc_recv); closes in its destructor
//   queue         a work queue: wait_for_members (fipc_queue_wait_for_members), fipc_queue_cancel; ends the queue
//                 in its destructor (fipc_queue_close)
//   service       a service: wait_for_members (fipc_service_wait_for_members), fipc_service_cancel; ends the
//                 service in its destructor (fipc_service_close)
//   send_slot     fipc_send_acquire's room in the ring: write the message, then commit() it (fipc_send_commit)
//   receive_slot  fipc_recv_acquire's message in the ring; frees its room in its destructor (fipc_recv_release)
//   canceler      cancels a listener, a connection, a queue or a service from any thread, and does nothing once it is
//                 closed
//   result<T>     a value or an error; status is result<void>
//   timeout       a std::chrono duration, rounded up to whole milliseconds, or fipc::forever / fipc::no_wait
//
// Results: every call that can fail returns a result<T> or a status, [[nodiscard]]. A result converts to true when it
// holds a value; error() says why it doesn't (errc::timeout, errc::disconnected, ...; error().name() is the C name).
// A timeout or the peer's end is an ordinary result: test for it.
//
//     auto conn = fipc::stream_connect("my_stream", std::chrono::seconds(5));
//     if (!conn)
//     {
//         std::fprintf(stderr, "connect: %s\n", conn.error().name()); // FIPC_TIMEOUT: no server in 5 s
//         return 1;
//     }
//     auto sent = conn->send("ping"); // waits for room as long as it takes, the default
//
// Handles: listener, connection, queue and service own their native handle and are move-only, like std::unique_ptr:
// moving one leaves the source empty, and the handle is closed exactly once. An empty one's calls return errc::invalid.
//
// Threads (fipc.h, "Threads"): on a connection, one thread at a time sends (send, send_many, send_acquire and its slot)
// and one thread at a time receives (receive, receive_into, receive_many, receive_acquire and its slot); the two may be
// different threads, running at once. A service client's call() sends and receives: no other thread sends or receives
// while it runs. On a listener, one thread at a time accepts. A stream's client and server run on different threads:
// stream_connect waits for the server's accept. A broadcast's, a queue's or a service's joiners need no thread of the
// owner's.
// cancel(), max_piece() and the wait_for_members calls may run on any thread. To stop a thread that waits: take a
// canceler first (get_canceler()), cancel from any thread, join the waiting thread, then destroy (or close()) the
// handle. A canceler shares the handle's native state without keeping it open: once the handle is closed, cancel() does
// nothing, and a close that comes while a cancel runs waits for it.
//
// Slots: a send_slot or receive_slot is its side's turn: while it lives, that side makes no other call, and it must
// not outlive its connection. A slot whose turn a later acquire ended anyway is disarmed: such a send_slot commits
// nothing (errc::invalid), and such a receive_slot releases nothing.
//
// Building: compile with C++20 and link the shared library (fastipc.dll with fastipc.lib, libfastipc.so or
// libfastipc.dylib). On Windows, define FASTIPC_SHARED so that fipc.h declares the functions dllimport. With MSVC and
// /EHs-c-, also define _HAS_EXCEPTIONS=0, as its standard library requires.

#ifndef FIPC_HPP
#define FIPC_HPP

#include <algorithm>
#include <atomic>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

#include "fipc.h"

namespace fipc
{

// === Results ===

// Why a call didn't succeed: fipc_result_t without FIPC_OK (fipc.h says what each means).
enum class errc : int
{
    timeout = FIPC_TIMEOUT,
    disconnected = FIPC_DISCONNECTED,
    cancelled = FIPC_CANCELLED,
    too_large = FIPC_TOO_LARGE,
    invalid = FIPC_INVALID,
    no_memory = FIPC_NO_MEMORY,
    addr_in_use = FIPC_ADDR_IN_USE,
    overrun = FIPC_OVERRUN,
    unsubscribed = FIPC_UNSUBSCRIBED,
    abandoned = FIPC_ABANDONED,
};

// A broadcast subscriber's role (fipc_broadcast_join).
enum class subscriber_role : int
{
    lossy = FIPC_SUBSCRIBER_LOSSY,  // never holds the publisher back; may lose messages (errc::overrun)
    gated = FIPC_SUBSCRIBER_GATED,  // misses nothing: the publisher waits for it
};

// A work queue member's role (fipc_queue_join, queue::wait_for_members).
enum class queue_role : int
{
    producer = FIPC_PRODUCER,  // sends
    consumer = FIPC_CONSUMER,  // takes each message of any producer
};

// A service member's role (fipc_service_join, service::wait_for_members).
enum class service_role : int
{
    client = FIPC_CLIENT,  // sends requests, and receives their replies in their order
    server = FIPC_SERVER,  // takes the next request of any client, and replies to it
};

// A call's error: its code, and for errc::too_large the message's length, which is the room a receive needs; the
// message stays queued. For errc::overrun (a lossy subscriber's receive), len is the number of messages it lost; for a
// wait_for_members call, the number of members joined. Compares equal to its code: `r.error() == fipc::errc::timeout`.
struct error
{
    errc code{};
    std::size_t len = 0;

    // The C API's name of the code: "FIPC_TIMEOUT", ...
    const char* name() const noexcept
    {
        return fipc_result_str(static_cast<fipc_result_t>(code));
    }

    bool operator==(errc other) const noexcept
    {
        return code == other;
    }
};

// A T or an error, as C++23's std::expected<T, error> (T is default-constructible). It converts to true when it holds
// a T; *r and r-> reach the T without a check, r.value() aborts the program if there is none.
template <class T>
class [[nodiscard]] result
{
  public:
    result(T v) noexcept(std::is_nothrow_move_constructible_v<T>) : value_(std::move(v))
    {
    }

    result(fipc::error e) noexcept : error_(e)
    {
    }

    bool has_value() const noexcept
    {
        return error_.code == errc{};
    }

    explicit operator bool() const noexcept
    {
        return has_value();
    }

    // The error; meaningless when the result holds a value.
    fipc::error error() const noexcept
    {
        return error_;
    }

    T& value() & noexcept
    {
        check();
        return value_;
    }

    const T& value() const& noexcept
    {
        check();
        return value_;
    }

    T&& value() && noexcept
    {
        check();
        return std::move(value_);
    }

    template <class U>
    T value_or(U&& fallback) const&
    {
        return has_value() ? value_ : static_cast<T>(std::forward<U>(fallback));
    }

    T& operator*() & noexcept
    {
        return value_;
    }

    const T& operator*() const& noexcept
    {
        return value_;
    }

    T&& operator*() && noexcept
    {
        return std::move(value_);
    }

    T* operator->() noexcept
    {
        return &value_;
    }

    const T* operator->() const noexcept
    {
        return &value_;
    }

  private:
    void check() const noexcept
    {
        if (!has_value())
            std::abort();
    }

    T value_{};
    fipc::error error_{};
};

// Success or an error: the result of a call that returns nothing.
template <>
class [[nodiscard]] result<void>
{
  public:
    result() noexcept = default;

    result(fipc::error e) noexcept : error_(e)
    {
    }

    bool has_value() const noexcept
    {
        return error_.code == errc{};
    }

    explicit operator bool() const noexcept
    {
        return has_value();
    }

    fipc::error error() const noexcept
    {
        return error_;
    }

    // Aborts the program if the call didn't succeed.
    void value() const noexcept
    {
        if (!has_value())
            std::abort();
    }

  private:
    fipc::error error_{};
};

using status = result<void>;

// === Timeouts ===

namespace detail
{

// `d` as the C API's milliseconds: rounded up to whole ones; 0 for zero or less; FIPC_FOREVER from INT_MAX on.
template <class Rep, class Period>
constexpr int to_milliseconds(std::chrono::duration<Rep, Period> d) noexcept
{
    const double ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(d).count();
    if (!(ms > 0))
        return 0;
    if (ms >= static_cast<double>((std::numeric_limits<int>::max)()))
        return FIPC_FOREVER;
    const int whole = static_cast<int>(ms);
    return static_cast<double>(whole) < ms ? whole + 1 : whole;
}

}  // namespace detail

// How long a call may wait: a std::chrono duration, rounded up to whole milliseconds (zero or negative doesn't wait;
// from INT_MAX milliseconds, 24.8 days, on it waits for ever), or one of the constants fipc::forever and
// fipc::no_wait. Calls take it last; those that send, receive or accept wait for ever by default.
class timeout
{
  public:
    template <class Rep, class Period>
    constexpr timeout(std::chrono::duration<Rep, Period> d) noexcept : ms_(detail::to_milliseconds(d))
    {
    }

    // Waits as long as it takes: fipc::forever.
    static constexpr timeout wait_forever() noexcept
    {
        return timeout(FIPC_FOREVER);
    }

    // The C API's timeout_ms: 0 doesn't wait, FIPC_FOREVER (-1) waits for ever.
    constexpr int milliseconds() const noexcept
    {
        return ms_;
    }

    constexpr bool operator==(const timeout&) const noexcept = default;

  private:
    explicit constexpr timeout(int ms) noexcept : ms_(ms)
    {
    }

    int ms_;
};

inline constexpr timeout forever = timeout::wait_forever();
inline constexpr timeout no_wait = std::chrono::milliseconds::zero();

// === Buffers ===

// A container a receive can resize to a message's length: std::string, std::vector<char>, std::vector<std::byte>,
// std::vector<unsigned char>, ...
template <class B>
concept byte_buffer = requires(B& b, std::size_t n) {
    b.resize(n);
    { b.size() } -> std::convertible_to<std::size_t>;
    { b.data() } -> std::convertible_to<void*>;
} && sizeof(*std::declval<B&>().data()) == 1;

class connection;
class listener;
class queue;
class service;

namespace detail
{

// A listener's native handle, shared by the listener and its cancelers; the last one closes it.
struct listener_state
{
    explicit listener_state(fipc_listener_t* h) noexcept : handle(h)
    {
    }

    ~listener_state()
    {
        fipc_listener_close(handle);
    }

    listener_state(const listener_state&) = delete;
    listener_state& operator=(const listener_state&) = delete;

    fipc_listener_t* const handle;
};

// A connection's native handle, shared by the connection and its cancelers; the last one closes it. The turns count
// each side's zero-copy acquires, so that a slot acts only on its own reservation or message; each side's counter is
// written by that side's thread alone, and the gaps keep them on cache lines of their own.
//
// A service's client keeps count, oldest first, of the requests whose replies it hasn't received yet, so that call()
// never returns the reply to another request: first the one whose call gave up on its reply (it timed out or was
// cancelled), which the next call receives and drops (at most one, as a call first drops what is owed), then those sent
// with send() or a slot's commit(), whose replies are the caller's to receive. A request is counted before its send,
// since a thread that receives may take its reply before the send returns, and uncounted if the send fails; a reply
// or errc::abandoned takes the oldest off. The sending side adds to sends_owed while the receiving side takes off,
// hence the atomic; calls_owed is the receiving side's (call() is both sides).
struct connection_state
{
    connection_state(fipc_conn_t* h, bool service_client) noexcept
        : handle(h), max_piece(fipc_max_piece(h)), client(service_client)
    {
    }

    ~connection_state()
    {
        fipc_close(handle);
    }

    connection_state(const connection_state&) = delete;
    connection_state& operator=(const connection_state&) = delete;

    // fipc_cancel, after the flag: a call() that doesn't see it sent its request before the cancel.
    void cancel() noexcept
    {
        cancelled.store(true, std::memory_order_release);
        fipc_cancel(handle);
    }

    // A service's client is about to send a request: its reply is owed from before the send.
    void count_request() noexcept
    {
        if (client)
            sends_owed.fetch_add(1, std::memory_order_acq_rel);
    }

    // A service's client is about to send `count` requests in a batch: their replies are owed from before the send.
    void count_requests(std::uint32_t count) noexcept
    {
        if (client)
            sends_owed.fetch_add(count, std::memory_order_acq_rel);
    }

    // `unsent` of the requests counted with count_requests() didn't go: nothing is owed to them.
    void requests_unsent(std::uint32_t unsent) noexcept
    {
        if (!client || unsent == 0)
            return;
        std::uint32_t owed = sends_owed.load(std::memory_order_acquire);
        while (!sends_owed.compare_exchange_weak(owed, owed > unsent ? owed - unsent : 0, std::memory_order_acq_rel))
        {
        }
    }

    // The send of a request counted with count_request() returned `code`: unless it went, nothing is owed to it.
    void request_sent(fipc_result_t code) noexcept
    {
        if (client && code != FIPC_OK)
            take_send();
    }

    // A service client's receive returned `code`: a reply or FIPC_ABANDONED answers the oldest request owed one.
    void answered(fipc_result_t code) noexcept
    {
        if (!client || (code != FIPC_OK && code != FIPC_ABANDONED))
            return;
        if (calls_owed > 0)
            calls_owed--;
        else
            take_send();
    }

    void take_send() noexcept
    {
        std::uint32_t owed = sends_owed.load(std::memory_order_acquire);
        while (owed > 0 && !sends_owed.compare_exchange_weak(owed, owed - 1, std::memory_order_acq_rel))
        {
        }
    }

    fipc_conn_t* const handle;
    const std::size_t max_piece;
    const bool client;  // a service's client (service_join), which counts the replies owed
    // Set by cancel() (any thread) before the native cancel: a client's call() then sends nothing, as it could never
    // receive the reply.
    std::atomic<bool> cancelled{false};
    std::atomic<std::uint32_t> sends_owed{0};
    unsigned char gap1[64];
    std::uint64_t send_turn = 0;
    unsigned char gap2[64];
    std::uint64_t receive_turn = 0;
    std::uint32_t calls_owed = 0;
};

// A queue's native handle, shared by the queue and its cancelers; the last one closes it, which ends the queue.
struct queue_state
{
    explicit queue_state(fipc_queue_t* h) noexcept : handle(h)
    {
    }

    ~queue_state()
    {
        fipc_queue_close(handle);
    }

    queue_state(const queue_state&) = delete;
    queue_state& operator=(const queue_state&) = delete;

    fipc_queue_t* const handle;
};

// A service's native handle, shared by the service and its cancelers; the last one closes it, which ends the service.
struct service_state
{
    explicit service_state(fipc_service_t* h) noexcept : handle(h)
    {
    }

    ~service_state()
    {
        fipc_service_close(handle);
    }

    service_state(const service_state&) = delete;
    service_state& operator=(const service_state&) = delete;

    fipc_service_t* const handle;
};

inline fipc::error error_of(fipc_result_t code, std::size_t len) noexcept
{
    return fipc::error{static_cast<errc>(code), code == FIPC_TOO_LARGE || code == FIPC_OVERRUN ? len : 0};
}

inline status status_of(fipc_result_t code) noexcept
{
    if (code == FIPC_OK)
        return {};
    return error_of(code, 0);
}

// `name` as a NUL-terminated string in `out`; false for one that can't be a name (too long, or with a NUL in it).
inline bool c_name(std::string_view name, char (&out)[256]) noexcept
{
    if (name.size() >= sizeof out || name.find('\0') != std::string_view::npos)
        return false;
    std::memcpy(out, name.data(), name.size());
    out[name.size()] = '\0';
    return true;
}

}  // namespace detail

// === Cancelling from another thread ===

// Cancels a listener, a connection, a queue or a service from any thread (fipc_listener_cancel, fipc_cancel,
// fipc_queue_cancel, fipc_service_cancel): every call of it that waits, then or later, returns errc::cancelled; calls
// that needn't wait still work. Final. Copyable. A canceler doesn't keep its handle open: once the handle is closed,
// cancel() does nothing.
class canceler
{
  public:
    canceler() noexcept = default;

    void cancel() const noexcept
    {
        if (auto state = connection_.lock())
            state->cancel();
        if (auto state = listener_.lock())
            fipc_listener_cancel(state->handle);
        if (auto state = queue_.lock())
            fipc_queue_cancel(state->handle);
        if (auto state = service_.lock())
            fipc_service_cancel(state->handle);
    }

  private:
    friend class connection;
    friend class listener;
    friend class queue;
    friend class service;

    std::weak_ptr<detail::connection_state> connection_;
    std::weak_ptr<detail::listener_state> listener_;
    std::weak_ptr<detail::queue_state> queue_;
    std::weak_ptr<detail::service_state> service_;
};

// === Zero-copy slots ===

// Room for one message in the ring, from connection::send_acquire (16-byte aligned): write the message into data(),
// then commit() it. Destroying a slot that wasn't committed sends nothing, and the next send reuses the room.
class send_slot
{
  public:
    send_slot() noexcept = default;

    send_slot(send_slot&& other) noexcept
        : state_(std::exchange(other.state_, nullptr)), data_(std::exchange(other.data_, nullptr)),
          size_(std::exchange(other.size_, 0)), turn_(other.turn_)
    {
    }

    send_slot& operator=(send_slot&& other) noexcept
    {
        state_ = std::exchange(other.state_, nullptr);
        data_ = std::exchange(other.data_, nullptr);
        size_ = std::exchange(other.size_, 0);
        turn_ = other.turn_;
        return *this;
    }

    send_slot(const send_slot&) = delete;
    send_slot& operator=(const send_slot&) = delete;

    std::byte* data() const noexcept
    {
        return data_;
    }

    // The acquired length.
    std::size_t size() const noexcept
    {
        return size_;
    }

    std::span<std::byte> bytes() const noexcept
    {
        return {data_, size_};
    }

    // Publishes the first `len` bytes (1 to size()) as one message; doesn't wait. errc::invalid for a length out of
    // range (the slot stays, for a valid one), a slot committed already, or one whose reservation a later send
    // replaced.
    status commit(std::size_t len) noexcept
    {
        if (state_ == nullptr || state_->send_turn != turn_)
            return fipc::error{errc::invalid};
        state_->count_request();
        const fipc_result_t code = fipc_send_commit(state_->handle, len);
        state_->request_sent(code);
        if (code == FIPC_OK)
            *this = send_slot{};
        return detail::status_of(code);
    }

    // Publishes the whole slot.
    status commit() noexcept
    {
        return commit(size_);
    }

  private:
    friend class connection;

    send_slot(detail::connection_state* state, void* data, std::size_t size) noexcept
        : state_(state), data_(static_cast<std::byte*>(data)), size_(size), turn_(state->send_turn)
    {
    }

    detail::connection_state* state_ = nullptr;
    std::byte* data_ = nullptr;
    std::size_t size_ = 0;
    std::uint64_t turn_ = 0;
};

// A message in the ring, from connection::receive_acquire (16-byte aligned, read-only). Its room stays taken until
// the slot is destroyed or release() is called. On a service's server the slot is the request it holds, and release()
// does nothing: the bytes stay valid until the reply is sent (send(), or a send_slot's commit()), and from then on
// bytes() and data() dangle, though the slot lives on.
class receive_slot
{
  public:
    receive_slot() noexcept = default;

    receive_slot(receive_slot&& other) noexcept
        : state_(std::exchange(other.state_, nullptr)), data_(std::exchange(other.data_, nullptr)),
          size_(std::exchange(other.size_, 0)), turn_(other.turn_)
    {
    }

    receive_slot& operator=(receive_slot&& other) noexcept
    {
        if (this != &other)
        {
            release();
            state_ = std::exchange(other.state_, nullptr);
            data_ = std::exchange(other.data_, nullptr);
            size_ = std::exchange(other.size_, 0);
            turn_ = other.turn_;
        }
        return *this;
    }

    receive_slot(const receive_slot&) = delete;
    receive_slot& operator=(const receive_slot&) = delete;

    ~receive_slot()
    {
        release();
    }

    const std::byte* data() const noexcept
    {
        return data_;
    }

    std::size_t size() const noexcept
    {
        return size_;
    }

    std::span<const std::byte> bytes() const noexcept
    {
        return {data_, size_};
    }

    // Frees the message's room in the ring (fipc_recv_release), as the destructor does.
    void release() noexcept
    {
        if (state_ != nullptr && state_->receive_turn == turn_)
            fipc_recv_release(state_->handle);
        state_ = nullptr;
        data_ = nullptr;
        size_ = 0;
    }

  private:
    friend class connection;

    receive_slot(detail::connection_state* state, const void* data, std::size_t size) noexcept
        : state_(state), data_(static_cast<const std::byte*>(data)), size_(size), turn_(state->receive_turn)
    {
    }

    detail::connection_state* state_ = nullptr;
    const std::byte* data_ = nullptr;
    std::size_t size_ = 0;
    std::uint64_t turn_ = 0;
};

// === Connections ===

// One connection: a stream's (two rings in shared memory, one per direction: a server gets one from listener::accept, a
// client from stream_connect), a broadcast's publisher (broadcast_create) or subscriber (broadcast_join), a queue's
// producer or consumer (queue_join), or a service's client or server (service_join; a client's call() sends a request
// and returns its reply). Move-only; the destructor closes it (fipc_close): a stream's peer gets errc::disconnected
// once it has received the messages this side completed; a publisher's close ends the broadcast; a server's close
// abandons the request it holds (its client gets errc::abandoned).
class connection
{
  public:
    connection() noexcept = default;

    // Takes over a connection of the C API (from fipc_stream_connect, fipc_stream_accept, fipc_broadcast_create or a
    // join); nullptr makes an empty one. A service client's call() needs the constructor below.
    explicit connection(fipc_conn_t* handle) : connection(handle, false)
    {
    }

    // Takes over a service member's connection of the C API (from fipc_service_join as `r`): a client's keeps count of
    // the replies owed to its requests, for call().
    connection(fipc_conn_t* handle, service_role r) : connection(handle, r == service_role::client)
    {
    }

    connection(connection&&) noexcept = default;
    connection& operator=(connection&&) noexcept = default;
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

    // Whether this holds a connection (it doesn't once moved from or closed).
    explicit operator bool() const noexcept
    {
        return state_ != nullptr;
    }

    // The native connection, for calls of fipc.h; it stays this object's (don't close it).
    fipc_conn_t* native_handle() const noexcept
    {
        return state_ ? state_->handle : nullptr;
    }

    // The longest message the zero-copy calls take: the capacity less 64 bytes (a broadcast's or a queue's: a quarter
    // of its capacity, its longest message; a service's: its max_message, its longest request or reply). Any thread.
    std::size_t max_piece() const noexcept
    {
        return state_ ? state_->max_piece : 0;
    }

    // Makes every call of the connection that waits, now or later, return errc::cancelled (fipc_cancel); calls that
    // needn't wait still work, but a service client's call() returns errc::cancelled at once (after its request's
    // checks) and sends nothing. Final. Any thread, while this object lives; a canceler also outlives it.
    void cancel() const noexcept
    {
        if (state_)
            state_->cancel();
    }

    fipc::canceler get_canceler() const noexcept
    {
        fipc::canceler c;
        c.connection_ = state_;
        return c;
    }

    // A publisher: waits up to `wait` until at least `at_least` subscribers are joined, and returns how many are
    // (fipc_broadcast_wait_for_members); at_least 0 returns the count at once. On errc::timeout and errc::cancelled,
    // error().len is the count. errc::invalid on any other connection, or for at_least over max_subscribers. Any
    // thread.
    result<std::uint32_t> wait_for_members(std::uint32_t at_least = 0, fipc::timeout wait = forever) const noexcept
    {
        std::uint32_t count = 0;
        const fipc_result_t code =
            fipc_broadcast_wait_for_members(native_handle(), at_least, &count, wait.milliseconds());
        if (code != FIPC_OK)
            return fipc::error{static_cast<errc>(code), count};
        return count;
    }

    // Ends the connection now, as the destructor does.
    void close() noexcept
    {
        state_.reset();
    }

    // --- Sending ---

    // Sends one message (at least 1 byte, any size) as fipc_send: waits up to `wait` for room for its first piece. A
    // service's client sends a request; a server replies to the request it holds (errc::invalid with none held).
    status send(std::span<const std::byte> message, fipc::timeout wait = forever) noexcept
    {
        return send_bytes(message.data(), message.size(), wait);
    }

    status send(std::string_view message, fipc::timeout wait = forever) noexcept
    {
        return send_bytes(message.data(), message.size(), wait);
    }

    // Sends `messages` (at least one, each of at least 1 byte) in order as fipc_send_many, and returns how many went:
    // they are published together, with at most one wake of the receivers for the batch, and `wait` bounds the whole
    // call (fipc::no_wait: only what fits now). On an error, error().len is how many went, all those before the message
    // that didn't. An empty message is errc::invalid before any goes. The messages go 64 per fipc_send_many call, each
    // call checking its messages before any of them goes (errc::too_large over max_piece() on a broadcast, a queue or a
    // service; a stream sends a longer message in pieces). A service's client sends a batch of requests, whose replies
    // are owed as send()'s are; a server's batch is errc::invalid. Batch both ends: a receiver that takes one message
    // per call falls behind a batching sender, and the two run slower than without batches.
    result<std::size_t> send_many(
        std::span<const std::span<const std::byte>> messages, fipc::timeout wait = forever
    ) noexcept
    {
        for (const auto& message : messages)
        {
            if (message.empty())
                return fipc::error{errc::invalid, 0};
        }
        const auto start =
            wait.milliseconds() > 0 ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
        constexpr std::size_t group = 64;
        const void* datas[group];
        std::size_t lens[group];
        std::size_t sent = 0;
        do
        {
            const std::size_t count = (std::min)(group, messages.size() - sent);
            for (std::size_t i = 0; i < count; i++)
            {
                datas[i] = messages[sent + i].data();
                lens[i] = messages[sent + i].size();
            }
            if (state_)
                state_->count_requests(static_cast<std::uint32_t>(count));
            std::size_t went = 0;
            const fipc_result_t code =
                fipc_send_many(native_handle(), datas, lens, count, &went, time_left(wait, start).milliseconds());
            if (state_)
                state_->requests_unsent(static_cast<std::uint32_t>(count - went));
            sent += went;
            if (code != FIPC_OK)
                return fipc::error{static_cast<errc>(code), sent};
        }
        while (sent < messages.size());
        return sent;
    }

    // Zero-copy send: waits up to `wait` for `len` (1 to max_piece()) contiguous bytes in the ring
    // (fipc_send_acquire). errc::too_large over max_piece(): send such a message with send().
    result<send_slot> send_acquire(std::size_t len, fipc::timeout wait = forever) noexcept
    {
        void* data = nullptr;
        const fipc_result_t code = fipc_send_acquire(native_handle(), len, &data, wait.milliseconds());
        if (code != FIPC_OK)
            return detail::error_of(code, len);
        ++state_->send_turn;
        return send_slot(state_.get(), data, len);
    }

    // --- Receiving ---

    // Receives one message of any size into `buf`, resized to its length, and returns the length: waits up to `wait`
    // for its first piece (fipc_recv). The buffer keeps its capacity from one message to the next. A service's client
    // receives the reply to its oldest request not yet answered (after a call() that gave up, that call's late reply),
    // or errc::abandoned if the server that took it ended first (that request counts as answered); a receive that
    // times out or is cancelled leaves the reply for the next one. A server receives the next request of any client,
    // and holds it until it replies. `wait` bounds the whole call: on a work queue's consumer or a service's server
    // another member may take the message first, and the call then waits for the next one with what is left of it.
    template <byte_buffer Buffer>
    result<std::size_t> receive(Buffer& buf, fipc::timeout wait = forever)
    {
        const auto start =
            wait.milliseconds() > 0 ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
        std::size_t len = 0;
        fipc_result_t code = fipc_recv(native_handle(), buf.data(), buf.size(), &len, wait.milliseconds());
        while (code == FIPC_TOO_LARGE)  // the message stays queued: make room, and take it (or the next, maybe longer)
        {
            buf.resize(len);
            code = fipc_recv(native_handle(), buf.data(), buf.size(), &len, time_left(wait, start).milliseconds());
        }
        answered(code);
        if (code != FIPC_OK)
            return detail::error_of(code, len);
        buf.resize(len);
        return len;
    }

    // Receives one message into `buf` and returns its length (fipc_recv). errc::too_large if it is longer than
    // `buf`: nothing is taken, and error().len is the room it needs.
    result<std::size_t> receive_into(std::span<std::byte> buf, fipc::timeout wait = forever) noexcept
    {
        return recv(buf.data(), buf.size(), wait);
    }

    result<std::size_t> receive_into(std::span<char> buf, fipc::timeout wait = forever) noexcept
    {
        return recv(buf.data(), buf.size(), wait);
    }

    // Receives a batch as fipc_recv_many: waits up to `wait` for the next message, then takes every further message
    // already there that fits the rest of `buf`, up to lens.size() (at least 1), packed back to back (message i is
    // lens[i] bytes, at the sum of the lengths before it), and returns how many. An error is the first message's, as
    // receive_into() returns it, with nothing else taken: errc::too_large (error().len is its length, and it stays
    // queued), errc::overrun (error().len is the number lost), a service client's errc::abandoned. After the first it
    // never waits, and stops before a message that isn't there yet, doesn't fit, is in pieces, or would bring another
    // result: the next receive returns it. A broadcast's subscriber takes, after its first, only the messages its
    // publisher has moved past; a service's client receives its replies in its requests' order.
    result<std::size_t> receive_many(
        std::span<std::byte> buf, std::span<std::size_t> lens, fipc::timeout wait = forever
    ) noexcept
    {
        std::size_t count = 0;
        const fipc_result_t code = fipc_recv_many(
            native_handle(), buf.data(), buf.size(), lens.data(), lens.size(), &count, wait.milliseconds()
        );
        if (code != FIPC_OK)
        {
            answered(code);
            return detail::error_of(code, lens.empty() ? 0 : lens[0]);
        }
        for (std::size_t i = 0; i < count; i++)
            answered(code);
        return count;
    }

    // Zero-copy receive: waits up to `wait` for a message and returns it in the ring (fipc_recv_acquire).
    // errc::too_large for a message in several pieces: error().len is its length, and it stays queued for receive().
    // A service's server: the request in place stays valid until its reply is sent, and the slot's release does
    // nothing.
    result<receive_slot> receive_acquire(fipc::timeout wait = forever) noexcept
    {
        const void* data = nullptr;
        std::size_t len = 0;
        const fipc_result_t code = fipc_recv_acquire(native_handle(), &data, &len, wait.milliseconds());
        answered(code);
        if (code != FIPC_OK)
            return detail::error_of(code, len);
        ++state_->receive_turn;
        return receive_slot(state_.get(), data, len);
    }

    // --- A service's client: a request and its reply ---

    // Sends `request` and receives its reply into `reply`, resized to its length, and returns the length, waiting up
    // to `wait` in all; errc::abandoned if the server that took the request ended before it replied (the request
    // counts as answered; it may or may not have been carried out, and it is never sent again). A call checks its
    // request first: errc::invalid for an empty one, errc::too_large over max_piece() (error().len is its length), at
    // once, even while a reply is owed. Then a call on a cancelled connection returns errc::cancelled at once, whatever
    // its timeout, and sends nothing: it could never receive the reply.
    //
    // A call never returns the reply to another request: the connection keeps count of the replies still owed. A call
    // that timed out or was cancelled waiting for its reply leaves that reply owed, and the next call first receives
    // and drops it (at most one is owed, since every call drops it first; an errc::abandoned in its place counts too),
    // within its own timeout and before it sends. A call that had to drop an owed reply sends its request only if its
    // timeout has time left: once a positive timeout has run out, it returns errc::timeout without sending, so no
    // server sees that request; with fipc::no_wait no step waits, and it sends if there is room. After a call that gave
    // up, a plain receive returns that call's late reply (or errc::abandoned) first: replies come in request order.
    //
    // errc::invalid while a request sent with send() or a slot's commit() still has its reply to be received (that
    // reply is the caller's: receive it first), also after the service's end, and on any connection other than a
    // service's client (from service_join, or the constructor that takes a service_role); errc::disconnected once the
    // service has ended. After an error, the contents of `reply` are unspecified. A call sends and receives: no
    // other thread may send or receive on the connection while it runs.
    template <byte_buffer Buffer>
    result<std::size_t> call(std::span<const std::byte> request, Buffer& reply, fipc::timeout wait = forever)
    {
        return call_bytes(request.data(), request.size(), reply, wait);
    }

    template <byte_buffer Buffer>
    result<std::size_t> call(std::string_view request, Buffer& reply, fipc::timeout wait = forever)
    {
        return call_bytes(request.data(), request.size(), reply, wait);
    }

  private:
    connection(fipc_conn_t* handle, bool service_client)
        : state_(handle ? std::make_shared<detail::connection_state>(handle, service_client) : nullptr)
    {
    }

    // What is left of `wait`, begun at `start`: fipc::forever and fipc::no_wait stay as they are, and a wait that has
    // run out is fipc::no_wait.
    static fipc::timeout time_left(fipc::timeout wait, std::chrono::steady_clock::time_point start) noexcept
    {
        if (wait.milliseconds() <= 0)
            return wait;
        return std::chrono::milliseconds(wait.milliseconds()) - (std::chrono::steady_clock::now() - start);
    }

    // A service client's count of the replies owed (detail::connection_state).
    void answered(fipc_result_t code) noexcept
    {
        if (state_)
            state_->answered(code);
    }

    status send_bytes(const void* data, std::size_t len, fipc::timeout wait) noexcept
    {
        if (state_)
            state_->count_request();
        const fipc_result_t code = fipc_send(native_handle(), data, len, wait.milliseconds());
        if (state_)
            state_->request_sent(code);
        return detail::status_of(code);
    }

    result<std::size_t> recv(void* buf, std::size_t cap, fipc::timeout wait) noexcept
    {
        std::size_t len = 0;
        const fipc_result_t code = fipc_recv(native_handle(), buf, cap, &len, wait.milliseconds());
        answered(code);
        if (code != FIPC_OK)
            return detail::error_of(code, len);
        return len;
    }

    template <byte_buffer Buffer>
    result<std::size_t> call_bytes(const void* request, std::size_t len, Buffer& reply, fipc::timeout wait)
    {
        const auto start =
            wait.milliseconds() > 0 ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
        detail::connection_state* const state = state_.get();
        if (state == nullptr || !state->client)
            return fipc::error{errc::invalid};
        if (len == 0)
            return fipc::error{errc::invalid};
        if (len > state->max_piece)
            return fipc::error{errc::too_large, len};
        if (state->cancelled.load(std::memory_order_acquire))
            return fipc::error{errc::cancelled};  // its reply could never be received: the request never goes
        if (state->sends_owed.load(std::memory_order_acquire) != 0)
            return fipc::error{errc::invalid};  // a reply to a request of send()'s: the caller's, not to drop
        bool dropped = false;
        while (state->calls_owed > 0)
        {
            // A reply owed to a call that gave up on it, received into `reply` and dropped (the receive takes it off
            // the count)
            auto owed = receive(reply, time_left(wait, start));
            if (!owed && owed.error() != errc::abandoned)
                return owed.error();
            dropped = true;
        }
        if (dropped && wait.milliseconds() > 0 && time_left(wait, start).milliseconds() == 0)
            return fipc::error{errc::timeout};  // its time went on the drop: the request isn't sent, no server sees it
        if (state->cancelled.load(std::memory_order_acquire))
            return fipc::error{errc::cancelled};  // cancelled while it dropped: the request never goes
        // Its reply is owed from before the send
        state->calls_owed = 1;
        const fipc_result_t code = fipc_send(state->handle, request, len, time_left(wait, start).milliseconds());
        if (code != FIPC_OK)
        {
            state->calls_owed = 0;
            return detail::error_of(code, 0);
        }
        return receive(reply, time_left(wait, start));  // a reply or errc::abandoned takes it off; else it stays owed
    }

    std::shared_ptr<detail::connection_state> state_;
};

// === Listeners ===

// A stream's name: its server accepts one client at a time. Move-only; the destructor stops listening and frees the
// name (fipc_listener_close). The connections it accepted stay open.
class listener
{
  public:
    listener() noexcept = default;

    // Takes over a listener of the C API (from fipc_stream_create); nullptr makes an empty one.
    explicit listener(fipc_listener_t* handle)
        : state_(handle ? std::make_shared<detail::listener_state>(handle) : nullptr)
    {
    }

    listener(listener&&) noexcept = default;
    listener& operator=(listener&&) noexcept = default;
    listener(const listener&) = delete;
    listener& operator=(const listener&) = delete;

    // Waits up to `wait` for a client, sets its connection up on the calling thread and returns it
    // (fipc_stream_accept). A call that times out in the middle of a client's setup keeps it for the next call, so
    // fipc::no_wait polls. One client at a time: errc::invalid while the connection this listener returned last is
    // open.
    result<connection> accept(fipc::timeout wait = forever)
    {
        fipc_conn_t* handle = nullptr;
        const fipc_result_t code = fipc_stream_accept(native_handle(), &handle, wait.milliseconds());
        if (code != FIPC_OK)
            return detail::error_of(code, 0);
        return connection(handle);
    }

    explicit operator bool() const noexcept
    {
        return state_ != nullptr;
    }

    // The native listener, for calls of fipc.h; it stays this object's (don't close it).
    fipc_listener_t* native_handle() const noexcept
    {
        return state_ ? state_->handle : nullptr;
    }

    // Makes every accept that waits, now or later, return errc::cancelled (fipc_listener_cancel). Final. Any thread,
    // while this object lives; a canceler also outlives it.
    void cancel() const noexcept
    {
        fipc_listener_cancel(native_handle());
    }

    fipc::canceler get_canceler() const noexcept
    {
        fipc::canceler c;
        c.listener_ = state_;
        return c;
    }

    // Stops listening now, as the destructor does.
    void close() noexcept
    {
        state_.reset();
    }

  private:
    std::shared_ptr<detail::listener_state> state_;
};

// === Work queues ===

// A work queue (fipc_queue_create), held by its owner. Move-only; the destructor, or close(), ends the queue
// (fipc_queue_close): producers get errc::disconnected, consumers receive what is left, then errc::disconnected. The
// owner joins its own queue with queue_join, as any member does; its members' connections stay open after the end.
class queue
{
  public:
    queue() noexcept = default;

    // Takes over a queue of the C API (from fipc_queue_create); nullptr makes an empty one.
    explicit queue(fipc_queue_t* handle) : state_(handle ? std::make_shared<detail::queue_state>(handle) : nullptr)
    {
    }

    queue(queue&&) noexcept = default;
    queue& operator=(queue&&) noexcept = default;
    queue(const queue&) = delete;
    queue& operator=(const queue&) = delete;

    explicit operator bool() const noexcept
    {
        return state_ != nullptr;
    }

    // The native queue, for calls of fipc.h; it stays this object's (don't close it).
    fipc_queue_t* native_handle() const noexcept
    {
        return state_ ? state_->handle : nullptr;
    }

    // Waits up to `wait` until at least `at_least` members of role `r` are joined, and returns how many are
    // (fipc_queue_wait_for_members); at_least 0 returns the count at once. On errc::timeout, errc::cancelled and
    // errc::disconnected, error().len is the count. errc::invalid for at_least over the role's maximum. Any thread.
    result<std::uint32_t> wait_for_members(queue_role r, std::uint32_t at_least = 0, fipc::timeout wait = forever)
        const noexcept
    {
        return wait_for(static_cast<int>(r), at_least, wait);
    }

    // As above, counting producers and consumers together.
    result<std::uint32_t> wait_for_members(std::uint32_t at_least = 0, fipc::timeout wait = forever) const noexcept
    {
        return wait_for(0, at_least, wait);
    }

    // Makes every wait_for_members that waits, now or later, return errc::cancelled (fipc_queue_cancel). Final. Any
    // thread, while this object lives; a canceler also outlives it.
    void cancel() const noexcept
    {
        fipc_queue_cancel(native_handle());
    }

    fipc::canceler get_canceler() const noexcept
    {
        fipc::canceler c;
        c.queue_ = state_;
        return c;
    }

    // Ends the queue now, as the destructor does.
    void close() noexcept
    {
        state_.reset();
    }

  private:
    result<std::uint32_t> wait_for(int r, std::uint32_t at_least, fipc::timeout wait) const noexcept
    {
        std::uint32_t count = 0;
        const fipc_result_t code =
            fipc_queue_wait_for_members(native_handle(), r, at_least, &count, wait.milliseconds());
        if (code != FIPC_OK)
            return fipc::error{static_cast<errc>(code), count};
        return count;
    }

    std::shared_ptr<detail::queue_state> state_;
};

// === Services ===

// A service (fipc_service_create), held by its owner. Move-only; the destructor, or close(), ends the service
// (fipc_service_close): clients' sends and servers' receives get errc::disconnected, and a client receives the replies
// that came before the end, then errc::disconnected. The owner joins its own service with service_join, as any member
// does; its members' connections stay open after the end.
class service
{
  public:
    service() noexcept = default;

    // Takes over a service of the C API (from fipc_service_create); nullptr makes an empty one.
    explicit service(fipc_service_t* handle)
        : state_(handle ? std::make_shared<detail::service_state>(handle) : nullptr)
    {
    }

    service(service&&) noexcept = default;
    service& operator=(service&&) noexcept = default;
    service(const service&) = delete;
    service& operator=(const service&) = delete;

    explicit operator bool() const noexcept
    {
        return state_ != nullptr;
    }

    // The native service, for calls of fipc.h; it stays this object's (don't close it).
    fipc_service_t* native_handle() const noexcept
    {
        return state_ ? state_->handle : nullptr;
    }

    // Waits up to `wait` until at least `at_least` members of role `r` are joined, and returns how many are
    // (fipc_service_wait_for_members); at_least 0 returns the count at once. On errc::timeout, errc::cancelled and
    // errc::disconnected, error().len is the count. errc::invalid for at_least over the role's maximum. Any thread.
    result<std::uint32_t> wait_for_members(service_role r, std::uint32_t at_least = 0, fipc::timeout wait = forever)
        const noexcept
    {
        return wait_for(static_cast<int>(r), at_least, wait);
    }

    // As above, counting clients and servers together.
    result<std::uint32_t> wait_for_members(std::uint32_t at_least = 0, fipc::timeout wait = forever) const noexcept
    {
        return wait_for(0, at_least, wait);
    }

    // Makes every wait_for_members that waits, now or later, return errc::cancelled (fipc_service_cancel). Final. Any
    // thread, while this object lives; a canceler also outlives it.
    void cancel() const noexcept
    {
        fipc_service_cancel(native_handle());
    }

    fipc::canceler get_canceler() const noexcept
    {
        fipc::canceler c;
        c.service_ = state_;
        return c;
    }

    // Ends the service now, as the destructor does.
    void close() noexcept
    {
        state_.reset();
    }

  private:
    result<std::uint32_t> wait_for(int r, std::uint32_t at_least, fipc::timeout wait) const noexcept
    {
        std::uint32_t count = 0;
        const fipc_result_t code =
            fipc_service_wait_for_members(native_handle(), r, at_least, &count, wait.milliseconds());
        if (code != FIPC_OK)
            return fipc::error{static_cast<errc>(code), count};
        return count;
    }

    std::shared_ptr<detail::service_state> state_;
};

// === Creating and joining ===

// Server: claims `name` for a stream with rings of `capacity` bytes each way (fipc_stream_create). Doesn't wait.
// errc::addr_in_use if another stream, broadcast, queue or service holds the name.
inline result<listener> stream_create(std::string_view name, std::size_t capacity)
{
    char c_name[256];
    if (!detail::c_name(name, c_name))
        return fipc::error{errc::invalid};
    fipc_listener_t* handle = nullptr;
    const fipc_result_t code = fipc_stream_create(c_name, capacity, &handle);
    if (code != FIPC_OK)
        return detail::error_of(code, 0);
    return listener(handle);
}

// Client: connects to the stream on `name`, waiting up to `wait` for its server's accept (fipc_stream_connect); the
// server runs on another thread or in another process. No handle can cancel it: use fipc::forever to wait however
// late the server starts, or a finite timeout to stay responsive.
inline result<connection> stream_connect(std::string_view name, fipc::timeout wait)
{
    char c_name[256];
    if (!detail::c_name(name, c_name))
        return fipc::error{errc::invalid};
    fipc_conn_t* handle = nullptr;
    const fipc_result_t code = fipc_stream_connect(c_name, &handle, wait.milliseconds());
    if (code != FIPC_OK)
        return detail::error_of(code, 0);
    return connection(handle);
}

// Publisher: claims `name` for a broadcast and returns the publisher's connection (fipc_broadcast_create). max_stall,
// a fipc::timeout as any wait: fipc::forever never unsubscribes a gated subscriber; fipc::no_wait refuses gated
// subscribers; a duration unsubscribes one held still that long. Destroying the publisher ends the broadcast.
inline result<connection> broadcast_create(
    std::string_view name, std::size_t capacity, std::uint32_t max_subscribers, fipc::timeout max_stall
)
{
    char c_name[256];
    if (!detail::c_name(name, c_name))
        return fipc::error{errc::invalid};
    fipc_conn_t* handle = nullptr;
    const fipc_result_t code =
        fipc_broadcast_create(c_name, capacity, max_subscribers, max_stall.milliseconds(), &handle);
    if (code != FIPC_OK)
        return detail::error_of(code, 0);
    return connection(handle);
}

// Subscriber: joins the broadcast on `name` as `r` (fipc_broadcast_join), waiting up to `wait`.
inline result<connection> broadcast_join(std::string_view name, subscriber_role r, fipc::timeout wait)
{
    char c_name[256];
    if (!detail::c_name(name, c_name))
        return fipc::error{errc::invalid};
    fipc_conn_t* handle = nullptr;
    const fipc_result_t code = fipc_broadcast_join(c_name, static_cast<int>(r), &handle, wait.milliseconds());
    if (code != FIPC_OK)
        return detail::error_of(code, 0);
    return connection(handle);
}

// Owner: claims `name` for a work queue (fipc_queue_create); its members join with queue_join.
inline result<queue> queue_create(
    std::string_view name, std::size_t capacity, std::uint32_t max_producers, std::uint32_t max_consumers
)
{
    char c_name[256];
    if (!detail::c_name(name, c_name))
        return fipc::error{errc::invalid};
    fipc_queue_t* handle = nullptr;
    const fipc_result_t code = fipc_queue_create(c_name, capacity, max_producers, max_consumers, &handle);
    if (code != FIPC_OK)
        return detail::error_of(code, 0);
    return queue(handle);
}

// Producer or consumer: joins the queue on `name` as `r` (fipc_queue_join), waiting up to `wait`.
inline result<connection> queue_join(std::string_view name, queue_role r, fipc::timeout wait)
{
    char c_name[256];
    if (!detail::c_name(name, c_name))
        return fipc::error{errc::invalid};
    fipc_conn_t* handle = nullptr;
    const fipc_result_t code = fipc_queue_join(c_name, static_cast<int>(r), &handle, wait.milliseconds());
    if (code != FIPC_OK)
        return detail::error_of(code, 0);
    return connection(handle);
}

// Owner: claims `name` for a service (fipc_service_create): each of up to `max_clients` clients gets a ring of
// `capacity` bytes for its requests and their replies, each of 1 to `max_message` bytes; up to `max_servers` servers
// take the requests. Its members join with service_join.
inline result<service> service_create(
    std::string_view name,
    std::size_t capacity,
    std::size_t max_message,
    std::uint32_t max_clients,
    std::uint32_t max_servers
)
{
    char c_name[256];
    if (!detail::c_name(name, c_name))
        return fipc::error{errc::invalid};
    fipc_service_t* handle = nullptr;
    const fipc_result_t code = fipc_service_create(c_name, capacity, max_message, max_clients, max_servers, &handle);
    if (code != FIPC_OK)
        return detail::error_of(code, 0);
    return service(handle);
}

// Client or server: joins the service on `name` as `r` (fipc_service_join), waiting up to `wait`.
inline result<connection> service_join(std::string_view name, service_role r, fipc::timeout wait)
{
    char c_name[256];
    if (!detail::c_name(name, c_name))
        return fipc::error{errc::invalid};
    fipc_conn_t* handle = nullptr;
    const fipc_result_t code = fipc_service_join(c_name, static_cast<int>(r), &handle, wait.milliseconds());
    if (code != FIPC_OK)
        return detail::error_of(code, 0);
    return connection(handle, r);
}

}  // namespace fipc

#endif  // FIPC_HPP
