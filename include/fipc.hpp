// fipc.hpp: FastIPC for C++20, a header-only layer over the C API of fipc.h.
//
// It wraps the 18 functions of fipc.h and nothing else: no ABI of its own, no exceptions (it works with
// -fno-exceptions and /EHs-c-), no RTTI. fipc.h documents every call's contract; the comments here say how the C++
// types map onto it.
//
//   listener      fipc_listen, fipc_accept, fipc_listener_cancel; closes in its destructor (fipc_listener_close)
//   connection    fipc_connect, the message and RPC calls, fipc_cancel, fipc_max_piece; closes in its destructor
//   send_slot     fipc_send_acquire's room in the ring: write the message, then commit() it (fipc_send_commit)
//   receive_slot  fipc_recv_acquire's message in the ring; frees its room in its destructor (fipc_recv_release)
//   canceler      cancels a listener or a connection from any thread, and does nothing once it is closed
//   result<T>     a value or an error; status is result<void>
//   timeout       a std::chrono duration, rounded up to whole milliseconds, or fipc::forever / fipc::no_wait
//
// Results: every call that can fail returns a result<T> or a status, [[nodiscard]]. A result converts to true when it
// holds a value; error() says why it doesn't (errc::timeout, errc::disconnected, ...; error().name() is the C name).
// A timeout or the peer's end is an ordinary result: test for it.
//
//     auto conn = fipc::connection::connect("my_channel", std::chrono::seconds(5));
//     if (!conn)
//     {
//         std::fprintf(stderr, "connect: %s\n", conn.error().name()); // FIPC_TIMEOUT: no server in 5 s
//         return 1;
//     }
//     auto id = conn->rpc_submit(1, "ping"); // waits for room as long as it takes, the default
//
// Handles: listener and connection own their native handle and are move-only, like std::unique_ptr: moving one
// leaves the source empty, and the handle is closed exactly once. An empty one's calls return errc::invalid.
//
// Threads (fipc.h, "Threads"): on a connection, one thread at a time sends (send, send_acquire and its slot,
// rpc_submit, rpc_respond) and one thread at a time receives (receive, receive_into, receive_acquire and its slot,
// rpc_receive, rpc_receive_into); the two may be different threads, running at once. On a listener, one thread at a
// time accepts. A client and its server run on different threads (of one process or two): connect waits for the
// server's accept. cancel() and max_piece() may run on any thread. To stop a thread that waits: take a canceler first
// (get_canceler()), cancel from any thread, join the waiting thread, then destroy (or close()) the handle. A
// canceler shares the handle's native state without keeping it open: once the handle is closed, cancel() does
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
};

// A call's error: its code, and for errc::too_large the message's length (an RPC message's: its payload's), which is
// the room a receive needs; the message stays queued. Compares equal to its code: `r.error() == fipc::errc::timeout`.
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

// === RPC messages ===

enum class rpc_kind : std::uint32_t
{
    request = FIPC_RPC_REQUEST,    // sent by rpc_submit: answer it with rpc_respond
    response = FIPC_RPC_RESPONSE,  // sent by rpc_respond
};

// An RPC message's header (fipc_rpc_msg_t without its reserved field); its payload is in the caller's buffer.
struct rpc_header
{
    std::uint64_t id = 0;  // the request's id, echoed in its response; a connection numbers them from 1
    rpc_kind kind = rpc_kind::request;
    std::uint32_t opcode = 0;  // the application's
    std::int32_t status = 0;   // the application's; 0 in requests
    std::size_t len = 0;       // the payload's length
};

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
struct connection_state
{
    explicit connection_state(fipc_conn_t* h) noexcept : handle(h), max_piece(fipc_max_piece(h))
    {
    }

    ~connection_state()
    {
        fipc_close(handle);
    }

    connection_state(const connection_state&) = delete;
    connection_state& operator=(const connection_state&) = delete;

    fipc_conn_t* const handle;
    const std::size_t max_piece;
    unsigned char gap1[64];
    std::uint64_t send_turn = 0;
    unsigned char gap2[64];
    std::uint64_t receive_turn = 0;
};

inline fipc::error error_of(fipc_result_t code, std::size_t len) noexcept
{
    return fipc::error{static_cast<errc>(code), code == FIPC_TOO_LARGE ? len : 0};
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

inline rpc_header header_of(const fipc_rpc_msg_t& msg) noexcept
{
    return rpc_header{
        msg.id, static_cast<rpc_kind>(msg.kind), msg.opcode, msg.status, static_cast<std::size_t>(msg.len)
    };
}

}  // namespace detail

// === Cancelling from another thread ===

// Cancels a listener or a connection from any thread (fipc_listener_cancel, fipc_cancel): every call of it that
// waits, then or later, returns errc::cancelled; calls that needn't wait still work. Final. Copyable. A canceler
// doesn't keep its handle open: once the handle is closed, cancel() does nothing.
class canceler
{
  public:
    canceler() noexcept = default;

    void cancel() const noexcept
    {
        if (auto state = connection_.lock())
            fipc_cancel(state->handle);
        if (auto state = listener_.lock())
            fipc_listener_cancel(state->handle);
    }

  private:
    friend class connection;
    friend class listener;

    std::weak_ptr<detail::connection_state> connection_;
    std::weak_ptr<detail::listener_state> listener_;
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
        const fipc_result_t code = fipc_send_commit(state_->handle, len);
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
// the slot is destroyed or release() is called.
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

// One connection: two rings in shared memory, one per direction. A server gets one from listener::accept, a client
// from connection::connect. Move-only; the destructor ends the connection (fipc_close): the peer gets
// errc::disconnected once it has received the messages this side completed.
class connection
{
  public:
    connection() noexcept = default;

    // Takes over a connection of the C API (from fipc_connect or fipc_accept); nullptr makes an empty one.
    explicit connection(fipc_conn_t* handle)
        : state_(handle ? std::make_shared<detail::connection_state>(handle) : nullptr)
    {
    }

    connection(connection&&) noexcept = default;
    connection& operator=(connection&&) noexcept = default;
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

    // Client: connects to the server listening on `name` and sets the connection up on the calling thread, waiting up
    // to `wait` for a server to listen and to call accept (fipc_connect); the server runs on another thread or in
    // another process. There is no handle to cancel until it returns: use fipc::forever to wait for a server however
    // late it starts, or a finite timeout to stay responsive.
    static result<connection> connect(std::string_view name, fipc::timeout wait)
    {
        char c_name[256];
        if (!detail::c_name(name, c_name))
            return fipc::error{errc::invalid};
        fipc_conn_t* handle = nullptr;
        const fipc_result_t code = fipc_connect(c_name, &handle, wait.milliseconds());
        if (code != FIPC_OK)
            return detail::error_of(code, 0);
        return connection(handle);
    }

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

    // The longest message the zero-copy calls take: the capacity less 64 bytes. Any thread.
    std::size_t max_piece() const noexcept
    {
        return state_ ? state_->max_piece : 0;
    }

    // Makes every call of the connection that waits, now or later, return errc::cancelled (fipc_cancel); calls that
    // needn't wait still work. Final. Any thread, while this object lives; a canceler also outlives it.
    void cancel() const noexcept
    {
        fipc_cancel(native_handle());
    }

    fipc::canceler get_canceler() const noexcept
    {
        fipc::canceler c;
        c.connection_ = state_;
        return c;
    }

    // Ends the connection now, as the destructor does.
    void close() noexcept
    {
        state_.reset();
    }

    // --- Sending ---

    // Sends one message (at least 1 byte, any size) as fipc_send: waits up to `wait` for room for its first piece.
    status send(std::span<const std::byte> message, fipc::timeout wait = forever) noexcept
    {
        return detail::status_of(fipc_send(native_handle(), message.data(), message.size(), wait.milliseconds()));
    }

    status send(std::string_view message, fipc::timeout wait = forever) noexcept
    {
        return detail::status_of(fipc_send(native_handle(), message.data(), message.size(), wait.milliseconds()));
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

    // Sends an RPC request (the payload may be empty) as send() does, and returns its id.
    result<std::uint64_t> rpc_submit(
        std::uint32_t opcode, std::span<const std::byte> payload, fipc::timeout wait = forever
    ) noexcept
    {
        return submit(opcode, payload.data(), payload.size(), wait);
    }

    result<std::uint64_t> rpc_submit(
        std::uint32_t opcode, std::string_view payload, fipc::timeout wait = forever
    ) noexcept
    {
        return submit(opcode, payload.data(), payload.size(), wait);
    }

    // Sends the response to request `id`, with an opcode (by convention the request's), the application's status
    // code and a payload (possibly empty), as send() does.
    status rpc_respond(
        std::uint64_t id,
        std::uint32_t opcode,
        std::int32_t status_code,
        std::span<const std::byte> payload,
        fipc::timeout wait = forever
    ) noexcept
    {
        return detail::status_of(fipc_rpc_respond(
            native_handle(), id, opcode, status_code, payload.data(), payload.size(), wait.milliseconds()
        ));
    }

    status rpc_respond(
        std::uint64_t id,
        std::uint32_t opcode,
        std::int32_t status_code,
        std::string_view payload,
        fipc::timeout wait = forever
    ) noexcept
    {
        return detail::status_of(fipc_rpc_respond(
            native_handle(), id, opcode, status_code, payload.data(), payload.size(), wait.milliseconds()
        ));
    }

    // --- Receiving ---

    // Receives one message of any size into `buf`, resized to its length, and returns the length: waits up to `wait`
    // for its first piece (fipc_recv). The buffer keeps its capacity from one message to the next.
    template <byte_buffer Buffer>
    result<std::size_t> receive(Buffer& buf, fipc::timeout wait = forever)
    {
        std::size_t len = 0;
        fipc_result_t code = fipc_recv(native_handle(), buf.data(), buf.size(), &len, wait.milliseconds());
        while (code == FIPC_TOO_LARGE)  // the message stays queued: make room, and take it
        {
            buf.resize(len);
            code = fipc_recv(native_handle(), buf.data(), buf.size(), &len, wait.milliseconds());
        }
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

    // Zero-copy receive: waits up to `wait` for a message and returns it in the ring (fipc_recv_acquire).
    // errc::too_large for a message in several pieces: error().len is its length, and it stays queued for receive().
    result<receive_slot> receive_acquire(fipc::timeout wait = forever) noexcept
    {
        const void* data = nullptr;
        std::size_t len = 0;
        const fipc_result_t code = fipc_recv_acquire(native_handle(), &data, &len, wait.milliseconds());
        if (code != FIPC_OK)
            return detail::error_of(code, len);
        ++state_->receive_turn;
        return receive_slot(state_.get(), data, len);
    }

    // Receives one RPC request or response, its payload into `payload`, resized to its length (fipc_rpc_recv).
    // errc::invalid for a message that isn't a well-formed RPC message, which is dropped.
    template <byte_buffer Buffer>
    result<rpc_header> rpc_receive(Buffer& payload, fipc::timeout wait = forever)
    {
        fipc_rpc_msg_t msg{};
        fipc_result_t code = fipc_rpc_recv(native_handle(), payload.data(), payload.size(), &msg, wait.milliseconds());
        while (code == FIPC_TOO_LARGE)
        {
            payload.resize(static_cast<std::size_t>(msg.len));
            code = fipc_rpc_recv(native_handle(), payload.data(), payload.size(), &msg, wait.milliseconds());
        }
        if (code != FIPC_OK)
            return detail::error_of(code, static_cast<std::size_t>(msg.len));
        payload.resize(static_cast<std::size_t>(msg.len));
        return detail::header_of(msg);
    }

    // Receives one RPC request or response with its payload in `buf` (fipc_rpc_recv). errc::too_large if the payload
    // is longer than `buf`: nothing is taken, and error().len is the room it needs.
    result<rpc_header> rpc_receive_into(std::span<std::byte> buf, fipc::timeout wait = forever) noexcept
    {
        return rpc_recv(buf.data(), buf.size(), wait);
    }

    result<rpc_header> rpc_receive_into(std::span<char> buf, fipc::timeout wait = forever) noexcept
    {
        return rpc_recv(buf.data(), buf.size(), wait);
    }

  private:
    result<std::uint64_t> submit(std::uint32_t opcode, const void* data, std::size_t len, fipc::timeout wait) noexcept
    {
        std::uint64_t id = 0;
        const fipc_result_t code = fipc_rpc_submit(native_handle(), opcode, data, len, &id, wait.milliseconds());
        if (code != FIPC_OK)
            return detail::error_of(code, len);
        return id;
    }

    result<std::size_t> recv(void* buf, std::size_t cap, fipc::timeout wait) noexcept
    {
        std::size_t len = 0;
        const fipc_result_t code = fipc_recv(native_handle(), buf, cap, &len, wait.milliseconds());
        if (code != FIPC_OK)
            return detail::error_of(code, len);
        return len;
    }

    result<rpc_header> rpc_recv(void* buf, std::size_t cap, fipc::timeout wait) noexcept
    {
        fipc_rpc_msg_t msg{};
        const fipc_result_t code = fipc_rpc_recv(native_handle(), buf, cap, &msg, wait.milliseconds());
        if (code != FIPC_OK)
            return detail::error_of(code, static_cast<std::size_t>(msg.len));
        return detail::header_of(msg);
    }

    std::shared_ptr<detail::connection_state> state_;
};

// === Listeners ===

// A server's name: it listens on it and hands over one client's connection at a time. Move-only; the destructor
// stops listening and frees the name (fipc_listener_close). The connections it accepted stay open.
class listener
{
  public:
    listener() noexcept = default;

    // Takes over a listener of the C API (from fipc_listen); nullptr makes an empty one.
    explicit listener(fipc_listener_t* handle)
        : state_(handle ? std::make_shared<detail::listener_state>(handle) : nullptr)
    {
    }

    listener(listener&&) noexcept = default;
    listener& operator=(listener&&) noexcept = default;
    listener(const listener&) = delete;
    listener& operator=(const listener&) = delete;

    // Claims `name` and listens on it, with rings of `capacity` bytes each way for every connection (fipc_listen).
    // Doesn't wait. errc::addr_in_use if another listener holds the name.
    static result<listener> listen(std::string_view name, std::size_t capacity)
    {
        char c_name[256];
        if (!detail::c_name(name, c_name))
            return fipc::error{errc::invalid};
        fipc_listener_t* handle = nullptr;
        const fipc_result_t code = fipc_listen(c_name, capacity, &handle);
        if (code != FIPC_OK)
            return detail::error_of(code, 0);
        return listener(handle);
    }

    // Waits up to `wait` for a client, sets its connection up on the calling thread and returns it (fipc_accept). A
    // call that times out in the middle of a client's setup keeps it for the next call, so fipc::no_wait polls. One
    // client at a time: errc::invalid while the connection this listener returned last is open.
    result<connection> accept(fipc::timeout wait = forever)
    {
        fipc_conn_t* handle = nullptr;
        const fipc_result_t code = fipc_accept(native_handle(), &handle, wait.milliseconds());
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

}  // namespace fipc

#endif  // FIPC_HPP
