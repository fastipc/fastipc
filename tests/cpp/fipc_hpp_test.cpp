// The tests of the C++ wrapper, include/fipc.hpp: how it maps the C API (handles that move and close once, results
// and timeouts, the zero-copy slots, cancel from another thread, the peer's end, broadcasts, work queues, services and
// a service client's call()), and, against other processes, a peer process that exits, subscriber processes and a
// Python peer (bindings/python).
//
// `fipc_hpp_test fast|slow <the test program> <the repository> [--no-interop] [filter...]` runs the tier's tests (with
// filters, only those whose name contains one). `zig build test` runs the fast tier, `zig build test-slow` the slow
// one; both build it with -fno-exceptions -fno-rtti and every warning an error. The Python peer is tests/cpp/peer.py,
// run by FIPC_TEST_PYTHON, else the repository's venv, else python; without one that has cffi the interop test says
// so and passes, unless FIPC_REQUIRE_INTEROP is set.

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS  // std::getenv
#endif

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <fipc.hpp>

#ifndef _WIN32
#include <spawn.h>

#include <sys/wait.h>

extern char** environ;
#endif

using namespace std::chrono_literals;

namespace
{

// === Checks ===

int failures = 0;

#define EXPECT(cond) expect((cond), #cond, __LINE__)

bool expect(bool ok, const char* what, int line)
{
    if (!ok)
    {
        failures++;
        std::fprintf(stderr, "  FAILED (line %d): %s\n", line, what);
    }
    return ok;
}

// The arguments: the test program itself (for the peer process) and the repository (for the Python peer).
std::string self_path;
std::string repository;
bool interop = true;

// A name no other test (or run) uses.
std::string unique_name(const char* prefix)
{
    static int counter = 0;
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    return "fipc_hpp_" + std::string(prefix) + "_" + std::to_string(nanos % 1000000000000) + "_"
         + std::to_string(counter++);
}

// `len` bytes of a pattern that differs at every offset of a ring's frame.
std::vector<std::byte> pattern(std::size_t len)
{
    std::vector<std::byte> bytes(len);
    for (std::size_t i = 0; i < len; i++)
        bytes[i] = static_cast<std::byte>((i * 31 + 7) & 0xFF);
    return bytes;
}

std::string text(std::span<const std::byte> bytes)
{
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

// A listener and a server and client connection on one name.
struct pair
{
    fipc::listener listener;
    fipc::connection client;
    fipc::connection server;
};

// Connects to `name` on a thread of its own while the caller accepts: stream_connect waits for the listener's accept,
// so a client and its server in one process run on different threads.
struct connector
{
    std::optional<fipc::result<fipc::connection>> result;
    std::thread thread;

    explicit connector(const std::string& name)
        : thread([this, name] { result.emplace(fipc::stream_connect(name, 10s)); })
    {
    }

    fipc::result<fipc::connection> join()
    {
        thread.join();
        return std::move(*result);
    }
};

bool open_pair(pair& p, const char* prefix, std::size_t capacity = 1 << 16)
{
    const std::string name = unique_name(prefix);
    auto listener = fipc::stream_create(name, capacity);
    if (!EXPECT(listener.has_value()))
        return false;
    connector connecting(name);
    auto server = listener->accept(10s);
    auto client = connecting.join();
    if (!EXPECT(client.has_value()))
        return false;
    if (!EXPECT(server.has_value()))
        return false;
    p.listener = std::move(*listener);
    p.client = std::move(*client);
    p.server = std::move(*server);
    return true;
}

// Runs `command` through the shell on a thread of its own; join() returns its exit status. Not std::system on POSIX
// systems: macOS's holds a lock until the command ends, so the processes of several would run one after another.
class process
{
  public:
    explicit process(std::string command)
    {
#ifdef _WIN32
        command = "\"" + command + "\"";  // cmd.exe strips the outer quotes
        thread_ = std::thread([this, command] { status_ = std::system(command.c_str()); });
#else
        thread_ = std::thread([this, command] { status_ = shell(command); });
#endif
    }

    process(const process&) = delete;
    process& operator=(const process&) = delete;

    int join()
    {
        thread_.join();
        return status_;
    }

  private:
#ifndef _WIN32
    // `sh -c command`, as std::system runs it: the wait status, or -1 if it didn't start
    static int shell(const std::string& command)
    {
        char sh[] = "sh", c[] = "-c";
        char* argv[] = {sh, c, const_cast<char*>(command.c_str()), nullptr};
        pid_t pid = 0;
        if (posix_spawn(&pid, "/bin/sh", nullptr, nullptr, argv, environ) != 0)
            return -1;
        int status = 0;
        while (waitpid(pid, &status, 0) == -1)
            if (errno != EINTR)
                return -1;
        return status;
    }
#endif

    std::thread thread_;
    int status_ = -1;
};

std::string in_quotes(const std::string& s)
{
    return "\"" + s + "\"";
}

// === Fast: in one process ===

static_assert(fipc::forever.milliseconds() == -1);
static_assert(fipc::no_wait.milliseconds() == 0);
static_assert(fipc::timeout(0ms).milliseconds() == 0);
static_assert(fipc::timeout(-5s).milliseconds() == 0);
static_assert(fipc::timeout(1ns).milliseconds() == 1);  // a partial millisecond waits a whole one
static_assert(fipc::timeout(1500us).milliseconds() == 2);
static_assert(fipc::timeout(5s).milliseconds() == 5000);
static_assert(fipc::timeout(std::chrono::duration<double>(0.25)).milliseconds() == 250);
static_assert(fipc::timeout(2147483646ms).milliseconds() == 2147483646);
static_assert(fipc::timeout(2147483647ms).milliseconds() == -1);  // from INT_MAX on: for ever
static_assert(fipc::timeout(std::chrono::hours::max()).milliseconds() == -1);

void results_and_errors()
{
    fipc::result<int> ok = 7;
    EXPECT(ok && ok.has_value() && *ok == 7 && ok.value() == 7);
    fipc::result<int> failed = fipc::error{fipc::errc::timeout};
    EXPECT(!failed && failed.error() == fipc::errc::timeout && failed.value_or(3) == 3);
    EXPECT(std::strcmp(failed.error().name(), "FIPC_TIMEOUT") == 0);
    EXPECT(std::strcmp(fipc::error{fipc::errc::addr_in_use}.name(), "FIPC_ADDR_IN_USE") == 0);
    fipc::status done;
    EXPECT(done.has_value());

    // The library's argument checks come back as results
    auto bad_capacity = fipc::stream_create(unique_name("capacity"), 1000);
    EXPECT(!bad_capacity && bad_capacity.error() == fipc::errc::invalid);
    EXPECT(fipc::stream_create("bad name", 1 << 16).error() == fipc::errc::invalid);
    EXPECT(fipc::stream_create(std::string_view("nul\0name", 8), 1 << 16).error() == fipc::errc::invalid);
    EXPECT(fipc::stream_create(std::string(300, 'a'), 1 << 16).error() == fipc::errc::invalid);
    const std::string name = unique_name("in_use");
    auto first = fipc::stream_create(name, 1 << 16);
    EXPECT(first.has_value());
    EXPECT(fipc::stream_create(name, 1 << 16).error() == fipc::errc::addr_in_use);
}

void handles_move_and_close_once()
{
    const std::string name = unique_name("moves");
    auto listened = fipc::stream_create(name, 1 << 16);
    if (!EXPECT(listened.has_value()))
        return;
    fipc::listener listener = std::move(*listened);
    EXPECT(listener && !*listened && listened->native_handle() == nullptr);
    EXPECT(listened->accept(fipc::no_wait).error() == fipc::errc::invalid);  // an empty listener

    connector connecting(name);
    auto accepted = listener.accept(10s);
    auto connected = connecting.join();
    if (!EXPECT(connected.has_value()))
        return;
    fipc::connection client = std::move(*connected);
    fipc::connection moved_from;
    EXPECT(!moved_from && moved_from.max_piece() == 0);
    EXPECT(moved_from.send("x").error() == fipc::errc::invalid);  // an empty connection
    if (!EXPECT(accepted.has_value()))
        return;
    fipc::connection server = std::move(*accepted);
    EXPECT(server.max_piece() == (1 << 16) - 64 && client.max_piece() == server.max_piece());

    // Move construction and assignment carry the handle; the source is empty, and nothing closes twice
    fipc::connection server2(std::move(server));
    EXPECT(!server && server2 && server.send("x").error() == fipc::errc::invalid);
    server = std::move(server2);
    EXPECT(server && !server2);
    EXPECT(client.send("hello").has_value());
    std::string message;
    EXPECT(server.receive(message, 10s).value_or(0) == 5 && message == "hello");

    // One client at a time: the listener hands over the next only once this one is closed
    EXPECT(listener.accept(fipc::no_wait).error() == fipc::errc::invalid);
    // Assigning over a connection closes the one it held: the peer sees its end
    server = fipc::connection();
    EXPECT(client.receive(message, 10s).error() == fipc::errc::disconnected);
    client.close();
    EXPECT(!client && client.send("x").error() == fipc::errc::invalid);

    // The listener's close frees the name at once
    listener.close();
    auto again = fipc::stream_create(name, 1 << 16);
    EXPECT(again.has_value());
    // The C API's handles can be adopted
    fipc_listener_t* raw = nullptr;
    if (EXPECT(fipc_stream_create(unique_name("adopt").c_str(), 1 << 16, &raw) == FIPC_OK))
    {
        fipc::listener adopted(raw);
        EXPECT(adopted.native_handle() == raw);
    }
}

void messages()
{
    pair p;
    if (!open_pair(p, "messages"))
        return;
    // Text and bytes, received into a growing buffer and into a fixed one
    EXPECT(p.client.send("ping").has_value());
    const auto bytes = pattern(300);
    EXPECT(p.client.send(bytes).has_value());
    std::string message;
    EXPECT(p.server.receive(message).value_or(0) == 4 && message == "ping");
    std::vector<std::byte> grown;
    EXPECT(p.server.receive(grown).value_or(0) == 300 && grown == bytes);

    // A buffer too small: nothing is taken, and the error says how much room the message needs
    EXPECT(p.client.send(bytes).has_value());
    char small[100];
    auto too_large = p.server.receive_into(small, 10s);
    EXPECT(!too_large && too_large.error() == fipc::errc::too_large && too_large.error().len == 300);
    std::vector<std::byte> fixed(300);
    EXPECT(p.server.receive_into(fixed, 10s).value_or(0) == 300 && fixed == bytes);

    // A message longer than the ring travels in pieces and arrives whole (sent while it is received)
    const auto big = pattern(200000);
    std::thread sender([&] { EXPECT(p.client.send(big, 10s).has_value()); });
    std::vector<std::byte> received;
    EXPECT(p.server.receive(received, 10s).value_or(0) == big.size() && received == big);
    sender.join();

    // Timeouts: nothing to receive
    EXPECT(p.server.receive(message, fipc::no_wait).error() == fipc::errc::timeout);
    const auto start = std::chrono::steady_clock::now();
    EXPECT(p.server.receive_into(small, 30ms).error() == fipc::errc::timeout);
    EXPECT(std::chrono::steady_clock::now() - start >= 25ms);
    EXPECT(fipc::stream_connect(unique_name("nobody"), fipc::no_wait).error() == fipc::errc::timeout);
    // An empty message is not a message
    EXPECT(p.client.send("").error() == fipc::errc::invalid);
}

void zero_copy_slots()
{
    pair p;
    if (!open_pair(p, "zero_copy"))
        return;
    // Write in the ring, commit part of it
    {
        auto slot = p.client.send_acquire(64);
        if (EXPECT(slot.has_value() && slot->size() == 64 && slot->bytes().size() == 64))
        {
            EXPECT(reinterpret_cast<std::uintptr_t>(slot->data()) % 16 == 0);
            std::memcpy(slot->data(), "hello", 5);
            EXPECT(slot->commit(0).error() == fipc::errc::invalid);  // out of range: the slot stays
            EXPECT(slot->commit(65).error() == fipc::errc::invalid);
            EXPECT(slot->commit(5).has_value());
            EXPECT(slot->commit(5).error() == fipc::errc::invalid);  // committed already
            EXPECT(slot->data() == nullptr && slot->size() == 0);
        }
    }
    {
        auto message = p.server.receive_acquire(10s);
        if (EXPECT(message.has_value()))
            EXPECT(text(message->bytes()) == "hello" && message->size() == 5);
    }  // released here

    // A slot destroyed without its commit sends nothing; the next send works
    {
        auto abandoned = p.client.send_acquire(32);
        EXPECT(abandoned.has_value());
    }
    std::string message;
    EXPECT(p.server.receive(message, fipc::no_wait).error() == fipc::errc::timeout);
    EXPECT(p.client.send("after").has_value());
    EXPECT(p.server.receive(message, 10s).has_value() && message == "after");

    // A slot whose reservation a later acquire replaced commits nothing; the later one commits its own
    {
        auto first = p.client.send_acquire(16);
        auto second = p.client.send_acquire(16);
        if (EXPECT(first.has_value() && second.has_value()))
        {
            std::memcpy(second->data(), "second", 6);
            EXPECT(first->commit(6).error() == fipc::errc::invalid);
            EXPECT(second->commit(6).has_value());
        }
    }
    {
        // Moved slots keep their turn; a receive slot outlived by a newer one releases nothing of the newer one's
        EXPECT(p.client.send("third").has_value());
        auto first = p.server.receive_acquire(10s);
        EXPECT(first.has_value() && text(first->bytes()) == "second");
        auto second = p.server.receive_acquire(10s);  // releases the first message
        if (EXPECT(first.has_value() && second.has_value()))
        {
            EXPECT(text(second->bytes()) == "third");
            fipc::receive_slot moved = std::move(*second);
            first->release();
            EXPECT(text(moved.bytes()) == "third" && second->data() == nullptr);
        }
    }

    // Longer than one piece: too large for zero-copy, both ways
    EXPECT(p.client.send_acquire(p.client.max_piece() + 1).error() == fipc::errc::too_large);
    const auto big = pattern(p.client.max_piece() + 100);
    std::thread sender([&] { EXPECT(p.client.send(big, 10s).has_value()); });
    auto pieces = p.server.receive_acquire(10s);
    EXPECT(!pieces && pieces.error() == fipc::errc::too_large && pieces.error().len == big.size());
    std::vector<std::byte> received;
    EXPECT(p.server.receive(received, 10s).value_or(0) == big.size() && received == big);
    sender.join();
}

void cancel_from_another_thread()
{
    // A listener: the thread that waits in accept returns cancelled
    const std::string name = unique_name("cancel");
    auto listener = fipc::stream_create(name, 1 << 16);
    if (!EXPECT(listener.has_value()))
        return;
    fipc::canceler accept_canceler = listener->get_canceler();
    fipc::errc accepted{};
    std::thread acceptor([&] { accepted = listener->accept().error().code; });
    std::this_thread::sleep_for(20ms);
    accept_canceler.cancel();
    acceptor.join();
    EXPECT(accepted == fipc::errc::cancelled);
    listener->close();
    accept_canceler.cancel();  // the listener is closed: nothing to do

    // A connection: the thread that waits in a receive returns cancelled; calls that needn't wait still work
    pair p;
    if (!open_pair(p, "cancel_conn"))
        return;
    fipc::canceler canceler = p.server.get_canceler();
    fipc::canceler copy = canceler;
    fipc::errc received{};
    std::thread receiver(
        [&]
        {
            std::string message;
            received = p.server.receive(message).error().code;
        }
    );
    std::this_thread::sleep_for(20ms);
    copy.cancel();
    receiver.join();
    EXPECT(received == fipc::errc::cancelled);
    EXPECT(p.server.send("still works", fipc::no_wait).has_value());
    std::string message;
    EXPECT(p.client.receive(message, 10s).has_value() && message == "still works");
    EXPECT(p.server.receive(message, 10s).error() == fipc::errc::cancelled);

    // A canceler outlives its connection; cancelling then does nothing
    p.server.close();
    canceler.cancel();
    fipc::canceler empty;
    empty.cancel();
}

void peer_end()
{
    pair p;
    if (!open_pair(p, "peer_end"))
        return;
    EXPECT(p.client.send("one").has_value() && p.client.send("two").has_value());
    p.client.close();
    // The messages the peer completed first, then its end; sends see it at once
    std::string message;
    EXPECT(p.server.receive(message, 10s).has_value() && message == "one");
    EXPECT(p.server.receive(message, 10s).has_value() && message == "two");
    EXPECT(p.server.receive(message, 10s).error() == fipc::errc::disconnected);
    EXPECT(p.server.send("late").error() == fipc::errc::disconnected);
}

// === Fast: broadcasts ===

// A subscriber of the broadcast on `name`, joined as `r`; an empty connection if it didn't join. The publisher's
// process admits it on a library thread, so the thread that created the broadcast may join it too.
fipc::connection subscribe(const std::string& name, fipc::subscriber_role r)
{
    auto sub = fipc::broadcast_join(name, r, 10s);
    if (!EXPECT(sub.has_value()))
        return {};
    return std::move(*sub);
}

// A broadcast on a name of its own (in `name`): its publisher, or an empty connection.
fipc::connection open_broadcast(
    std::string& name, const char* prefix, std::size_t capacity, std::uint32_t max_subscribers, fipc::timeout max_stall
)
{
    name = unique_name(prefix);
    auto pub = fipc::broadcast_create(name, capacity, max_subscribers, max_stall);
    if (!EXPECT(pub.has_value()))
        return {};
    return std::move(*pub);
}

void broadcast_roles()
{
    // broadcast_create's argument checks, and its name against any stream, broadcast or queue
    EXPECT(fipc::broadcast_create(unique_name("bc_capacity"), 1000, 1, fipc::forever).error() == fipc::errc::invalid);
    EXPECT(fipc::broadcast_create(unique_name("bc_subs"), 1 << 16, 0, fipc::forever).error() == fipc::errc::invalid);
    EXPECT(fipc::broadcast_create(unique_name("bc_subs"), 1 << 16, 1025, fipc::forever).error() == fipc::errc::invalid);
    EXPECT(fipc::broadcast_create("bad name", 1 << 16, 1, fipc::forever).error() == fipc::errc::invalid);
    std::string name;
    fipc::connection pub = open_broadcast(name, "bc_roles", 1 << 16, 4, fipc::forever);
    if (!pub)
        return;
    EXPECT(fipc::broadcast_create(name, 1 << 16, 1, fipc::forever).error() == fipc::errc::addr_in_use);
    EXPECT(fipc::stream_create(name, 1 << 16).error() == fipc::errc::addr_in_use);
    EXPECT(fipc::queue_create(name, 4096, 1, 1).error() == fipc::errc::addr_in_use);
    // A stream's client or a queue's member of a broadcast, or a subscriber of a stream, fails at once
    EXPECT(fipc::stream_connect(name, 5s).error() == fipc::errc::invalid);
    EXPECT(fipc::queue_join(name, fipc::queue_role::producer, 5s).error() == fipc::errc::invalid);
    const std::string server_name = unique_name("bc_server");
    auto server = fipc::stream_create(server_name, 1 << 16);
    if (!EXPECT(server.has_value()))
        return;
    fipc::errc joined{};
    std::thread subscriber(
        [&] { joined = fipc::broadcast_join(server_name, fipc::subscriber_role::lossy, 5s).error().code; }
    );
    EXPECT(server->accept(1s).error() == fipc::errc::timeout);
    subscriber.join();
    EXPECT(joined == fipc::errc::invalid);
    EXPECT(
        fipc::broadcast_join(unique_name("bc_nobody"), fipc::subscriber_role::lossy, fipc::no_wait).error()
        == fipc::errc::timeout
    );
    EXPECT(fipc::broadcast_join(name, static_cast<fipc::subscriber_role>(5), 5s).error() == fipc::errc::invalid);
    EXPECT(
        fipc::broadcast_join(name, static_cast<fipc::subscriber_role>(FIPC_PRODUCER), 5s).error() == fipc::errc::invalid
    );

    fipc::connection lossy = subscribe(name, fipc::subscriber_role::lossy);
    fipc::connection gated = subscribe(name, fipc::subscriber_role::gated);
    if (!lossy || !gated)
        return;
    // One piece: a quarter of the capacity, on the publisher and on every subscriber
    EXPECT(pub.max_piece() == (1 << 16) / 4);
    EXPECT(lossy.max_piece() == (1 << 16) / 4 && gated.max_piece() == (1 << 16) / 4);

    // Every message goes to every subscriber: copied into a growing buffer or a fixed one, or in place (gated)
    const auto bytes = pattern(300);
    EXPECT(pub.send("one").has_value());
    EXPECT(pub.send(bytes).has_value());
    {
        auto slot = pub.send_acquire(5);
        if (EXPECT(slot.has_value()))
        {
            std::memcpy(slot->data(), "three", 5);
            EXPECT(slot->commit(5).has_value());
        }
    }
    std::string message;
    EXPECT(lossy.receive(message, 10s).value_or(0) == 3 && message == "one");
    std::vector<std::byte> grown;
    EXPECT(lossy.receive(grown, 10s).value_or(0) == 300 && grown == bytes);
    char small[100];
    EXPECT(lossy.receive_into(small, 10s).value_or(0) == 5 && std::string_view(small, 5) == "three");
    EXPECT(gated.receive(message, 10s).has_value() && message == "one");
    auto too_large = gated.receive_into(small, 10s);  // the message stays
    EXPECT(!too_large && too_large.error() == fipc::errc::too_large && too_large.error().len == 300);
    {
        auto held = gated.receive_acquire(10s);
        EXPECT(held.has_value() && held->size() == 300 && std::memcmp(held->data(), bytes.data(), 300) == 0);
    }
    {
        auto held = gated.receive_acquire(10s);
        EXPECT(held.has_value() && text(held->bytes()) == "three");
    }

    // What a role can't use is errc::invalid
    EXPECT(pub.receive(message, fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(pub.receive_acquire(fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(pub.send(std::string(pub.max_piece() + 1, 'x')).error() == fipc::errc::too_large);
    EXPECT(pub.send("").error() == fipc::errc::invalid);
    EXPECT(lossy.receive_acquire(fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(lossy.send("x").error() == fipc::errc::invalid);
    EXPECT(gated.send("x").error() == fipc::errc::invalid);
    EXPECT(gated.send_acquire(4).error() == fipc::errc::invalid);
    EXPECT(lossy.receive(message, fipc::no_wait).error() == fipc::errc::timeout);
    EXPECT(gated.receive(message, fipc::no_wait).error() == fipc::errc::timeout);

    // A subscriber's waiting receive ends with cancel, from another thread
    fipc::canceler canceler = gated.get_canceler();
    fipc::errc received{};
    std::thread receiver([&] { received = gated.receive_acquire().error().code; });
    std::this_thread::sleep_for(20ms);
    canceler.cancel();
    receiver.join();
    EXPECT(received == fipc::errc::cancelled);
    // A subscriber's close leaves the broadcast; the publisher goes on
    gated.close();
    lossy.close();
    EXPECT(pub.send("on", fipc::no_wait).has_value());
}

// A lossy subscriber that falls a lap behind: errc::overrun once, then a recent message, on to the last.
void broadcast_overrun()
{
    std::string name;
    fipc::connection pub = open_broadcast(name, "bc_overrun", 4096, 1, fipc::forever);
    if (!pub)
        return;
    fipc::connection sub = subscribe(name, fipc::subscriber_role::lossy);
    if (!sub)
        return;
    const int count = 3 * 64;  // three laps of the 64 cells
    for (int i = 0; i < count; i++)
        EXPECT(pub.send(std::to_string(i), fipc::no_wait).has_value());
    std::string message;
    auto lapped = sub.receive(message, fipc::no_wait);
    EXPECT(!lapped && lapped.error() == fipc::errc::overrun);
    EXPECT(std::strcmp(fipc::error{fipc::errc::overrun}.name(), "FIPC_OVERRUN") == 0);
    int first = -1;
    int last = -1;
    while (sub.receive(message, fipc::no_wait))
    {
        const int seq = std::atoi(message.c_str());
        if (first >= 0)
            EXPECT(seq == last + 1);
        else
            first = seq;
        last = seq;
    }
    EXPECT(first >= count - 64 && last == count - 1);
    EXPECT(sub.receive(message, fipc::no_wait).error() == fipc::errc::timeout);
}

// errc::overrun's error().len is the number of messages lost, through receive and receive_into. Messages of up to 64
// bytes take 64 bytes, and the publisher marks every fourth as a place to go on from (docs/protocol.md §9.6).
void broadcast_overrun_lost()
{
    std::string name;
    fipc::connection pub = open_broadcast(name, "bc_lost", 4096, 1, fipc::forever);
    if (!pub)
        return;
    fipc::connection sub = subscribe(name, fipc::subscriber_role::lossy);
    if (!sub)
        return;
    for (int i = 0; i < 144; i++)  // two laps of the 64 cells and then some: it goes on from 140
        EXPECT(pub.send(std::to_string(i), fipc::no_wait).has_value());
    std::string message;
    auto lapped = sub.receive(message, fipc::no_wait);
    EXPECT(!lapped && lapped.error() == fipc::errc::overrun && lapped.error().len == 140);
    EXPECT(sub.receive(message, fipc::no_wait) && message == "140");
    for (int i = 141; i < 144; i++)
        EXPECT(sub.receive(message, fipc::no_wait) && message == std::to_string(i));
    for (int i = 144; i < 284; i++)  // it goes on from 280
        EXPECT(pub.send(std::to_string(i), fipc::no_wait).has_value());
    char buf[64];
    auto lapped_into = sub.receive_into(std::span<char>(buf), fipc::no_wait);
    EXPECT(!lapped_into && lapped_into.error() == fipc::errc::overrun && lapped_into.error().len == 280 - 144);
    auto next = sub.receive_into(std::span<char>(buf), fipc::no_wait);
    EXPECT(next && std::string(buf, *next) == "280");
}

// The publisher's count of joined subscribers (wait_for_members): at once, a wait that a join on another thread ends,
// the timeout and the cancel, whose error().len is the count, and errc::invalid on a subscriber, on an empty
// connection or over max_subscribers.
void broadcast_members()
{
    std::string name;
    fipc::connection pub = open_broadcast(name, "bc_count", 4096, 2, fipc::forever);
    if (!pub)
        return;
    EXPECT(pub.wait_for_members().value_or(99) == 0);
    EXPECT(pub.wait_for_members(3, fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(fipc::connection().wait_for_members(0, fipc::no_wait).error() == fipc::errc::invalid);
    fipc::result<std::uint32_t> waited = fipc::error{fipc::errc::invalid};
    std::thread waiter([&] { waited = pub.wait_for_members(1, 10s); });
    fipc::connection first = subscribe(name, fipc::subscriber_role::lossy);
    waiter.join();
    EXPECT(waited.value_or(99) == 1);
    EXPECT(first.wait_for_members(0, fipc::no_wait).error() == fipc::errc::invalid);
    auto timed_out = pub.wait_for_members(2, 50ms);
    EXPECT(!timed_out && timed_out.error() == fipc::errc::timeout && timed_out.error().len == 1);
    fipc::canceler canceler = pub.get_canceler();
    std::thread canceled([&] { waited = pub.wait_for_members(2); });
    std::this_thread::sleep_for(20ms);
    canceler.cancel();
    canceled.join();
    EXPECT(!waited && waited.error() == fipc::errc::cancelled && waited.error().len == 1);
    EXPECT(pub.wait_for_members(1).value_or(99) == 1);
}

// A gated subscriber that holds the publisher back for max_stall is unsubscribed: errc::unsubscribed from then on, and
// the publisher goes on. fipc::no_wait refuses gated subscribers; with fipc::forever the publisher's send times out.
void broadcast_unsubscribed()
{
    std::string name;
    fipc::connection pub = open_broadcast(name, "bc_unsub", 4096, 2, 200ms);
    if (!pub)
        return;
    fipc::connection held = subscribe(name, fipc::subscriber_role::gated);
    if (!held)
        return;
    for (int i = 0; i < 64; i++)  // the 64 cells, never received
        EXPECT(pub.send(std::to_string(i), fipc::no_wait).has_value());
    EXPECT(pub.send("waits", fipc::no_wait).error() == fipc::errc::timeout);
    const auto start = std::chrono::steady_clock::now();
    EXPECT(pub.send("waits", 10s).has_value());
    EXPECT(std::chrono::steady_clock::now() - start >= 100ms);
    std::string message;
    auto dropped = held.receive(message, 10s);
    EXPECT(!dropped && dropped.error() == fipc::errc::unsubscribed);
    EXPECT(std::strcmp(dropped.error().name(), "FIPC_UNSUBSCRIBED") == 0);
    EXPECT(held.receive_acquire(fipc::no_wait).error() == fipc::errc::unsubscribed);
    for (int i = 0; i < 200; i++)
        EXPECT(pub.send(std::to_string(i), fipc::no_wait).has_value());
    held.close();

    // no_wait: gated subscribers are refused, lossy ones join
    fipc::connection lossy_only = open_broadcast(name, "bc_no_gated", 4096, 2, fipc::no_wait);
    if (!lossy_only)
        return;
    auto gated_join = fipc::broadcast_join(name, fipc::subscriber_role::gated, 5s);
    EXPECT(!gated_join && gated_join.error() == fipc::errc::invalid);
    EXPECT(static_cast<bool>(subscribe(name, fipc::subscriber_role::lossy)));

    // forever: the publisher's send times out, and the subscriber stays
    fipc::connection patient = open_broadcast(name, "bc_forever", 4096, 1, fipc::forever);
    if (!patient)
        return;
    fipc::connection slow = subscribe(name, fipc::subscriber_role::gated);
    if (!slow)
        return;
    for (int i = 0; i < 64; i++)
        EXPECT(patient.send(std::to_string(i), fipc::no_wait).has_value());
    EXPECT(patient.send("waits", 300ms).error() == fipc::errc::timeout);
    EXPECT(slow.receive(message, fipc::no_wait).has_value() && message == "0");
    EXPECT(patient.send("64", fipc::no_wait).has_value());
}

// max_stall is a fipc::timeout as any wait: fipc::forever, and a duration from INT_MAX milliseconds on, never
// unsubscribes; fipc::no_wait, and a duration of zero or less, refuses gated subscribers.
void broadcast_max_stall()
{
    static_assert(fipc::timeout(std::chrono::hours(24 * 30)).milliseconds() == FIPC_FOREVER);
    static_assert(fipc::timeout(-5ms) == fipc::no_wait);
    std::string name;
    for (const fipc::timeout refusing : {fipc::no_wait, fipc::timeout(-5ms)})
    {
        fipc::connection pub = open_broadcast(name, "bc_stall_none", 4096, 1, refusing);
        if (!pub)
            return;
        auto gated_join = fipc::broadcast_join(name, fipc::subscriber_role::gated, 5s);
        EXPECT(!gated_join && gated_join.error() == fipc::errc::invalid);
    }
    // A month is never, as fipc::forever: the publisher's send times out, and the subscriber stays
    fipc::connection pub = open_broadcast(name, "bc_stall_never", 4096, 1, std::chrono::hours(24 * 30));
    if (!pub)
        return;
    fipc::connection slow = subscribe(name, fipc::subscriber_role::gated);
    if (!slow)
        return;
    for (int i = 0; i < 64; i++)
        EXPECT(pub.send(std::to_string(i), fipc::no_wait).has_value());
    EXPECT(pub.send("waits", 300ms).error() == fipc::errc::timeout);
    std::string message;
    EXPECT(slow.receive(message, fipc::no_wait).has_value() && message == "0");
    EXPECT(pub.send("64", fipc::no_wait).has_value());
}

// The publisher's close ends the broadcast: subscribers receive what it sent, then errc::disconnected; the name is free
// at once.
void broadcast_publisher_close()
{
    std::string name;
    fipc::connection pub = open_broadcast(name, "bc_close", 1 << 16, 2, fipc::forever);
    if (!pub)
        return;
    fipc::connection lossy = subscribe(name, fipc::subscriber_role::lossy);
    fipc::connection gated = subscribe(name, fipc::subscriber_role::gated);
    if (!lossy || !gated)
        return;
    EXPECT(pub.send("one").has_value() && pub.send("two").has_value());
    // A receive that waits when the publisher closes: the messages, then the end
    std::string waited;
    fipc::errc end{};
    std::thread receiver(
        [&]
        {
            std::string message;
            for (const char* expected : {"one", "two", "three"})
                if (lossy.receive(message, 10s).has_value() && message == expected)
                    waited += message;
            end = lossy.receive(message, 10s).error().code;
        }
    );
    std::this_thread::sleep_for(20ms);
    EXPECT(pub.send("three").has_value());
    pub.close();
    receiver.join();
    EXPECT(waited == "onetwothree" && end == fipc::errc::disconnected);
    std::string message;
    EXPECT(gated.receive(message, 10s).has_value() && message == "one");
    {
        auto held = gated.receive_acquire(10s);
        EXPECT(held.has_value() && text(held->bytes()) == "two");
    }
    EXPECT(gated.receive(message, 10s).has_value() && message == "three");
    EXPECT(gated.receive(message, 10s).error() == fipc::errc::disconnected);
    EXPECT(gated.receive_acquire(10s).error() == fipc::errc::disconnected);
    EXPECT(pub.send("late").error() == fipc::errc::invalid);  // closed: an empty connection
    auto again = fipc::broadcast_create(name, 1 << 16, 2, fipc::forever);
    EXPECT(again.has_value());
}

// === Work queues ===

// A member of the queue on `name`, joined as `r`; an empty connection if it didn't join. The owner's process admits it
// on a library thread, so the thread that created the queue may join it too.
fipc::connection enlist(const std::string& name, fipc::queue_role r)
{
    auto member = fipc::queue_join(name, r, 10s);
    if (!EXPECT(member.has_value()))
        return {};
    return std::move(*member);
}

// queue_create's checks, the owner's own member (joined on the thread that created the queue), joined producers and
// consumers, each role's calls, and the end at the queue's close.
void queue_roles()
{
    const std::string name = unique_name("mq_hpp");
    EXPECT(fipc::queue_create(name, 2048, 1, 1).error() == fipc::errc::invalid);
    EXPECT(fipc::queue_create(name, 4096, 0, 1).error() == fipc::errc::invalid);
    EXPECT(fipc::queue_create(name, 4096, 1, 65).error() == fipc::errc::invalid);
    EXPECT(fipc::queue_create("bad name", 4096, 1, 1).error() == fipc::errc::invalid);
    {
        auto first = fipc::queue_create(name, 4096, 1, 1);
        EXPECT(first.has_value() && *first && first->native_handle() != nullptr);
        EXPECT(fipc::queue_create(name, 4096, 1, 1).error() == fipc::errc::addr_in_use);
        EXPECT(fipc::stream_create(name, 1 << 16).error() == fipc::errc::addr_in_use);
        EXPECT(fipc::broadcast_create(name, 4096, 1, fipc::forever).error() == fipc::errc::addr_in_use);
    }
    auto owner = fipc::queue_create(name, 1 << 16, 2, 2);
    if (!EXPECT(owner.has_value() && *owner))
        return;
    fipc::connection own = enlist(name, fipc::queue_role::producer);
    if (!own)
        return;
    EXPECT(own.max_piece() == (1 << 16) / 4);
    // A role of another kind, a subscriber, and a stream's client of a queue fail at once
    EXPECT(
        fipc::queue_join(name, static_cast<fipc::queue_role>(FIPC_SUBSCRIBER_LOSSY), 5s).error() == fipc::errc::invalid
    );
    EXPECT(fipc::broadcast_join(name, fipc::subscriber_role::lossy, 5s).error() == fipc::errc::invalid);
    EXPECT(fipc::stream_connect(name, 5s).error() == fipc::errc::invalid);
    EXPECT(
        fipc::queue_join(unique_name("mq_nobody"), fipc::queue_role::consumer, fipc::no_wait).error()
        == fipc::errc::timeout
    );
    fipc::connection consumer = enlist(name, fipc::queue_role::consumer);
    fipc::connection producer = enlist(name, fipc::queue_role::producer);
    if (!consumer || !producer)
        return;

    // Each message once: the own producer's and a joined one's, copied or in place
    EXPECT(own.send("job 1").has_value());
    EXPECT(producer.send("job 2").has_value());
    std::string first;
    std::string second;
    EXPECT(consumer.receive(first, 10s).has_value());
    {
        auto held = consumer.receive_acquire(10s);
        if (EXPECT(held.has_value()))
            second = text(held->bytes());
    }
    EXPECT((first == "job 1" && second == "job 2") || (first == "job 2" && second == "job 1"));
    {
        auto slot = producer.send_acquire(5);
        if (EXPECT(slot.has_value()))
        {
            std::memcpy(slot->data(), "job 3", 5);
            EXPECT(slot->commit().has_value());
        }
    }
    std::string message;
    EXPECT(consumer.receive(message, 10s).has_value() && message == "job 3");
    EXPECT(consumer.receive(message, fipc::no_wait).error() == fipc::errc::timeout);

    // What each role can't use
    EXPECT(consumer.send("x").error() == fipc::errc::invalid);
    EXPECT(producer.receive(message, fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(own.receive(message, fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(producer.wait_for_members().error() == fipc::errc::invalid);

    // The queue's close ends it: producers get errc::disconnected, the consumer what is left, then the same
    EXPECT(producer.send("job 4").has_value());
    owner->close();
    EXPECT(!*owner);
    EXPECT(own.send("late").error() == fipc::errc::disconnected);
    EXPECT(consumer.receive(message, 10s).has_value() && message == "job 4");
    EXPECT(consumer.receive(message, 10s).error() == fipc::errc::disconnected);
    fipc::errc late{};
    for (int i = 0; i < 500; i++)
    {
        late = producer.send("late", fipc::no_wait).error().code;
        if (late == fipc::errc::disconnected)
            break;
        std::this_thread::sleep_for(10ms);
    }
    EXPECT(late == fipc::errc::disconnected);
}

// The owner's count of joined members (queue::wait_for_members), by role and of both: at once, a wait that a join on
// another thread ends, the timeout, errc::invalid over a role's maximum, for another role or on an empty queue; then
// cancel, from a canceler and from the queue itself, whose error().len is the count, and a canceler that outlives its
// queue.
void queue_members()
{
    const std::string name = unique_name("mq_count");
    auto created = fipc::queue_create(name, 4096, 2, 1);
    if (!EXPECT(created.has_value()))
        return;
    fipc::queue owner = std::move(*created);
    EXPECT(owner && !*created && created->native_handle() == nullptr);
    EXPECT(created->wait_for_members(0, fipc::no_wait).error() == fipc::errc::invalid);  // an empty queue
    EXPECT(owner.wait_for_members().value_or(99) == 0);
    EXPECT(owner.wait_for_members(fipc::queue_role::consumer).value_or(99) == 0);
    EXPECT(owner.wait_for_members(fipc::queue_role::producer, 3, fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(owner.wait_for_members(fipc::queue_role::consumer, 2, fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(owner.wait_for_members(4, fipc::no_wait).error() == fipc::errc::invalid);  // over both maxima together
    EXPECT(
        owner.wait_for_members(static_cast<fipc::queue_role>(FIPC_SUBSCRIBER_GATED), 0, fipc::no_wait).error()
        == fipc::errc::invalid
    );

    fipc::result<std::uint32_t> waited = fipc::error{fipc::errc::invalid};
    std::thread waiter([&] { waited = owner.wait_for_members(fipc::queue_role::consumer, 1, 10s); });
    fipc::connection consumer = enlist(name, fipc::queue_role::consumer);
    waiter.join();
    EXPECT(waited.value_or(99) == 1);
    auto timed_out = owner.wait_for_members(fipc::queue_role::producer, 1, 50ms);
    EXPECT(!timed_out && timed_out.error() == fipc::errc::timeout && timed_out.error().len == 0);
    fipc::connection producer = enlist(name, fipc::queue_role::producer);
    if (!consumer || !producer)
        return;
    EXPECT(owner.wait_for_members(fipc::queue_role::producer, 1, 10s).value_or(99) == 1);
    EXPECT(owner.wait_for_members(2, 10s).value_or(99) == 2);

    // A canceler's cancel from another thread ends a waiting wait_for_members; it is final
    fipc::canceler canceler = owner.get_canceler();
    std::thread canceled([&] { waited = owner.wait_for_members(fipc::queue_role::producer, 2); });
    std::this_thread::sleep_for(20ms);
    canceler.cancel();
    canceled.join();
    EXPECT(!waited && waited.error() == fipc::errc::cancelled && waited.error().len == 1);
    EXPECT(owner.wait_for_members(3, 10s).error() == fipc::errc::cancelled);
    // The queue goes on: its members are unaffected
    EXPECT(producer.send("job").has_value());
    std::string message;
    EXPECT(consumer.receive(message, 10s).has_value() && message == "job");
    // A canceler outlives its queue; cancelling then does nothing
    owner.close();
    canceler.cancel();

    // The queue's own cancel, from another thread than the one that waits
    auto other = fipc::queue_create(unique_name("mq_cancel"), 4096, 1, 1);
    if (!EXPECT(other.has_value()))
        return;
    std::thread waiting([&] { waited = other->wait_for_members(1); });
    std::this_thread::sleep_for(20ms);
    other->cancel();
    waiting.join();
    EXPECT(!waited && waited.error() == fipc::errc::cancelled && waited.error().len == 0);
}

// The queue's destructor ends it: a producer's send returns errc::disconnected, a consumer receives what was queued,
// then errc::disconnected; the name is free at once.
void queue_destructor_ends()
{
    const std::string name = unique_name("mq_end");
    fipc::connection producer;
    fipc::connection consumer;
    {
        auto owner = fipc::queue_create(name, 4096, 1, 1);
        if (!EXPECT(owner.has_value()))
            return;
        producer = enlist(name, fipc::queue_role::producer);
        consumer = enlist(name, fipc::queue_role::consumer);
        if (!producer || !consumer)
            return;
        EXPECT(producer.send("one").has_value() && producer.send("two").has_value());
    }
    EXPECT(producer.send("late", fipc::no_wait).error() == fipc::errc::disconnected);
    std::string first;
    std::string second;
    EXPECT(consumer.receive(first, 10s).has_value() && consumer.receive(second, 10s).has_value());
    EXPECT((first == "one" && second == "two") || (first == "two" && second == "one"));
    std::string message;
    EXPECT(consumer.receive(message, 10s).error() == fipc::errc::disconnected);
    auto again = fipc::queue_create(name, 4096, 1, 1);
    EXPECT(again.has_value());
}

// === Services ===

// A member of the service on `name`, joined as `r`; an empty connection if it didn't join. The owner's process admits
// it on a library thread, so the thread that created the service may join it too.
fipc::connection member_of(const std::string& name, fipc::service_role r)
{
    auto member = fipc::service_join(name, r, 10s);
    if (!EXPECT(member.has_value()))
        return {};
    return std::move(*member);
}

// A server's answer to one request: takes it (copied) and replies with "<request> done"; false if it took none.
bool answer(fipc::connection& server, std::string& request, fipc::timeout wait = 10s)
{
    if (!server.receive(request, wait))
        return false;
    return server.send(request + " done").has_value();
}

// service_create's checks and its name against every kind, the owner's own members (joined on the thread that created
// the service), joiners of other kinds and roles, request and reply copied and in place, requests in flight and their
// replies in order, and what each role can't use.
void service_roles()
{
    const std::string name = unique_name("sv_hpp");
    EXPECT(fipc::service_create(name, 2048, 64, 1, 1).error() == fipc::errc::invalid);
    EXPECT(fipc::service_create(name, 4096, 0, 1, 1).error() == fipc::errc::invalid);
    EXPECT(fipc::service_create(name, 4096, 1025, 1, 1).error() == fipc::errc::invalid);  // over capacity / 4
    EXPECT(fipc::service_create(name, 4096, 64, 0, 1).error() == fipc::errc::invalid);
    EXPECT(fipc::service_create(name, 4096, 64, 1, 65).error() == fipc::errc::invalid);
    EXPECT(fipc::service_create("bad name", 4096, 64, 1, 1).error() == fipc::errc::invalid);
    {
        auto first = fipc::service_create(name, 4096, 64, 1, 1);
        EXPECT(first.has_value() && *first && first->native_handle() != nullptr);
        EXPECT(fipc::service_create(name, 4096, 64, 1, 1).error() == fipc::errc::addr_in_use);
        EXPECT(fipc::stream_create(name, 1 << 16).error() == fipc::errc::addr_in_use);
        EXPECT(fipc::broadcast_create(name, 4096, 1, fipc::forever).error() == fipc::errc::addr_in_use);
        EXPECT(fipc::queue_create(name, 4096, 1, 1).error() == fipc::errc::addr_in_use);
    }
    auto owner = fipc::service_create(name, 1 << 14, 1000, 2, 2);
    if (!EXPECT(owner.has_value() && *owner))
        return;
    fipc::connection server = member_of(name, fipc::service_role::server);
    fipc::connection client = member_of(name, fipc::service_role::client);
    if (!server || !client)
        return;
    EXPECT(server.max_piece() == 1000 && client.max_piece() == 1000);

    // A role of another kind, and the members of other kinds, fail at once; nobody on a name: the join's timeout
    EXPECT(fipc::service_join(name, static_cast<fipc::service_role>(FIPC_PRODUCER), 5s).error() == fipc::errc::invalid);
    EXPECT(
        fipc::service_join(name, static_cast<fipc::service_role>(FIPC_SUBSCRIBER_LOSSY), 5s).error()
        == fipc::errc::invalid
    );
    EXPECT(fipc::queue_join(name, fipc::queue_role::consumer, 5s).error() == fipc::errc::invalid);
    EXPECT(fipc::broadcast_join(name, fipc::subscriber_role::lossy, 5s).error() == fipc::errc::invalid);
    EXPECT(fipc::stream_connect(name, 5s).error() == fipc::errc::invalid);
    {
        const std::string queue_name = unique_name("sv_hpp_mq");
        auto queue = fipc::queue_create(queue_name, 4096, 1, 1);
        if (EXPECT(queue.has_value()))
            EXPECT(fipc::service_join(queue_name, fipc::service_role::client, 5s).error() == fipc::errc::invalid);
    }
    EXPECT(
        fipc::service_join(unique_name("sv_nobody"), fipc::service_role::client, fipc::no_wait).error()
        == fipc::errc::timeout
    );

    // A request and its reply, copied; a server holds one request at a time, until it replies
    std::string request;
    std::string reply;
    EXPECT(client.send("ping").has_value());
    EXPECT(server.receive(request, 10s).value_or(0) == 4 && request == "ping");
    EXPECT(server.receive(request, fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(server.send("ping done").has_value());
    EXPECT(client.receive(reply, 10s).value_or(0) == 9 && reply == "ping done");

    // In place: the client's request and the server's reply in slots, the request held while the reply is written
    {
        auto slot = client.send_acquire(5);
        if (EXPECT(slot.has_value()))
        {
            std::memcpy(slot->data(), "place", 5);
            EXPECT(slot->commit().has_value());
        }
    }
    {
        auto held = server.receive_acquire(10s);
        if (EXPECT(held.has_value()))
        {
            EXPECT(text(held->bytes()) == "place");
            auto room = server.send_acquire(6);
            if (EXPECT(room.has_value()))
            {
                std::memcpy(room->data(), "placed", 6);
                EXPECT(text(held->bytes()) == "place");  // the request stays valid until the reply is sent
                EXPECT(room->commit().has_value());
            }
        }
    }
    {
        auto got = client.receive_acquire(10s);
        EXPECT(got.has_value() && text(got->bytes()) == "placed");
    }

    // Requests in flight: the n-th reply received answers the n-th request
    for (int i = 0; i < 3; i++)
        EXPECT(client.send("job " + std::to_string(i)).has_value());
    for (int i = 0; i < 3; i++)
        EXPECT(answer(server, request) && request == "job " + std::to_string(i));
    for (int i = 0; i < 3; i++)
        EXPECT(client.receive(reply, 10s).has_value() && reply == "job " + std::to_string(i) + " done");

    // What each role can't use; a client with no reply to come times out, a server with no request too
    std::array<std::byte, 1001> big{};
    EXPECT(client.send(std::span<const std::byte>(big)).error() == fipc::errc::too_large);
    EXPECT(client.send_acquire(1001).error() == fipc::errc::too_large);
    EXPECT(server.send("x").error() == fipc::errc::invalid);  // no request held
    EXPECT(server.send_acquire(1).error() == fipc::errc::invalid);
    EXPECT(client.receive(reply, fipc::no_wait).error() == fipc::errc::timeout);
    EXPECT(server.receive(request, fipc::no_wait).error() == fipc::errc::timeout);
    EXPECT(client.wait_for_members().error() == fipc::errc::invalid);
    EXPECT(server.wait_for_members().error() == fipc::errc::invalid);
}

// Several clients and one server: each client's replies are its own. Then a pool: two servers on threads of their own
// share the requests of three clients, each with requests in flight, and every reply goes to the client that asked, in
// the order of its requests.
void service_clients_and_pool()
{
    const std::string name = unique_name("sv_pool");
    auto owner = fipc::service_create(name, 1 << 14, 256, 3, 2);
    if (!EXPECT(owner.has_value()))
        return;
    std::vector<fipc::connection> clients;
    for (int c = 0; c < 3; c++)
        clients.push_back(member_of(name, fipc::service_role::client));
    fipc::connection first = member_of(name, fipc::service_role::server);
    for (const auto& client : clients)
        if (!client)
            return;
    if (!first)
        return;

    // One server: each client's request, answered in any order, comes back to it
    for (std::size_t c = 0; c < 3; c++)
        EXPECT(clients[c].send("client " + std::to_string(c)).has_value());
    std::string request;
    for (int i = 0; i < 3; i++)
        EXPECT(answer(first, request));
    std::string reply;
    for (std::size_t c = 0; c < 3; c++)
        EXPECT(clients[c].receive(reply, 10s).has_value() && reply == "client " + std::to_string(c) + " done");

    // A pool of two servers, each answering on its own thread until it is cancelled
    fipc::connection second = member_of(name, fipc::service_role::server);
    if (!second)
        return;
    std::atomic<int> answered[2] = {0, 0};
    auto serve = [](fipc::connection& server, std::atomic<int>& count)
    {
        std::string taken;
        while (answer(server, taken, fipc::forever))
            count++;
    };
    std::thread one([&] { serve(first, answered[0]); });
    std::thread two([&] { serve(second, answered[1]); });
    constexpr int rounds = 50;
    std::vector<std::thread> asking;
    std::atomic<int> wrong{0};
    for (std::size_t c = 0; c < 3; c++)
        asking.emplace_back(
            [&, c]
            {
                fipc::connection& client = clients[c];
                std::string got;
                for (int i = 0; i < rounds; i += 2)  // two requests in flight at a time
                {
                    const std::string a = std::to_string(c) + "/" + std::to_string(i);
                    const std::string b = std::to_string(c) + "/" + std::to_string(i + 1);
                    if (!client.send(a, 10s) || !client.send(b, 10s))
                        wrong++;
                    if (!client.receive(got, 10s) || got != a + " done")
                        wrong++;
                    if (!client.receive(got, 10s) || got != b + " done")
                        wrong++;
                }
            }
        );
    for (auto& t : asking)
        t.join();
    first.cancel();
    second.cancel();
    one.join();
    two.join();
    EXPECT(wrong == 0);
    EXPECT(answered[0] + answered[1] == 3 * rounds);
}

// A server that closes holding a request abandons it: its client's receive returns errc::abandoned with len 0 (copied
// and in place), that request counts as answered, and the next request goes to another server and gets its reply.
void service_abandoned()
{
    const std::string name = unique_name("sv_abandon");
    auto owner = fipc::service_create(name, 4096, 64, 1, 2);
    if (!EXPECT(owner.has_value()))
        return;
    fipc::connection client = member_of(name, fipc::service_role::client);
    if (!client)
        return;
    std::string request;
    std::string reply;
    for (int in_place = 0; in_place < 2; in_place++)
    {
        fipc::connection leaving = member_of(name, fipc::service_role::server);
        if (!leaving)
            return;
        EXPECT(client.send("lost").has_value());
        EXPECT(leaving.receive(request, 10s).has_value() && request == "lost");
        leaving.close();
        if (in_place == 0)
        {
            auto got = client.receive(reply, 10s);
            EXPECT(!got && got.error() == fipc::errc::abandoned && got.error().len == 0);
            EXPECT(std::strcmp(got.error().name(), "FIPC_ABANDONED") == 0);
        }
        else
        {
            auto got = client.receive_acquire(10s);
            EXPECT(!got && got.error() == fipc::errc::abandoned && got.error().len == 0);
        }
    }
    fipc::connection server = member_of(name, fipc::service_role::server);
    if (!server)
        return;
    EXPECT(client.send("next").has_value());
    EXPECT(answer(server, request) && request == "next");
    EXPECT(client.receive(reply, 10s).has_value() && reply == "next done");
}

// The owner's count of joined members (service::wait_for_members), by role and of both: at once, a wait that a join on
// another thread ends, the timeout, errc::invalid over a role's maximum, for another role or on an empty service; then
// cancel, from a canceler and from the service itself, whose error().len is the count, and a canceler that outlives
// its service.
void service_members()
{
    const std::string name = unique_name("sv_count");
    auto created = fipc::service_create(name, 4096, 64, 1, 2);
    if (!EXPECT(created.has_value()))
        return;
    fipc::service owner = std::move(*created);
    EXPECT(owner && !*created && created->native_handle() == nullptr);
    EXPECT(created->wait_for_members(0, fipc::no_wait).error() == fipc::errc::invalid);  // an empty service
    EXPECT(owner.wait_for_members().value_or(99) == 0);
    EXPECT(owner.wait_for_members(fipc::service_role::client).value_or(99) == 0);
    EXPECT(owner.wait_for_members(fipc::service_role::client, 2, fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(owner.wait_for_members(fipc::service_role::server, 3, fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(owner.wait_for_members(4, fipc::no_wait).error() == fipc::errc::invalid);  // over both maxima together
    EXPECT(
        owner.wait_for_members(static_cast<fipc::service_role>(FIPC_CONSUMER), 0, fipc::no_wait).error()
        == fipc::errc::invalid
    );

    fipc::result<std::uint32_t> waited = fipc::error{fipc::errc::invalid};
    std::thread waiter([&] { waited = owner.wait_for_members(fipc::service_role::client, 1, 10s); });
    fipc::connection client = member_of(name, fipc::service_role::client);
    waiter.join();
    EXPECT(waited.value_or(99) == 1);
    auto timed_out = owner.wait_for_members(fipc::service_role::server, 1, 50ms);
    EXPECT(!timed_out && timed_out.error() == fipc::errc::timeout && timed_out.error().len == 0);
    fipc::connection server = member_of(name, fipc::service_role::server);
    if (!client || !server)
        return;
    EXPECT(owner.wait_for_members(fipc::service_role::server, 1, 10s).value_or(99) == 1);
    EXPECT(owner.wait_for_members(2, 10s).value_or(99) == 2);

    // A canceler's cancel from another thread ends a waiting wait_for_members; it is final
    fipc::canceler canceler = owner.get_canceler();
    std::thread canceled([&] { waited = owner.wait_for_members(fipc::service_role::server, 2); });
    std::this_thread::sleep_for(20ms);
    canceler.cancel();
    canceled.join();
    EXPECT(!waited && waited.error() == fipc::errc::cancelled && waited.error().len == 1);
    EXPECT(owner.wait_for_members(3, 10s).error() == fipc::errc::cancelled);
    // The service goes on: its members are unaffected
    EXPECT(client.send("ping").has_value());
    std::string request;
    std::string reply;
    EXPECT(answer(server, request) && request == "ping");
    EXPECT(client.receive(reply, 10s).has_value() && reply == "ping done");
    // A canceler outlives its service; cancelling then does nothing
    owner.close();
    canceler.cancel();

    // The service's own cancel, from another thread than the one that waits
    auto other = fipc::service_create(unique_name("sv_cancel"), 4096, 64, 1, 1);
    if (!EXPECT(other.has_value()))
        return;
    std::thread waiting([&] { waited = other->wait_for_members(1); });
    std::this_thread::sleep_for(20ms);
    other->cancel();
    waiting.join();
    EXPECT(!waited && waited.error() == fipc::errc::cancelled && waited.error().len == 0);
}

// The service's destructor ends it: a client receives the reply that came before the end, then errc::disconnected
// at its request without one, and its sends return errc::disconnected; a server's reply and receive return it too;
// the name is free at once.
void service_destructor_ends()
{
    const std::string name = unique_name("sv_end");
    fipc::connection client;
    fipc::connection server;
    std::string request;
    {
        auto owner = fipc::service_create(name, 4096, 64, 1, 1);
        if (!EXPECT(owner.has_value()))
            return;
        client = member_of(name, fipc::service_role::client);
        server = member_of(name, fipc::service_role::server);
        if (!client || !server)
            return;
        EXPECT(client.send("one").has_value() && client.send("two").has_value());
        EXPECT(answer(server, request) && request == "one");
        EXPECT(server.receive(request, 10s).has_value() && request == "two");  // held, unanswered, at the end
    }
    EXPECT(client.send("late", fipc::no_wait).error() == fipc::errc::disconnected);
    std::string reply;
    EXPECT(client.receive(reply, 10s).has_value() && reply == "one done");
    EXPECT(client.receive(reply, 10s).error() == fipc::errc::disconnected);
    EXPECT(server.send("two done").error() == fipc::errc::disconnected);
    EXPECT(server.receive(request, fipc::no_wait).error() == fipc::errc::disconnected);
    auto again = fipc::service_create(name, 4096, 64, 1, 1);
    EXPECT(again.has_value());
}

// === A service client's call() ===

// A service with one client and one server, the client's requests and replies of up to 64 bytes.
struct service_pair
{
    fipc::service owner;
    fipc::connection client;
    fipc::connection server;
};

bool open_service(service_pair& p, const char* prefix, std::size_t capacity = 4096, std::size_t max_message = 64)
{
    const std::string name = unique_name(prefix);
    auto owner = fipc::service_create(name, capacity, max_message, 1, 1);
    if (!EXPECT(owner.has_value()))
        return false;
    p.owner = std::move(*owner);
    p.client = member_of(name, fipc::service_role::client);
    p.server = member_of(name, fipc::service_role::server);
    return p.client && p.server;
}

// A buffer whose resize() takes 20 ms: a receive into it that has to grow it spends that much of its wait.
struct slow_buffer
{
    std::string bytes;

    void resize(std::size_t n)
    {
        std::this_thread::sleep_for(20ms);
        bytes.resize(n);
    }

    std::size_t size() const
    {
        return bytes.size();
    }

    char* data()
    {
        return bytes.data();
    }
};

// call() sends a request and returns its reply, one piece of 1 to max_piece() bytes, which it checks first; it works
// on a service's client only: from service_join, or a raw handle taken over with its role.
void service_call()
{
    service_pair p;
    if (!open_service(p, "sv_call"))
        return;
    std::string request;
    std::string reply;
    std::thread answering([&] { EXPECT(answer(p.server, request) && answer(p.server, request)); });
    EXPECT(p.client.call("ping", reply, 10s).value_or(0) == 9 && reply == "ping done");
    std::vector<std::byte> bytes = pattern(59);  // its reply is of max_piece() bytes, the longest
    std::vector<char> reply_bytes;
    EXPECT(p.client.call(std::span<const std::byte>(bytes), reply_bytes, 10s).value_or(0) == 64);
    answering.join();
    EXPECT(text(std::span<const std::byte>(bytes)) + " done" == std::string(reply_bytes.data(), reply_bytes.size()));

    // The request's checks: nothing is sent, and nothing owed
    auto empty = p.client.call("", reply, 10s);
    EXPECT(!empty && empty.error() == fipc::errc::invalid);
    std::vector<std::byte> too_long(65);
    auto long_one = p.client.call(std::span<const std::byte>(too_long), reply, 10s);
    EXPECT(!long_one && long_one.error() == fipc::errc::too_large && long_one.error().len == 65);
    EXPECT(p.server.receive(request, 50ms).error() == fipc::errc::timeout);

    // Any other connection: a server, a stream's, an empty one, a client's handle taken over without its role
    EXPECT(p.server.call("x", reply, fipc::no_wait).error() == fipc::errc::invalid);
    EXPECT(fipc::connection().call("x", reply, fipc::no_wait).error() == fipc::errc::invalid);
    pair stream;
    if (open_pair(stream, "sv_call_stream"))
        EXPECT(stream.client.call("x", reply, fipc::no_wait).error() == fipc::errc::invalid);
    const std::string name = unique_name("sv_call_raw");
    auto owner = fipc::service_create(name, 4096, 64, 2, 1);
    if (!EXPECT(owner.has_value()))
        return;
    fipc::connection server = member_of(name, fipc::service_role::server);
    fipc_conn_t* handle = nullptr;
    EXPECT(fipc_service_join(name.c_str(), FIPC_CLIENT, &handle, 10000) == FIPC_OK);
    fipc::connection unknown(handle);
    EXPECT(unknown.call("x", reply, fipc::no_wait).error() == fipc::errc::invalid);
    handle = nullptr;
    EXPECT(fipc_service_join(name.c_str(), FIPC_CLIENT, &handle, 10000) == FIPC_OK);
    fipc::connection client(handle, fipc::service_role::client);
    std::thread replying([&] { EXPECT(answer(server, request)); });
    EXPECT(client.call("raw", reply, 10s).has_value() && reply == "raw done");
    replying.join();
}

// A call whose receive timed out leaves its reply owed; the next call receives and drops it (within its own timeout)
// before it sends, so it returns the reply to its own request, and a plain receive returns a late reply first. A call
// refuses (errc::invalid) while a request sent with send() or a slot's commit() still has its reply to be received.
void service_call_owed()
{
    service_pair p;
    if (!open_service(p, "sv_call_owed"))
        return;
    std::string request;
    std::string reply;
    EXPECT(p.client.call("late", reply, 50ms).error() == fipc::errc::timeout);  // went: no reply in time
    EXPECT(answer(p.server, request) && request == "late");  // the late reply, owed to the call that gave up
    {
        std::thread answering([&] { EXPECT(answer(p.server, request)); });
        EXPECT(p.client.call("next", reply, 10s).has_value() && reply == "next done");  // not "late done"
        answering.join();
    }
    EXPECT(p.client.receive(reply, fipc::no_wait).error() == fipc::errc::timeout);  // nothing more owed

    // A call whose owed reply hasn't come waits for it within its own timeout, and gives up before it sends
    EXPECT(p.client.call("a", reply, fipc::no_wait).error() == fipc::errc::timeout);  // a went; its reply is owed
    EXPECT(p.client.call("b", reply, 50ms).error() == fipc::errc::timeout);           // a's reply isn't there: no b
    EXPECT(answer(p.server, request) && request == "a");
    EXPECT(p.server.receive(request, fipc::no_wait).error() == fipc::errc::timeout);  // no b
    {
        fipc::result<std::size_t> taken = fipc::error{fipc::errc::invalid};
        std::thread taking([&] { taken = p.server.receive(request, 10s); });
        EXPECT(p.client.call("c", reply, 200ms).error() == fipc::errc::timeout);  // drops "a done", sends c, gives up
        taking.join();
        EXPECT(taken.has_value() && request == "c");
    }
    EXPECT(p.server.send("c done").has_value());                            // owed to the call that gave up
    EXPECT(p.client.receive(reply, 10s).has_value() && reply == "c done");  // a plain receive takes it, in order
    EXPECT(p.client.receive(reply, fipc::no_wait).error() == fipc::errc::timeout);

    // A request of send() whose reply hasn't been received: call refuses, and leaves it for the caller's receive
    EXPECT(p.client.send("mine").has_value());
    EXPECT(p.client.call("x", reply, 10s).error() == fipc::errc::invalid);
    EXPECT(answer(p.server, request));
    EXPECT(p.client.receive(reply, 10s).has_value() && reply == "mine done");
    // ... and so does one of a slot's commit, received in place
    {
        auto slot = p.client.send_acquire(4);
        if (EXPECT(slot.has_value()))
        {
            std::memcpy(slot->data(), "slot", 4);
            EXPECT(slot->commit().has_value());
        }
    }
    EXPECT(p.client.call("x", reply, 10s).error() == fipc::errc::invalid);
    EXPECT(answer(p.server, request));
    {
        auto got = p.client.receive_acquire(10s);
        EXPECT(got.has_value() && text(got->bytes()) == "slot done");
    }
    // A send that fails owes nothing
    EXPECT(p.client.send("", fipc::no_wait).error() == fipc::errc::invalid);
    {
        std::thread answering([&] { EXPECT(answer(p.server, request)); });
        EXPECT(p.client.call("x", reply, 10s).has_value() && reply == "x done");
        answering.join();
    }
    EXPECT(p.client.receive(reply, fipc::no_wait).error() == fipc::errc::timeout);
}

// A call whose timeout runs out while it drops an owed reply never sends its request; fipc::no_wait waits at no step,
// drops a reply that is there and sends.
void service_call_deadline()
{
    service_pair p;
    if (!open_service(p, "sv_call_deadline"))
        return;
    std::string request;
    std::string reply;
    EXPECT(p.client.call("late", reply, 50ms).error() == fipc::errc::timeout);  // went: no reply in time
    EXPECT(answer(p.server, request) && request == "late");  // the late reply has come once the send returns
    // The drop finds the late reply at once, but growing the buffer for it takes 20 ms: by then the call's 5 ms have
    // run out, so its request isn't sent
    slow_buffer slow;
    EXPECT(p.client.call("never", slow, 5ms).error() == fipc::errc::timeout);
    EXPECT(slow.bytes == "late done");                                              // what was dropped
    EXPECT(p.server.receive(request, 50ms).error() == fipc::errc::timeout);         // the server never saw it
    EXPECT(p.client.receive(reply, fipc::no_wait).error() == fipc::errc::timeout);  // nothing is owed
    {
        std::thread answering([&] { EXPECT(answer(p.server, request)); });
        EXPECT(p.client.call("next", reply, 10s).has_value() && reply == "next done");
        answering.join();
    }

    // fipc::no_wait: with the owed reply there, the call drops it and sends
    EXPECT(p.client.call("a", reply, fipc::no_wait).error() == fipc::errc::timeout);  // a went; its reply is owed
    EXPECT(answer(p.server, request) && request == "a");
    EXPECT(p.client.call("b", reply, fipc::no_wait).error() == fipc::errc::timeout);  // drops "a done", sends b
    EXPECT(answer(p.server, request) && request == "b");
    EXPECT(p.client.receive(reply, 10s).has_value() && reply == "b done");  // a plain receive takes the late reply
}

// A call returns errc::abandoned when the server that took its request ends before it replies; an errc::abandoned
// owed to a call that gave up counts as its reply, and the next call drops it.
void service_call_abandoned()
{
    const std::string name = unique_name("sv_call_abandoned");
    auto owner = fipc::service_create(name, 4096, 64, 1, 2);
    if (!EXPECT(owner.has_value()))
        return;
    fipc::connection client = member_of(name, fipc::service_role::client);
    fipc::connection quitter = member_of(name, fipc::service_role::server);
    if (!client || !quitter)
        return;
    std::string request;
    std::string reply;
    {
        fipc::result<std::size_t> called = fipc::error{fipc::errc::invalid};
        std::thread calling([&] { called = client.call("lost", reply, 10s); });
        EXPECT(quitter.receive(request, 10s).has_value() && request == "lost");
        quitter.close();  // holding it: the request is answered errc::abandoned
        calling.join();
        EXPECT(!called && called.error() == fipc::errc::abandoned);
    }
    quitter = member_of(name, fipc::service_role::server);
    if (!quitter)
        return;
    EXPECT(client.call("lost too", reply, 50ms).error() == fipc::errc::timeout);  // went: no reply in time
    EXPECT(quitter.receive(request, 10s).has_value() && request == "lost too");
    quitter.close();  // the errc::abandoned is owed to the call that gave up
    fipc::connection server = member_of(name, fipc::service_role::server);
    if (!server)
        return;
    std::thread answering([&] { EXPECT(answer(server, request)); });
    EXPECT(client.call("next", reply, 10s).has_value() && reply == "next done");  // drops it, gets its own
    answering.join();
    EXPECT(client.receive(reply, fipc::no_wait).error() == fipc::errc::timeout);  // nothing more owed
}

// After the service's end: errc::disconnected from the drop of an owed reply that never came, or from the send with
// nothing owed; errc::invalid while a reply to a request of send() is owed, which never comes.
void service_call_end()
{
    std::string request;
    std::string reply;
    {
        service_pair p;
        if (!open_service(p, "sv_call_end_owed"))
            return;
        EXPECT(p.client.call("held", reply, 50ms).error() == fipc::errc::timeout);  // its reply is owed
        EXPECT(p.server.receive(request, 10s).has_value() && request == "held");
        p.owner.close();
        EXPECT(p.client.call("x", reply, 10s).error() == fipc::errc::disconnected);
    }
    {
        service_pair p;
        if (!open_service(p, "sv_call_end_send"))
            return;
        p.owner.close();
        EXPECT(p.client.call("x", reply, 10s).error() == fipc::errc::disconnected);
        EXPECT(p.client.call("", reply, 10s).error() == fipc::errc::invalid);  // the request is checked first
    }
    {
        service_pair p;
        if (!open_service(p, "sv_call_end_sent"))
            return;
        EXPECT(p.client.send("mine").has_value());
        p.owner.close();
        EXPECT(p.client.call("x", reply, 10s).error() == fipc::errc::invalid);
        EXPECT(p.client.receive(reply, 10s).error() == fipc::errc::disconnected);  // that reply never comes
    }
}

// A cancel from another thread ends a call's wait for its reply, which stays owed: a receive that needn't wait takes
// it. A call on a cancelled connection returns errc::cancelled at once, after its request's checks, whatever its
// timeout, and sends nothing; the connection's own cancel() and a canceler's both count.
void service_call_cancel()
{
    service_pair p;
    if (!open_service(p, "sv_call_cancel"))
        return;
    std::string request;
    std::string reply;
    const fipc::canceler canceler = p.client.get_canceler();
    {
        fipc::result<std::size_t> called = fipc::error{fipc::errc::invalid};
        std::thread calling([&] { called = p.client.call("cancelled", reply); });
        EXPECT(p.server.receive(request, 10s).has_value() && request == "cancelled");
        canceler.cancel();
        calling.join();
        EXPECT(!called && called.error() == fipc::errc::cancelled);
    }
    EXPECT(p.server.send("cancelled done").has_value());  // owed to the cancelled call
    std::string late;
    EXPECT(p.client.receive(late, fipc::no_wait).has_value() && late == "cancelled done");
    EXPECT(p.client.receive(late, 10s).error() == fipc::errc::cancelled);  // final
    EXPECT(p.client.call("x", reply, fipc::no_wait).error() == fipc::errc::cancelled);

    service_pair q;
    if (!open_service(q, "sv_call_cancelled"))
        return;
    q.client.cancel();
    EXPECT(q.client.call("", reply, 10s).error() == fipc::errc::invalid);  // the request's checks come first
    std::vector<std::byte> too_long(65);
    EXPECT(q.client.call(std::span<const std::byte>(too_long), reply, 10s).error() == fipc::errc::too_large);
    for (const fipc::timeout wait : {fipc::forever, fipc::no_wait, fipc::timeout(10s)})
        EXPECT(q.client.call("never sent", reply, wait).error() == fipc::errc::cancelled);
    EXPECT(q.server.receive(request, 200ms).error() == fipc::errc::timeout);  // no request went
}

// Batches on a stream (send_many, receive_many): in order, packed back to back; a buffer that stops a batch, and a
// first message too long for it; NO_WAIT's partial send, whose error().len says how many went; more messages than one
// fipc_send_many takes at a time, sent while they are received; an empty message refused before any goes.
void batches()
{
    pair p;
    if (!open_pair(p, "batches", 1024))
        return;
    std::vector<std::vector<std::byte>> storage;
    for (std::size_t i = 0; i < 6; i++)
        storage.push_back(pattern(10 + i * 20));
    const std::vector<std::span<const std::byte>> messages(storage.begin(), storage.end());
    EXPECT(p.client.send_many(messages).value_or(0) == 6);
    std::array<std::byte, 4096> buf{};
    std::array<std::size_t, 16> lens{};
    auto got = p.server.receive_many(buf, lens, 10s);
    if (EXPECT(got.has_value() && *got == 6))
    {
        std::size_t at = 0;
        for (std::size_t i = 0; i < 6; i++)
        {
            EXPECT(lens[i] == storage[i].size() && std::memcmp(buf.data() + at, storage[i].data(), lens[i]) == 0);
            at += lens[i];
        }
    }
    EXPECT(p.client.send_many(messages).has_value());
    EXPECT(p.server.receive_many(std::span(buf).first(10 + 30), lens, 10s).value_or(0) == 2);
    auto too_large = p.server.receive_many(std::span(buf).first(10), lens, 10s);
    EXPECT(!too_large && too_large.error() == fipc::errc::too_large && too_large.error().len == 50);
    EXPECT(p.server.receive_many(buf, lens, 10s).value_or(0) == 4);

    // Frames of 128 bytes: the ring of 1 KiB holds 8, fewer where its end is padded
    const auto hundred = pattern(100);
    const std::vector<std::span<const std::byte>> many(100, hundred);
    auto partial = p.client.send_many(many, fipc::no_wait);
    EXPECT(!partial && partial.error() == fipc::errc::timeout && partial.error().len >= 1 && partial.error().len <= 8);
    std::size_t received = 0;
    while (received < partial.error().len)
    {
        auto batch = p.server.receive_many(buf, lens, 10s);
        if (!EXPECT(batch.has_value()))
            return;
        received += *batch;
    }
    std::thread sender([&] { EXPECT(p.client.send_many(many, 10s).value_or(0) == 100); });
    received = 0;
    while (received < 100)
    {
        auto batch = p.server.receive_many(buf, lens, 10s);
        if (!EXPECT(batch.has_value()))
            break;
        for (std::size_t i = 0; i < *batch; i++)
            EXPECT(lens[i] == 100 && std::memcmp(buf.data() + i * 100, hundred.data(), 100) == 0);
        received += *batch;
    }
    sender.join();
    EXPECT(received == 100);

    const std::vector<std::span<const std::byte>> with_empty{hundred, std::span<const std::byte>()};
    auto empty = p.client.send_many(with_empty, fipc::no_wait);
    EXPECT(!empty && empty.error() == fipc::errc::invalid && empty.error().len == 0);
    EXPECT(p.server.receive_many(buf, lens, fipc::no_wait).error() == fipc::errc::timeout);
}

// A service client's batch of requests is owed as send()'s are: call() refuses until their replies, received with
// receive_many, in order; a server's batch and batched receive are errc::invalid.
void service_batches()
{
    const std::string name = unique_name("sv_hpp_batch");
    auto owner = fipc::service_create(name, 1 << 14, 64, 1, 1);
    if (!EXPECT(owner.has_value() && *owner))
        return;
    fipc::connection server = member_of(name, fipc::service_role::server);
    fipc::connection client = member_of(name, fipc::service_role::client);
    if (!server || !client)
        return;
    const std::string_view texts[] = {"a", "bb", "ccc"};
    std::vector<std::span<const std::byte>> requests;
    for (const std::string_view text : texts)
        requests.push_back(std::as_bytes(std::span(text)));
    EXPECT(client.send_many(requests).value_or(0) == 3);
    std::string reply;
    EXPECT(client.call("d", reply, fipc::no_wait).error() == fipc::errc::invalid);  // those replies are the caller's
    std::string request;
    for (const std::string_view text : texts)
        EXPECT(answer(server, request) && request == text);
    std::array<std::byte, 256> buf{};
    std::array<std::size_t, 8> lens{};
    std::string replies;
    std::size_t received = 0;
    while (received < 3)
    {
        auto batch = client.receive_many(buf, lens, 10s);
        if (!EXPECT(batch.has_value()))
            return;
        std::size_t at = 0;
        for (std::size_t i = 0; i < *batch; i++)
        {
            replies += std::string(reinterpret_cast<const char*>(buf.data()) + at, lens[i]) + "|";
            at += lens[i];
        }
        received += *batch;
    }
    EXPECT(received == 3 && replies == "a done|bb done|ccc done|");
    std::thread answering([&] { EXPECT(answer(server, request) && request == "e"); });
    EXPECT(client.call("e", reply, 10s).value_or(0) == 6 && reply == "e done");  // nothing owed now
    answering.join();
    EXPECT(server.send_many(requests).error() == fipc::errc::invalid);
    EXPECT(server.receive_many(buf, lens, fipc::no_wait).error() == fipc::errc::invalid);
}

// === Slow: other processes ===

// The peer process exits without closing its connection (as a crash would): the server gets the messages it sent,
// then errc::disconnected.
void peer_process_exits()
{
    const std::string name = unique_name("exit");
    auto listener = fipc::stream_create(name, 1 << 16);
    if (!EXPECT(listener.has_value()))
        return;
    process child(in_quotes(self_path) + " child-exit " + name);
    auto conn = listener->accept(30s);
    if (EXPECT(conn.has_value()))
    {
        std::string message;
        for (const char* expected : {"one", "two", "three"})
            EXPECT(conn->receive(message, 30s).has_value() && message == expected);
        EXPECT(conn->receive(message, 30s).error() == fipc::errc::disconnected);
    }
    EXPECT(child.join() == 0);
}

// The child of peer_process_exits: connects, sends three messages, and exits without closing anything.
int child_exit(const char* name)
{
    auto conn = fipc::stream_connect(name, 30s);
    if (!conn)
        return 1;
    for (const char* message : {"one", "two", "three"})
        if (!conn->send(message))
            return 2;
    std::_Exit(0);
}

// Message `i` of the broadcast's stream of subscriber_processes: its number, then letters, 8 to 307 bytes.
std::string stream_message(int i)
{
    std::string message = std::to_string(i) + ":";
    const std::size_t len = 8 + static_cast<std::size_t>(i) % 300;
    while (message.size() < len)
        message += static_cast<char>('a' + (message.size() + static_cast<std::size_t>(i)) % 26);
    return message;
}

// C++ subscriber processes of a publisher that uses the C API in this process: a lossy one, a gated one and a gated
// one that receives in place each get the whole stream in order, then errc::disconnected at the publisher's close.
// The publisher waits until the three are joined, and each says it joined over a connection of its own (a subscriber
// receives only what is sent after it joined).
void subscriber_processes()
{
    const std::string name = unique_name("bc_procs");
    const std::string joined_name = name + "_joined";
    const int count = 2000;  // in 1 MiB, never a lap: the lossy one misses nothing either
    fipc_conn_t* publisher = nullptr;
    if (!EXPECT(fipc_broadcast_create(name.c_str(), 1 << 20, 4, FIPC_FOREVER, &publisher) == FIPC_OK))
        return;
    auto joined = fipc::stream_create(joined_name, 1 << 16);
    if (!EXPECT(joined.has_value()))
    {
        fipc_close(publisher);
        return;
    }
    const char* roles[] = {"lossy", "gated", "gated-inplace"};
    std::vector<std::unique_ptr<process>> children;
    for (const char* r : roles)
        children.push_back(std::make_unique<process>(
            in_quotes(self_path) + " child-subscribe " + name + " " + r + " " + std::to_string(count)
        ));
    std::uint32_t members = 0;
    bool ready = EXPECT(fipc_broadcast_wait_for_members(publisher, 3, &members, 30000) == FIPC_OK && members == 3);
    for (std::size_t i = 0; i < children.size() && ready; i++)
    {
        auto conn = joined->accept(30s);
        std::string message;
        ready = EXPECT(conn.has_value() && conn->receive(message, 30s).has_value() && message == "joined");
    }
    for (int i = 0; i < count && ready; i++)
    {
        const std::string message = stream_message(i);
        ready = EXPECT(fipc_send(publisher, message.data(), message.size(), 30000) == FIPC_OK);
    }
    fipc_close(publisher);
    for (auto& child : children)
        EXPECT(child->join() == 0);
}

// The child of subscriber_processes: joins as `role`, says so, and checks the stream up to the publisher's close.
int child_subscribe(const char* name, std::string_view role, int count)
{
    const fipc::subscriber_role r = role == "lossy" ? fipc::subscriber_role::lossy : fipc::subscriber_role::gated;
    auto sub = fipc::broadcast_join(name, r, 30s);
    if (!sub)
    {
        std::fprintf(stderr, "child-subscribe %s: join: %s\n", name, sub.error().name());
        return 1;
    }
    {
        auto said = fipc::stream_connect(std::string(name) + "_joined", 30s);
        if (!said || !said->send("joined"))
            return 2;
    }
    std::string message;
    for (int i = 0; i < count; i++)
    {
        bool same = false;
        if (role == "gated-inplace")
        {
            auto held = sub->receive_acquire(30s);
            same = held && text(held->bytes()) == stream_message(i);
        }
        else
        {
            same = sub->receive(message, 30s) && message == stream_message(i);
        }
        if (!same)
        {
            std::fprintf(stderr, "child-subscribe %s: message %d isn't the one sent\n", name, i);
            return 3;
        }
    }
    auto end = sub->receive(message, 30s);
    if (end || end.error() != fipc::errc::disconnected)
    {
        std::fprintf(
            stderr, "child-subscribe %s: after the stream: %s\n", name, end ? "a message" : end.error().name()
        );
        return 4;
    }
    return 0;
}

// A Python interpreter with cffi (FIPC_TEST_PYTHON, the repository's venv, python), or "" to skip the test.
std::string python()
{
#ifdef _WIN32
    const std::string venv = repository + "/venv/Scripts/python.exe";
    const char* quiet = " >nul 2>&1";
    const char* fallback = "python";
#else
    const std::string venv = repository + "/venv/bin/python";
    const char* quiet = " >/dev/null 2>&1";
    const char* fallback = "python3";
#endif
    std::vector<std::string> candidates;
    if (const char* given = std::getenv("FIPC_TEST_PYTHON"))
        candidates.push_back(given);
    candidates.push_back(venv);
    candidates.push_back(fallback);
    for (const std::string& candidate : candidates)
    {
        std::string probe = in_quotes(candidate) + " -c \"import cffi\"" + quiet;
#ifdef _WIN32
        probe = "\"" + probe + "\"";
#endif
        if (std::system(probe.c_str()) == 0)
            return candidate;
    }
    return "";
}

process start_python(const std::string& interpreter, const char* mode, const std::string& name)
{
    return process(
        in_quotes(interpreter) + " " + in_quotes(repository + "/tests/cpp/peer.py") + " " + mode + " " + name + " "
        + in_quotes(repository + "/bindings/python")
    );
}

void python_peer()
{
    if (!interop)
    {
        std::printf("  skipped: no interop in this build (TSan)\n");
        return;
    }
    const std::string interpreter = python();
    if (interpreter.empty())
    {
        EXPECT(std::getenv("FIPC_REQUIRE_INTEROP") == nullptr);
        std::printf("  skipped: no Python with cffi (set FIPC_TEST_PYTHON)\n");
        return;
    }

    // A C++ server and a Python client, a message and its answer both ways
    {
        const std::string name = unique_name("py_client");
        auto listener = fipc::stream_create(name, 1 << 16);
        if (!EXPECT(listener.has_value()))
            return;
        process peer = start_python(interpreter, "client", name);
        auto conn = listener->accept(60s);
        if (EXPECT(conn.has_value()))
        {
            std::string message;
            EXPECT(conn->receive(message, 30s).has_value() && message == "hello from Python");
            EXPECT(conn->send("HELLO FROM PYTHON", 30s).has_value());
            EXPECT(conn->send("C++", 30s).has_value());
            EXPECT(conn->receive(message, 30s).has_value() && message == "C++C++");
            conn->close();
        }
        EXPECT(peer.join() == 0);
    }

    // A Python server and a C++ client: a message longer than the ring, sent while the answer is received
    {
        const std::string name = unique_name("py_server");
        process peer = start_python(interpreter, "server", name);
        auto conn = fipc::stream_connect(name, 60s);
        if (EXPECT(conn.has_value()))
        {
            std::string big(300000, 'x');
            for (std::size_t i = 0; i < big.size(); i++)
                big[i] = static_cast<char>('a' + i % 26);
            bool sent = false;
            std::thread sender([&] { sent = conn->send(big, 30s).has_value(); });
            std::string answer;
            auto received = conn->receive(answer, 30s);
            sender.join();
            std::string upper = big;
            for (char& c : upper)
                c = static_cast<char>(c - 'a' + 'A');
            EXPECT(sent && received.has_value() && answer == upper);
            conn->close();
        }
        EXPECT(peer.join() == 0);
    }
}

// The Python binding keeps the library loaded (Linux, macOS: it must never be unloaded): CFFI unloads it when the
// interpreter exits, and a library thread still on its way out of the library's code then crashes the exit. The peer
// closes CFFI's handle itself, then calls into the library.
void python_keeps_library()
{
    if (!interop)
    {
        std::printf("  skipped: no interop in this build (TSan)\n");
        return;
    }
    const std::string interpreter = python();
    if (interpreter.empty())
    {
        EXPECT(std::getenv("FIPC_REQUIRE_INTEROP") == nullptr);
        std::printf("  skipped: no Python with cffi (set FIPC_TEST_PYTHON)\n");
        return;
    }
    process peer = start_python(interpreter, "pinned", unique_name("py_pinned"));
    EXPECT(peer.join() == 0);
}

// The run's tier ("fast: " or "slow: ") and filters, and the number of tests it ran.
std::string tier;
std::vector<const char*> filters;
int ran = 0;

// Runs `test` if its name has the run's tier and, with filters, contains one.
void run(const char* name, void (*test)())
{
    const std::string_view view = name;
    bool selected = view.starts_with(tier);
    if (selected && !filters.empty())
    {
        selected = false;
        for (const char* filter : filters)
            selected = selected || view.find(filter) != std::string_view::npos;
    }
    if (!selected)
        return;
    const int before = failures;
    std::printf("%s\n", name);
    std::fflush(stdout);
    test();
    std::printf("  %s\n", failures == before ? "ok" : "FAILED");
    ran++;
}

}  // namespace

int main(int argc, char** argv)
{
    if (argc == 3 && std::strcmp(argv[1], "child-exit") == 0)
        return child_exit(argv[2]);
    if (argc == 5 && std::strcmp(argv[1], "child-subscribe") == 0)
        return child_subscribe(argv[2], argv[3], std::atoi(argv[4]));
    if (argc < 4)
    {
        std::fprintf(stderr, "usage: fipc_hpp_test fast|slow <test program> <repository> [--no-interop] [filter...]\n");
        return 2;
    }
    tier = std::string(argv[1]) + ": ";
    self_path = argv[2];
    repository = argv[3];
    for (int i = 4; i < argc; i++)
    {
        if (std::strcmp(argv[i], "--no-interop") == 0)
            interop = false;
        else
            filters.push_back(argv[i]);
    }

    // A run that hangs ends after 240 s, far longer than the whole tier takes
    static std::atomic<bool> finished{false};
    std::thread watchdog(
        []
        {
            const auto deadline = std::chrono::steady_clock::now() + 240s;
            while (!finished.load())
            {
                if (std::chrono::steady_clock::now() > deadline)
                {
                    std::fprintf(stderr, "fipc_hpp_test: timed out\n");
                    std::_Exit(1);
                }
                std::this_thread::sleep_for(50ms);
            }
        }
    );

    run("fast: hpp results and errors", results_and_errors);
    run("fast: hpp handles move and close once", handles_move_and_close_once);
    run("fast: hpp messages", messages);
    run("fast: hpp zero-copy slots", zero_copy_slots);
    run("fast: hpp cancel from another thread", cancel_from_another_thread);
    run("fast: hpp peer's end", peer_end);
    run("fast: hpp broadcast: broadcast_create, broadcast_join and each role's calls", broadcast_roles);
    run("fast: hpp broadcast: a lapped lossy subscriber gets errc::overrun", broadcast_overrun);
    run("fast: hpp broadcast: errc::overrun's len is the number of messages lost", broadcast_overrun_lost);
    run("fast: hpp broadcast: the publisher's wait_for_members", broadcast_members);
    run("fast: hpp broadcast: a stalled gated subscriber gets errc::unsubscribed", broadcast_unsubscribed);
    run("fast: hpp broadcast: max_stall's forever and no_wait, as durations too", broadcast_max_stall);
    run("fast: hpp broadcast: the publisher's close", broadcast_publisher_close);
    run("fast: hpp queue: queue_create, queue_join, each role's calls and the end", queue_roles);
    run("fast: hpp queue: wait_for_members, cancel and a canceler", queue_members);
    run("fast: hpp queue: the destructor ends the queue", queue_destructor_ends);
    run("fast: hpp service: service_create, service_join, each role's calls and replies in order", service_roles);
    run("fast: hpp service: several clients, one server, then a pool of servers", service_clients_and_pool);
    run("fast: hpp service: a server's close abandons its request (errc::abandoned)", service_abandoned);
    run("fast: hpp service: wait_for_members, cancel and a canceler", service_members);
    run("fast: hpp service: the destructor ends the service", service_destructor_ends);
    run("fast: hpp service: call() sends a request and returns its reply, on a client only", service_call);
    run("fast: hpp service: a call that gave up leaves its reply owed, and the next call drops it", service_call_owed);
    run("fast: hpp service: a call whose timeout runs out on the drop sends nothing", service_call_deadline);
    run("fast: hpp service: a call's errc::abandoned, owed or not", service_call_abandoned);
    run("fast: hpp service: a call after the service's end", service_call_end);
    run("fast: hpp service: a call's cancel, and a call on a cancelled connection", service_call_cancel);
    run("fast: hpp batches: send_many and receive_many on a stream", batches);
    run("fast: hpp service: a batch of requests is owed as send()'s are", service_batches);
    run("slow: hpp peer process exits", peer_process_exits);
    run("slow: hpp broadcast: C++ subscriber processes of a C publisher", subscriber_processes);
    run("slow: hpp python peer", python_peer);
    run("slow: hpp python keeps the library loaded", python_keeps_library);
    finished = true;
    watchdog.join();
    std::printf("fipc_hpp_test %s%d test(s), %d failed check(s)\n", tier.c_str(), ran, failures);
    return failures == 0 ? 0 : 1;
}
