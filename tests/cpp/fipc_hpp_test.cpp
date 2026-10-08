// The tests of the C++ wrapper, include/fipc.hpp: how it maps the C API (handles that move and close once, results
// and timeouts, the zero-copy slots, RPC, cancel from another thread, the peer's end), and, against other processes,
// a peer process that exits and a Python peer (bindings/python).
//
// `fipc_hpp_test fast|slow <the test program> <the repository> [--no-interop] [filter...]` runs the tier's tests (with
// filters, only those whose name contains one). `zig build test` runs the fast tier, `zig build test-slow` the slow
// one; both build it with -fno-exceptions -fno-rtti and every warning an error. The Python peer is tests/cpp/peer.py,
// run by FIPC_TEST_PYTHON, else the repository's venv, else python; without one that has cffi the interop test says
// so and passes, unless FIPC_REQUIRE_INTEROP is set.

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS  // std::getenv
#endif

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <fipc.hpp>

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

// Connects to `name` on a thread of its own while the caller accepts: connect waits for the listener's accept, so a
// client and its server in one process run on different threads.
struct connector
{
    std::optional<fipc::result<fipc::connection>> result;
    std::thread thread;

    explicit connector(const std::string& name)
        : thread([this, name] { result.emplace(fipc::connection::connect(name, 10s)); })
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
    auto listener = fipc::listener::listen(name, capacity);
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

// Runs `command` through the shell (std::system) on a thread of its own; join() returns its exit status.
class process
{
  public:
    explicit process(std::string command)
    {
#ifdef _WIN32
        command = "\"" + command + "\"";  // cmd.exe strips the outer quotes
#endif
        thread_ = std::thread([this, command] { status_ = std::system(command.c_str()); });
    }

    process(const process&) = delete;
    process& operator=(const process&) = delete;

    int join()
    {
        thread_.join();
        return status_;
    }

  private:
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
    auto bad_capacity = fipc::listener::listen(unique_name("capacity"), 1000);
    EXPECT(!bad_capacity && bad_capacity.error() == fipc::errc::invalid);
    EXPECT(fipc::listener::listen("bad name", 1 << 16).error() == fipc::errc::invalid);
    EXPECT(fipc::listener::listen(std::string_view("nul\0name", 8), 1 << 16).error() == fipc::errc::invalid);
    EXPECT(fipc::listener::listen(std::string(300, 'a'), 1 << 16).error() == fipc::errc::invalid);
    const std::string name = unique_name("in_use");
    auto first = fipc::listener::listen(name, 1 << 16);
    EXPECT(first.has_value());
    EXPECT(fipc::listener::listen(name, 1 << 16).error() == fipc::errc::addr_in_use);
}

void handles_move_and_close_once()
{
    const std::string name = unique_name("moves");
    auto listened = fipc::listener::listen(name, 1 << 16);
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
    auto again = fipc::listener::listen(name, 1 << 16);
    EXPECT(again.has_value());
    // The C API's handles can be adopted
    fipc_listener_t* raw = nullptr;
    if (EXPECT(fipc_listen(unique_name("adopt").c_str(), 1 << 16, &raw) == FIPC_OK))
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
    EXPECT(fipc::connection::connect(unique_name("nobody"), fipc::no_wait).error() == fipc::errc::timeout);
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

void rpc_round_trip()
{
    pair p;
    if (!open_pair(p, "rpc"))
        return;
    auto id = p.client.rpc_submit(1, "ping");
    EXPECT(id.has_value() && *id == 1);  // a connection numbers its requests from 1

    std::string payload;
    auto request = p.server.rpc_receive(payload, 10s);
    if (EXPECT(request.has_value()))
    {
        EXPECT(request->kind == fipc::rpc_kind::request && request->opcode == 1 && request->id == 1);
        EXPECT(request->status == 0 && request->len == 4 && payload == "ping");
        EXPECT(p.server.rpc_respond(request->id, request->opcode, 7, "PING").has_value());
    }
    char buf[64];
    auto response = p.client.rpc_receive_into(buf, 10s);
    if (EXPECT(response.has_value()))
    {
        EXPECT(response->kind == fipc::rpc_kind::response && response->id == 1 && response->status == 7);
        EXPECT(std::string(buf, response->len) == "PING");
    }

    // Empty payloads, bytes, and a payload too large for the buffer
    EXPECT(p.client.rpc_submit(2, std::span<const std::byte>()).value_or(0) == 2);
    auto empty = p.server.rpc_receive(payload, 10s);
    EXPECT(empty.has_value() && empty->len == 0 && payload.empty() && empty->opcode == 2);
    const auto bytes = pattern(1000);
    EXPECT(p.server.rpc_respond(2, 2, -3, bytes).has_value());
    auto too_large = p.client.rpc_receive_into(buf, 10s);
    EXPECT(too_large.error() == fipc::errc::too_large && too_large.error().len == 1000);
    std::vector<std::byte> grown;
    auto reply = p.client.rpc_receive(grown, 10s);
    EXPECT(reply.has_value() && reply->status == -3 && grown == bytes);

    // A plain message isn't an RPC message: dropped, errc::invalid
    EXPECT(p.client.send("plain").has_value());
    EXPECT(p.server.rpc_receive(payload, 10s).error() == fipc::errc::invalid);
    EXPECT(p.server.rpc_receive(payload, fipc::no_wait).error() == fipc::errc::timeout);
}

void cancel_from_another_thread()
{
    // A listener: the thread that waits in accept returns cancelled
    const std::string name = unique_name("cancel");
    auto listener = fipc::listener::listen(name, 1 << 16);
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

// === Slow: other processes ===

// The peer process exits without closing its connection (as a crash would): the server gets the messages it sent,
// then errc::disconnected.
void peer_process_exits()
{
    const std::string name = unique_name("exit");
    auto listener = fipc::listener::listen(name, 1 << 16);
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
    auto conn = fipc::connection::connect(name, 30s);
    if (!conn)
        return 1;
    for (const char* message : {"one", "two", "three"})
        if (!conn->send(message))
            return 2;
    std::_Exit(0);
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

    // A C++ server and a Python client, RPC both ways
    {
        const std::string name = unique_name("py_client");
        auto listener = fipc::listener::listen(name, 1 << 16);
        if (!EXPECT(listener.has_value()))
            return;
        process peer = start_python(interpreter, "client", name);
        auto conn = listener->accept(60s);
        if (EXPECT(conn.has_value()))
        {
            std::string payload;
            auto request = conn->rpc_receive(payload, 30s);
            EXPECT(request.has_value() && request->opcode == 7 && payload == "hello from Python");
            EXPECT(conn->rpc_respond(request->id, 7, 42, "HELLO FROM PYTHON", 30s).has_value());
            auto id = conn->rpc_submit(9, "C++", 30s);
            auto reply = conn->rpc_receive(payload, 30s);
            EXPECT(id && reply && reply->id == *id && reply->status == 3 && payload == "C++C++");
            conn->close();
        }
        EXPECT(peer.join() == 0);
    }

    // A Python server and a C++ client: a request longer than the ring, sent while the reply is received
    {
        const std::string name = unique_name("py_server");
        process peer = start_python(interpreter, "server", name);
        auto conn = fipc::connection::connect(name, 60s);
        if (EXPECT(conn.has_value()))
        {
            std::string big(300000, 'x');
            for (std::size_t i = 0; i < big.size(); i++)
                big[i] = static_cast<char>('a' + i % 26);
            std::uint64_t id = 0;
            std::thread sender([&] { id = conn->rpc_submit(2, big, 30s).value_or(0); });
            std::string payload;
            auto reply = conn->rpc_receive(payload, 30s);
            sender.join();
            std::string upper = big;
            for (char& c : upper)
                c = static_cast<char>(c - 'a' + 'A');
            EXPECT(reply.has_value() && reply->id == id && reply->status == -1 && payload == upper);
            conn->close();
        }
        EXPECT(peer.join() == 0);
    }
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

    // A test that hangs ends the run: no test takes a minute
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
    run("fast: hpp rpc round trip", rpc_round_trip);
    run("fast: hpp cancel from another thread", cancel_from_another_thread);
    run("fast: hpp peer's end", peer_end);
    run("slow: hpp peer process exits", peer_process_exits);
    run("slow: hpp python peer", python_peer);
    finished = true;
    watchdog.join();
    std::printf("fipc_hpp_test %s%d test(s), %d failed check(s)\n", tier.c_str(), ran, failures);
    return failures == 0 ? 0 : 1;
}
