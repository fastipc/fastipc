// The comparison's harness: what every library's adapter implements, and the clock.
//
// A case runs one role per process (run.py starts them): a stream's sender and receiver (or its ping and echo), a
// broadcast's publisher and subscribers, a work queue's producers and consumers, a service's clients and servers.
// Every role talks to its peers through one library's endpoint. The work is the same for every library: a sender
// starts with the message in its own buffer, and a receiver ends with it in memory of its own (a library that copies
// receives into the receiver's buffer; one that hands out shared memory has it copied out).
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace alt
{

enum class Pattern
{
    stream,
    broadcast,
    queue,
    service
};

// tx/rx: a stream's sender and receiver; ping/echo: a stream's round trips; pub/sub: a broadcast; producer/consumer:
// a work queue; client/server: a service.
enum class Role
{
    tx,
    rx,
    ping,
    echo,
    pub,
    sub,
    producer,
    consumer,
    client,
    server
};

struct Params
{
    std::string name;  // unique for each run of a case
    std::string dir;   // a private directory for the run's files (socket files, Aeron's driver)
    size_t size = 16;  // a message's length (a service's requests and replies alike)
    int index = 0;     // this member's index among its role's
    int subscribers = 1, producers = 1, consumers = 1, clients = 1, servers = 1;
    bool lossy = false;  // a broadcast whose subscribers may lose messages rather than hold the publisher back
    int port = 0;        // TCP
    size_t buffer = 0;   // --buffer: the bytes a library that takes a buffer size buffers (0: its usual size)
    int batch = 1;       // --batch: messages per call, a sender's sends and a receiver's most per receive
};

// A receiver's consumer of messages: `data` is in memory of the receiver's own.
struct Sink
{
    virtual void on(const uint8_t* data, size_t len) = 0;
};

struct Endpoint
{
    virtual ~Endpoint() = default;
    // Sends one message, waiting for room while the peer holds it back.
    virtual void send(const uint8_t* data, size_t len) = 0;

    // Sends n messages of len bytes each, back to back at `data` (--batch): a library with batch calls overrides it
    // with one call.
    virtual void send_batch(const uint8_t* data, size_t len, int n)
    {
        for (int k = 0; k < n; k++)
            send(data + k * len, len);
    }

    // Delivers at least one message to `sink` (with --batch up to that many, taken by one of the library's calls), or
    // none if nothing came within timeout_ms (0: looks once), and returns how many it delivered.
    virtual int recv(Sink& sink, int timeout_ms) = 0;
};

// A library: which cases it has (a pattern it lacks, a broadcast mode, a pool of servers), its endpoints, and whether
// it has calls that send and receive messages in batches (--batch).
struct Lib
{
    const char* name;
    bool (*supports)(Pattern, const Params&);
    std::unique_ptr<Endpoint> (*open)(Pattern, Role, const Params&);
    bool batches = false;
};

const Lib* find_lib(const std::string& name);

// The CPU's counter (x86: the TSC; ARM64: the generic timer), which every process reads alike.
uint64_t ticks();
double ticks_per_second();

// The ring, buffer or queue depth a library that counts messages gets: --buffer's bytes of messages, at least 8 of
// them; without it, 512 KiB of messages (fastipc's usual ring), 8 to 1024 of them.
inline size_t depth_for(const Params& p)
{
    if (p.buffer)
        return p.buffer / p.size < 8 ? 8 : p.buffer / p.size;
    size_t depth = (512 * 1024) / p.size;
    return depth < 8 ? 8 : depth > 1024 ? 1024 : depth;
}

[[noreturn]] void fail(const char* what, const std::string& detail = "");

}  // namespace alt
