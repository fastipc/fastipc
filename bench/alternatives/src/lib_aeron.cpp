// Aeron through its C client (aeronc.h) over aeron:ipc, with the C media driver (aeronmd, which run.py starts) in a
// process of its own and its default configuration (its directory on Linux in /dev/shm, as by default). Publications are exclusive (one publisher each); a receive polls
// up to its fragment limit (16 by default) through a fragment assembler (a message longer than the MTU arrives in fragments), and its
// handler copies each message out of the log buffer. Every receive polls: Aeron's receive never blocks.
// ALT_AERON_FRAGMENT_LIMIT=<n> polls up to n fragments instead of 16 (run.py sets it: AERON_FRAGMENT_LIMIT, 1, or
// --aeron-fragment-limit).
// --batch N: a sender offers N messages at a time as one block of frames it formats itself
// (aeron_exclusive_publication_offer_block: one copy into the term, one store of its tail), and a receive polls up to N
// fragments, whatever ALT_AERON_FRAGMENT_LIMIT says.
// --buffer B sets the publication's term length to 2B (term-length in its channel), so that its window, half a term,
// is B: a power of two, 64 KiB to 1 GiB, a term at least 8 messages long (Aeron's longest message is an eighth of a
// term); another B isn't run. Without it, the default term (64 MiB, a 32 MiB window).
//   stream     an exclusive publication and one subscription (a round trip: a stream id each way); flow control holds
//              the publisher back at the subscriber's window
//   broadcast  an exclusive publication and many subscriptions of one stream, each held to by flow control
// No work queue, no service: every subscription of a stream receives every message.
#include "harness.hpp"

extern "C"
{
#include <aeronc.h>

#include <protocol/aeron_udp_protocol.h>
}

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

namespace alt
{
namespace
{

constexpr const char* channel = "aeron:ipc";

// The publications' channel: with --buffer, its term length.
std::string publication_channel(const Params& p)
{
    return p.buffer ? std::string(channel) + "?term-length=" + std::to_string(2 * p.buffer) : std::string(channel);
}

// The most fragments one poll takes: 16, or ALT_AERON_FRAGMENT_LIMIT.
size_t fragment_limit()
{
    const char* text = std::getenv("ALT_AERON_FRAGMENT_LIMIT");
    int limit = text ? std::atoi(text) : 16;
    return limit > 0 ? static_cast<size_t>(limit) : 16;
}

[[noreturn]] void aeron_fail(const char* what)
{
    fail(what, aeron_errmsg());
}

struct AeronEndpoint : Endpoint
{
    aeron_context_t* context = nullptr;
    aeron_t* client = nullptr;
    aeron_exclusive_publication_t* publication = nullptr;
    aeron_subscription_t* subscription = nullptr;
    aeron_fragment_assembler_t* assembler = nullptr;
    std::vector<uint8_t> buffer;
    std::string publication_uri;
    Sink* sink = nullptr;
    int delivered = 0;
    size_t limit;
    // --batch: the block of frames a batch is offered as, and the publication's constants its headers take.
    std::vector<uint8_t> block;
    aeron_publication_constants_t constants{};

    // The driver's directory: AERON_DIR, which run.py sets for the driver and every client alike.
    AeronEndpoint(const Params& p)
        : buffer(p.size), publication_uri(publication_channel(p)),
          limit(p.batch > 1 ? static_cast<size_t>(p.batch) : fragment_limit())
    {
        if (aeron_context_init(&context) < 0)
            aeron_fail("aeron_context_init");
        if (aeron_init(&client, context) < 0)
            aeron_fail("aeron_init");
        if (aeron_start(client) < 0)
            aeron_fail("aeron_start");
        if (aeron_fragment_assembler_create(&assembler, on_message, this) < 0)
            aeron_fail("aeron_fragment_assembler");
    }

    ~AeronEndpoint() override
    {
        if (publication)
            aeron_exclusive_publication_close(publication, nullptr, nullptr);
        if (subscription)
            aeron_subscription_close(subscription, nullptr, nullptr);
        aeron_close(client);
        aeron_context_close(context);
        aeron_fragment_assembler_delete(assembler);
    }

    void add_publication(int32_t stream_id)
    {
        aeron_async_add_exclusive_publication_t* async = nullptr;
        if (aeron_async_add_exclusive_publication(&async, client, publication_uri.c_str(), stream_id) < 0)
        {
            aeron_fail("aeron_async_add_exclusive_publication");
        }
        int done;
        while ((done = aeron_async_add_exclusive_publication_poll(&publication, async)) == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (done < 0)
            aeron_fail("aeron_async_add_exclusive_publication_poll");
        while (!aeron_exclusive_publication_is_connected(publication))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void add_subscription(int32_t stream_id)
    {
        aeron_async_add_subscription_t* async = nullptr;
        if (aeron_async_add_subscription(&async, client, channel, stream_id, nullptr, nullptr, nullptr, nullptr) < 0)
        {
            aeron_fail("aeron_async_add_subscription");
        }
        int done;
        while ((done = aeron_async_add_subscription_poll(&subscription, async)) == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (done < 0)
            aeron_fail("aeron_async_add_subscription_poll");
    }

    static void on_message(void* clientd, const uint8_t* data, size_t length, aeron_header_t*)
    {
        auto* self = static_cast<AeronEndpoint*>(clientd);
        std::memcpy(self->buffer.data(), data, length < self->buffer.size() ? length : self->buffer.size());
        self->sink->on(self->buffer.data(), length);
        self->delivered++;
    }

    void send(const uint8_t* data, size_t len) override
    {
        for (;;)
        {
            int64_t result = aeron_exclusive_publication_offer(publication, data, len, nullptr, nullptr);
            if (result >= 0)
                return;
            if (result == AERON_PUBLICATION_CLOSED || result == AERON_PUBLICATION_ERROR
                || result == AERON_PUBLICATION_MAX_POSITION_EXCEEDED)
            {
                aeron_fail("aeron_exclusive_publication_offer");
            }
        }
    }

    // The n messages as one block of unfragmented frames at the publication's position; a block that doesn't fit the rest
    // of the term goes as single offers, which pad the term and move on to the next.
    void send_batch(const uint8_t* data, size_t len, int n) override
    {
        if (!constants.term_buffer_length && aeron_exclusive_publication_constants(publication, &constants) < 0)
            aeron_fail("aeron_exclusive_publication_constants");
        const size_t frame = AERON_DATA_HEADER_LENGTH + len;
        const size_t aligned = (frame + AERON_FRAME_ALIGNMENT - 1) & ~size_t(AERON_FRAME_ALIGNMENT - 1);
        const size_t total = aligned * n;
        block.resize(total);
        const size_t term_length = constants.term_buffer_length;
        for (;;)
        {
            int64_t position = aeron_exclusive_publication_position(publication);
            if (position < 0)
                aeron_fail("aeron_exclusive_publication_position");
            int32_t term_offset = static_cast<int32_t>(position & static_cast<int64_t>(term_length - 1));
            if (term_length - term_offset < total)
            {
                for (int k = 0; k < n; k++)
                    send(data + k * len, len);
                return;
            }
            int32_t term_id =
                constants.initial_term_id + static_cast<int32_t>(position >> constants.position_bits_to_shift);
            for (int k = 0; k < n; k++)
            {
                auto* header = reinterpret_cast<aeron_data_header_t*>(block.data() + k * aligned);
                header->frame_header.frame_length = static_cast<int32_t>(frame);
                header->frame_header.version = AERON_FRAME_HEADER_VERSION;
                header->frame_header.flags = AERON_DATA_HEADER_UNFRAGMENTED;
                header->frame_header.type = AERON_HDR_TYPE_DATA;
                header->term_offset = term_offset + static_cast<int32_t>(k * aligned);
                header->session_id = constants.session_id;
                header->stream_id = constants.stream_id;
                header->term_id = term_id;
                header->reserved_value = 0;
                std::memcpy(block.data() + k * aligned + AERON_DATA_HEADER_LENGTH, data + k * len, len);
            }
            int64_t result = aeron_exclusive_publication_offer_block(publication, block.data(), total);
            if (result >= 0)
                return;
            if (result == AERON_PUBLICATION_CLOSED || result == AERON_PUBLICATION_ERROR
                || result == AERON_PUBLICATION_MAX_POSITION_EXCEEDED)
            {
                aeron_fail("aeron_exclusive_publication_offer_block");
            }
        }
    }

    int recv(Sink& target, int timeout_ms) override
    {
        sink = &target;
        delivered = 0;
        auto attempt = [&]
        {
            if (aeron_subscription_poll(subscription, aeron_fragment_assembler_handler, assembler, limit) < 0)
            {
                aeron_fail("aeron_subscription_poll");
            }
            return delivered > 0;
        };
        if (attempt() || timeout_ms == 0)
            return delivered;
        uint64_t deadline = ticks() + static_cast<uint64_t>(ticks_per_second() * timeout_ms / 1000);
        for (;;)
        {
            for (int k = 0; k < 256; k++)
            {
                if (attempt())
                    return delivered;
            }
            if (ticks() >= deadline)
                return 0;
        }
    }
};

bool supports(Pattern pattern, const Params& p)
{
    size_t term = 2 * p.buffer;
    bool takes =
        !p.buffer || ((term & (term - 1)) == 0 && term >= 64 * 1024 && term <= (size_t(1) << 30) && p.size <= term / 8);
    return (pattern == Pattern::stream || pattern == Pattern::broadcast) && !p.lossy && takes;
}

// Each run gets stream ids of its own, from the index run.py puts in the name's last digits.
int32_t stream_base(const Params& p)
{
    size_t digits = p.name.find_last_not_of("0123456789");
    return 1000 + 2 * std::stoi(p.name.substr(digits + 1));
}

std::unique_ptr<Endpoint> open(Pattern, Role role, const Params& p)
{
    auto endpoint = std::make_unique<AeronEndpoint>(p);
    int32_t forward = stream_base(p), back = forward + 1;
    switch (role)
    {
        case Role::tx:
        case Role::pub:
            endpoint->add_publication(forward);
            break;
        case Role::rx:
        case Role::sub:
            endpoint->add_subscription(forward);
            break;
        case Role::ping:
            endpoint->add_subscription(back);
            endpoint->add_publication(forward);
            break;
        case Role::echo:
            endpoint->add_subscription(forward);
            endpoint->add_publication(back);
            break;
        default:
            fail("aeron: no such role");
    }
    return endpoint;
}

}  // namespace

extern const Lib lib_aeron{"aeron", supports, open, true};

}  // namespace alt
