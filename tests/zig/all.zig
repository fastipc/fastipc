//! The root of the C-API test suite and of the native API's multi-process tests: every test file of tests/zig, those
//! that apply to every kind at the top, each kind's in its folder. The build compiles it twice, once per tier, keeping
//! the tests whose names start with "fast: " (`zig build test`) or "slow: " (`zig build test-slow`).

test {
    _ = @import("api_test.zig");
    _ = @import("names_test.zig");
    _ = @import("admission_test.zig");
    _ = @import("batch_test.zig");
    _ = @import("native_peer_death_test.zig");
    _ = @import("stream/lifecycle_test.zig");
    _ = @import("stream/messages_test.zig");
    _ = @import("stream/wake_test.zig");
    _ = @import("stream/zero_copy_test.zig");
    _ = @import("stream/cancel_test.zig");
    _ = @import("stream/session_end_test.zig");
    _ = @import("stream/peer_death_test.zig");
    _ = @import("broadcast/broadcast_test.zig");
    _ = @import("broadcast/lossy_test.zig");
    _ = @import("broadcast/gated_test.zig");
    _ = @import("broadcast/unsubscribe_test.zig");
    _ = @import("broadcast/poller_test.zig");
    _ = @import("broadcast/count_test.zig");
    _ = @import("broadcast/processes_test.zig");
    _ = @import("broadcast/peer_death_test.zig");
    _ = @import("queue/queue_test.zig");
    _ = @import("queue/delivery_test.zig");
    _ = @import("queue/end_test.zig");
    _ = @import("queue/processes_test.zig");
    _ = @import("service/service_test.zig");
    _ = @import("service/calls_test.zig");
    _ = @import("service/end_test.zig");
    _ = @import("service/processes_test.zig");
}
