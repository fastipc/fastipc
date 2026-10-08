//! The root of the C-API test suite and of the native API's multi-process tests: every test file of tests/zig. The
//! build compiles it twice, once per tier, keeping the tests whose names start with "fast: " (`zig build test`) or
//! "slow: " (`zig build test-slow`).

test {
    _ = @import("api_test.zig");
    _ = @import("lifecycle_test.zig");
    _ = @import("names_test.zig");
    _ = @import("messages_test.zig");
    _ = @import("wake_test.zig");
    _ = @import("zero_copy_test.zig");
    _ = @import("rpc_test.zig");
    _ = @import("cancel_test.zig");
    _ = @import("session_end_test.zig");
    _ = @import("peer_death_test.zig");
    _ = @import("native_peer_death_test.zig");
}
