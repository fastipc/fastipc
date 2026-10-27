//! The two one-way fences of the channels' seqlocks (docs/protocol.md §9.6): a load fence orders the loads before it
//! before every access after it, a store fence the stores before it before the stores after it. They pair up as
//! message passing does: a writer stores A, `storeFence`, stores B; a reader loads B, `loadFence`, loads A: if the
//! reader saw B, it sees A (or newer).
//!
//! On x86-64 the hardware already keeps loads in order with loads and stores with stores (TSO): each fence is a
//! compiler barrier only. On aarch64 they are `dmb ishld` and `dmb ishst`, cheaper than the full `dmb ish` of a
//! sequentially consistent fence. Both are compiler barriers too: the compiler moves no memory access across them.

const builtin = @import("builtin");

/// Orders every load before it before every load and store after it.
pub inline fn loadFence() void {
    switch (builtin.cpu.arch) {
        .x86_64 => asm volatile ("" ::: .{ .memory = true }),
        .aarch64 => asm volatile ("dmb ishld" ::: .{ .memory = true }),
        else => @compileError("FastIPC supports x86-64 and aarch64"),
    }
}

/// Orders every store before it before every store after it.
pub inline fn storeFence() void {
    switch (builtin.cpu.arch) {
        .x86_64 => asm volatile ("" ::: .{ .memory = true }),
        .aarch64 => asm volatile ("dmb ishst" ::: .{ .memory = true }),
        else => @compileError("FastIPC supports x86-64 and aarch64"),
    }
}
