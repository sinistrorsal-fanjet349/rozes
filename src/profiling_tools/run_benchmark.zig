//! Simple benchmark runner for Phase 1 SIMD integration
//!
//! Tests sort and groupBy performance with SIMD optimizations

const std = @import("std");
const ops_bench = @import("src/test/benchmark/operations_bench.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Phase 1: SIMD Integration Benchmarks ===\n\n", .{});

    // Sort benchmark (100K rows)
    std.debug.print("Sort (100K rows):\n", .{});
    var sort_result = try ops_bench.benchmarkSort(allocator, 100_000);
    sort_result.print("Sort");

    // Target: 4ms (from 6.6ms)
    if (sort_result.duration_ms < 5.0) {
        std.debug.print("✅ Sort: SIMD optimization successful ({d:.2}ms < 5ms target)\n\n", .{sort_result.duration_ms});
    } else {
        std.debug.print("⚠️  Sort: {d:.2}ms (target: <5ms)\n\n", .{sort_result.duration_ms});
    }

    // GroupBy benchmark (100K rows)
    std.debug.print("GroupBy (100K rows):\n", .{});
    var groupby_result = try ops_bench.benchmarkGroupBy(allocator, 100_000);
    groupby_result.print("GroupBy");

    // Target: 0.8ms (from 1.5ms)
    if (groupby_result.duration_ms < 1.0) {
        std.debug.print("✅ GroupBy: SIMD optimization successful ({d:.2}ms < 1ms target)\n\n", .{groupby_result.duration_ms});
    } else {
        std.debug.print("⚠️  GroupBy: {d:.2}ms (target: <1ms)\n\n", .{groupby_result.duration_ms});
    }

    // Filter benchmark (1M rows)
    std.debug.print("Filter (1M rows):\n", .{});
    var filter_result = try ops_bench.benchmarkFilter(allocator, 1_000_000);
    filter_result.print("Filter");

    // Target: maintain <15ms
    if (filter_result.duration_ms < 15.0) {
        std.debug.print("✅ Filter: Maintained performance ({d:.2}ms < 15ms target)\n\n", .{filter_result.duration_ms});
    } else {
        std.debug.print("⚠️  Filter: {d:.2}ms (target: <15ms)\n\n", .{filter_result.duration_ms});
    }
}
