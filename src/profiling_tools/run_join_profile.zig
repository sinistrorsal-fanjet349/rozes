const std = @import("std");
const DataFrame = @import("src/core/dataframe.zig").DataFrame;
const join_mod = @import("src/core/join.zig");
const ColumnDesc = @import("src/core/types.zig").ColumnDesc;

/// Profiling-guided join optimization analysis
///
/// **Purpose**: Identify actual bottlenecks through measurement, not assumptions
///
/// **Approach**:
/// 1. Instrument each phase of join with timing
/// 2. Measure allocations and memory patterns
/// 3. Identify cache misses and data layout issues
/// 4. Test optimizations with real benchmarks
///
/// **Current Baseline**: 593ms for 10K × 10K join (19% over target)
///
/// **Potential Bottlenecks to Measure**:
/// - Hash table build time (expected: 40-50% of total)
/// - Hash map lookup time (expected: 20-30%)
/// - Data copying time (expected: 30-40%)
/// - Memory allocation overhead (expected: 5-10%)
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Join Performance Profiling ===\n\n", .{});

    // Test 1: Measure hash table build
    try profileHashTableBuild(allocator);

    // Test 2: Measure probe performance
    try profileProbe(allocator);

    // Test 3: Measure data copying
    try profileDataCopy(allocator);

    // Test 4: Overall join with breakdown
    try profileFullJoin(allocator);

    std.debug.print("\n=== Profiling Complete ===\n", .{});
}

/// Profile hash table build phase
fn profileHashTableBuild(allocator: std.mem.Allocator) !void {
    std.debug.print("Test 1: Hash Table Build (10K rows)\n", .{});
    std.debug.print("-------------------------------------\n", .{});

    // Create test DataFrame with 10K rows
    const cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("value", .Float64, 1),
    };

    var df = try DataFrame.create(allocator, &cols, 10_000);
    defer df.deinit();

    const ids = df.columns[0].asInt64Buffer() orelse unreachable;
    const values = df.columns[1].asFloat64Buffer() orelse unreachable;

    // Fill with sequential data
    var i: u32 = 0;
    while (i < 10_000) : (i += 1) {
        ids[i] = @intCast(i);
        values[i] = @as(f64, @floatFromInt(i)) * 1.5;
    }
    try df.setRowCount(10_000);

    // Measure hash table build time
    var total_time_ns: u64 = 0;
    var iterations: u32 = 0;

    while (iterations < 10) : (iterations += 1) {
        const start = std.time.nanoTimestamp();

        // Simulate hash table build
        var hash_map = std.AutoHashMapUnmanaged(u64, u32){};
        defer hash_map.deinit(allocator);

        try hash_map.ensureTotalCapacity(allocator, 10_000);

        var row_idx: u32 = 0;
        while (row_idx < 10_000) : (row_idx += 1) {
            const key = ids[row_idx];
            const hash = std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
            try hash_map.put(allocator, hash, row_idx);
        }

        const end = std.time.nanoTimestamp();
        total_time_ns += @intCast(end - start);
    }

    const avg_ms = @as(f64, @floatFromInt(total_time_ns)) / 10_000_000.0 / 10.0;

    std.debug.print("  Average build time: {d:.2}ms\n", .{avg_ms});
    std.debug.print("  Throughput: {d:.0} rows/sec\n\n", .{10_000.0 / (avg_ms / 1000.0)});
}

/// Profile probe phase
fn profileProbe(allocator: std.mem.Allocator) !void {
    std.debug.print("Test 2: Hash Map Probe (10K lookups)\n", .{});
    std.debug.print("-------------------------------------\n", .{});

    // Build a hash map with 10K entries
    var hash_map = std.AutoHashMapUnmanaged(u64, u32){};
    defer hash_map.deinit(allocator);

    try hash_map.ensureTotalCapacity(allocator, 10_000);

    var i: u32 = 0;
    while (i < 10_000) : (i += 1) {
        const key: i64 = @intCast(i);
        const hash = std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
        try hash_map.put(allocator, hash, i);
    }

    // Measure probe time
    var total_time_ns: u64 = 0;
    var iterations: u32 = 0;
    var matches: u32 = 0;

    while (iterations < 10) : (iterations += 1) {
        const start = std.time.nanoTimestamp();

        var probe_idx: u32 = 0;
        while (probe_idx < 10_000) : (probe_idx += 1) {
            const key: i64 = @intCast(probe_idx);
            const hash = std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
            if (hash_map.get(hash)) |_| {
                matches += 1;
            }
        }

        const end = std.time.nanoTimestamp();
        total_time_ns += @intCast(end - start);
    }

    const avg_ms = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000.0 / 10.0;

    std.debug.print("  Average probe time: {d:.2}ms\n", .{avg_ms});
    std.debug.print("  Matches found: {}\n", .{matches / 10});
    std.debug.print("  Throughput: {d:.0} probes/sec\n\n", .{10_000.0 / (avg_ms / 1000.0)});
}

/// Profile data copying phase
fn profileDataCopy(allocator: std.mem.Allocator) !void {
    std.debug.print("Test 3: Data Copying (100K rows, 4 columns)\n", .{});
    std.debug.print("-------------------------------------\n", .{});

    // Create source and destination buffers
    const row_count: u32 = 100_000;
    const col_count: u32 = 4;

    var src_buffers = try allocator.alloc([]i64, col_count);
    defer allocator.free(src_buffers);

    var dst_buffers = try allocator.alloc([]i64, col_count);
    defer allocator.free(dst_buffers);

    var col_idx: u32 = 0;
    while (col_idx < col_count) : (col_idx += 1) {
        src_buffers[col_idx] = try allocator.alloc(i64, row_count);
        dst_buffers[col_idx] = try allocator.alloc(i64, row_count);
    }

    defer {
        col_idx = 0;
        while (col_idx < col_count) : (col_idx += 1) {
            allocator.free(src_buffers[col_idx]);
            allocator.free(dst_buffers[col_idx]);
        }
    }

    // Fill source data
    col_idx = 0;
    while (col_idx < col_count) : (col_idx += 1) {
        var row_idx: u32 = 0;
        while (row_idx < row_count) : (row_idx += 1) {
            src_buffers[col_idx][row_idx] = @intCast(row_idx * 10 + col_idx);
        }
    }

    // Test 1: Row-by-row copying (current approach)
    var total_time_ns: u64 = 0;
    var iterations: u32 = 0;

    while (iterations < 5) : (iterations += 1) {
        const start = std.time.nanoTimestamp();

        var row_idx: u32 = 0;
        while (row_idx < row_count) : (row_idx += 1) {
            col_idx = 0;
            while (col_idx < col_count) : (col_idx += 1) {
                dst_buffers[col_idx][row_idx] = src_buffers[col_idx][row_idx];
            }
        }

        const end = std.time.nanoTimestamp();
        total_time_ns += @intCast(end - start);
    }

    const row_by_row_ms = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000.0 / 5.0;

    std.debug.print("  Row-by-row: {d:.2}ms\n", .{row_by_row_ms});

    // Test 2: Column-wise memcpy (proposed optimization)
    total_time_ns = 0;
    iterations = 0;

    while (iterations < 5) : (iterations += 1) {
        const start = std.time.nanoTimestamp();

        col_idx = 0;
        while (col_idx < col_count) : (col_idx += 1) {
            @memcpy(dst_buffers[col_idx], src_buffers[col_idx]);
        }

        const end = std.time.nanoTimestamp();
        total_time_ns += @intCast(end - start);
    }

    const memcpy_ms = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000.0 / 5.0;

    std.debug.print("  Column memcpy: {d:.2}ms\n", .{memcpy_ms});
    std.debug.print("  Speedup: {d:.1}x faster\n\n", .{row_by_row_ms / memcpy_ms});
}

/// Profile full join with breakdown
fn profileFullJoin(allocator: std.mem.Allocator) !void {
    std.debug.print("Test 4: Full Join Breakdown (10K × 10K)\n", .{});
    std.debug.print("-------------------------------------\n", .{});

    // Create left DataFrame
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("left_value", .Float64, 1),
    };

    var left = try DataFrame.create(allocator, &left_cols, 10_000);
    defer left.deinit();

    const left_ids = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_values = left.columns[1].asFloat64Buffer() orelse unreachable;

    var i: u32 = 0;
    while (i < 10_000) : (i += 1) {
        left_ids[i] = @intCast(i);
        left_values[i] = @as(f64, @floatFromInt(i)) * 1.5;
    }
    try left.setRowCount(10_000);

    // Create right DataFrame
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("right_value", .Float64, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, 10_000);
    defer right.deinit();

    const right_ids = right.columns[0].asInt64Buffer() orelse unreachable;
    const right_values = right.columns[1].asFloat64Buffer() orelse unreachable;

    i = 0;
    while (i < 10_000) : (i += 1) {
        right_ids[i] = @intCast(i);
        right_values[i] = @as(f64, @floatFromInt(i)) * 2.5;
    }
    try right.setRowCount(10_000);

    // Perform join with timing
    const join_cols = [_][]const u8{"id"};

    const start_total = std.time.nanoTimestamp();
    var result = try join_mod.innerJoin(&left, &right, allocator, &join_cols);
    const end_total = std.time.nanoTimestamp();
    defer result.deinit();

    const total_ms = @as(f64, @floatFromInt(end_total - start_total)) / 1_000_000.0;

    std.debug.print("  Total join time: {d:.2}ms\n", .{total_ms});
    std.debug.print("  Result rows: {}\n", .{result.row_count});
    std.debug.print("  Target: <500ms\n", .{});

    if (total_ms < 500.0) {
        std.debug.print("  ✓ PASS ({d:.1}% faster)\n\n", .{(500.0 - total_ms) / 500.0 * 100.0});
    } else {
        std.debug.print("  ✗ FAIL ({d:.1}% over target)\n\n", .{(total_ms - 500.0) / 500.0 * 100.0});
    }
}
