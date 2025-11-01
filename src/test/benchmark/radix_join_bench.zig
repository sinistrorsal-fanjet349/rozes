const std = @import("std");
const benchmark = @import("benchmark.zig");
const BenchmarkResult = benchmark.BenchmarkResult;

// Import from the rozes module (provided by build.zig)
const rozes = @import("rozes");
const radix_join = rozes.radix_join;
const DataFrame = rozes.DataFrame;
const Allocator = std.mem.Allocator;

/// Benchmark comparison: Radix Join vs Standard Hash Join
///
/// **Test Setup**:
/// - Create two DataFrames with Int64 join keys
/// - Left: size rows, Right: size rows
/// - Join on single Int64 column
/// - Measure join time for both approaches
///
/// **Expected Results**:
/// - Radix join: 2-3× faster than standard hash join for Int64 keys
/// - Speedup increases with dataset size (better amortization of partitioning cost)
/// - Target: <10ms for 10K×10K join with radix, <30ms with standard hash
///
/// **Breakdown**:
/// - Radix Join:
///   - Partitioning: 20-30% of time (build histogram + scatter)
///   - Hash table build: 30-40% of time (per-partition hash tables)
///   - Probe: 30-40% of time (SIMD + bloom filter optimized)
///   - Result assembly: 10-20% of time
///
/// - Standard Hash Join:
///   - Hash table build: 40-50% of time (single large hash table)
///   - Probe: 40-50% of time (no SIMD, no bloom filter)
///   - Result assembly: 10-20% of time
pub fn benchmarkRadixVsStandardJoin(allocator: Allocator, size: usize) !void {
    std.debug.assert(size > 0);
    std.debug.assert(size <= radix_join.MAX_TOTAL_ROWS);

    std.debug.print("\n" ++
        "========================================\n" ++
        "Radix Join vs Standard Hash Join\n" ++
        "Dataset: {d} × {d} rows\n" ++
        "========================================\n", .{ size, size });

    // Create test DataFrames
    const col_descs = [_]rozes.ColumnDesc{
        rozes.ColumnDesc.init("id", .Int64, 0),
        rozes.ColumnDesc.init("value", .Int64, 0),
    };

    var left_df = try DataFrame.create(allocator, &col_descs, @intCast(size));
    defer left_df.deinit();

    var right_df = try DataFrame.create(allocator, &col_descs, @intCast(size));
    defer right_df.deinit();

    // Fill with data (50% key overlap for realistic join selectivity)
    const left_id_data = left_df.columns[0].asInt64Buffer() orelse unreachable;
    const left_value_data = left_df.columns[1].asInt64Buffer() orelse unreachable;

    const right_id_data = right_df.columns[0].asInt64Buffer() orelse unreachable;
    const right_value_data = right_df.columns[1].asInt64Buffer() orelse unreachable;

    var i: u32 = 0;
    while (i < size) : (i += 1) {
        left_id_data[i] = @intCast(i % (size / 2)); // 50% unique keys
        left_value_data[i] = @intCast(i * 10);

        right_id_data[i] = @intCast(i % (size / 2)); // Same distribution
        right_value_data[i] = @intCast(i * 20);
    }

    try left_df.setRowCount(@intCast(size));
    try right_df.setRowCount(@intCast(size));

    // Benchmark 1: Radix Join (optimized for Int64)
    std.debug.print("\n[1/2] Radix Join (Int64 optimized)...\n", .{});

    const radix_start = std.time.nanoTimestamp();
    var radix_result = try left_df.innerJoin(allocator, &right_df, &[_][]const u8{"id"});
    const radix_end = std.time.nanoTimestamp();
    defer radix_result.deinit();

    const radix_time_ms = @as(f64, @floatFromInt(radix_end - radix_start)) / 1_000_000.0;
    std.debug.print("  ✓ Radix join: {d:.2}ms ({} result rows)\n", .{ radix_time_ms, radix_result.row_count });

    // Benchmark 2: Standard Hash Join (fallback path)
    // Force standard hash join by using a different join column name temporarily
    std.debug.print("\n[2/2] Standard Hash Join (baseline)...\n", .{});

    // Create DataFrames with Float64 to force standard hash join path
    const float_col_descs = [_]rozes.ColumnDesc{
        rozes.ColumnDesc.init("id", .Float64, 0),
        rozes.ColumnDesc.init("value", .Float64, 0),
    };

    var left_float = try DataFrame.create(allocator, &float_col_descs, @intCast(size));
    defer left_float.deinit();

    var right_float = try DataFrame.create(allocator, &float_col_descs, @intCast(size));
    defer right_float.deinit();

    // Copy data as Float64
    const left_float_id = left_float.columns[0].asFloat64Buffer() orelse unreachable;
    const left_float_value = left_float.columns[1].asFloat64Buffer() orelse unreachable;

    const right_float_id = right_float.columns[0].asFloat64Buffer() orelse unreachable;
    const right_float_value = right_float.columns[1].asFloat64Buffer() orelse unreachable;

    var j: u32 = 0;
    while (j < size) : (j += 1) {
        left_float_id[j] = @floatFromInt(j % (size / 2));
        left_float_value[j] = @floatFromInt(j * 10);

        right_float_id[j] = @floatFromInt(j % (size / 2));
        right_float_value[j] = @floatFromInt(j * 20);
    }

    try left_float.setRowCount(@intCast(size));
    try right_float.setRowCount(@intCast(size));

    const hash_start = std.time.nanoTimestamp();
    var hash_result = try left_float.innerJoin(allocator, &right_float, &[_][]const u8{"id"});
    const hash_end = std.time.nanoTimestamp();
    defer hash_result.deinit();

    const hash_time_ms = @as(f64, @floatFromInt(hash_end - hash_start)) / 1_000_000.0;
    std.debug.print("  ✓ Standard hash join: {d:.2}ms ({} result rows)\n", .{ hash_time_ms, hash_result.row_count });

    // Compare results
    std.debug.print("\n" ++
        "========================================\n" ++
        "RESULTS\n" ++
        "========================================\n", .{});
    std.debug.print("Radix Join:    {d:.2}ms\n", .{radix_time_ms});
    std.debug.print("Standard Join: {d:.2}ms\n", .{hash_time_ms});

    const speedup = hash_time_ms / radix_time_ms;
    std.debug.print("Speedup:       {d:.2}× ", .{speedup});

    if (speedup >= 2.0) {
        std.debug.print("✓ PASS (target: 2-3×)\n", .{});
    } else if (speedup >= 1.5) {
        std.debug.print("⚠ PARTIAL (below 2× target)\n", .{});
    } else {
        std.debug.print("✗ FAIL (below 1.5×)\n", .{});
    }

    // Verify correctness - both should produce same number of rows
    if (radix_result.row_count != hash_result.row_count) {
        std.debug.print("\n⚠ WARNING: Result row counts differ! " ++
            "Radix: {}, Standard: {}\n", .{ radix_result.row_count, hash_result.row_count });
    }

    std.debug.print("========================================\n\n", .{});
}

/// Benchmark radix join SIMD probe performance
///
/// **Expected Results**:
/// - SIMD probe: 1.5-2× faster than scalar for large partitions
/// - Speedup increases with partition size (more opportunities for SIMD batching)
/// - Target: <0.5ms for 10K row probe with 1000 operations

pub fn benchmarkRadixJoinProbe(allocator: Allocator, size: usize) !BenchmarkResult {
    std.debug.assert(size > 0);
    std.debug.assert(size <= radix_join.MAX_TOTAL_ROWS);

    // Create test data with some duplicate keys
    const test_data = try allocator.alloc(radix_join.KeyValue, size);
    defer allocator.free(test_data);

    // Fill with keys that have 10% duplicates
    var i: u32 = 0;
    while (i < size) : (i += 1) {
        const key = @as(i64, @intCast(i / 10)); // Every 10 rows share a key
        test_data[i] = radix_join.KeyValue.init(key, i);
    }

    // Build hash table
    var hash_table = try radix_join.buildHashTable(allocator, test_data);
    defer hash_table.deinit();

    // Benchmark probe operations
    const num_probes: usize = 1000;
    var matches = std.ArrayListUnmanaged(u32){};
    defer matches.deinit(allocator);

    // Warm up
    var warmup: usize = 0;
    while (warmup < 10) : (warmup += 1) {
        matches.clearRetainingCapacity();
        try hash_table.probe(42, &matches, allocator);
    }

    // Actual benchmark
    const start = std.time.nanoTimestamp();

    var probe_idx: usize = 0;
    while (probe_idx < num_probes) : (probe_idx += 1) {
        matches.clearRetainingCapacity();
        const key = @as(i64, @intCast(probe_idx % (size / 10)));
        try hash_table.probe(key, &matches, allocator);
    }

    const end = std.time.nanoTimestamp();

    // Return benchmark result with probe count as "rows processed"
    return BenchmarkResult.compute(start, end, num_probes);
}

/// Benchmark bloom filter early rejection for sparse joins
///
/// **Expected Results**:
/// - Bloom filter rejects non-matching keys immediately (~2-5 CPU cycles)
/// - Without bloom filter: Full hash table probe (~10-50 CPU cycles)
/// - Speedup: 2-10× for sparse joins (80%+ non-matching keys)
/// - Target: <0.2ms for 10K row table with 1000 non-matching probes
pub fn benchmarkBloomFilterRejection(allocator: Allocator, size: usize) !BenchmarkResult {
    std.debug.assert(size > 0);
    std.debug.assert(size <= radix_join.MAX_TOTAL_ROWS);

    // Create test data with known keys (0 to size-1)
    const test_data = try allocator.alloc(radix_join.KeyValue, size);
    defer allocator.free(test_data);

    var i: u32 = 0;
    while (i < size) : (i += 1) {
        const key = @as(i64, @intCast(i));
        test_data[i] = radix_join.KeyValue.init(key, i);
    }

    // Build hash table (with bloom filter)
    var hash_table = try radix_join.buildHashTable(allocator, test_data);
    defer hash_table.deinit();

    // Benchmark probes for NON-EXISTENT keys (bloom filter should reject early)
    const num_probes: usize = 1000;
    var matches = std.ArrayListUnmanaged(u32){};
    defer matches.deinit(allocator);

    // Warm up
    var warmup: usize = 0;
    while (warmup < 10) : (warmup += 1) {
        matches.clearRetainingCapacity();
        const non_existent_key = @as(i64, @intCast(size + warmup + 1000000));
        try hash_table.probe(non_existent_key, &matches, allocator);
    }

    // Actual benchmark - probe for keys NOT in hash table
    const start = std.time.nanoTimestamp();

    var probe_idx: usize = 0;
    while (probe_idx < num_probes) : (probe_idx += 1) {
        matches.clearRetainingCapacity();
        // Probe for keys way outside the range (guaranteed not to exist)
        const non_existent_key = @as(i64, @intCast(size + probe_idx + 1000000));
        try hash_table.probe(non_existent_key, &matches, allocator);

        // Verify bloom filter worked (no matches found)
        std.debug.assert(matches.items.len == 0);
    }

    const end = std.time.nanoTimestamp();

    // Report bloom filter statistics
    const fpr = hash_table.bloom_filter.getFalsePositiveRate();
    std.debug.print("  Bloom filter FPR: {d:.2}%\n", .{fpr * 100.0});
    std.debug.print("  Elements in bloom filter: {}\n", .{hash_table.bloom_filter.element_count});

    // Return benchmark result
    return BenchmarkResult.compute(start, end, num_probes);
}
