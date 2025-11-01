//! Radix Join Large Dataset Correctness Test
//!
//! Verifies that radix join produces correct results on datasets with 1M+ rows.
//! This is a stress test to ensure the implementation handles large-scale joins.
//!
//! **Performance Note**: This test takes ~10-20 seconds to run.
//! Part of Milestone 1.2.0 Phase 2 completion.

const std = @import("std");
const testing = std.testing;
const rozes = @import("rozes");
const DataFrame = rozes.DataFrame;
const ColumnDesc = rozes.ColumnDesc;

/// Verify radix join correctness with 1M×1M join
///
/// **Test Strategy**:
/// 1. Create two DataFrames with 1M rows each
/// 2. Join on Int64 column (triggers radix join optimization)
/// 3. Verify result correctness by checking row count and values
/// 4. Compare against expected result
///
/// **Expected Result**:
/// - Input: left (1M rows, keys 0..999999), right (1M rows, keys 0..999999)
/// - Join on "id" column
/// - Expected: 1M matching rows (every left row has exactly one right match)
test "Radix join correctness: 1M × 1M rows (uniform distribution)" {
    const allocator = testing.allocator;

    std.debug.print("\n[Integration Test] Radix join 1M × 1M rows\n", .{});
    std.debug.print("  This may take 10-20 seconds...\n", .{});

    const size: u32 = 1_000_000;

    // Create left DataFrame
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("left_value", .Int64, 1),
    };

    var left = try DataFrame.create(allocator, &left_cols, size);
    defer left.deinit();

    const left_id_data = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_value_data = left.columns[1].asInt64Buffer() orelse unreachable;

    std.debug.print("  [1/5] Generating left DataFrame (1M rows)...\n", .{});
    var i: u32 = 0;
    while (i < size) : (i += 1) {
        left_id_data[i] = @intCast(i); // Unique keys 0..999999
        left_value_data[i] = @intCast(i * 10);
    }
    try left.setRowCount(size);

    // Create right DataFrame
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("right_value", .Int64, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, size);
    defer right.deinit();

    const right_id_data = right.columns[0].asInt64Buffer() orelse unreachable;
    const right_value_data = right.columns[1].asInt64Buffer() orelse unreachable;

    std.debug.print("  [2/5] Generating right DataFrame (1M rows)...\n", .{});
    i = 0;
    while (i < size) : (i += 1) {
        right_id_data[i] = @intCast(i); // Same keys 0..999999
        right_value_data[i] = @intCast(i * 20);
    }
    try right.setRowCount(size);

    // Perform join
    std.debug.print("  [3/5] Performing radix join (1M × 1M)...\n", .{});
    const join_start = std.time.nanoTimestamp();

    var result = try left.innerJoin(allocator, &right, &[_][]const u8{"id"});
    defer result.deinit();

    const join_end = std.time.nanoTimestamp();
    const join_duration_ms = @as(f64, @floatFromInt(join_end - join_start)) / 1_000_000.0;

    std.debug.print("  [4/5] Join completed in {d:.2}ms\n", .{join_duration_ms});

    // Verify result correctness
    std.debug.print("  [5/5] Verifying correctness...\n", .{});

    // Should have exactly 1M rows (every left row matches exactly one right row)
    try testing.expectEqual(size, result.row_count);

    // Should have 4 columns (id, left_value, id_right, right_value)
    try testing.expectEqual(@as(usize, 4), result.columns.len);

    // Verify column names
    try testing.expectEqualStrings("id", result.columns[0].name);
    try testing.expectEqualStrings("left_value", result.columns[1].name);
    try testing.expectEqualStrings("id_right", result.columns[2].name);
    try testing.expectEqualStrings("right_value", result.columns[3].name);

    // Spot check: verify first 100 rows have correct values
    const result_id = result.columns[0].asInt64Buffer() orelse unreachable;
    const result_left_value = result.columns[1].asInt64Buffer() orelse unreachable;
    const result_right_value = result.columns[3].asInt64Buffer() orelse unreachable;

    i = 0;
    while (i < 100) : (i += 1) {
        try testing.expectEqual(@as(i64, @intCast(i)), result_id[i]);
        try testing.expectEqual(@as(i64, @intCast(i * 10)), result_left_value[i]);
        try testing.expectEqual(@as(i64, @intCast(i * 20)), result_right_value[i]);
    }

    std.debug.print("  ✓ Correctness verified!\n", .{});
    std.debug.print("  ✓ Result: {} rows, {} columns\n", .{ result.row_count, result.columns.len });
    std.debug.print("  ✓ Join time: {d:.2}ms\n\n", .{join_duration_ms});
}

/// Verify radix join with 50% key overlap (realistic join scenario)
test "Radix join correctness: 1M × 1M rows (50% overlap)" {
    const allocator = testing.allocator;

    std.debug.print("\n[Integration Test] Radix join 1M × 1M (50%% overlap)\n", .{});

    const size: u32 = 1_000_000;

    // Create left DataFrame
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("left_value", .Int64, 1),
    };

    var left = try DataFrame.create(allocator, &left_cols, size);
    defer left.deinit();

    const left_id_data = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_value_data = left.columns[1].asInt64Buffer() orelse unreachable;

    std.debug.print("  [1/4] Generating left DataFrame (1M rows)...\n", .{});
    var i: u32 = 0;
    while (i < size) : (i += 1) {
        left_id_data[i] = @intCast(i % 500_000); // Keys 0..499999 (50% unique)
        left_value_data[i] = @intCast(i);
    }
    try left.setRowCount(size);

    // Create right DataFrame
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("right_value", .Int64, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, size);
    defer right.deinit();

    const right_id_data = right.columns[0].asInt64Buffer() orelse unreachable;
    const right_value_data = right.columns[1].asInt64Buffer() orelse unreachable;

    std.debug.print("  [2/4] Generating right DataFrame (1M rows)...\n", .{});
    i = 0;
    while (i < size) : (i += 1) {
        right_id_data[i] = @intCast(i % 500_000); // Same 50% unique keys
        right_value_data[i] = @intCast(i);
    }
    try right.setRowCount(size);

    // Perform join
    std.debug.print("  [3/4] Performing radix join...\n", .{});
    const join_start = std.time.nanoTimestamp();

    var result = try left.innerJoin(allocator, &right, &[_][]const u8{"id"});
    defer result.deinit();

    const join_end = std.time.nanoTimestamp();
    const join_duration_ms = @as(f64, @floatFromInt(join_end - join_start)) / 1_000_000.0;

    std.debug.print("  [4/4] Join completed in {d:.2}ms\n", .{join_duration_ms});

    // Verify result correctness
    // Each left row (1M) matches 2 right rows (2M total matches)
    // Each right row (1M) matches 2 left rows
    // Result: 1M * 2 = 2M rows (Cartesian product within each key group)
    const expected_rows: u32 = 2_000_000;
    try testing.expectEqual(expected_rows, result.row_count);

    std.debug.print("  ✓ Correctness verified!\n", .{});
    std.debug.print("  ✓ Result: {} rows (expected: {})\n", .{ result.row_count, expected_rows });
    std.debug.print("  ✓ Join time: {d:.2}ms\n\n", .{join_duration_ms});
}

/// Performance benchmark: compare radix join vs standard hash join on 1M rows
test "Radix join performance: 1M × 1M (vs standard hash join)" {
    const allocator = testing.allocator;

    std.debug.print("\n[Performance Benchmark] Radix vs Standard Hash Join (1M × 1M)\n", .{});

    const size: u32 = 1_000_000;

    // Create Int64 DataFrames (triggers radix join)
    const int_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("value", .Int64, 1),
    };

    var left_int = try DataFrame.create(allocator, &int_cols, size);
    defer left_int.deinit();

    const left_int_id = left_int.columns[0].asInt64Buffer() orelse unreachable;
    const left_int_value = left_int.columns[1].asInt64Buffer() orelse unreachable;

    std.debug.print("  [1/5] Generating Int64 DataFrames...\n", .{});
    var i: u32 = 0;
    while (i < size) : (i += 1) {
        left_int_id[i] = @intCast(i % 500_000);
        left_int_value[i] = @intCast(i * 10);
    }
    try left_int.setRowCount(size);

    var right_int = try DataFrame.create(allocator, &int_cols, size);
    defer right_int.deinit();

    const right_int_id = right_int.columns[0].asInt64Buffer() orelse unreachable;
    const right_int_value = right_int.columns[1].asInt64Buffer() orelse unreachable;

    i = 0;
    while (i < size) : (i += 1) {
        right_int_id[i] = @intCast(i % 500_000);
        right_int_value[i] = @intCast(i * 20);
    }
    try right_int.setRowCount(size);

    // Benchmark radix join (Int64)
    std.debug.print("  [2/5] Benchmarking radix join (Int64)...\n", .{});
    const radix_start = std.time.nanoTimestamp();
    var radix_result = try left_int.innerJoin(allocator, &right_int, &[_][]const u8{"id"});
    const radix_end = std.time.nanoTimestamp();
    defer radix_result.deinit();

    const radix_ms = @as(f64, @floatFromInt(radix_end - radix_start)) / 1_000_000.0;
    std.debug.print("    Radix join: {d:.2}ms\n", .{radix_ms});

    // Create Float64 DataFrames (forces standard hash join)
    const float_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Float64, 0),
        ColumnDesc.init("value", .Float64, 1),
    };

    var left_float = try DataFrame.create(allocator, &float_cols, size);
    defer left_float.deinit();

    const left_float_id = left_float.columns[0].asFloat64Buffer() orelse unreachable;
    const left_float_value = left_float.columns[1].asFloat64Buffer() orelse unreachable;

    std.debug.print("  [3/5] Generating Float64 DataFrames...\n", .{});
    i = 0;
    while (i < size) : (i += 1) {
        left_float_id[i] = @floatFromInt(i % 500_000);
        left_float_value[i] = @floatFromInt(i * 10);
    }
    try left_float.setRowCount(size);

    var right_float = try DataFrame.create(allocator, &float_cols, size);
    defer right_float.deinit();

    const right_float_id = right_float.columns[0].asFloat64Buffer() orelse unreachable;
    const right_float_value = right_float.columns[1].asFloat64Buffer() orelse unreachable;

    i = 0;
    while (i < size) : (i += 1) {
        right_float_id[i] = @floatFromInt(i % 500_000);
        right_float_value[i] = @floatFromInt(i * 20);
    }
    try right_float.setRowCount(size);

    // Benchmark standard hash join (Float64)
    std.debug.print("  [4/5] Benchmarking standard hash join (Float64)...\n", .{});
    const hash_start = std.time.nanoTimestamp();
    var hash_result = try left_float.innerJoin(allocator, &right_float, &[_][]const u8{"id"});
    const hash_end = std.time.nanoTimestamp();
    defer hash_result.deinit();

    const hash_ms = @as(f64, @floatFromInt(hash_end - hash_start)) / 1_000_000.0;
    std.debug.print("    Standard hash join: {d:.2}ms\n", .{hash_ms});

    // Verify both produce same result count
    try testing.expectEqual(radix_result.row_count, hash_result.row_count);

    // Calculate speedup
    std.debug.print("  [5/5] Results:\n", .{});
    const speedup = hash_ms / radix_ms;
    std.debug.print("    Radix join:    {d:.2}ms\n", .{radix_ms});
    std.debug.print("    Standard join: {d:.2}ms\n", .{hash_ms});
    std.debug.print("    Speedup:       {d:.2}×\n", .{speedup});

    if (speedup >= 2.0) {
        std.debug.print("    ✓ PASS: Radix join ≥2× faster\n\n", .{});
    } else if (speedup >= 1.5) {
        std.debug.print("    ⚠ PARTIAL: Radix join {d:.2}× faster (target: 2×)\n\n", .{speedup});
    } else {
        std.debug.print("    ✗ FAIL: Radix join only {d:.2}× faster (target: 2×)\n\n", .{speedup});
        // Don't fail test - just report performance
    }
}
