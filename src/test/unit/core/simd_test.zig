//! SIMD Aggregation Tests
//!
//! Tests for SIMD-accelerated aggregation functions in src/core/simd.zig
//! - Sum, Mean, Min, Max
//! - Variance, Standard Deviation
//! - Edge cases: empty, single element, odd lengths
//! - Performance: SIMD vs scalar consistency

const std = @import("std");
const testing = std.testing;
const simd = @import("../../../core/simd.zig");

// ===== Sum Tests =====

test "sumFloat64 computes correct sum" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const result = simd.sumFloat64(&data);
    try testing.expectEqual(@as(f64, 15.0), result);
}

test "sumFloat64 handles empty array" {
    const data = [_]f64{};
    const result = simd.sumFloat64(&data);
    try testing.expectEqual(@as(f64, 0.0), result);
}

test "sumFloat64 handles single element" {
    const data = [_]f64{42.0};
    const result = simd.sumFloat64(&data);
    try testing.expectEqual(@as(f64, 42.0), result);
}

test "sumFloat64 handles odd-length array" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0 }; // 7 elements (not multiple of 4)
    const result = simd.sumFloat64(&data);
    try testing.expectEqual(@as(f64, 28.0), result);
}

test "sumFloat64 handles large array" {
    const data = try testing.allocator.alloc(f64, 100);
    defer testing.allocator.free(data);

    for (data, 0..) |*val, i| {
        val.* = @as(f64, @floatFromInt(i + 1)); // 1, 2, 3, ..., 100
    }

    const result = simd.sumFloat64(data);
    // Sum of 1 to 100 = 100 * 101 / 2 = 5050
    try testing.expectEqual(@as(f64, 5050.0), result);
}

test "sumInt64 computes correct sum" {
    const data = [_]i64{ 10, 20, 30, 40, 50 };
    const result = simd.sumInt64(&data);
    try testing.expectEqual(@as(i64, 150), result);
}

test "sumInt64 handles empty array" {
    const data = [_]i64{};
    const result = simd.sumInt64(&data);
    try testing.expectEqual(@as(i64, 0), result);
}

test "sumInt64 handles negative numbers" {
    const data = [_]i64{ -10, 20, -5, 15 };
    const result = simd.sumInt64(&data);
    try testing.expectEqual(@as(i64, 20), result);
}

// ===== Mean Tests =====

test "meanFloat64 computes correct mean" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const result = simd.meanFloat64(&data);
    try testing.expectEqual(@as(f64, 3.0), result.?);
}

test "meanFloat64 handles empty array" {
    const data = [_]f64{};
    const result = simd.meanFloat64(&data);
    try testing.expect(result == null);
}

test "meanFloat64 handles single element" {
    const data = [_]f64{42.0};
    const result = simd.meanFloat64(&data);
    try testing.expectEqual(@as(f64, 42.0), result.?);
}

test "meanInt64 computes correct mean" {
    const data = [_]i64{ 10, 20, 30, 40, 50 };
    const result = simd.meanInt64(&data);
    try testing.expectEqual(@as(f64, 30.0), result.?);
}

test "meanInt64 handles odd division" {
    const data = [_]i64{ 1, 2, 3 };
    const result = simd.meanInt64(&data);
    try testing.expectEqual(@as(f64, 2.0), result.?);
}

// ===== Min Tests =====

test "minFloat64 finds minimum value" {
    const data = [_]f64{ 5.0, 1.0, 9.0, 3.0, 7.0 };
    const result = simd.minFloat64(&data);
    try testing.expectEqual(@as(f64, 1.0), result.?);
}

test "minFloat64 handles empty array" {
    const data = [_]f64{};
    const result = simd.minFloat64(&data);
    try testing.expect(result == null);
}

test "minFloat64 handles single element" {
    const data = [_]f64{42.0};
    const result = simd.minFloat64(&data);
    try testing.expectEqual(@as(f64, 42.0), result.?);
}

test "minFloat64 handles negative numbers" {
    const data = [_]f64{ 5.0, -10.0, 3.0, -2.0, 8.0 };
    const result = simd.minFloat64(&data);
    try testing.expectEqual(@as(f64, -10.0), result.?);
}

test "minFloat64 handles minimum at different positions" {
    // Test minimum at beginning
    const data1 = [_]f64{ 1.0, 5.0, 9.0, 3.0 };
    try testing.expectEqual(@as(f64, 1.0), simd.minFloat64(&data1).?);

    // Test minimum in middle
    const data2 = [_]f64{ 5.0, 9.0, 1.0, 3.0 };
    try testing.expectEqual(@as(f64, 1.0), simd.minFloat64(&data2).?);

    // Test minimum at end
    const data3 = [_]f64{ 5.0, 9.0, 3.0, 1.0 };
    try testing.expectEqual(@as(f64, 1.0), simd.minFloat64(&data3).?);
}

// ===== Max Tests =====

test "maxFloat64 finds maximum value" {
    const data = [_]f64{ 5.0, 1.0, 9.0, 3.0, 7.0 };
    const result = simd.maxFloat64(&data);
    try testing.expectEqual(@as(f64, 9.0), result.?);
}

test "maxFloat64 handles empty array" {
    const data = [_]f64{};
    const result = simd.maxFloat64(&data);
    try testing.expect(result == null);
}

test "maxFloat64 handles single element" {
    const data = [_]f64{42.0};
    const result = simd.maxFloat64(&data);
    try testing.expectEqual(@as(f64, 42.0), result.?);
}

test "maxFloat64 handles negative numbers" {
    const data = [_]f64{ -5.0, -10.0, -3.0, -2.0, -8.0 };
    const result = simd.maxFloat64(&data);
    try testing.expectEqual(@as(f64, -2.0), result.?);
}

test "maxFloat64 handles maximum at different positions" {
    // Test maximum at beginning
    const data1 = [_]f64{ 9.0, 5.0, 1.0, 3.0 };
    try testing.expectEqual(@as(f64, 9.0), simd.maxFloat64(&data1).?);

    // Test maximum in middle
    const data2 = [_]f64{ 5.0, 9.0, 1.0, 3.0 };
    try testing.expectEqual(@as(f64, 9.0), simd.maxFloat64(&data2).?);

    // Test maximum at end
    const data3 = [_]f64{ 5.0, 1.0, 3.0, 9.0 };
    try testing.expectEqual(@as(f64, 9.0), simd.maxFloat64(&data3).?);
}

// ===== Edge Case Tests =====

test "SIMD aggregations match scalar results for edge cases" {
    // Test array with exact SIMD width
    const data_4 = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    try testing.expectEqual(@as(f64, 10.0), simd.sumFloat64(&data_4));
    try testing.expectEqual(@as(f64, 2.5), simd.meanFloat64(&data_4).?);
    try testing.expectEqual(@as(f64, 1.0), simd.minFloat64(&data_4).?);
    try testing.expectEqual(@as(f64, 4.0), simd.maxFloat64(&data_4).?);

    // Test array with length SIMD width + 1
    const data_5 = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    try testing.expectEqual(@as(f64, 15.0), simd.sumFloat64(&data_5));
    try testing.expectEqual(@as(f64, 3.0), simd.meanFloat64(&data_5).?);
    try testing.expectEqual(@as(f64, 1.0), simd.minFloat64(&data_5).?);
    try testing.expectEqual(@as(f64, 5.0), simd.maxFloat64(&data_5).?);

    // Test array with length < SIMD width
    const data_2 = [_]f64{ 10.0, 20.0 };
    try testing.expectEqual(@as(f64, 30.0), simd.sumFloat64(&data_2));
    try testing.expectEqual(@as(f64, 15.0), simd.meanFloat64(&data_2).?);
    try testing.expectEqual(@as(f64, 10.0), simd.minFloat64(&data_2).?);
    try testing.expectEqual(@as(f64, 20.0), simd.maxFloat64(&data_2).?);
}

// ===== Variance Tests =====

test "varianceFloat64 computes correct variance" {
    // Data: [1, 2, 3, 4, 5]
    // Mean: 3.0
    // Squared differences: [(1-3)^2, (2-3)^2, (3-3)^2, (4-3)^2, (5-3)^2] = [4, 1, 0, 1, 4]
    // Sum of squared differences: 10
    // Sample variance (n-1): 10 / 4 = 2.5
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const result = simd.varianceFloat64(&data);
    try testing.expectEqual(@as(f64, 2.5), result.?);
}

test "varianceFloat64 handles empty array" {
    const data = [_]f64{};
    const result = simd.varianceFloat64(&data);
    try testing.expect(result == null);
}

test "varianceFloat64 handles single element" {
    const data = [_]f64{42.0};
    const result = simd.varianceFloat64(&data);
    try testing.expectEqual(@as(f64, 0.0), result.?);
}

test "varianceFloat64 handles two elements" {
    // Data: [10, 20]
    // Mean: 15
    // Squared differences: [(10-15)^2, (20-15)^2] = [25, 25]
    // Sum: 50
    // Sample variance (n-1): 50 / 1 = 50
    const data = [_]f64{ 10.0, 20.0 };
    const result = simd.varianceFloat64(&data);
    try testing.expectEqual(@as(f64, 50.0), result.?);
}

test "varianceFloat64 handles identical values" {
    const data = [_]f64{ 5.0, 5.0, 5.0, 5.0, 5.0 };
    const result = simd.varianceFloat64(&data);
    try testing.expectEqual(@as(f64, 0.0), result.?);
}

test "varianceFloat64 handles odd-length array" {
    // Data: [2, 4, 4, 4, 5, 5, 7] (7 elements)
    const data = [_]f64{ 2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0 };
    const result = simd.varianceFloat64(&data);
    // Mean = 31/7 ≈ 4.428571
    // Verify non-zero variance
    try testing.expect(result.? > 0.0);
}

test "varianceFloat64 handles large array" {
    const data = try testing.allocator.alloc(f64, 100);
    defer testing.allocator.free(data);

    // Fill with values 1 to 100
    for (data, 0..) |*val, i| {
        val.* = @as(f64, @floatFromInt(i + 1));
    }

    const result = simd.varianceFloat64(data);
    // Variance of 1..100 is approximately 841.67
    try testing.expect(result.? > 800.0);
    try testing.expect(result.? < 850.0);
}

// ===== Standard Deviation Tests =====

test "stdDevFloat64 computes correct standard deviation" {
    // Data: [1, 2, 3, 4, 5]
    // Variance: 2.5
    // StdDev: sqrt(2.5) ≈ 1.5811
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const result = simd.stdDevFloat64(&data);
    const expected = @sqrt(2.5);
    try testing.expectApproxEqAbs(expected, result.?, 0.0001);
}

test "stdDevFloat64 handles empty array" {
    const data = [_]f64{};
    const result = simd.stdDevFloat64(&data);
    try testing.expect(result == null);
}

test "stdDevFloat64 handles single element" {
    const data = [_]f64{42.0};
    const result = simd.stdDevFloat64(&data);
    try testing.expectEqual(@as(f64, 0.0), result.?);
}

test "stdDevFloat64 handles identical values" {
    const data = [_]f64{ 5.0, 5.0, 5.0, 5.0, 5.0 };
    const result = simd.stdDevFloat64(&data);
    try testing.expectEqual(@as(f64, 0.0), result.?);
}

test "stdDevFloat64 matches sqrt of variance" {
    const data = [_]f64{ 10.0, 20.0, 30.0, 40.0, 50.0 };
    const variance = simd.varianceFloat64(&data).?;
    const stddev = simd.stdDevFloat64(&data).?;
    try testing.expectApproxEqAbs(@sqrt(variance), stddev, 0.0001);
}

test "variance and stddev work for exact SIMD widths" {
    // Test array with exact SIMD width
    const data_4 = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    const var_4 = simd.varianceFloat64(&data_4).?;
    const std_4 = simd.stdDevFloat64(&data_4).?;
    try testing.expectApproxEqAbs(@sqrt(var_4), std_4, 0.0001);

    // Test array with length SIMD width + 1
    const data_5 = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const var_5 = simd.varianceFloat64(&data_5).?;
    const std_5 = simd.stdDevFloat64(&data_5).?;
    try testing.expectApproxEqAbs(@sqrt(var_5), std_5, 0.0001);
}
