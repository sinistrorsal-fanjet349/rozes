//! Unit tests for statistical operations

const std = @import("std");
const testing = std.testing;
const DataFrame = @import("../../../core/dataframe.zig").DataFrame;
const Series = @import("../../../core/series.zig").Series;
const stats = @import("../../../core/stats.zig");
const types = @import("../../../core/types.zig");
const ColumnDesc = types.ColumnDesc;

test "variance computes correct sample variance for Int64" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asInt64Buffer() orelse unreachable;
    // Data: [1, 2, 3, 4, 5] → mean = 3, variance = 2.5
    data[0] = 1;
    data[1] = 2;
    data[2] = 3;
    data[3] = 4;
    data[4] = 5;
    try df.setRowCount(5);

    const var_val = try stats.variance(&df, "values");

    try testing.expect(var_val != null);
    try testing.expectApproxEqAbs(@as(f64, 2.5), var_val.?, 0.0001);
}

test "variance computes correct sample variance for Float64" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    // Data: [1.5, 2.5, 3.5, 4.5, 5.5] → mean = 3.5, variance = 2.5
    data[0] = 1.5;
    data[1] = 2.5;
    data[2] = 3.5;
    data[3] = 4.5;
    data[4] = 5.5;
    try df.setRowCount(5);

    const var_val = try stats.variance(&df, "values");

    try testing.expect(var_val != null);
    try testing.expectApproxEqAbs(@as(f64, 2.5), var_val.?, 0.0001);
}

test "variance returns 0 for single value" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 1);
    defer df.deinit();

    const data = df.columns[0].asInt64Buffer() orelse unreachable;
    data[0] = 42;
    try df.setRowCount(1);

    const var_val = try stats.variance(&df, "values");

    try testing.expect(var_val != null);
    try testing.expectEqual(@as(f64, 0.0), var_val.?);
}

test "variance returns null for empty DataFrame" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 0);
    defer df.deinit();

    const var_val = try stats.variance(&df, "values");

    try testing.expect(var_val == null);
}

test "stdDev computes correct standard deviation" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asInt64Buffer() orelse unreachable;
    // Data: [1, 2, 3, 4, 5] → variance = 2.5, stdDev = sqrt(2.5) ≈ 1.5811
    data[0] = 1;
    data[1] = 2;
    data[2] = 3;
    data[3] = 4;
    data[4] = 5;
    try df.setRowCount(5);

    const std_dev = try stats.stdDev(&df, "values");

    try testing.expect(std_dev != null);
    try testing.expectApproxEqAbs(@as(f64, 1.5811), std_dev.?, 0.0001);
}

test "median computes correct value for odd-length array" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asInt64Buffer() orelse unreachable;
    // Data: [10, 20, 30, 40, 50] → median = 30
    data[0] = 10;
    data[1] = 20;
    data[2] = 30;
    data[3] = 40;
    data[4] = 50;
    try df.setRowCount(5);

    const median_val = try stats.median(&df, "values", allocator);

    try testing.expect(median_val != null);
    try testing.expectApproxEqAbs(@as(f64, 30.0), median_val.?, 0.0001);
}

test "median computes correct value for even-length array" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 4);
    defer df.deinit();

    const data = df.columns[0].asInt64Buffer() orelse unreachable;
    // Data: [10, 20, 30, 40] → median = (20 + 30) / 2 = 25
    data[0] = 10;
    data[1] = 20;
    data[2] = 30;
    data[3] = 40;
    try df.setRowCount(4);

    const median_val = try stats.median(&df, "values", allocator);

    try testing.expect(median_val != null);
    try testing.expectApproxEqAbs(@as(f64, 25.0), median_val.?, 0.0001);
}

test "quantile computes 25th percentile correctly" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asInt64Buffer() orelse unreachable;
    // Data: [10, 20, 30, 40, 50] → Q1 (25th percentile) = 20
    data[0] = 10;
    data[1] = 20;
    data[2] = 30;
    data[3] = 40;
    data[4] = 50;
    try df.setRowCount(5);

    const q25 = try stats.quantile(&df, "values", allocator, 0.25);

    try testing.expect(q25 != null);
    try testing.expectApproxEqAbs(@as(f64, 20.0), q25.?, 0.0001);
}

test "quantile computes 75th percentile correctly" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asInt64Buffer() orelse unreachable;
    // Data: [10, 20, 30, 40, 50] → Q3 (75th percentile) = 40
    data[0] = 10;
    data[1] = 20;
    data[2] = 30;
    data[3] = 40;
    data[4] = 50;
    try df.setRowCount(5);

    const q75 = try stats.quantile(&df, "values", allocator, 0.75);

    try testing.expect(q75 != null);
    try testing.expectApproxEqAbs(@as(f64, 40.0), q75.?, 0.0001);
}

test "corrMatrix computes identity for single column" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("x", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    data[0] = 1.0;
    data[1] = 2.0;
    data[2] = 3.0;
    data[3] = 4.0;
    data[4] = 5.0;
    try df.setRowCount(5);

    var matrix = try stats.corrMatrix(&df, allocator, &[_][]const u8{"x"});
    defer {
        for (matrix) |row| {
            allocator.free(row);
        }
        allocator.free(matrix);
    }

    // 1×1 matrix should be [1.0] (perfect correlation with self)
    try testing.expectEqual(@as(usize, 1), matrix.len);
    try testing.expectApproxEqAbs(@as(f64, 1.0), matrix[0][0], 0.0001);
}

test "corrMatrix computes perfect positive correlation" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("x", .Float64, 0),
        ColumnDesc.init("y", .Float64, 1),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const x_data = df.columns[0].asFloat64Buffer() orelse unreachable;
    const y_data = df.columns[1].asFloat64Buffer() orelse unreachable;

    // Perfect positive correlation: y = 2x
    x_data[0] = 1.0;
    x_data[1] = 2.0;
    x_data[2] = 3.0;
    x_data[3] = 4.0;
    x_data[4] = 5.0;

    y_data[0] = 2.0;
    y_data[1] = 4.0;
    y_data[2] = 6.0;
    y_data[3] = 8.0;
    y_data[4] = 10.0;

    try df.setRowCount(5);

    var matrix = try stats.corrMatrix(&df, allocator, &[_][]const u8{ "x", "y" });
    defer {
        for (matrix) |row| {
            allocator.free(row);
        }
        allocator.free(matrix);
    }

    // Correlation matrix:
    // [1.0, 1.0]
    // [1.0, 1.0]
    try testing.expectApproxEqAbs(@as(f64, 1.0), matrix[0][0], 0.0001); // x with x
    try testing.expectApproxEqAbs(@as(f64, 1.0), matrix[0][1], 0.0001); // x with y
    try testing.expectApproxEqAbs(@as(f64, 1.0), matrix[1][0], 0.0001); // y with x
    try testing.expectApproxEqAbs(@as(f64, 1.0), matrix[1][1], 0.0001); // y with y
}

test "corrMatrix computes perfect negative correlation" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("x", .Float64, 0),
        ColumnDesc.init("y", .Float64, 1),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const x_data = df.columns[0].asFloat64Buffer() orelse unreachable;
    const y_data = df.columns[1].asFloat64Buffer() orelse unreachable;

    // Perfect negative correlation: y = -2x + 12
    x_data[0] = 1.0;
    x_data[1] = 2.0;
    x_data[2] = 3.0;
    x_data[3] = 4.0;
    x_data[4] = 5.0;

    y_data[0] = 10.0;
    y_data[1] = 8.0;
    y_data[2] = 6.0;
    y_data[3] = 4.0;
    y_data[4] = 2.0;

    try df.setRowCount(5);

    var matrix = try stats.corrMatrix(&df, allocator, &[_][]const u8{ "x", "y" });
    defer {
        for (matrix) |row| {
            allocator.free(row);
        }
        allocator.free(matrix);
    }

    // Correlation matrix:
    // [1.0, -1.0]
    // [-1.0, 1.0]
    try testing.expectApproxEqAbs(@as(f64, 1.0), matrix[0][0], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, -1.0), matrix[0][1], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, -1.0), matrix[1][0], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 1.0), matrix[1][1], 0.0001);
}

test "corrMatrix handles zero variance (all same values)" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("x", .Float64, 0),
        ColumnDesc.init("y", .Float64, 1),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const x_data = df.columns[0].asFloat64Buffer() orelse unreachable;
    const y_data = df.columns[1].asFloat64Buffer() orelse unreachable;

    // x has variance, y has zero variance (all same)
    x_data[0] = 1.0;
    x_data[1] = 2.0;
    x_data[2] = 3.0;
    x_data[3] = 4.0;
    x_data[4] = 5.0;

    y_data[0] = 10.0;
    y_data[1] = 10.0;
    y_data[2] = 10.0;
    y_data[3] = 10.0;
    y_data[4] = 10.0;

    try df.setRowCount(5);

    var matrix = try stats.corrMatrix(&df, allocator, &[_][]const u8{ "x", "y" });
    defer {
        for (matrix) |row| {
            allocator.free(row);
        }
        allocator.free(matrix);
    }

    // When one column has zero variance, correlation is undefined (returns 0.0)
    try testing.expectApproxEqAbs(@as(f64, 1.0), matrix[0][0], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 0.0), matrix[0][1], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 0.0), matrix[1][0], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 1.0), matrix[1][1], 0.0001);
}

test "rank assigns correct ranks with First method" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asInt64Buffer() orelse unreachable;
    // Data: [30, 10, 50, 20, 40] → ranks (First): [3, 1, 5, 2, 4]
    data[0] = 30;
    data[1] = 10;
    data[2] = 50;
    data[3] = 20;
    data[4] = 40;
    try df.setRowCount(5);

    var ranked = try stats.rank(&df.columns[0], allocator, .First);
    defer allocator.free(ranked.data.Float64);

    const ranks = ranked.asFloat64() orelse unreachable;

    try testing.expectApproxEqAbs(@as(f64, 3.0), ranks[0], 0.0001); // 30 → rank 3
    try testing.expectApproxEqAbs(@as(f64, 1.0), ranks[1], 0.0001); // 10 → rank 1
    try testing.expectApproxEqAbs(@as(f64, 5.0), ranks[2], 0.0001); // 50 → rank 5
    try testing.expectApproxEqAbs(@as(f64, 2.0), ranks[3], 0.0001); // 20 → rank 2
    try testing.expectApproxEqAbs(@as(f64, 4.0), ranks[4], 0.0001); // 40 → rank 4
}

test "rank handles single value" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 1);
    defer df.deinit();

    const data = df.columns[0].asInt64Buffer() orelse unreachable;
    data[0] = 42;
    try df.setRowCount(1);

    var ranked = try stats.rank(&df.columns[0], allocator, .First);
    defer allocator.free(ranked.data.Float64);

    const ranks = ranked.asFloat64() orelse unreachable;

    try testing.expectApproxEqAbs(@as(f64, 1.0), ranks[0], 0.0001); // Single value → rank 1
}

test "statistics module has no memory leaks (1000 iterations)" {
    const allocator = testing.allocator;

    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        const cols = [_]ColumnDesc{
            ColumnDesc.init("x", .Float64, 0),
            ColumnDesc.init("y", .Float64, 1),
        };

        var df = try DataFrame.create(allocator, &cols, 10);
        defer df.deinit();

        const x_data = df.columns[0].asFloat64Buffer() orelse unreachable;
        const y_data = df.columns[1].asFloat64Buffer() orelse unreachable;

        var j: u32 = 0;
        while (j < 10) : (j += 1) {
            x_data[j] = @as(f64, @floatFromInt(j));
            y_data[j] = @as(f64, @floatFromInt(j * 2));
        }
        try df.setRowCount(10);

        // Test all operations
        _ = try stats.variance(&df, "x");
        _ = try stats.stdDev(&df, "x");
        _ = try stats.median(&df, "x", allocator);
        _ = try stats.quantile(&df, "x", allocator, 0.75);

        var matrix = try stats.corrMatrix(&df, allocator, &[_][]const u8{ "x", "y" });
        for (matrix) |row| {
            allocator.free(row);
        }
        allocator.free(matrix);

        var ranked = try stats.rank(&df.columns[0], allocator, .First);
        allocator.free(ranked.data.Float64);
    }

    // testing.allocator will report leaks automatically
}
