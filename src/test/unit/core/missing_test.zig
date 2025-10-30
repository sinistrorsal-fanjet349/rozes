//! Unit tests for missing value operations

const std = @import("std");
const testing = std.testing;
const DataFrame = @import("../../../core/dataframe.zig").DataFrame;
const Series = @import("../../../core/series.zig").Series;
const missing = @import("../../../core/missing.zig");
const types = @import("../../../core/types.zig");
const ColumnDesc = types.ColumnDesc;

test "fillna with Constant fills NaN values in Float64" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    data[0] = 10.0;
    data[1] = std.math.nan(f64);
    data[2] = 30.0;
    data[3] = std.math.nan(f64);
    data[4] = 50.0;
    try df.setRowCount(5);

    var filled = try missing.fillna(&df.columns[0], allocator, .Constant, -1.0);
    defer allocator.free(filled.data.Float64);

    const result = filled.asFloat64() orelse unreachable;

    try testing.expectApproxEqAbs(@as(f64, 10.0), result[0], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, -1.0), result[1], 0.0001); // Filled
    try testing.expectApproxEqAbs(@as(f64, 30.0), result[2], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, -1.0), result[3], 0.0001); // Filled
    try testing.expectApproxEqAbs(@as(f64, 50.0), result[4], 0.0001);
}

test "fillna with ForwardFill uses previous value" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    data[0] = 10.0;
    data[1] = std.math.nan(f64);
    data[2] = std.math.nan(f64);
    data[3] = 40.0;
    data[4] = std.math.nan(f64);
    try df.setRowCount(5);

    var filled = try missing.fillna(&df.columns[0], allocator, .ForwardFill, null);
    defer allocator.free(filled.data.Float64);

    const result = filled.asFloat64() orelse unreachable;

    try testing.expectApproxEqAbs(@as(f64, 10.0), result[0], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 10.0), result[1], 0.0001); // Forward filled
    try testing.expectApproxEqAbs(@as(f64, 10.0), result[2], 0.0001); // Forward filled
    try testing.expectApproxEqAbs(@as(f64, 40.0), result[3], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 40.0), result[4], 0.0001); // Forward filled
}

test "fillna with BackwardFill uses next value" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    data[0] = std.math.nan(f64);
    data[1] = 20.0;
    data[2] = std.math.nan(f64);
    data[3] = std.math.nan(f64);
    data[4] = 50.0;
    try df.setRowCount(5);

    var filled = try missing.fillna(&df.columns[0], allocator, .BackwardFill, null);
    defer allocator.free(filled.data.Float64);

    const result = filled.asFloat64() orelse unreachable;

    try testing.expectApproxEqAbs(@as(f64, 20.0), result[0], 0.0001); // Backward filled
    try testing.expectApproxEqAbs(@as(f64, 20.0), result[1], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 50.0), result[2], 0.0001); // Backward filled
    try testing.expectApproxEqAbs(@as(f64, 50.0), result[3], 0.0001); // Backward filled
    try testing.expectApproxEqAbs(@as(f64, 50.0), result[4], 0.0001);
}

test "fillna with Interpolate uses linear interpolation" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    data[0] = 10.0;
    data[1] = std.math.nan(f64);
    data[2] = std.math.nan(f64);
    data[3] = 40.0;
    data[4] = std.math.nan(f64);
    try df.setRowCount(5);

    var filled = try missing.fillna(&df.columns[0], allocator, .Interpolate, null);
    defer allocator.free(filled.data.Float64);

    const result = filled.asFloat64() orelse unreachable;

    try testing.expectApproxEqAbs(@as(f64, 10.0), result[0], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 20.0), result[1], 0.0001); // Interpolated (10 + 1/3 * (40-10))
    try testing.expectApproxEqAbs(@as(f64, 30.0), result[2], 0.0001); // Interpolated (10 + 2/3 * (40-10))
    try testing.expectApproxEqAbs(@as(f64, 40.0), result[3], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 40.0), result[4], 0.0001); // Use last valid
}

test "fillna handles all missing values" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 3);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    data[0] = std.math.nan(f64);
    data[1] = std.math.nan(f64);
    data[2] = std.math.nan(f64);
    try df.setRowCount(3);

    // Constant fill should work
    var filled = try missing.fillna(&df.columns[0], allocator, .Constant, 99.0);
    defer allocator.free(filled.data.Float64);

    const result = filled.asFloat64() orelse unreachable;

    try testing.expectApproxEqAbs(@as(f64, 99.0), result[0], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 99.0), result[1], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 99.0), result[2], 0.0001);
}

test "fillna handles first value missing (ForwardFill)" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 3);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    data[0] = std.math.nan(f64);
    data[1] = 20.0;
    data[2] = 30.0;
    try df.setRowCount(3);

    var filled = try missing.fillna(&df.columns[0], allocator, .ForwardFill, null);
    defer allocator.free(filled.data.Float64);

    const result = filled.asFloat64() orelse unreachable;

    try testing.expectApproxEqAbs(@as(f64, 0.0), result[0], 0.0001); // No previous value, use 0
    try testing.expectApproxEqAbs(@as(f64, 20.0), result[1], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 30.0), result[2], 0.0001);
}

test "fillna handles last value missing (BackwardFill)" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 3);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    data[0] = 10.0;
    data[1] = 20.0;
    data[2] = std.math.nan(f64);
    try df.setRowCount(3);

    var filled = try missing.fillna(&df.columns[0], allocator, .BackwardFill, null);
    defer allocator.free(filled.data.Float64);

    const result = filled.asFloat64() orelse unreachable;

    try testing.expectApproxEqAbs(@as(f64, 10.0), result[0], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 20.0), result[1], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 0.0), result[2], 0.0001); // No next value, use 0
}

test "dropna removes rows with ANY missing values" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("x", .Float64, 0),
        ColumnDesc.init("y", .Float64, 1),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const x_data = df.columns[0].asFloat64Buffer() orelse unreachable;
    const y_data = df.columns[1].asFloat64Buffer() orelse unreachable;

    // Row 0: (10, 20) - no missing
    x_data[0] = 10.0;
    y_data[0] = 20.0;

    // Row 1: (NaN, 40) - x missing
    x_data[1] = std.math.nan(f64);
    y_data[1] = 40.0;

    // Row 2: (30, NaN) - y missing
    x_data[2] = 30.0;
    y_data[2] = std.math.nan(f64);

    // Row 3: (40, 80) - no missing
    x_data[3] = 40.0;
    y_data[3] = 80.0;

    // Row 4: (NaN, NaN) - both missing
    x_data[4] = std.math.nan(f64);
    y_data[4] = std.math.nan(f64);

    try df.setRowCount(5);

    var clean = try missing.dropna(&df, allocator, .{});
    defer clean.deinit();

    // Should keep only rows 0 and 3 (no missing values)
    try testing.expectEqual(@as(u32, 2), clean.len());

    const x_clean = clean.columns[0].asFloat64() orelse unreachable;
    const y_clean = clean.columns[1].asFloat64() orelse unreachable;

    try testing.expectApproxEqAbs(@as(f64, 10.0), x_clean[0], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 20.0), y_clean[0], 0.0001);

    try testing.expectApproxEqAbs(@as(f64, 40.0), x_clean[1], 0.0001);
    try testing.expectApproxEqAbs(@as(f64, 80.0), y_clean[1], 0.0001);
}

test "dropna with how=All removes only rows with ALL missing" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("x", .Float64, 0),
        ColumnDesc.init("y", .Float64, 1),
    };

    var df = try DataFrame.create(allocator, &cols, 4);
    defer df.deinit();

    const x_data = df.columns[0].asFloat64Buffer() orelse unreachable;
    const y_data = df.columns[1].asFloat64Buffer() orelse unreachable;

    // Row 0: (10, 20) - no missing
    x_data[0] = 10.0;
    y_data[0] = 20.0;

    // Row 1: (NaN, 40) - x missing (keep - not all missing)
    x_data[1] = std.math.nan(f64);
    y_data[1] = 40.0;

    // Row 2: (30, NaN) - y missing (keep - not all missing)
    x_data[2] = 30.0;
    y_data[2] = std.math.nan(f64);

    // Row 3: (NaN, NaN) - both missing (drop)
    x_data[3] = std.math.nan(f64);
    y_data[3] = std.math.nan(f64);

    try df.setRowCount(4);

    var clean = try missing.dropna(&df, allocator, .{ .how = .All });
    defer clean.deinit();

    // Should keep rows 0, 1, 2 (not all columns missing)
    try testing.expectEqual(@as(u32, 3), clean.len());
}

test "dropna with subset checks only specified columns" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("x", .Float64, 0),
        ColumnDesc.init("y", .Float64, 1),
        ColumnDesc.init("z", .Float64, 2),
    };

    var df = try DataFrame.create(allocator, &cols, 3);
    defer df.deinit();

    const x_data = df.columns[0].asFloat64Buffer() orelse unreachable;
    const y_data = df.columns[1].asFloat64Buffer() orelse unreachable;
    const z_data = df.columns[2].asFloat64Buffer() orelse unreachable;

    // Row 0: (10, 20, NaN) - z missing (keep - not checking z)
    x_data[0] = 10.0;
    y_data[0] = 20.0;
    z_data[0] = std.math.nan(f64);

    // Row 1: (NaN, 40, 60) - x missing (drop - checking x)
    x_data[1] = std.math.nan(f64);
    y_data[1] = 40.0;
    z_data[1] = 60.0;

    // Row 2: (30, NaN, 90) - y missing (drop - checking y)
    x_data[2] = 30.0;
    y_data[2] = std.math.nan(f64);
    z_data[2] = 90.0;

    try df.setRowCount(3);

    // Only check columns x and y (ignore z)
    var subset = [_][]const u8{ "x", "y" };
    var clean = try missing.dropna(&df, allocator, .{ .subset = &subset });
    defer clean.deinit();

    // Should keep only row 0 (x and y not missing, ignore z)
    try testing.expectEqual(@as(u32, 1), clean.len());

    const x_clean = clean.columns[0].asFloat64() orelse unreachable;
    try testing.expectApproxEqAbs(@as(f64, 10.0), x_clean[0], 0.0001);
}

test "isna creates boolean mask for missing values" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    data[0] = 10.0;
    data[1] = std.math.nan(f64);
    data[2] = 30.0;
    data[3] = std.math.nan(f64);
    data[4] = 50.0;
    try df.setRowCount(5);

    var mask = try missing.isna(&df.columns[0], allocator);
    defer allocator.free(mask.data.Bool);

    const result = mask.asBool() orelse unreachable;

    try testing.expectEqual(false, result[0]);
    try testing.expectEqual(true, result[1]); // NaN
    try testing.expectEqual(false, result[2]);
    try testing.expectEqual(true, result[3]); // NaN
    try testing.expectEqual(false, result[4]);
}

test "notna creates boolean mask for non-missing values" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("values", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 5);
    defer df.deinit();

    const data = df.columns[0].asFloat64Buffer() orelse unreachable;
    data[0] = 10.0;
    data[1] = std.math.nan(f64);
    data[2] = 30.0;
    data[3] = std.math.nan(f64);
    data[4] = 50.0;
    try df.setRowCount(5);

    var mask = try missing.notna(&df.columns[0], allocator);
    defer allocator.free(mask.data.Bool);

    const result = mask.asBool() orelse unreachable;

    try testing.expectEqual(true, result[0]);
    try testing.expectEqual(false, result[1]); // NaN
    try testing.expectEqual(true, result[2]);
    try testing.expectEqual(false, result[3]); // NaN
    try testing.expectEqual(true, result[4]);
}

test "missing module has no memory leaks (1000 iterations)" {
    const allocator = testing.allocator;

    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        const cols = [_]ColumnDesc{
            ColumnDesc.init("values", .Float64, 0),
        };

        var df = try DataFrame.create(allocator, &cols, 10);
        defer df.deinit();

        const data = df.columns[0].asFloat64Buffer() orelse unreachable;

        var j: u32 = 0;
        while (j < 10) : (j += 1) {
            data[j] = if (j % 3 == 0) std.math.nan(f64) else @as(f64, @floatFromInt(j));
        }
        try df.setRowCount(10);

        // Test all operations
        var filled_const = try missing.fillna(&df.columns[0], allocator, .Constant, -1.0);
        allocator.free(filled_const.data.Float64);

        var filled_ffill = try missing.fillna(&df.columns[0], allocator, .ForwardFill, null);
        allocator.free(filled_ffill.data.Float64);

        var filled_bfill = try missing.fillna(&df.columns[0], allocator, .BackwardFill, null);
        allocator.free(filled_bfill.data.Float64);

        var filled_interp = try missing.fillna(&df.columns[0], allocator, .Interpolate, null);
        allocator.free(filled_interp.data.Float64);

        var mask_isna = try missing.isna(&df.columns[0], allocator);
        allocator.free(mask_isna.data.Bool);

        var mask_notna = try missing.notna(&df.columns[0], allocator);
        allocator.free(mask_notna.data.Bool);

        var dropped = try missing.dropna(&df, allocator, .{});
        dropped.deinit();
    }

    // testing.allocator will report leaks automatically
}
