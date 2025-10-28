//! Comprehensive tests for DataFrame core functionality
//!
//! This test suite covers:
//! - create() - DataFrame construction with various column configurations
//! - deinit() - Memory management and cleanup
//! - len() / columnCount() / isEmpty() - Size queries
//! - column() / columnMut() / columnIndex() / columnAt() - Column access
//! - hasColumn() / columnNames() - Column introspection
//! - setRowCount() - Row management
//! - row() - Row access
//!
//! Tests follow Tiger Style: 2+ assertions per test, explicit error handling,
//! bounded loops verification, memory leak detection.

const std = @import("std");
const testing = std.testing;
const DataFrame = @import("../../../core/dataframe.zig").DataFrame;
const Series = @import("../../../core/series.zig").Series;
const ColumnDesc = @import("../../../core/types.zig").ColumnDesc;
const ValueType = @import("../../../core/types.zig").ValueType;

// =============================================================================
// CREATE & DEINIT TESTS (5 tests)
// =============================================================================

test "DataFrame.create: single column" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("value", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    try testing.expectEqual(@as(usize, 1), df.columnCount());
    try testing.expectEqual(@as(u32, 0), df.len()); // Row count starts at 0
}

test "DataFrame.create: multiple columns with different types" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("age", .Int64, 0),
        ColumnDesc.init("salary", .Float64, 1),
        ColumnDesc.init("active", .Bool, 2),
        ColumnDesc.init("name", .String, 3),
    };

    var df = try DataFrame.create(allocator, &cols, 100);
    defer df.deinit();

    try testing.expectEqual(@as(usize, 4), df.columnCount());
    try testing.expectEqual(@as(u32, 0), df.len());

    // Verify each column exists
    try testing.expect(df.hasColumn("age"));
    try testing.expect(df.hasColumn("salary"));
    try testing.expect(df.hasColumn("active"));
    try testing.expect(df.hasColumn("name"));
}

test "DataFrame.create: with large capacity" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("x", .Float64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 1_000_000);
    defer df.deinit();

    try testing.expectEqual(@as(usize, 1), df.columnCount());
    try testing.expectEqual(@as(u32, 0), df.len());
}

test "DataFrame.deinit: releases all memory" {
    const allocator = testing.allocator;

    // Create and destroy 1000 DataFrames to verify no memory leaks
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        const cols = [_]ColumnDesc{
            ColumnDesc.init("a", .Int64, 0),
            ColumnDesc.init("b", .Float64, 1),
            ColumnDesc.init("c", .Bool, 2),
        };

        var df = try DataFrame.create(allocator, &cols, 100);
        df.deinit();
    }

    // testing.allocator will report leaks automatically
}

test "DataFrame.create: column names are copied (not referenced)" {
    const allocator = testing.allocator;

    // Create column names in a scope that will be freed
    var name_buffer: [10]u8 = undefined;
    const col_name = try std.fmt.bufPrint(&name_buffer, "col_0", .{});

    const cols = [_]ColumnDesc{
        ColumnDesc.init(col_name, .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    // Overwrite the source buffer (DataFrame should have its own copy)
    @memset(&name_buffer, 0xFF);

    // Column name should still be valid
    try testing.expect(df.hasColumn("col_0"));
}

// =============================================================================
// SIZE QUERY TESTS (4 tests)
// =============================================================================

test "DataFrame.len: returns current row count" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("value", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 100);
    defer df.deinit();

    try testing.expectEqual(@as(u32, 0), df.len());

    try df.setRowCount(50);
    try testing.expectEqual(@as(u32, 50), df.len());
}

test "DataFrame.columnCount: returns number of columns" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("a", .Int64, 0),
        ColumnDesc.init("b", .Float64, 1),
        ColumnDesc.init("c", .Bool, 2),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    try testing.expectEqual(@as(usize, 3), df.columnCount());
}

test "DataFrame.isEmpty: true when row count is zero" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("value", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    try testing.expect(df.isEmpty());

    try df.setRowCount(1);
    try testing.expect(!df.isEmpty());
}

test "DataFrame.isEmpty: false when DataFrame has rows" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("value", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    try df.setRowCount(5);

    try testing.expect(!df.isEmpty());
    try testing.expectEqual(@as(u32, 5), df.len());
}

// =============================================================================
// COLUMN ACCESS TESTS (10 tests)
// =============================================================================

test "DataFrame.column: retrieves column by name" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("age", .Int64, 0),
        ColumnDesc.init("score", .Float64, 1),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    const age_col = df.column("age");
    try testing.expect(age_col != null);
    try testing.expect(age_col.?.value_type == .Int64);

    const score_col = df.column("score");
    try testing.expect(score_col != null);
    try testing.expect(score_col.?.value_type == .Float64);
}

test "DataFrame.column: returns null for non-existent column" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("existing", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    const result = df.column("nonexistent");
    try testing.expect(result == null);
}

test "DataFrame.columnMut: retrieves mutable column" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("value", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    const col = df.columnMut("value");
    try testing.expect(col != null);

    // Verify we can mutate the column
    const data = col.?.asInt64Buffer();
    try testing.expect(data != null);
}

test "DataFrame.columnIndex: finds correct index" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("first", .Int64, 0),
        ColumnDesc.init("second", .Float64, 1),
        ColumnDesc.init("third", .Bool, 2),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    try testing.expectEqual(@as(?usize, 0), df.columnIndex("first"));
    try testing.expectEqual(@as(?usize, 1), df.columnIndex("second"));
    try testing.expectEqual(@as(?usize, 2), df.columnIndex("third"));
}

test "DataFrame.columnIndex: returns null for non-existent column" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("only", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    try testing.expectEqual(@as(?usize, null), df.columnIndex("missing"));
}

test "DataFrame.columnAt: retrieves column by index" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("first", .Int64, 0),
        ColumnDesc.init("second", .Float64, 1),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    const col0 = try df.columnAt(0);
    try testing.expectEqualStrings("first", col0.name);

    const col1 = try df.columnAt(1);
    try testing.expectEqualStrings("second", col1.name);
}

test "DataFrame.columnAt: error on out of bounds index" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("only", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    try testing.expectError(error.IndexOutOfBounds, df.columnAt(1));
    try testing.expectError(error.IndexOutOfBounds, df.columnAt(100));
}

test "DataFrame.hasColumn: detects existing columns" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("exists", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    try testing.expect(df.hasColumn("exists"));
    try testing.expect(!df.hasColumn("missing"));
}

test "DataFrame.hasColumn: case-sensitive matching" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("Name", .String, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    try testing.expect(df.hasColumn("Name"));
    try testing.expect(!df.hasColumn("name")); // Different case
    try testing.expect(!df.hasColumn("NAME"));
}

test "DataFrame.columnNames: returns all column names" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("first", .Int64, 0),
        ColumnDesc.init("second", .Float64, 1),
        ColumnDesc.init("third", .Bool, 2),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    const names = try df.columnNames(allocator);
    defer allocator.free(names);

    try testing.expectEqual(@as(usize, 3), names.len);
    try testing.expectEqualStrings("first", names[0]);
    try testing.expectEqualStrings("second", names[1]);
    try testing.expectEqualStrings("third", names[2]);
}

// =============================================================================
// ROW MANAGEMENT TESTS (5 tests)
// =============================================================================

test "DataFrame.setRowCount: updates row count" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("value", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 100);
    defer df.deinit();

    try testing.expectEqual(@as(u32, 0), df.len());

    try df.setRowCount(50);
    try testing.expectEqual(@as(u32, 50), df.len());

    try df.setRowCount(75);
    try testing.expectEqual(@as(u32, 75), df.len());
}

test "DataFrame.setRowCount: error when exceeding capacity" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("value", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    // Try to set row count beyond capacity
    try testing.expectError(error.InsufficientCapacity, df.setRowCount(100));
}

test "DataFrame.setRowCount: updates all columns" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("a", .Int64, 0),
        ColumnDesc.init("b", .Float64, 1),
        ColumnDesc.init("c", .Bool, 2),
    };

    var df = try DataFrame.create(allocator, &cols, 50);
    defer df.deinit();

    try df.setRowCount(25);

    // Verify all columns have correct length
    try testing.expectEqual(@as(u32, 25), df.column("a").?.length);
    try testing.expectEqual(@as(u32, 25), df.column("b").?.length);
    try testing.expectEqual(@as(u32, 25), df.column("c").?.length);
}

test "DataFrame.row: creates valid RowRef" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("value", .Float64, 1),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    const id_col = df.columnMut("id").?;
    const ids = id_col.asInt64Buffer().?;
    ids[0] = 100;
    ids[1] = 200;

    try df.setRowCount(2);

    const row0 = try df.row(0);
    const row1 = try df.row(1);

    // Verify row references work
    try testing.expectEqual(@as(i64, 100), row0.getInt64("id").?);
    try testing.expectEqual(@as(i64, 200), row1.getInt64("id").?);
}

test "DataFrame.row: error on out of bounds index" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("value", .Int64, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 10);
    defer df.deinit();

    try df.setRowCount(5);

    // Valid indices: 0-4
    _ = try df.row(0);
    _ = try df.row(4);

    // Invalid indices
    try testing.expectError(error.IndexOutOfBounds, df.row(5));
    try testing.expectError(error.IndexOutOfBounds, df.row(100));
}

// =============================================================================
// INTEGRATION TESTS (3 tests)
// =============================================================================

test "Integration: create DataFrame and populate data" {
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("age", .Int64, 0),
        ColumnDesc.init("salary", .Float64, 1),
        ColumnDesc.init("active", .Bool, 2),
    };

    var df = try DataFrame.create(allocator, &cols, 3);
    defer df.deinit();

    // Populate age column
    const age_col = df.columnMut("age").?;
    const ages = age_col.asInt64Buffer().?;
    ages[0] = 25;
    ages[1] = 30;
    ages[2] = 35;

    // Populate salary column
    const salary_col = df.columnMut("salary").?;
    const salaries = salary_col.asFloat64Buffer().?;
    salaries[0] = 50000.0;
    salaries[1] = 75000.0;
    salaries[2] = 100000.0;

    // Populate active column
    const active_col = df.columnMut("active").?;
    const active = active_col.asBoolBuffer().?;
    active[0] = true;
    active[1] = false;
    active[2] = true;

    try df.setRowCount(3);

    // Verify data
    try testing.expectEqual(@as(u32, 3), df.len());
    try testing.expectEqual(@as(i64, 25), df.column("age").?.asInt64().?[0]);
    try testing.expectApproxEqRel(@as(f64, 50000.0), df.column("salary").?.asFloat64().?[0], 0.01);
    try testing.expectEqual(true, df.column("active").?.asBool().?[0]);
}

test "Integration: large DataFrame with many columns" {
    const allocator = testing.allocator;

    // Create DataFrame with 50 columns
    var col_list = try allocator.alloc(ColumnDesc, 50);
    defer allocator.free(col_list);

    var i: u32 = 0;
    while (i < 50) : (i += 1) {
        const name = try std.fmt.allocPrint(allocator, "col_{}", .{i});
        col_list[i] = ColumnDesc.init(name, .Int64, i);
    }

    var df = try DataFrame.create(allocator, col_list, 100);
    defer {
        // Free column names
        for (col_list) |col| {
            allocator.free(col.name);
        }
        df.deinit();
    }

    try testing.expectEqual(@as(usize, 50), df.columnCount());

    // Verify all columns exist
    i = 0;
    while (i < 50) : (i += 1) {
        const name = try std.fmt.allocPrint(allocator, "col_{}", .{i});
        defer allocator.free(name);
        try testing.expect(df.hasColumn(name));
    }
}

test "Integration: memory leak verification with repeated creation" {
    const allocator = testing.allocator;

    // Create and destroy DataFrames rapidly
    var iter: u32 = 0;
    while (iter < 100) : (iter += 1) {
        const cols = [_]ColumnDesc{
            ColumnDesc.init("a", .Int64, 0),
            ColumnDesc.init("b", .Float64, 1),
            ColumnDesc.init("c", .Bool, 2),
            ColumnDesc.init("d", .String, 3),
        };

        var df = try DataFrame.create(allocator, &cols, 1000);

        // Populate some data
        try df.setRowCount(500);

        const a_col = df.columnMut("a").?;
        const a_data = a_col.asInt64Buffer().?;
        a_data[0] = @intCast(iter);

        df.deinit();
    }

    // testing.allocator will report leaks if any exist
}
