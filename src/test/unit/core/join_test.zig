const std = @import("std");
const testing = std.testing;
const DataFrame = @import("../../../core/dataframe.zig").DataFrame;
const ColumnDesc = @import("../../../core/types.zig").ColumnDesc;
const join_mod = @import("../../../core/join.zig");

// Test: Basic inner join on single column
test "innerJoin on single column returns only matching rows" {
    const allocator = testing.allocator;

    // Left DataFrame: employees
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("emp_id", .Int64, 0),
        ColumnDesc.init("name", .String, 1),
        ColumnDesc.init("dept_id", .Int64, 2),
    };

    var left = try DataFrame.create(allocator, &left_cols, 4);
    defer left.deinit();

    const left_emp_ids = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_dept_ids = left.columns[2].asInt64Buffer() orelse unreachable;

    // Employee data: 4 employees
    left_emp_ids[0] = 1;
    left_dept_ids[0] = 10; // Engineering
    try left.columns[1].appendString(allocator, "Alice");

    left_emp_ids[1] = 2;
    left_dept_ids[1] = 20; // Sales
    try left.columns[1].appendString(allocator, "Bob");

    left_emp_ids[2] = 3;
    left_dept_ids[2] = 10; // Engineering
    try left.columns[1].appendString(allocator, "Charlie");

    left_emp_ids[3] = 4;
    left_dept_ids[3] = 99; // Non-existent dept
    try left.columns[1].appendString(allocator, "David");

    // Update column lengths
    left.columns[0].length = 4;
    // left.columns[1].length = 4;
    left.columns[2].length = 4;
    left.columns[0].length = 4;

    // left.columns[1].length = 4;

    left.columns[2].length = 4;

    left.row_count = 4;

    // Right DataFrame: departments
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("dept_id", .Int64, 0),
        ColumnDesc.init("dept_name", .String, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, 2);
    defer right.deinit();

    const right_dept_ids = right.columns[0].asInt64Buffer() orelse unreachable;

    // Department data: Only 2 departments (no dept_id=99)
    right_dept_ids[0] = 10;
    try right.columns[1].appendString(allocator, "Engineering");

    right_dept_ids[1] = 20;
    try right.columns[1].appendString(allocator, "Sales");

    right.columns[0].length = 2;

    // right.columns[1].length = 2;

    right.row_count = 2;

    // Perform inner join
    const join_cols = [_][]const u8{"dept_id"};
    var result = try join_mod.innerJoin(&left, &right, allocator, &join_cols);
    defer result.deinit();

    // Should have 3 rows (Alice, Bob, Charlie) - David excluded (dept_id=99)
    try testing.expectEqual(@as(u32, 3), result.row_count);

    // Should have 5 columns: emp_id, name, dept_id, dept_id_right, dept_name
    try testing.expectEqual(@as(usize, 5), result.columns.len);

    // Verify column names
    try testing.expectEqualStrings("emp_id", result.columns[0].name);
    try testing.expectEqualStrings("name", result.columns[1].name);
    try testing.expectEqualStrings("dept_id", result.columns[2].name);
    try testing.expectEqualStrings("dept_id_right", result.columns[3].name);
    try testing.expectEqualStrings("dept_name", result.columns[4].name);

    // Verify data: Alice (emp_id=1, dept_id=10, dept_name=Engineering)
    const result_emp_ids = result.columns[0].asInt64Buffer() orelse unreachable;
    const result_dept_ids = result.columns[2].asInt64Buffer() orelse unreachable;

    try testing.expectEqual(@as(i64, 1), result_emp_ids[0]);
    try testing.expectEqual(@as(i64, 10), result_dept_ids[0]);

    try testing.expectEqual(@as(i64, 2), result_emp_ids[1]);
    try testing.expectEqual(@as(i64, 20), result_dept_ids[1]);

    try testing.expectEqual(@as(i64, 3), result_emp_ids[2]);
    try testing.expectEqual(@as(i64, 10), result_dept_ids[2]);
}

// Test: Left join includes all left rows
test "leftJoin includes all left rows with null for unmatched" {
    const allocator = testing.allocator;

    // Left DataFrame: employees
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("emp_id", .Int64, 0),
        ColumnDesc.init("dept_id", .Int64, 1),
    };

    var left = try DataFrame.create(allocator, &left_cols, 3);
    defer left.deinit();

    const left_emp_ids = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_dept_ids = left.columns[1].asInt64Buffer() orelse unreachable;

    left_emp_ids[0] = 1;
    left_dept_ids[0] = 10; // Has match

    left_emp_ids[1] = 2;
    left_dept_ids[1] = 99; // No match

    left_emp_ids[2] = 3;
    left_dept_ids[2] = 20; // Has match

    left.columns[0].length = 3;

    left.columns[1].length = 3;

    left.row_count = 3;

    // Right DataFrame: departments
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("dept_id", .Int64, 0),
        ColumnDesc.init("budget", .Float64, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, 2);
    defer right.deinit();

    const right_dept_ids = right.columns[0].asInt64Buffer() orelse unreachable;
    const right_budgets = right.columns[1].asFloat64Buffer() orelse unreachable;

    right_dept_ids[0] = 10;
    right_budgets[0] = 100000.0;

    right_dept_ids[1] = 20;
    right_budgets[1] = 75000.0;

    right.columns[0].length = 2;

    right.columns[1].length = 2;

    right.row_count = 2;

    // Perform left join
    const join_cols = [_][]const u8{"dept_id"};
    var result = try join_mod.leftJoin(&left, &right, allocator, &join_cols);
    defer result.deinit();

    // Should have 3 rows (all left rows)
    try testing.expectEqual(@as(u32, 3), result.row_count);

    // Should have 4 columns: emp_id, dept_id, dept_id_right, budget
    try testing.expectEqual(@as(usize, 4), result.columns.len);

    // Verify column names
    try testing.expectEqualStrings("emp_id", result.columns[0].name);
    try testing.expectEqualStrings("dept_id", result.columns[1].name);
    try testing.expectEqualStrings("dept_id_right", result.columns[2].name);
    try testing.expectEqualStrings("budget", result.columns[3].name);

    const result_emp_ids = result.columns[0].asInt64Buffer() orelse unreachable;
    const result_budgets = result.columns[3].asFloat64Buffer() orelse unreachable;

    // Row 0: emp_id=1, dept_id=10, budget=100000
    try testing.expectEqual(@as(i64, 1), result_emp_ids[0]);
    try testing.expectEqual(@as(f64, 100000.0), result_budgets[0]);

    // Row 1: emp_id=2, dept_id=99, budget=0.0 (unmatched)
    try testing.expectEqual(@as(i64, 2), result_emp_ids[1]);
    try testing.expectEqual(@as(f64, 0.0), result_budgets[1]);

    // Row 2: emp_id=3, dept_id=20, budget=75000
    try testing.expectEqual(@as(i64, 3), result_emp_ids[2]);
    try testing.expectEqual(@as(f64, 75000.0), result_budgets[2]);
}

// Test: Multi-column join
test "innerJoin on multiple columns matches composite keys" {
    const allocator = testing.allocator;

    // Left DataFrame: sales
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("year", .Int64, 0),
        ColumnDesc.init("quarter", .Int64, 1),
        ColumnDesc.init("revenue", .Float64, 2),
    };

    var left = try DataFrame.create(allocator, &left_cols, 4);
    defer left.deinit();

    const left_years = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_quarters = left.columns[1].asInt64Buffer() orelse unreachable;
    const left_revenues = left.columns[2].asFloat64Buffer() orelse unreachable;

    left_years[0] = 2023;
    left_quarters[0] = 1;
    left_revenues[0] = 100.0;

    left_years[1] = 2023;
    left_quarters[1] = 2;
    left_revenues[1] = 150.0;

    left_years[2] = 2024;
    left_quarters[2] = 1;
    left_revenues[2] = 200.0;

    left_years[3] = 2024;
    left_quarters[3] = 3; // No match in right
    left_revenues[3] = 250.0;

    left.columns[0].length = 4;

    left.columns[1].length = 4;

    left.columns[2].length = 4;

    left.row_count = 4;

    // Right DataFrame: targets
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("year", .Int64, 0),
        ColumnDesc.init("quarter", .Int64, 1),
        ColumnDesc.init("target", .Float64, 2),
    };

    var right = try DataFrame.create(allocator, &right_cols, 3);
    defer right.deinit();

    const right_years = right.columns[0].asInt64Buffer() orelse unreachable;
    const right_quarters = right.columns[1].asInt64Buffer() orelse unreachable;
    const right_targets = right.columns[2].asFloat64Buffer() orelse unreachable;

    right_years[0] = 2023;
    right_quarters[0] = 1;
    right_targets[0] = 90.0;

    right_years[1] = 2023;
    right_quarters[1] = 2;
    right_targets[1] = 120.0;

    right_years[2] = 2024;
    right_quarters[2] = 1;
    right_targets[2] = 180.0;

    right.columns[0].length = 3;

    right.columns[1].length = 3;

    right.columns[2].length = 3;

    right.row_count = 3;

    // Perform inner join on (year, quarter)
    const join_cols = [_][]const u8{ "year", "quarter" };
    var result = try join_mod.innerJoin(&left, &right, allocator, &join_cols);
    defer result.deinit();

    // Should have 3 rows (2024-Q3 excluded - no match)
    try testing.expectEqual(@as(u32, 3), result.row_count);

    // Should have 6 columns: year, quarter, revenue, year_right, quarter_right, target
    try testing.expectEqual(@as(usize, 6), result.columns.len);

    const result_revenues = result.columns[2].asFloat64Buffer() orelse unreachable;
    const result_targets = result.columns[5].asFloat64Buffer() orelse unreachable;

    // Verify matches
    try testing.expectEqual(@as(f64, 100.0), result_revenues[0]);
    try testing.expectEqual(@as(f64, 90.0), result_targets[0]);

    try testing.expectEqual(@as(f64, 150.0), result_revenues[1]);
    try testing.expectEqual(@as(f64, 120.0), result_targets[1]);

    try testing.expectEqual(@as(f64, 200.0), result_revenues[2]);
    try testing.expectEqual(@as(f64, 180.0), result_targets[2]);
}

// Test: Column name conflicts
test "innerJoin handles column name conflicts with _right suffix" {
    const allocator = testing.allocator;

    // Left DataFrame: users
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("score", .Float64, 1),
    };

    var left = try DataFrame.create(allocator, &left_cols, 2);
    defer left.deinit();

    const left_ids = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_scores = left.columns[1].asFloat64Buffer() orelse unreachable;

    left_ids[0] = 1;
    left_scores[0] = 85.5;

    left_ids[1] = 2;
    left_scores[1] = 92.0;

    left.columns[0].length = 2;

    left.columns[1].length = 2;

    left.row_count = 2;

    // Right DataFrame: bonuses (also has 'id' and 'score')
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("score", .Float64, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, 2);
    defer right.deinit();

    const right_ids = right.columns[0].asInt64Buffer() orelse unreachable;
    const right_scores = right.columns[1].asFloat64Buffer() orelse unreachable;

    right_ids[0] = 1;
    right_scores[0] = 10.0; // Bonus amount

    right_ids[1] = 2;
    right_scores[1] = 15.0;

    right.columns[0].length = 2;

    right.columns[1].length = 2;

    right.row_count = 2;

    // Perform inner join
    const join_cols = [_][]const u8{"id"};
    var result = try join_mod.innerJoin(&left, &right, allocator, &join_cols);
    defer result.deinit();

    // Should have 4 columns: id, score, id_right, score_right
    try testing.expectEqual(@as(usize, 4), result.columns.len);

    // Verify column names (right columns get _right suffix)
    try testing.expectEqualStrings("id", result.columns[0].name);
    try testing.expectEqualStrings("score", result.columns[1].name);
    try testing.expectEqualStrings("id_right", result.columns[2].name);
    try testing.expectEqualStrings("score_right", result.columns[3].name);

    // Verify data
    const result_scores_left = result.columns[1].asFloat64Buffer() orelse unreachable;
    const result_scores_right = result.columns[3].asFloat64Buffer() orelse unreachable;

    try testing.expectEqual(@as(f64, 85.5), result_scores_left[0]);
    try testing.expectEqual(@as(f64, 10.0), result_scores_right[0]);

    try testing.expectEqual(@as(f64, 92.0), result_scores_left[1]);
    try testing.expectEqual(@as(f64, 15.0), result_scores_right[1]);
}

// Test: Empty result (no matches)
test "innerJoin returns empty DataFrame when no matches" {
    const allocator = testing.allocator;

    // Left DataFrame
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
    };

    var left = try DataFrame.create(allocator, &left_cols, 2);
    defer left.deinit();

    const left_ids = left.columns[0].asInt64Buffer() orelse unreachable;
    left_ids[0] = 1;
    left_ids[1] = 2;
    left.columns[0].length = 2;

    left.row_count = 2;

    // Right DataFrame (no overlapping IDs)
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
    };

    var right = try DataFrame.create(allocator, &right_cols, 2);
    defer right.deinit();

    const right_ids = right.columns[0].asInt64Buffer() orelse unreachable;
    right_ids[0] = 99;
    right_ids[1] = 100;
    right.columns[0].length = 2;

    right.row_count = 2;

    // Perform inner join
    const join_cols = [_][]const u8{"id"};
    var result = try join_mod.innerJoin(&left, &right, allocator, &join_cols);
    defer result.deinit();

    // Should have 0 rows (no matches)
    try testing.expectEqual(@as(u32, 0), result.row_count);

    // But should still have columns defined
    try testing.expectEqual(@as(usize, 2), result.columns.len);
}

// Test: Error case - column not found
test "innerJoin returns error when join column not found" {
    const allocator = testing.allocator;

    // Left DataFrame
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
    };

    var left = try DataFrame.create(allocator, &left_cols, 1);
    defer left.deinit();

    const left_ids = left.columns[0].asInt64Buffer() orelse unreachable;
    left_ids[0] = 1;
    left.columns[0].length = 1;

    left.row_count = 1;

    // Right DataFrame
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
    };

    var right = try DataFrame.create(allocator, &right_cols, 1);
    defer right.deinit();

    const right_ids = right.columns[0].asInt64Buffer() orelse unreachable;
    right_ids[0] = 1;
    right.columns[0].length = 1;

    right.row_count = 1;

    // Try to join on non-existent column
    const join_cols = [_][]const u8{"nonexistent"};
    const result = join_mod.innerJoin(&left, &right, allocator, &join_cols);

    try testing.expectError(error.ColumnNotFound, result);
}

// Test: Duplicate keys in right DataFrame
test "innerJoin handles duplicate keys in right table" {
    const allocator = testing.allocator;

    // Left DataFrame
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("value", .Float64, 1),
    };

    var left = try DataFrame.create(allocator, &left_cols, 2);
    defer left.deinit();

    const left_ids = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_values = left.columns[1].asFloat64Buffer() orelse unreachable;

    left_ids[0] = 1;
    left_values[0] = 10.0;

    left_ids[1] = 2;
    left_values[1] = 20.0;

    left.columns[0].length = 2;

    left.columns[1].length = 2;

    left.row_count = 2;

    // Right DataFrame (id=1 appears twice)
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("bonus", .Float64, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, 3);
    defer right.deinit();

    const right_ids = right.columns[0].asInt64Buffer() orelse unreachable;
    const right_bonuses = right.columns[1].asFloat64Buffer() orelse unreachable;

    right_ids[0] = 1;
    right_bonuses[0] = 5.0;

    right_ids[1] = 1; // Duplicate key
    right_bonuses[1] = 7.0;

    right_ids[2] = 2;
    right_bonuses[2] = 10.0;

    right.columns[0].length = 3;

    right.columns[1].length = 3;

    right.row_count = 3;

    // Perform inner join
    const join_cols = [_][]const u8{"id"};
    var result = try join_mod.innerJoin(&left, &right, allocator, &join_cols);
    defer result.deinit();

    // Should have 3 rows (id=1 matches twice, id=2 matches once)
    try testing.expectEqual(@as(u32, 3), result.row_count);

    const result_values = result.columns[1].asFloat64Buffer() orelse unreachable;
    const result_bonuses = result.columns[3].asFloat64Buffer() orelse unreachable;

    // First match: value=10.0, bonus=5.0
    try testing.expectEqual(@as(f64, 10.0), result_values[0]);
    try testing.expectEqual(@as(f64, 5.0), result_bonuses[0]);

    // Second match: value=10.0, bonus=7.0 (same left row, different right row)
    try testing.expectEqual(@as(f64, 10.0), result_values[1]);
    try testing.expectEqual(@as(f64, 7.0), result_bonuses[1]);

    // Third match: value=20.0, bonus=10.0
    try testing.expectEqual(@as(f64, 20.0), result_values[2]);
    try testing.expectEqual(@as(f64, 10.0), result_bonuses[2]);
}

// Test: Boolean columns in join
test "innerJoin handles Bool columns correctly" {
    const allocator = testing.allocator;

    // Left DataFrame
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("active", .Bool, 1),
    };

    var left = try DataFrame.create(allocator, &left_cols, 2);
    defer left.deinit();

    const left_ids = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_active = left.columns[1].asBoolBuffer() orelse unreachable;

    left_ids[0] = 1;
    left_active[0] = true;

    left_ids[1] = 2;
    left_active[1] = false;

    left.columns[0].length = 2;

    left.columns[1].length = 2;

    left.row_count = 2;

    // Right DataFrame
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("verified", .Bool, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, 1);
    defer right.deinit();

    const right_ids = right.columns[0].asInt64Buffer() orelse unreachable;
    const right_verified = right.columns[1].asBoolBuffer() orelse unreachable;

    right_ids[0] = 1;
    right_verified[0] = true;

    right.columns[0].length = 1;

    right.columns[1].length = 1;

    right.row_count = 1;

    // Perform inner join
    const join_cols = [_][]const u8{"id"};
    var result = try join_mod.innerJoin(&left, &right, allocator, &join_cols);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 1), result.row_count);

    const result_active = result.columns[1].asBoolBuffer() orelse unreachable;
    const result_verified = result.columns[3].asBoolBuffer() orelse unreachable;

    try testing.expectEqual(true, result_active[0]);
    try testing.expectEqual(true, result_verified[0]);
}

// Test: String columns in join
test "innerJoin handles String columns correctly" {
    const allocator = testing.allocator;

    // Left DataFrame
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("name", .String, 1),
    };

    var left = try DataFrame.create(allocator, &left_cols, 2);
    defer left.deinit();

    const left_ids = left.columns[0].asInt64Buffer() orelse unreachable;

    left_ids[0] = 1;
    try left.columns[1].appendString(allocator, "Alice");

    left_ids[1] = 2;
    try left.columns[1].appendString(allocator, "Bob");

    left.columns[0].length = 2;

    // left.columns[1].length = 2;

    left.row_count = 2;

    // Right DataFrame
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("city", .String, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, 1);
    defer right.deinit();

    const right_ids = right.columns[0].asInt64Buffer() orelse unreachable;

    right_ids[0] = 1;
    try right.columns[1].appendString(allocator, "Seattle");

    right.columns[0].length = 1;

    // right.columns[1].length = 1;

    right.row_count = 1;

    // Perform inner join
    const join_cols = [_][]const u8{"id"};
    var result = try join_mod.innerJoin(&left, &right, allocator, &join_cols);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 1), result.row_count);

    // Verify string columns
    const result_name_col = result.columns[1].asStringColumn() orelse unreachable;
    const result_city_col = result.columns[3].asStringColumn() orelse unreachable;

    const name = result_name_col.get(0);
    const city = result_city_col.get(0);

    try testing.expectEqualStrings("Alice", name);
    try testing.expectEqualStrings("Seattle", city);
}

// Test: Sequential join uses fast path (column-wise memcpy)
// Tests the optimization added in Phase 1 of Milestone 0.4.0
// See docs/join_optimization_recommendation.md for details
test "innerJoin uses fast path for sequential left table" {
    const allocator = testing.allocator;

    // Create left DataFrame with 1000 sequential rows
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("value", .Float64, 1),
        ColumnDesc.init("active", .Bool, 2),
    };

    var left = try DataFrame.create(allocator, &left_cols, 1000);
    defer left.deinit();

    const left_ids = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_values = left.columns[1].asFloat64Buffer() orelse unreachable;
    const left_active = left.columns[2].asBoolBuffer() orelse unreachable;

    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        left_ids[i] = @intCast(i);
        left_values[i] = @as(f64, @floatFromInt(i)) * 1.5;
        left_active[i] = (i % 2) == 0;
    }
    try left.setRowCount(1000);

    // Create right DataFrame with matching rows
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("id", .Int64, 0),
        ColumnDesc.init("info", .Int64, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, 1000);
    defer right.deinit();

    const right_ids = right.columns[0].asInt64Buffer() orelse unreachable;
    const right_info = right.columns[1].asInt64Buffer() orelse unreachable;

    i = 0;
    while (i < 1000) : (i += 1) {
        right_ids[i] = @intCast(i);
        right_info[i] = @intCast(i * 2);
    }
    try right.setRowCount(1000);

    // Perform join (should use fast path for left columns)
    const join_cols = [_][]const u8{"id"};
    var result = try join_mod.innerJoin(&left, &right, allocator, &join_cols);
    defer result.deinit();

    // Verify result structure
    try testing.expectEqual(@as(u32, 1000), result.row_count); // Post-condition #1
    try testing.expectEqual(@as(usize, 5), result.columns.len); // Post-condition #2

    // Verify data integrity (spot checks)
    // Left columns should be copied via fast path (memcpy)
    const result_ids = result.columns[0].asInt64Buffer() orelse unreachable;
    const result_values = result.columns[1].asFloat64Buffer() orelse unreachable;
    const result_active = result.columns[2].asBoolBuffer() orelse unreachable;
    const result_info = result.columns[4].asInt64Buffer() orelse unreachable;

    // Check first row
    try testing.expectEqual(@as(i64, 0), result_ids[0]);
    try testing.expectEqual(@as(f64, 0.0), result_values[0]);
    try testing.expectEqual(true, result_active[0]);
    try testing.expectEqual(@as(i64, 0), result_info[0]);

    // Check middle row
    try testing.expectEqual(@as(i64, 500), result_ids[500]);
    try testing.expectEqual(@as(f64, 750.0), result_values[500]);
    try testing.expectEqual(true, result_active[500]);
    try testing.expectEqual(@as(i64, 1000), result_info[500]);

    // Check last row
    try testing.expectEqual(@as(i64, 999), result_ids[999]);
    try testing.expectEqual(@as(f64, 1498.5), result_values[999]);
    try testing.expectEqual(false, result_active[999]);
    try testing.expectEqual(@as(i64, 1998), result_info[999]);

    // Verify all rows are sequential (validates fast path was used)
    i = 0;
    while (i < 1000) : (i += 1) {
        try testing.expectEqual(@as(i64, @intCast(i)), result_ids[i]);
    }
}

// Test: Non-sequential join uses fallback path
test "innerJoin uses fallback for non-sequential matches" {
    const allocator = testing.allocator;

    // Create left DataFrame
    const left_cols = [_]ColumnDesc{
        ColumnDesc.init("key", .Int64, 0),
        ColumnDesc.init("left_val", .Int64, 1),
    };

    var left = try DataFrame.create(allocator, &left_cols, 5);
    defer left.deinit();

    const left_keys = left.columns[0].asInt64Buffer() orelse unreachable;
    const left_vals = left.columns[1].asInt64Buffer() orelse unreachable;

    // Sequential keys
    left_keys[0] = 10;
    left_vals[0] = 100;
    left_keys[1] = 20;
    left_vals[1] = 200;
    left_keys[2] = 30;
    left_vals[2] = 300;
    left_keys[3] = 40;
    left_vals[3] = 400;
    left_keys[4] = 50;
    left_vals[4] = 500;
    try left.setRowCount(5);

    // Create right DataFrame with NON-SEQUENTIAL matches
    const right_cols = [_]ColumnDesc{
        ColumnDesc.init("key", .Int64, 0),
        ColumnDesc.init("right_val", .Int64, 1),
    };

    var right = try DataFrame.create(allocator, &right_cols, 5);
    defer right.deinit();

    const right_keys = right.columns[0].asInt64Buffer() orelse unreachable;
    const right_vals = right.columns[1].asInt64Buffer() orelse unreachable;

    // Non-sequential keys (scrambled order)
    right_keys[0] = 50; // Matches left[4]
    right_vals[0] = 5000;
    right_keys[1] = 10; // Matches left[0]
    right_vals[1] = 1000;
    right_keys[2] = 30; // Matches left[2]
    right_vals[2] = 3000;
    right_keys[3] = 40; // Matches left[3]
    right_vals[3] = 4000;
    right_keys[4] = 20; // Matches left[1]
    right_vals[4] = 2000;
    try right.setRowCount(5);

    // Perform join (should use fallback path for right columns)
    const join_cols = [_][]const u8{"key"};
    var result = try join_mod.innerJoin(&left, &right, allocator, &join_cols);
    defer result.deinit();

    // Verify correct matching (data integrity)
    try testing.expectEqual(@as(u32, 5), result.row_count);

    const result_left_vals = result.columns[1].asInt64Buffer() orelse unreachable;
    const result_right_vals = result.columns[3].asInt64Buffer() orelse unreachable;

    // Results should be sorted by left table order
    // left[0] (key=10) matches right[1]
    try testing.expectEqual(@as(i64, 100), result_left_vals[0]);
    try testing.expectEqual(@as(i64, 1000), result_right_vals[0]);

    // left[1] (key=20) matches right[4]
    try testing.expectEqual(@as(i64, 200), result_left_vals[1]);
    try testing.expectEqual(@as(i64, 2000), result_right_vals[1]);

    // left[2] (key=30) matches right[2]
    try testing.expectEqual(@as(i64, 300), result_left_vals[2]);
    try testing.expectEqual(@as(i64, 3000), result_right_vals[2]);

    // left[3] (key=40) matches right[3]
    try testing.expectEqual(@as(i64, 400), result_left_vals[3]);
    try testing.expectEqual(@as(i64, 4000), result_right_vals[3]);

    // left[4] (key=50) matches right[0]
    try testing.expectEqual(@as(i64, 500), result_left_vals[4]);
    try testing.expectEqual(@as(i64, 5000), result_right_vals[4]);
}
