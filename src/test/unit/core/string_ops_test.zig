//! Unit tests for string operations (lower, upper, trim, contains, replace, split, etc.)
//!
//! Test coverage:
//! - Case conversion: lower, upper
//! - Whitespace handling: trim, strip
//! - Pattern matching: contains, startsWith, endsWith
//! - Replacement: replace
//! - Extraction: slice, len
//! - Splitting: split
//! - UTF-8 edge cases: emoji, CJK characters, special symbols
//! - Empty string handling
//! - Memory safety (1000 iterations)

const std = @import("std");
const testing = std.testing;
const Series = @import("../../../core/series.zig").Series;
const string_ops = @import("../../../core/string_ops.zig");

// Helper function to create string Series
fn createStringSeries(allocator: std.mem.Allocator, strings: []const []const u8) !Series {
    const data = try allocator.alloc([]const u8, strings.len);
    for (strings, 0..) |str, i| {
        const copy = try allocator.alloc(u8, str.len);
        @memcpy(copy, str);
        data[i] = copy;
    }
    return Series{
        .name = "test",
        .valueType = .String,
        .data = .{ .String = data },
        .length = @intCast(strings.len),
    };
}

// Helper to free string Series
fn freeStringSeries(allocator: std.mem.Allocator, series: Series) void {
    for (series.data.String) |str| {
        allocator.free(str);
    }
    allocator.free(series.data.String);
}

// Test: lower() converts to lowercase
test "lower converts strings to lowercase" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "HELLO", "World", "MiXeD" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.lower(&series, allocator);
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("hello", result.data.String[0]);
    try testing.expectEqualStrings("world", result.data.String[1]);
    try testing.expectEqualStrings("mixed", result.data.String[2]);
}

// Test: upper() converts to uppercase
test "upper converts strings to uppercase" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hello", "World", "MiXeD" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.upper(&series, allocator);
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("HELLO", result.data.String[0]);
    try testing.expectEqualStrings("WORLD", result.data.String[1]);
    try testing.expectEqualStrings("MIXED", result.data.String[2]);
}

// Test: trim() removes leading/trailing whitespace
test "trim removes leading and trailing whitespace" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "  hello  ", "\tworld\n", "  mixed\t\n" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.trim(&series, allocator);
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("hello", result.data.String[0]);
    try testing.expectEqualStrings("world", result.data.String[1]);
    try testing.expectEqualStrings("mixed", result.data.String[2]);
}

// Test: trim() handles strings with no whitespace
test "trim handles strings without whitespace" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hello", "world" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.trim(&series, allocator);
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("hello", result.data.String[0]);
    try testing.expectEqualStrings("world", result.data.String[1]);
}

// Test: strip() is alias for trim()
test "strip is alias for trim" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "  hello  ", "  world  " };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.strip(&series, allocator);
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("hello", result.data.String[0]);
    try testing.expectEqualStrings("world", result.data.String[1]);
}

// Test: contains() finds substring
test "contains finds substring in strings" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hello world", "foo bar", "hello" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.contains(&series, allocator, "hello");
    defer allocator.free(result.data.Bool);

    try testing.expect(result.data.Bool[0]); // "hello world" contains "hello"
    try testing.expect(!result.data.Bool[1]); // "foo bar" does not contain "hello"
    try testing.expect(result.data.Bool[2]); // "hello" contains "hello"
}

// Test: contains() is case-sensitive
test "contains is case-sensitive" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "Hello", "HELLO", "hello" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.contains(&series, allocator, "hello");
    defer allocator.free(result.data.Bool);

    try testing.expect(!result.data.Bool[0]); // "Hello" != "hello"
    try testing.expect(!result.data.Bool[1]); // "HELLO" != "hello"
    try testing.expect(result.data.Bool[2]); // "hello" == "hello"
}

// Test: replace() replaces all occurrences
test "replace replaces all occurrences of substring" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hello hello", "world", "hello world hello" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.replace(&series, allocator, "hello", "hi");
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("hi hi", result.data.String[0]);
    try testing.expectEqualStrings("world", result.data.String[1]);
    try testing.expectEqualStrings("hi world hi", result.data.String[2]);
}

// Test: replace() handles no matches
test "replace handles strings with no matches" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "foo bar", "baz" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.replace(&series, allocator, "hello", "hi");
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("foo bar", result.data.String[0]);
    try testing.expectEqualStrings("baz", result.data.String[1]);
}

// Test: replace() with different length replacement
test "replace handles different length replacement" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "a_b_c", "x_y" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.replace(&series, allocator, "_", "---");
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("a---b---c", result.data.String[0]);
    try testing.expectEqualStrings("x---y", result.data.String[1]);
}

// Test: slice() extracts substring
test "slice extracts substring by indices" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hello world", "foo", "12345" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.slice(&series, allocator, 0, 5);
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("hello", result.data.String[0]);
    try testing.expectEqualStrings("foo", result.data.String[1]); // Clamped to length
    try testing.expectEqualStrings("12345", result.data.String[2]);
}

// Test: slice() handles out-of-bounds indices
test "slice handles out-of-bounds indices" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hi", "world" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.slice(&series, allocator, 0, 100);
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("hi", result.data.String[0]); // Clamped
    try testing.expectEqualStrings("world", result.data.String[1]); // Clamped
}

// Test: len() returns string lengths
test "len returns string lengths" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hello", "hi", "world!" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.len(&series, allocator);
    defer allocator.free(result.data.Int64);

    try testing.expectEqual(@as(i64, 5), result.data.Int64[0]);
    try testing.expectEqual(@as(i64, 2), result.data.Int64[1]);
    try testing.expectEqual(@as(i64, 6), result.data.Int64[2]);
}

// Test: len() handles empty strings
test "len handles empty strings" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "", "hello", "" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.len(&series, allocator);
    defer allocator.free(result.data.Int64);

    try testing.expectEqual(@as(i64, 0), result.data.Int64[0]);
    try testing.expectEqual(@as(i64, 5), result.data.Int64[1]);
    try testing.expectEqual(@as(i64, 0), result.data.Int64[2]);
}

// Test: split() splits by delimiter
test "split splits strings by delimiter" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "a,b,c", "x,y", "z" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    var result = try string_ops.split(&series, allocator, ",");
    defer result.free(allocator);

    // Should have 3 parts (max splits in any row)
    try testing.expectEqual(@as(u32, 3), result.num_parts);

    // Part 0: ["a", "x", "z"]
    try testing.expectEqualStrings("a", result.parts[0].data.String[0]);
    try testing.expectEqualStrings("x", result.parts[0].data.String[1]);
    try testing.expectEqualStrings("z", result.parts[0].data.String[2]);

    // Part 1: ["b", "y", ""]
    try testing.expectEqualStrings("b", result.parts[1].data.String[0]);
    try testing.expectEqualStrings("y", result.parts[1].data.String[1]);
    try testing.expectEqualStrings("", result.parts[1].data.String[2]);

    // Part 2: ["c", "", ""]
    try testing.expectEqualStrings("c", result.parts[2].data.String[0]);
    try testing.expectEqualStrings("", result.parts[2].data.String[1]);
    try testing.expectEqualStrings("", result.parts[2].data.String[2]);
}

// Test: split() with no delimiter in some strings
test "split handles strings without delimiter" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hello", "foo,bar" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    var result = try string_ops.split(&series, allocator, ",");
    defer result.free(allocator);

    try testing.expectEqual(@as(u32, 2), result.num_parts);

    // Part 0: ["hello", "foo"]
    try testing.expectEqualStrings("hello", result.parts[0].data.String[0]);
    try testing.expectEqualStrings("foo", result.parts[0].data.String[1]);

    // Part 1: ["", "bar"]
    try testing.expectEqualStrings("", result.parts[1].data.String[0]);
    try testing.expectEqualStrings("bar", result.parts[1].data.String[1]);
}

// Test: startsWith() checks prefix
test "startsWith checks if strings start with prefix" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hello world", "hi there", "world" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.startsWith(&series, allocator, "hello");
    defer allocator.free(result.data.Bool);

    try testing.expect(result.data.Bool[0]); // "hello world" starts with "hello"
    try testing.expect(!result.data.Bool[1]); // "hi there" does not
    try testing.expect(!result.data.Bool[2]); // "world" does not
}

// Test: endsWith() checks suffix
test "endsWith checks if strings end with suffix" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hello world", "world", "hello" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.endsWith(&series, allocator, "world");
    defer allocator.free(result.data.Bool);

    try testing.expect(result.data.Bool[0]); // "hello world" ends with "world"
    try testing.expect(result.data.Bool[1]); // "world" ends with "world"
    try testing.expect(!result.data.Bool[2]); // "hello" does not
}

// Test: UTF-8 handling - emoji
test "lower handles emoji correctly" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "Hello 👋", "WORLD 🌍" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.lower(&series, allocator);
    defer freeStringSeries(allocator, result);

    // Emoji should be preserved
    try testing.expectEqualStrings("hello 👋", result.data.String[0]);
    try testing.expectEqualStrings("world 🌍", result.data.String[1]);
}

// Test: UTF-8 handling - CJK characters
test "upper preserves CJK characters" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "hello 你好", "world 世界" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.upper(&series, allocator);
    defer freeStringSeries(allocator, result);

    // CJK characters should be preserved
    try testing.expectEqualStrings("HELLO 你好", result.data.String[0]);
    try testing.expectEqualStrings("WORLD 世界", result.data.String[1]);
}

// Test: UTF-8 handling - special symbols
test "trim handles UTF-8 symbols correctly" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "  €100  ", "  £50  " };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = try string_ops.trim(&series, allocator);
    defer freeStringSeries(allocator, result);

    try testing.expectEqualStrings("€100", result.data.String[0]);
    try testing.expectEqualStrings("£50", result.data.String[1]);
}

// Test: Empty strings
test "operations handle empty strings correctly" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "", "hello", "" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const lower_result = try string_ops.lower(&series, allocator);
    defer freeStringSeries(allocator, lower_result);
    try testing.expectEqual(@as(usize, 0), lower_result.data.String[0].len);

    const upper_result = try string_ops.upper(&series, allocator);
    defer freeStringSeries(allocator, upper_result);
    try testing.expectEqual(@as(usize, 0), upper_result.data.String[0].len);

    const trim_result = try string_ops.trim(&series, allocator);
    defer freeStringSeries(allocator, trim_result);
    try testing.expectEqual(@as(usize, 0), trim_result.data.String[0].len);
}

// Test: contains with empty strings
test "contains with empty pattern should error" {
    const allocator = testing.allocator;
    const input = [_][]const u8{"hello"};
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = string_ops.contains(&series, allocator, "");
    try testing.expectError(error.EmptyPattern, result);
}

// Test: replace with empty pattern should error
test "replace with empty pattern should error" {
    const allocator = testing.allocator;
    const input = [_][]const u8{"hello"};
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = string_ops.replace(&series, allocator, "", "x");
    try testing.expectError(error.EmptyPattern, result);
}

// Test: split with empty delimiter should error
test "split with empty delimiter should error" {
    const allocator = testing.allocator;
    const input = [_][]const u8{"hello"};
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    const result = string_ops.split(&series, allocator, "");
    try testing.expectError(error.EmptyDelimiter, result);
}

// Test: Memory leak testing (1000 iterations)
test "string operations do not leak memory" {
    const allocator = testing.allocator;
    const input = [_][]const u8{ "  HELLO WORLD  ", "foo bar" };
    const series = try createStringSeries(allocator, &input);
    defer freeStringSeries(allocator, series);

    // Run 1000 iterations of each operation
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        const lower_result = try string_ops.lower(&series, allocator);
        freeStringSeries(allocator, lower_result);

        const upper_result = try string_ops.upper(&series, allocator);
        freeStringSeries(allocator, upper_result);

        const trim_result = try string_ops.trim(&series, allocator);
        freeStringSeries(allocator, trim_result);

        const contains_result = try string_ops.contains(&series, allocator, "HELLO");
        allocator.free(contains_result.data.Bool);

        const replace_result = try string_ops.replace(&series, allocator, "HELLO", "hi");
        freeStringSeries(allocator, replace_result);

        const len_result = try string_ops.len(&series, allocator);
        allocator.free(len_result.data.Int64);
    }

    // testing.allocator will report leaks automatically
}

// Test: Large dataset performance
test "string operations handle large datasets efficiently" {
    const allocator = testing.allocator;
    const size = 1000;

    // Create large dataset
    var strings = try allocator.alloc([]const u8, size);
    defer {
        for (strings) |str| {
            allocator.free(str);
        }
        allocator.free(strings);
    }

    for (0..size) |i| {
        const str = try std.fmt.allocPrint(allocator, "test_string_{d}", .{i});
        strings[i] = str;
    }

    const data = try allocator.alloc([]const u8, size);
    for (strings, 0..) |str, i| {
        const copy = try allocator.alloc(u8, str.len);
        @memcpy(copy, str);
        data[i] = copy;
    }

    const series = Series{
        .name = "large",
        .valueType = .String,
        .data = .{ .String = data },
        .length = size,
    };

    // Test operations on large dataset
    const lower_result = try string_ops.lower(&series, allocator);
    defer freeStringSeries(allocator, lower_result);

    const upper_result = try string_ops.upper(&series, allocator);
    defer freeStringSeries(allocator, upper_result);

    const trim_result = try string_ops.trim(&series, allocator);
    defer freeStringSeries(allocator, trim_result);

    try testing.expectEqual(size, lower_result.length);
    try testing.expectEqual(size, upper_result.length);
    try testing.expectEqual(size, trim_result.length);

    freeStringSeries(allocator, series);
}
