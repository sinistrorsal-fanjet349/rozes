//! Unit tests for rich error handling infrastructure (Phase 4)
//!
//! Tests:
//! - RichError builder pattern
//! - Error formatting with context
//! - ErrorCode conversion
//! - Edge cases (empty fields, truncation)

const std = @import("std");
const testing = std.testing;
const types = @import("../../../core/types.zig");
const RichError = types.RichError;
const ErrorCode = types.ErrorCode;

test "RichError.init creates error with message" {
    const err = RichError.init(.TypeMismatch, "Invalid integer format");

    try testing.expectEqual(ErrorCode.TypeMismatch, err.code);
    try testing.expectEqualStrings("Invalid integer format", err.message);
    try testing.expectEqual(@as(?u32, null), err.row_number);
    try testing.expectEqual(@as(?[]const u8, null), err.column_name);
    try testing.expectEqual(@as(?[]const u8, null), err.field_value);
    try testing.expectEqual(@as(?[]const u8, null), err.hint);
}

test "RichError builder pattern adds row context" {
    const err = RichError.init(.TypeMismatch, "Type error")
        .withRow(47823);

    try testing.expectEqual(@as(?u32, 47823), err.row_number);
    try testing.expectEqualStrings("Type error", err.message);
}

test "RichError builder pattern adds column context" {
    const err = RichError.init(.TypeMismatch, "Type error")
        .withColumn("age");

    try testing.expect(err.column_name != null);
    try testing.expectEqualStrings("age", err.column_name.?);
}

test "RichError builder pattern adds field value" {
    const err = RichError.init(.TypeMismatch, "Type error")
        .withFieldValue("abc");

    try testing.expect(err.field_value != null);
    try testing.expectEqualStrings("abc", err.field_value.?);
}

test "RichError builder pattern adds hint" {
    const err = RichError.init(.InvalidOptions, "Config error")
        .withHint("Use type_inference=false to parse as String");

    try testing.expect(err.hint != null);
    try testing.expectEqualStrings("Use type_inference=false to parse as String", err.hint.?);
}

test "RichError builder pattern chains multiple contexts" {
    const err = RichError.init(.TypeMismatch, "Invalid integer")
        .withRow(47823)
        .withColumn("age")
        .withFieldValue("abc")
        .withHint("Use schema to parse as String");

    try testing.expectEqual(@as(?u32, 47823), err.row_number);
    try testing.expectEqualStrings("age", err.column_name.?);
    try testing.expectEqualStrings("abc", err.field_value.?);
    try testing.expectEqualStrings("Use schema to parse as String", err.hint.?);
}

test "RichError.format creates basic error message" {
    const err = RichError.init(.TypeMismatch, "Type mismatch");

    const formatted = try err.format(testing.allocator);
    defer testing.allocator.free(formatted);

    try testing.expect(std.mem.indexOf(u8, formatted, "RozesError: Type mismatch") != null);
}

test "RichError.format includes row and column context" {
    const err = RichError.init(.TypeMismatch, "Invalid integer format")
        .withRow(47823)
        .withColumn("age");

    const formatted = try err.format(testing.allocator);
    defer testing.allocator.free(formatted);

    // Check main message
    try testing.expect(std.mem.indexOf(u8, formatted, "RozesError: Invalid integer format") != null);

    // Check row number
    try testing.expect(std.mem.indexOf(u8, formatted, "at row 47823") != null);

    // Check column name
    try testing.expect(std.mem.indexOf(u8, formatted, "in column 'age'") != null);
}

test "RichError.format includes field value" {
    const err = RichError.init(.TypeMismatch, "Invalid integer")
        .withRow(100)
        .withColumn("age")
        .withFieldValue("abc");

    const formatted = try err.format(testing.allocator);
    defer testing.allocator.free(formatted);

    try testing.expect(std.mem.indexOf(u8, formatted, "Field value: \"abc\"") != null);
}

test "RichError.format truncates long field values" {
    // Create 200-character field value
    var long_value: [200]u8 = undefined;
    for (&long_value, 0..) |*c, i| {
        c.* = 'A';
        _ = i;
    }

    const err = RichError.init(.TypeMismatch, "Field too long")
        .withFieldValue(&long_value);

    const formatted = try err.format(testing.allocator);
    defer testing.allocator.free(formatted);

    // Should truncate to 100 chars + "..."
    try testing.expect(std.mem.indexOf(u8, formatted, "Field value: \"") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "...") != null);

    // Count 'A' characters in formatted string (should be exactly 100)
    var count: u32 = 0;
    for (formatted) |c| {
        if (c == 'A') count += 1;
    }
    try testing.expectEqual(@as(u32, 100), count);
}

test "RichError.format includes hint" {
    const err = RichError.init(.TypeMismatch, "Invalid integer")
        .withRow(100)
        .withColumn("age")
        .withFieldValue("abc")
        .withHint("Use type_inference=false and specify schema to parse as String");

    const formatted = try err.format(testing.allocator);
    defer testing.allocator.free(formatted);

    try testing.expect(std.mem.indexOf(u8, formatted, "Hint: Use type_inference=false") != null);
}

test "RichError.format full example" {
    // Simulate real error from CSV parsing
    const err = RichError.init(.TypeMismatch, "Type mismatch in column 'age' at row 47,823")
        .withRow(47823)
        .withColumn("age")
        .withFieldValue("abc")
        .withHint("Use type_inference=false and specify schema to parse as String");

    const formatted = try err.format(testing.allocator);
    defer testing.allocator.free(formatted);

    // Verify complete error message
    try testing.expect(std.mem.indexOf(u8, formatted, "RozesError: Type mismatch") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "at row 47823") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "in column 'age'") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "Field value: \"abc\"") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "Hint: Use type_inference=false") != null);

    // Print for manual inspection
    std.debug.print("\n{s}\n", .{formatted});
}

test "ErrorCode.fromError converts common errors" {
    try testing.expectEqual(ErrorCode.OutOfMemory, ErrorCode.fromError(error.OutOfMemory));
    try testing.expectEqual(ErrorCode.InvalidFormat, ErrorCode.fromError(error.InvalidCSV));
    try testing.expectEqual(ErrorCode.TypeMismatch, ErrorCode.fromError(error.TypeMismatch));
    try testing.expectEqual(ErrorCode.ColumnNotFound, ErrorCode.fromError(error.ColumnNotFound));
    try testing.expectEqual(ErrorCode.IndexOutOfBounds, ErrorCode.fromError(error.IndexOutOfBounds));
    try testing.expectEqual(ErrorCode.InvalidOptions, ErrorCode.fromError(error.InvalidOptions));
    try testing.expectEqual(ErrorCode.CSVTooLarge, ErrorCode.fromError(error.CSVTooLarge));
}

test "ErrorCode.fromError handles unknown errors" {
    // Should default to InvalidFormat for unknown errors
    try testing.expectEqual(ErrorCode.InvalidFormat, ErrorCode.fromError(error.FileNotFound));
}

test "ErrorCode has negative values for JavaScript interop" {
    try testing.expect(@intFromEnum(ErrorCode.Success) == 0);
    try testing.expect(@intFromEnum(ErrorCode.OutOfMemory) < 0);
    try testing.expect(@intFromEnum(ErrorCode.InvalidFormat) < 0);
    try testing.expect(@intFromEnum(ErrorCode.TypeMismatch) < 0);
}

test "RichError memory leak test (1000 iterations)" {
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        const err = RichError.init(.TypeMismatch, "Test error")
            .withRow(i)
            .withColumn("test_col")
            .withFieldValue("test_value")
            .withHint("Test hint");

        const formatted = try err.format(testing.allocator);
        testing.allocator.free(formatted);
    }

    // testing.allocator will report leaks automatically
}

test "RichError with empty string field value" {
    const err = RichError.init(.TypeMismatch, "Empty field")
        .withFieldValue("");

    const formatted = try err.format(testing.allocator);
    defer testing.allocator.free(formatted);

    // Should handle empty field value gracefully
    try testing.expect(std.mem.indexOf(u8, formatted, "Field value: \"\"") != null);
}

test "RichError with special characters in field value" {
    const err = RichError.init(.InvalidFormat, "Special chars")
        .withFieldValue("\"Hello\nWorld\"\t\r");

    const formatted = try err.format(testing.allocator);
    defer testing.allocator.free(formatted);

    // Should include special characters as-is (no escaping)
    try testing.expect(std.mem.indexOf(u8, formatted, "Field value:") != null);
}
