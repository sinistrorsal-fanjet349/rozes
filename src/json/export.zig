//! JSON Export - DataFrame to JSON
//!
//! Supports 3 export formats:
//! 1. Line-delimited JSON (NDJSON)
//! 2. JSON Array
//! 3. Columnar JSON (most efficient)
//!
//! See docs/TODO.md Phase 5 for specifications.

const std = @import("std");
const DataFrame = @import("../core/dataframe.zig").DataFrame;
const ValueType = @import("../core/types.zig").ValueType;
const JSONFormat = @import("parser.zig").JSONFormat;

const MAX_JSON_SIZE: u32 = 1_000_000_000; // 1GB max

/// JSON export options
pub const ExportOptions = struct {
    /// JSON format to export
    format: JSONFormat = .LineDelimited,

    /// Pretty-print output (default: false for compact)
    pretty: bool = false,

    /// Indent size for pretty printing (default: 2 spaces)
    indent: u8 = 2,
};

/// Export DataFrame to JSON string
pub fn toJSON(
    df: *const DataFrame,
    allocator: std.mem.Allocator,
    opts: ExportOptions,
) ![]const u8 {
    std.debug.assert(df.row_count > 0 or df.columns.len > 0); // Valid DataFrame
    std.debug.assert(opts.indent > 0 and opts.indent <= 8); // Reasonable indent

    return switch (opts.format) {
        .LineDelimited => try exportNDJSON(df, allocator, opts),
        .Array => try exportArray(df, allocator, opts),
        .Columnar => try exportColumnar(df, allocator, opts),
    };
}

/// Export as NDJSON format
fn exportNDJSON(
    df: *const DataFrame,
    allocator: std.mem.Allocator,
    opts: ExportOptions,
) ![]const u8 {
    std.debug.assert(df.row_count <= 4_000_000_000); // Max rows
    std.debug.assert(df.columns.len <= 10_000); // Max columns

    _ = df;
    _ = allocator;
    _ = opts;

    // TODO(Phase 5): Implement NDJSON export
    // For each row:
    // 1. Start object: {
    // 2. For each column: "col_name": value,
    // 3. End object: }\n
    // 4. Append to buffer

    return error.NotImplemented;
}

/// Export as JSON array format
fn exportArray(
    df: *const DataFrame,
    allocator: std.mem.Allocator,
    opts: ExportOptions,
) ![]const u8 {
    std.debug.assert(df.row_count <= 4_000_000_000); // Max rows
    std.debug.assert(df.columns.len <= 10_000); // Max columns

    _ = df;
    _ = allocator;
    _ = opts;

    // TODO(Phase 5): Implement Array export
    // 1. Start array: [
    // 2. For each row: {col_name: value, ...},
    // 3. End array: ]
    // 4. Handle pretty printing with indents

    return error.NotImplemented;
}

/// Export as columnar JSON format
fn exportColumnar(
    df: *const DataFrame,
    allocator: std.mem.Allocator,
    opts: ExportOptions,
) ![]const u8 {
    std.debug.assert(df.row_count <= 4_000_000_000); // Max rows
    std.debug.assert(df.columns.len <= 10_000); // Max columns

    _ = df;
    _ = allocator;
    _ = opts;

    // TODO(Phase 5): Implement Columnar export
    // {
    //   "col1": [val1, val2, ...],
    //   "col2": [val1, val2, ...]
    // }
    // Most efficient: DataFrame is already columnar!

    return error.NotImplemented;
}

// Tests
test "toJSON returns NotImplemented for NDJSON" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("name", .String, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 0);
    defer df.deinit();

    try testing.expectError(error.NotImplemented, toJSON(&df, allocator, .{ .format = .LineDelimited }));
}

test "toJSON returns NotImplemented for Array" {
    const testing = std.testing.allocator;
    const allocator = testing;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("name", .String, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 0);
    defer df.deinit();

    try testing.expectError(error.NotImplemented, toJSON(&df, allocator, .{ .format = .Array }));
}

test "toJSON returns NotImplemented for Columnar" {
    const testing = std.testing.allocator;
    const allocator = testing;

    const cols = [_]ColumnDesc{
        ColumnDesc.init("name", .String, 0),
    };

    var df = try DataFrame.create(allocator, &cols, 0);
    defer df.deinit();

    try testing.expectError(error.NotImplemented, toJSON(&df, allocator, .{ .format = .Columnar }));
}
