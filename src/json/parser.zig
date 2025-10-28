//! JSON Parser - DataFrame from JSON
//!
//! Supports 3 formats:
//! 1. Line-delimited JSON (NDJSON): `{"name": "Alice", "age": 30}\n{"name": "Bob", "age": 25}\n`
//! 2. JSON Array: `[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]`
//! 3. Columnar JSON: `{"name": ["Alice", "Bob"], "age": [30, 25]}`
//!
//! See docs/TODO.md Phase 5 for specifications.

const std = @import("std");
const DataFrame = @import("../core/dataframe.zig").DataFrame;
const types = @import("../core/types.zig");
const ValueType = types.ValueType;
const ColumnDesc = types.ColumnDesc;

const MAX_JSON_SIZE: u32 = 1_000_000_000; // 1GB max
const MAX_COLUMNS: u32 = 10_000;
const MAX_ROWS: u32 = 4_000_000_000; // u32 limit
const MAX_NESTING_DEPTH: u32 = 32; // Prevent stack overflow

/// JSON parsing format
pub const JSONFormat = enum {
    /// Line-delimited JSON (NDJSON)
    /// Example: {"name": "Alice", "age": 30}\n{"name": "Bob", "age": 25}\n
    LineDelimited,

    /// JSON array of objects
    /// Example: [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    Array,

    /// Columnar JSON (most efficient for DataFrames)
    /// Example: {"name": ["Alice", "Bob"], "age": [30, 25]}
    Columnar,
};

/// JSON parsing options
pub const JSONOptions = struct {
    /// JSON format to parse
    format: JSONFormat = .LineDelimited,

    /// Automatically infer column types (default: true)
    type_inference: bool = true,

    /// Schema specification (overrides type inference if provided)
    schema: ?[]ColumnDesc = null,

    /// Validates JSON options
    pub fn validate(self: JSONOptions) !void {
        std.debug.assert(@intFromEnum(self.format) >= 0); // Valid enum value
        std.debug.assert(self.type_inference or self.schema != null); // Must have types

        if (self.schema) |sch| {
            if (sch.len == 0) return error.EmptySchema;
            if (sch.len > MAX_COLUMNS) return error.TooManyColumns;
        }
    }
};

/// JSON Parser state machine
pub const JSONParser = struct {
    allocator: std.mem.Allocator,
    buffer: []const u8,
    opts: JSONOptions,

    /// Initialize JSON parser
    pub fn init(
        allocator: std.mem.Allocator,
        buffer: []const u8,
        opts: JSONOptions,
    ) !JSONParser {
        std.debug.assert(buffer.len > 0); // Non-empty buffer
        std.debug.assert(buffer.len <= MAX_JSON_SIZE); // Size check

        try opts.validate();

        return JSONParser{
            .allocator = allocator,
            .buffer = buffer,
            .opts = opts,
        };
    }

    /// Parse JSON to DataFrame
    pub fn toDataFrame(self: *JSONParser) !DataFrame {
        std.debug.assert(self.buffer.len > 0); // Valid buffer
        std.debug.assert(self.buffer.len <= MAX_JSON_SIZE); // Size limit

        return switch (self.opts.format) {
            .LineDelimited => try self.parseNDJSON(),
            .Array => try self.parseArray(),
            .Columnar => try self.parseColumnar(),
        };
    }

    /// Parse NDJSON format
    fn parseNDJSON(self: *JSONParser) !DataFrame {
        std.debug.assert(self.buffer.len > 0); // Non-empty input
        std.debug.assert(self.opts.format == .LineDelimited); // Correct format

        // TODO(Phase 5): Implement NDJSON parser
        // Line-by-line parsing:
        // 1. Split buffer by newlines
        // 2. Parse each line as JSON object
        // 3. Collect all objects into rows
        // 4. Infer column types
        // 5. Build DataFrame

        _ = self;
        return error.NotImplemented;
    }

    /// Parse JSON array format
    fn parseArray(self: *JSONParser) !DataFrame {
        std.debug.assert(self.buffer.len > 0); // Non-empty input
        std.debug.assert(self.opts.format == .Array); // Correct format

        // TODO(Phase 5): Implement Array parser
        // 1. Parse entire JSON array using std.json
        // 2. Validate all elements are objects
        // 3. Collect all objects into rows
        // 4. Infer column types
        // 5. Build DataFrame

        _ = self;
        return error.NotImplemented;
    }

    /// Parse columnar JSON format
    fn parseColumnar(self: *JSONParser) !DataFrame {
        std.debug.assert(self.buffer.len > 0); // Non-empty input
        std.debug.assert(self.opts.format == .Columnar); // Correct format

        // TODO(Phase 5): Implement Columnar parser
        // 1. Parse JSON object using std.json
        // 2. Validate all values are arrays
        // 3. Validate all arrays have same length
        // 4. Infer column types from array elements
        // 5. Build DataFrame (already in columnar format!)

        _ = self;
        return error.NotImplemented;
    }

    /// Infer column type from JSON value
    fn inferTypeFromValue(value: std.json.Value) ValueType {
        std.debug.assert(@intFromEnum(value) >= 0); // Valid JSON value type

        return switch (value) {
            .integer => .Int64,
            .float => .Float64,
            .bool => .Bool,
            .string => .String,
            .null => .Null,
            .array, .object => .String, // Serialize complex types as strings
        };
    }

    /// Clean up parser resources
    pub fn deinit(self: *JSONParser) void {
        std.debug.assert(self.buffer.len <= MAX_JSON_SIZE); // Invariant check
        _ = self;
        // Nothing to cleanup (buffer is owned by caller)
    }
};

// Tests
test "JSONOptions.validate accepts valid options" {
    const opts = JSONOptions{};
    try opts.validate();
}

test "JSONOptions.validate rejects empty schema" {
    const testing = std.testing;

    const opts = JSONOptions{
        .type_inference = false,
        .schema = &[_]ColumnDesc{}, // Empty schema
    };

    try testing.expectError(error.EmptySchema, opts.validate());
}

test "JSONParser.init accepts valid JSON" {
    const allocator = std.testing.allocator;
    const json = "{\"name\": \"Alice\", \"age\": 30}";

    var parser = try JSONParser.init(allocator, json, .{});
    defer parser.deinit();

    std.testing.expect(parser.buffer.len > 0) catch unreachable;
}

test "JSONParser.toDataFrame returns NotImplemented for NDJSON" {
    const allocator = std.testing.allocator;
    const json = "{\"name\": \"Alice\", \"age\": 30}\n";

    var parser = try JSONParser.init(allocator, json, .{ .format = .LineDelimited });
    defer parser.deinit();

    std.testing.expectError(error.NotImplemented, parser.toDataFrame()) catch unreachable;
}

test "JSONParser.toDataFrame returns NotImplemented for Array" {
    const allocator = std.testing.allocator;
    const json = "[{\"name\": \"Alice\"}]";

    var parser = try JSONParser.init(allocator, json, .{ .format = .Array });
    defer parser.deinit();

    std.testing.expectError(error.NotImplemented, parser.toDataFrame()) catch unreachable;
}

test "JSONParser.toDataFrame returns NotImplemented for Columnar" {
    const allocator = std.testing.allocator;
    const json = "{\"name\": [\"Alice\"]}";

    var parser = try JSONParser.init(allocator, json, .{ .format = .Columnar });
    defer parser.deinit();

    std.testing.expectError(error.NotImplemented, parser.toDataFrame()) catch unreachable;
}

test "inferTypeFromValue detects types correctly" {
    const testing = std.testing;

    try testing.expectEqual(ValueType.Int64, JSONParser.inferTypeFromValue(.{ .integer = 42 }));
    try testing.expectEqual(ValueType.Float64, JSONParser.inferTypeFromValue(.{ .float = 3.14 }));
    try testing.expectEqual(ValueType.Bool, JSONParser.inferTypeFromValue(.{ .bool = true }));
    try testing.expectEqual(ValueType.String, JSONParser.inferTypeFromValue(.{ .string = "test" }));
    try testing.expectEqual(ValueType.Null, JSONParser.inferTypeFromValue(.null));
}
