//! Rozes WebAssembly Entry Point
//!
//! This module serves as the root for the Wasm build, exposing
//! WebAssembly-specific export functions for JavaScript interaction.

const std = @import("std");
const rozes = @import("rozes.zig");
const DataFrame = rozes.DataFrame;
const CSVParser = rozes.CSVParser;
const CSVOptions = rozes.CSVOptions;
const ValueType = rozes.ValueType;

// ============================================================================
// Constants
// ============================================================================

const MAX_DATAFRAMES: u32 = 1000; // Maximum concurrent DataFrames
const INVALID_HANDLE: i32 = -1;

// ============================================================================
// Error Codes (returned to JavaScript)
// ============================================================================

pub const ErrorCode = enum(i32) {
    Success = 0,
    OutOfMemory = -1,
    InvalidFormat = -2,
    InvalidHandle = -3,
    ColumnNotFound = -4,
    TypeMismatch = -5,
    IndexOutOfBounds = -6,
    TooManyDataFrames = -7,
    InvalidOptions = -8,
    _,

    pub fn fromError(err: anyerror) ErrorCode {
        return switch (err) {
            error.OutOfMemory => .OutOfMemory,
            error.InvalidFormat => .InvalidFormat,
            error.ColumnNotFound => .ColumnNotFound,
            error.TypeMismatch => .TypeMismatch,
            error.IndexOutOfBounds => .IndexOutOfBounds,
            else => @enumFromInt(-100),
        };
    }
};

// ============================================================================
// DataFrame Handle Registry
// ============================================================================

const DataFrameRegistry = struct {
    frames: [MAX_DATAFRAMES]?*DataFrame,
    next_id: u32,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) DataFrameRegistry {
        const reg = DataFrameRegistry{
            .frames = [_]?*DataFrame{null} ** MAX_DATAFRAMES,
            .next_id = 0,
            .allocator = allocator,
        };
        return reg;
    }

    fn register(self: *DataFrameRegistry, df: *DataFrame) !i32 {
        std.debug.assert(self.next_id < MAX_DATAFRAMES * 2); // Pre-condition: No overflow
        std.debug.assert(@intFromPtr(df) != 0); // Pre-condition: Non-null DataFrame

        var i: u32 = 0;
        while (i < MAX_DATAFRAMES) : (i += 1) {
            if (self.frames[i] == null) {
                self.frames[i] = df;
                self.next_id = i + 1;
                return @intCast(i);
            }
        }

        std.debug.assert(i == MAX_DATAFRAMES); // Post-condition: Loop exhausted all slots
        return error.TooManyDataFrames;
    }

    fn get(self: *DataFrameRegistry, handle: i32) ?*DataFrame {
        std.debug.assert(handle >= 0); // Pre-condition: Non-negative handle
        std.debug.assert(handle < MAX_DATAFRAMES); // Pre-condition: Within bounds

        const idx: u32 = @intCast(handle);
        const result = self.frames[idx];

        std.debug.assert(result == null or @intFromPtr(result.?) != 0); // Post-condition: Valid pointer if non-null
        return result;
    }

    fn unregister(self: *DataFrameRegistry, handle: i32) void {
        std.debug.assert(handle >= 0); // Pre-condition: Non-negative handle
        std.debug.assert(handle < MAX_DATAFRAMES); // Pre-condition: Within bounds

        const idx: u32 = @intCast(handle);
        self.frames[idx] = null;

        std.debug.assert(self.frames[idx] == null); // Post-condition: Slot is now null
    }
};

// ============================================================================
// Wasm Memory Management
// ============================================================================

// Use GeneralPurposeAllocator for Wasm
// Wasm linear memory is managed by the runtime, no need for manual @wasmMemoryGrow
var gpa = std.heap.GeneralPurposeAllocator(.{}){};

// Global registry
var registry: DataFrameRegistry = undefined;
var registry_initialized = false;

fn ensureRegistryInitialized() void {
    const was_initialized = registry_initialized;

    if (!registry_initialized) {
        registry = DataFrameRegistry.init(gpa.allocator());
        registry_initialized = true;
    }

    std.debug.assert(registry_initialized == true); // Post-condition: Always initialized after call
    std.debug.assert(!was_initialized or registry.next_id >= 0); // Invariant: If already initialized, registry valid
}

fn getAllocator() std.mem.Allocator {
    std.debug.assert(registry_initialized); // Must call ensureRegistryInitialized first
    return gpa.allocator();
}

// ============================================================================
// Wasm Exported Functions
// ============================================================================

/// Parse CSV buffer and return DataFrame handle
export fn rozes_parseCSV(
    csv_ptr: [*]const u8,
    csv_len: u32,
    opts_json_ptr: [*]const u8,
    opts_json_len: u32,
) i32 {
    ensureRegistryInitialized();

    std.debug.assert(csv_len > 0);
    std.debug.assert(csv_len <= 1_000_000_000);

    const csv_buffer = csv_ptr[0..csv_len];

    const opts: CSVOptions = if (opts_json_len > 0) blk: {
        const opts_json = opts_json_ptr[0..opts_json_len];
        break :blk parseCSVOptionsJSON(opts_json) catch {
            return @intFromEnum(ErrorCode.InvalidOptions);
        };
    } else CSVOptions{};

    const allocator = getAllocator();

    // Create CSV parser
    var parser = CSVParser.init(allocator, csv_buffer, opts) catch {
        return @intFromEnum(ErrorCode.OutOfMemory);
    };
    defer parser.deinit();

    // Parse to DataFrame
    const df_ptr = allocator.create(DataFrame) catch {
        return @intFromEnum(ErrorCode.OutOfMemory);
    };

    df_ptr.* = parser.toDataFrame() catch |err| {
        // Log error context with row/column information
        const field_preview = if (parser.current_field.items.len > 0)
            parser.current_field.items[0..@min(50, parser.current_field.items.len)]
        else
            "";

        std.log.err("CSV parsing failed: {} at row {} col {} - field preview: '{s}'", .{
            err,
            parser.current_row_index,
            parser.current_col_index,
            field_preview,
        });

        allocator.destroy(df_ptr);
        return @intFromEnum(ErrorCode.fromError(err));
    };

    const handle = registry.register(df_ptr) catch {
        df_ptr.deinit();
        allocator.destroy(df_ptr);
        return @intFromEnum(ErrorCode.TooManyDataFrames);
    };

    std.debug.assert(handle >= 0); // Post-condition: Valid handle returned
    std.debug.assert(handle < MAX_DATAFRAMES); // Post-condition: Within bounds

    return handle;
}

/// Get DataFrame dimensions
export fn rozes_getDimensions(
    handle: i32,
    out_rows: *u32,
    out_cols: *u32,
) i32 {
    std.debug.assert(handle >= 0);

    const df = registry.get(handle) orelse {
        return @intFromEnum(ErrorCode.InvalidHandle);
    };

    out_rows.* = df.row_count;
    out_cols.* = @intCast(df.columns.len);

    return @intFromEnum(ErrorCode.Success);
}

/// Get Float64 column data (zero-copy access)
export fn rozes_getColumnF64(
    handle: i32,
    col_name_ptr: [*]const u8,
    col_name_len: u32,
    out_ptr: *u32,
    out_len: *u32,
) i32 {
    std.debug.assert(handle >= 0);
    std.debug.assert(col_name_len > 0);
    std.debug.assert(@intFromPtr(out_ptr) != 0); // Non-null output pointer
    std.debug.assert(@intFromPtr(out_len) != 0); // Non-null output pointer

    const df = registry.get(handle) orelse {
        return @intFromEnum(ErrorCode.InvalidHandle);
    };

    const col_name = col_name_ptr[0..col_name_len];
    const series = df.column(col_name) orelse {
        return @intFromEnum(ErrorCode.ColumnNotFound);
    };

    const data = series.asFloat64() orelse {
        return @intFromEnum(ErrorCode.TypeMismatch);
    };

    const ptr_val: u32 = @intCast(@intFromPtr(data.ptr));
    out_ptr.* = ptr_val;
    out_len.* = @intCast(data.len);

    std.debug.assert(ptr_val != 0); // Post-condition: Non-null pointer
    std.debug.assert(out_len.* == @as(u32, @intCast(data.len))); // Post-condition: Length matches

    return @intFromEnum(ErrorCode.Success);
}

/// Get Int64 column data (zero-copy access)
export fn rozes_getColumnI64(
    handle: i32,
    col_name_ptr: [*]const u8,
    col_name_len: u32,
    out_ptr: *u32,
    out_len: *u32,
) i32 {
    std.debug.assert(handle >= 0);
    std.debug.assert(col_name_len > 0);
    std.debug.assert(@intFromPtr(out_ptr) != 0); // Non-null output pointer
    std.debug.assert(@intFromPtr(out_len) != 0); // Non-null output pointer

    const df = registry.get(handle) orelse {
        return @intFromEnum(ErrorCode.InvalidHandle);
    };

    const col_name = col_name_ptr[0..col_name_len];
    const series = df.column(col_name) orelse {
        return @intFromEnum(ErrorCode.ColumnNotFound);
    };

    const data = series.asInt64() orelse {
        return @intFromEnum(ErrorCode.TypeMismatch);
    };

    const ptr_val: u32 = @intCast(@intFromPtr(data.ptr));
    out_ptr.* = ptr_val;
    out_len.* = @intCast(data.len);

    std.debug.assert(ptr_val != 0); // Post-condition: Non-null pointer
    std.debug.assert(out_len.* == @as(u32, @intCast(data.len))); // Post-condition: Length matches

    return @intFromEnum(ErrorCode.Success);
}

/// Get column names as JSON array
export fn rozes_getColumnNames(
    handle: i32,
    out_buffer: [*]u8,
    buffer_len: u32,
    out_written: *u32,
) i32 {
    std.debug.assert(handle >= 0);
    std.debug.assert(buffer_len > 0);

    const df = registry.get(handle) orelse {
        return @intFromEnum(ErrorCode.InvalidHandle);
    };

    var buffer = out_buffer[0..buffer_len];
    var pos: usize = 0;

    buffer[pos] = '[';
    pos += 1;

    for (df.columns, 0..) |col, i| {
        if (i > 0) {
            buffer[pos] = ',';
            pos += 1;
        }

        buffer[pos] = '"';
        pos += 1;

        const name_bytes = std.mem.sliceTo(col.name, 0);
        if (pos + name_bytes.len + 1 >= buffer_len) {
            return @intFromEnum(ErrorCode.OutOfMemory);
        }

        @memcpy(buffer[pos .. pos + name_bytes.len], name_bytes);
        pos += name_bytes.len;

        buffer[pos] = '"';
        pos += 1;
    }

    buffer[pos] = ']';
    pos += 1;

    out_written.* = @intCast(pos);
    return @intFromEnum(ErrorCode.Success);
}

/// Free DataFrame and release memory
export fn rozes_free(handle: i32) void {
    std.debug.assert(handle >= 0);

    const df = registry.get(handle) orelse return;

    df.deinit();

    const allocator = getAllocator();
    allocator.destroy(df);

    registry.unregister(handle);
}

/// Allocate memory buffer for JavaScript
/// Returns pointer to allocated memory, or 0 on failure
export fn rozes_alloc(size: u32) u32 {
    std.debug.assert(size > 0); // Pre-condition: Non-zero size
    std.debug.assert(size <= 1_000_000_000); // Pre-condition: Reasonable limit (1GB)

    const allocator = getAllocator();
    const mem = allocator.alloc(u8, size) catch return 0;

    const ptr: u32 = @intCast(@intFromPtr(mem.ptr));

    std.debug.assert(ptr != 0); // Post-condition: Non-null pointer
    std.debug.assert(ptr % 8 == 0 or size < 8); // Post-condition: Aligned for most types
    return ptr;
}

/// Free memory buffer allocated by rozes_alloc
export fn rozes_free_buffer(ptr: u32, size: u32) void {
    std.debug.assert(ptr != 0); // Pre-condition: Non-null pointer
    std.debug.assert(size > 0); // Pre-condition: Non-zero size

    const allocator = getAllocator();
    const mem = @as([*]u8, @ptrFromInt(ptr))[0..size];
    allocator.free(mem);
}

/// Get Bool column data (zero-copy access)
export fn rozes_getColumnBool(
    handle: i32,
    col_name_ptr: [*]const u8,
    col_name_len: u32,
    out_ptr: *u32,
    out_len: *u32,
) i32 {
    std.debug.assert(handle >= 0);
    std.debug.assert(col_name_len > 0);
    std.debug.assert(@intFromPtr(out_ptr) != 0); // Non-null output pointer
    std.debug.assert(@intFromPtr(out_len) != 0); // Non-null output pointer

    const df = registry.get(handle) orelse {
        return @intFromEnum(ErrorCode.InvalidHandle);
    };

    const col_name = col_name_ptr[0..col_name_len];
    const series = df.column(col_name) orelse {
        return @intFromEnum(ErrorCode.ColumnNotFound);
    };

    const data = series.asBool() orelse {
        return @intFromEnum(ErrorCode.TypeMismatch);
    };

    const ptr_val: u32 = @intCast(@intFromPtr(data.ptr));
    out_ptr.* = ptr_val;
    out_len.* = @intCast(data.len);

    std.debug.assert(ptr_val != 0); // Post-condition: Non-null pointer
    std.debug.assert(out_len.* == @as(u32, @intCast(data.len))); // Post-condition: Length matches

    return @intFromEnum(ErrorCode.Success);
}

/// Get String column data
/// Returns array of string pointers and lengths
/// out_offsets_ptr: pointer to array of offsets (u32[])
/// out_buffer_ptr: pointer to concatenated string buffer
export fn rozes_getColumnString(
    handle: i32,
    col_name_ptr: [*]const u8,
    col_name_len: u32,
    out_offsets_ptr: *u32,
    out_offsets_len: *u32,
    out_buffer_ptr: *u32,
    out_buffer_len: *u32,
) i32 {
    std.debug.assert(handle >= 0);
    std.debug.assert(col_name_len > 0);
    std.debug.assert(@intFromPtr(out_offsets_ptr) != 0); // Non-null
    std.debug.assert(@intFromPtr(out_offsets_len) != 0); // Non-null
    std.debug.assert(@intFromPtr(out_buffer_ptr) != 0); // Non-null
    std.debug.assert(@intFromPtr(out_buffer_len) != 0); // Non-null

    const df = registry.get(handle) orelse {
        return @intFromEnum(ErrorCode.InvalidHandle);
    };

    const col_name = col_name_ptr[0..col_name_len];
    const series = df.column(col_name) orelse {
        return @intFromEnum(ErrorCode.ColumnNotFound);
    };

    // String columns are stored as [][]const u8
    const strings = switch (series.data) {
        .String => |s| s[0..series.length],
        else => return @intFromEnum(ErrorCode.TypeMismatch),
    };

    // For MVP, we need to return the string array structure
    // This is complex - for now, return TypeMismatch
    // TODO: Implement proper string column export in 0.2.0
    _ = strings; // Only discard strings since it's not used in assertions

    std.debug.assert(false); // Not implemented yet
    return @intFromEnum(ErrorCode.TypeMismatch);
}

// ============================================================================
// Helper Functions
// ============================================================================

fn parseCSVOptionsJSON(json: []const u8) !CSVOptions {
    std.debug.assert(json.len > 0);
    std.debug.assert(json.len < 10_000); // Reasonable limit

    const allocator = getAllocator();

    // Define the JSON structure we expect
    const ParsedOptions = struct {
        delimiter: ?[]const u8 = null,
        has_headers: ?bool = null,
        skip_blank_lines: ?bool = null,
        trim_whitespace: ?bool = null,
    };

    const parsed = try std.json.parseFromSlice(
        ParsedOptions,
        allocator,
        json,
        .{},
    );
    defer parsed.deinit();

    var opts = CSVOptions{};

    // Apply parsed values if present
    if (parsed.value.has_headers) |v| opts.has_headers = v;
    if (parsed.value.skip_blank_lines) |v| opts.skip_blank_lines = v;
    if (parsed.value.trim_whitespace) |v| opts.trim_whitespace = v;
    if (parsed.value.delimiter) |d| {
        if (d.len == 1) {
            opts.delimiter = d[0];
        }
    }

    std.debug.assert(opts.delimiter != 0); // Post-condition: Valid delimiter
    std.debug.assert(opts.delimiter == ',' or opts.delimiter == '\t' or opts.delimiter == ';'); // Post-condition: Known delimiters

    return opts;
}
