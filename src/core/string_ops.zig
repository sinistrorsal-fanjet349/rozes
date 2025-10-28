//! String column operations for data cleaning and transformation
//!
//! This module provides string manipulation functions for Series with String columns.
//! These are essential for data cleaning, text normalization, and feature extraction.
//!
//! Supported operations:
//! - Case conversion: lower, upper
//! - Whitespace handling: trim, strip
//! - Pattern matching: contains
//! - Replacement: replace
//! - Extraction: slice, len
//! - Splitting: split (returns array of Series)
//!
//! Tiger Style:
//! - 2+ assertions per function
//! - Bounded loops with explicit MAX constants
//! - Functions ≤70 lines
//! - Explicit types (u32, not usize)
//! - UTF-8 aware

const std = @import("std");
const Allocator = std.mem.Allocator;
const Series = @import("series.zig").Series;
const ValueType = @import("types.zig").ValueType;

/// Maximum string length for operations
pub const MAX_STRING_LENGTH: u32 = 1_000_000;

/// Convert all characters to lowercase
///
/// **UTF-8 Support**: Currently only handles ASCII (A-Z → a-z).
/// Non-ASCII characters (emoji, CJK, accents) are preserved unchanged.
///
/// **Limitation**: Full Unicode case conversion requires ICU library or Unicode tables.
/// For MVP, we validate UTF-8 and handle ASCII. Full Unicode support deferred to 1.0.0.
///
/// **Complexity**: O(n) where n = total string length
pub fn lower(series: *const Series, allocator: Allocator) !Series {
    // Tiger Style: Assertions
    std.debug.assert(series.valueType == .String);
    std.debug.assert(series.length > 0);

    if (series.valueType != .String) return error.InvalidType;

    const strings = series.data.String;
    const result = try allocator.alloc([]const u8, series.length);

    var i: u32 = 0;
    while (i < series.length) : (i += 1) {
        const str = strings[i];
        if (str.len > MAX_STRING_LENGTH) return error.StringTooLong;

        // ✅ Validate UTF-8
        if (!std.unicode.utf8ValidateSlice(str)) {
            // Clean up already allocated strings
            var cleanup_idx: u32 = 0;
            while (cleanup_idx < i) : (cleanup_idx += 1) {
                allocator.free(result[cleanup_idx]);
            }
            allocator.free(result);
            return error.InvalidUtf8;
        }

        const lower_str = try allocator.alloc(u8, str.len);

        // Bounded character iteration (ASCII lowercasing only)
        var j: u32 = 0;
        while (j < MAX_STRING_LENGTH and j < str.len) : (j += 1) {
            lower_str[j] = std.ascii.toLower(str[j]);
        }
        std.debug.assert(j == str.len); // Post-condition: processed all characters

        result[i] = lower_str;
    }

    return Series{
        .name = series.name,
        .valueType = .String,
        .data = .{ .String = result },
        .length = series.length,
    };
}

/// Convert all characters to uppercase
///
/// **UTF-8 Support**: Currently only handles ASCII (a-z → A-Z).
/// Non-ASCII characters (emoji, CJK, accents) are preserved unchanged.
///
/// **Limitation**: Full Unicode case conversion requires ICU library or Unicode tables.
/// For MVP, we validate UTF-8 and handle ASCII. Full Unicode support deferred to 1.0.0.
///
/// **Complexity**: O(n) where n = total string length
pub fn upper(series: *const Series, allocator: Allocator) !Series {
    // Tiger Style: Assertions
    std.debug.assert(series.valueType == .String);
    std.debug.assert(series.length > 0);

    if (series.valueType != .String) return error.InvalidType;

    const strings = series.data.String;
    const result = try allocator.alloc([]const u8, series.length);

    var i: u32 = 0;
    while (i < series.length) : (i += 1) {
        const str = strings[i];
        if (str.len > MAX_STRING_LENGTH) return error.StringTooLong;

        // ✅ Validate UTF-8
        if (!std.unicode.utf8ValidateSlice(str)) {
            // Clean up already allocated strings
            var cleanup_idx: u32 = 0;
            while (cleanup_idx < i) : (cleanup_idx += 1) {
                allocator.free(result[cleanup_idx]);
            }
            allocator.free(result);
            return error.InvalidUtf8;
        }

        const upper_str = try allocator.alloc(u8, str.len);

        // Bounded character iteration (ASCII uppercasing only)
        var j: u32 = 0;
        while (j < MAX_STRING_LENGTH and j < str.len) : (j += 1) {
            upper_str[j] = std.ascii.toUpper(str[j]);
        }
        std.debug.assert(j == str.len); // Post-condition: processed all characters

        result[i] = upper_str;
    }

    return Series{
        .name = series.name,
        .valueType = .String,
        .data = .{ .String = result },
        .length = series.length,
    };
}

/// Remove leading and trailing whitespace
pub fn trim(series: *const Series, allocator: Allocator) !Series {
    // Tiger Style: Assertions
    std.debug.assert(series.valueType == .String);
    std.debug.assert(series.length > 0);

    if (series.valueType != .String) return error.InvalidType;

    const strings = series.data.String;
    const result = try allocator.alloc([]const u8, series.length);

    var i: u32 = 0;
    while (i < series.length) : (i += 1) {
        const str = strings[i];
        const trimmed = std.mem.trim(u8, str, &std.ascii.whitespace);

        const trimmed_copy = try allocator.alloc(u8, trimmed.len);
        @memcpy(trimmed_copy, trimmed);
        result[i] = trimmed_copy;
    }

    return Series{
        .name = series.name,
        .valueType = .String,
        .data = .{ .String = result },
        .length = series.length,
    };
}

/// Alias for trim (pandas compatibility)
pub fn strip(series: *const Series, allocator: Allocator) !Series {
    return trim(series, allocator);
}

/// Check if string contains a substring
pub fn contains(series: *const Series, allocator: Allocator, pattern: []const u8) !Series {
    // Tiger Style: Assertions
    std.debug.assert(series.valueType == .String);
    std.debug.assert(series.length > 0);
    std.debug.assert(pattern.len > 0);

    if (series.valueType != .String) return error.InvalidType;
    if (pattern.len == 0) return error.EmptyPattern;

    const strings = series.data.String;
    const result = try allocator.alloc(bool, series.length);

    var i: u32 = 0;
    while (i < series.length) : (i += 1) {
        const str = strings[i];
        result[i] = std.mem.indexOf(u8, str, pattern) != null;
    }

    return Series{
        .name = series.name,
        .valueType = .Bool,
        .data = .{ .Bool = result },
        .length = series.length,
    };
}

/// Replace all occurrences of a substring
pub fn replace(series: *const Series, allocator: Allocator, from: []const u8, to: []const u8) !Series {
    // Tiger Style: Assertions
    std.debug.assert(series.valueType == .String);
    std.debug.assert(series.length > 0);
    std.debug.assert(from.len > 0);

    if (series.valueType != .String) return error.InvalidType;
    if (from.len == 0) return error.EmptyPattern;

    const strings = series.data.String;
    const result = try allocator.alloc([]const u8, series.length);

    var i: u32 = 0;
    while (i < series.length) : (i += 1) {
        const str = strings[i];

        // Count occurrences to allocate result
        var count: u32 = 0;
        var pos: usize = 0;
        while (pos < str.len) {
            if (std.mem.indexOf(u8, str[pos..], from)) |idx| {
                count += 1;
                pos += idx + from.len;
            } else {
                break;
            }
        }

        if (count == 0) {
            // No matches, copy original
            const copy = try allocator.alloc(u8, str.len);
            @memcpy(copy, str);
            result[i] = copy;
            continue;
        }

        // Allocate new string with replacements
        const new_len = str.len - count * from.len + count * to.len;
        const replaced = try allocator.alloc(u8, new_len);

        var write_pos: usize = 0;
        var read_pos: usize = 0;
        while (read_pos < str.len) {
            if (std.mem.indexOf(u8, str[read_pos..], from)) |idx| {
                // Copy before match
                @memcpy(replaced[write_pos .. write_pos + idx], str[read_pos .. read_pos + idx]);
                write_pos += idx;

                // Copy replacement
                @memcpy(replaced[write_pos .. write_pos + to.len], to);
                write_pos += to.len;

                read_pos += idx + from.len;
            } else {
                // Copy remaining
                @memcpy(replaced[write_pos..], str[read_pos..]);
                break;
            }
        }

        result[i] = replaced;
    }

    return Series{
        .name = series.name,
        .valueType = .String,
        .data = .{ .String = result },
        .length = series.length,
    };
}

/// Extract substring by slice
pub fn slice(series: *const Series, allocator: Allocator, start: u32, end: u32) !Series {
    // Tiger Style: Assertions
    std.debug.assert(series.valueType == .String);
    std.debug.assert(series.length > 0);
    std.debug.assert(start <= end);

    if (series.valueType != .String) return error.InvalidType;
    if (start > end) return error.InvalidSlice;

    const strings = series.data.String;
    const result = try allocator.alloc([]const u8, series.length);

    var i: u32 = 0;
    while (i < series.length) : (i += 1) {
        const str = strings[i];
        const actual_start = @min(start, @as(u32, @intCast(str.len)));
        const actual_end = @min(end, @as(u32, @intCast(str.len)));

        const sliced = str[actual_start..actual_end];
        const sliced_copy = try allocator.alloc(u8, sliced.len);
        @memcpy(sliced_copy, sliced);
        result[i] = sliced_copy;
    }

    return Series{
        .name = series.name,
        .valueType = .String,
        .data = .{ .String = result },
        .length = series.length,
    };
}

/// Get string length for each element
pub fn len(series: *const Series, allocator: Allocator) !Series {
    // Tiger Style: Assertions
    std.debug.assert(series.valueType == .String);
    std.debug.assert(series.length > 0);

    if (series.valueType != .String) return error.InvalidType;

    const strings = series.data.String;
    const result = try allocator.alloc(i64, series.length);

    var i: u32 = 0;
    while (i < series.length) : (i += 1) {
        result[i] = @intCast(strings[i].len);
    }

    return Series{
        .name = series.name,
        .valueType = .Int64,
        .data = .{ .Int64 = result },
        .length = series.length,
    };
}

/// Split result - array of Series (one per split part)
pub const SplitResult = struct {
    parts: []Series,
    num_parts: u32,

    pub fn free(self: *SplitResult, allocator: Allocator) void {
        std.debug.assert(self.num_parts > 0);
        std.debug.assert(self.parts.len == self.num_parts);

        // Bounded iteration over parts
        var part_idx: u32 = 0;
        while (part_idx < MAX_STRING_LENGTH and part_idx < self.parts.len) : (part_idx += 1) {
            const part = &self.parts[part_idx];
            if (part.valueType == .String) {
                const strings = part.data.String;

                // Bounded iteration over strings
                var str_idx: u32 = 0;
                while (str_idx < MAX_STRING_LENGTH and str_idx < strings.len) : (str_idx += 1) {
                    allocator.free(strings[str_idx]);
                }
                std.debug.assert(str_idx == strings.len); // Post-condition

                allocator.free(strings);
            }
        }
        std.debug.assert(part_idx == self.parts.len); // Post-condition

        allocator.free(self.parts);
    }
};

/// Split string by delimiter (returns array of Series)
pub fn split(series: *const Series, allocator: Allocator, delimiter: []const u8) !SplitResult {
    // Tiger Style: Assertions
    std.debug.assert(series.valueType == .String);
    std.debug.assert(series.length > 0);
    std.debug.assert(delimiter.len > 0);

    if (series.valueType != .String) return error.InvalidType;
    if (delimiter.len == 0) return error.EmptyDelimiter;

    const strings = series.data.String;

    // First pass: determine max number of parts
    var max_parts: u32 = 0;
    var i: u32 = 0;
    while (i < series.length) : (i += 1) {
        const str = strings[i];
        var count: u32 = 1;
        var pos: usize = 0;
        while (pos < str.len) {
            if (std.mem.indexOf(u8, str[pos..], delimiter)) |idx| {
                count += 1;
                pos += idx + delimiter.len;
            } else {
                break;
            }
        }
        if (count > max_parts) max_parts = count;
    }

    // Allocate Series array
    const parts = try allocator.alloc(Series, max_parts);

    // Initialize each part Series
    var part_idx: u32 = 0;
    while (part_idx < max_parts) : (part_idx += 1) {
        const part_data = try allocator.alloc([]const u8, series.length);
        parts[part_idx] = Series{
            .name = series.name,
            .valueType = .String,
            .data = .{ .String = part_data },
            .length = series.length,
        };
    }

    // Second pass: split and populate
    i = 0;
    while (i < series.length) : (i += 1) {
        const str = strings[i];
        var current_part: u32 = 0;
        var start_pos: usize = 0;

        while (start_pos < str.len and current_part < max_parts) {
            if (std.mem.indexOf(u8, str[start_pos..], delimiter)) |idx| {
                const part_str = str[start_pos .. start_pos + idx];
                const part_copy = try allocator.alloc(u8, part_str.len);
                @memcpy(part_copy, part_str);
                parts[current_part].data.String[i] = part_copy;

                start_pos += idx + delimiter.len;
                current_part += 1;
            } else {
                // Last part
                const part_str = str[start_pos..];
                const part_copy = try allocator.alloc(u8, part_str.len);
                @memcpy(part_copy, part_str);
                parts[current_part].data.String[i] = part_copy;
                current_part += 1;
                break;
            }
        }

        // Fill remaining parts with empty strings
        while (current_part < max_parts) : (current_part += 1) {
            const empty = try allocator.alloc(u8, 0);
            parts[current_part].data.String[i] = empty;
        }
    }

    return SplitResult{
        .parts = parts,
        .num_parts = max_parts,
    };
}

/// Check if string starts with a prefix
pub fn startsWith(series: *const Series, allocator: Allocator, prefix: []const u8) !Series {
    // Tiger Style: Assertions
    std.debug.assert(series.valueType == .String);
    std.debug.assert(series.length > 0);
    std.debug.assert(prefix.len > 0);

    if (series.valueType != .String) return error.InvalidType;

    const strings = series.data.String;
    const result = try allocator.alloc(bool, series.length);

    var i: u32 = 0;
    while (i < series.length) : (i += 1) {
        const str = strings[i];
        result[i] = std.mem.startsWith(u8, str, prefix);
    }

    return Series{
        .name = series.name,
        .valueType = .Bool,
        .data = .{ .Bool = result },
        .length = series.length,
    };
}

/// Check if string ends with a suffix
pub fn endsWith(series: *const Series, allocator: Allocator, suffix: []const u8) !Series {
    // Tiger Style: Assertions
    std.debug.assert(series.valueType == .String);
    std.debug.assert(series.length > 0);
    std.debug.assert(suffix.len > 0);

    if (series.valueType != .String) return error.InvalidType;

    const strings = series.data.String;
    const result = try allocator.alloc(bool, series.length);

    var i: u32 = 0;
    while (i < series.length) : (i += 1) {
        const str = strings[i];
        result[i] = std.mem.endsWith(u8, str, suffix);
    }

    return Series{
        .name = series.name,
        .valueType = .Bool,
        .data = .{ .Bool = result },
        .length = series.length,
    };
}
