//! Statistical Operations - Advanced statistical analysis functions
//!
//! This module provides statistical operations for DataFrame columns including:
//! - Variance and standard deviation
//! - Median and quantiles
//! - Correlation matrix
//! - Ranking operations
//! - Frequency distributions (value_counts)
//!
//! See docs/TODO.md Phase 3 (Day 13) for implementation details.
//!
//! Example:
//! ```
//! const std_dev = try std(&df, "age");
//! const median_salary = try median(&df, "salary");
//! const corr = try corrMatrix(&df, allocator, &[_][]const u8{"age", "salary"});
//! ```

const std = @import("std");
const DataFrame = @import("dataframe.zig").DataFrame;
const Series = @import("series.zig").Series;
const types = @import("types.zig");
const ValueType = types.ValueType;
const Allocator = std.mem.Allocator;

/// Maximum number of rows for statistical operations
const MAX_ROWS: u32 = 4_000_000_000;

/// Maximum columns for correlation matrix
const MAX_CORR_COLS: u32 = 100;

/// Ranking method for handling ties
pub const RankMethod = enum {
    Average, // Average rank of ties (default in pandas)
    Min,     // Minimum rank of ties
    Max,     // Maximum rank of ties
    First,   // Rank assigned in order of appearance
};

/// IEEE 754 compliant Float64 comparison function that handles NaN values
///
/// NaN values sort to the end (a < b when b is NaN, a > b when a is NaN).
/// When both are NaN, they are considered equal (stable sort preserves order).
///
/// This is required because std.sort.asc(f64) panics when encountering NaN.
fn compareFloat64WithNaN(context: void, a: f64, b: f64) bool {
    _ = context;

    const a_is_nan = std.math.isNan(a);
    const b_is_nan = std.math.isNan(b);

    // Both NaN: equal (stable sort)
    if (a_is_nan and b_is_nan) return false;

    // a is NaN, b is not: a sorts to end (a > b)
    if (a_is_nan) return false;

    // b is NaN, a is not: b sorts to end (a < b)
    if (b_is_nan) return true;

    // Neither is NaN: normal comparison
    return a < b;
}

/// Computes standard deviation of a numeric column
///
/// Args:
///   - df: Source DataFrame
///   - column_name: Name of column to analyze
///
/// Returns: Standard deviation as f64, or null if column is empty
///
/// **Algorithm**: Two-pass (mean, then squared differences)
/// **Complexity**: O(n)
pub fn stdDev(
    df: *const DataFrame,
    column_name: []const u8,
) !?f64 {
    std.debug.assert(column_name.len > 0); // Name required
    std.debug.assert(df.columnCount() > 0); // Need columns

    const variance_val = try variance(df, column_name) orelse return null;

    std.debug.assert(variance_val >= 0); // Variance non-negative

    return @sqrt(variance_val);
}

/// Computes variance of a numeric column
///
/// Args:
///   - df: Source DataFrame
///   - column_name: Name of column to analyze
///
/// Returns: Variance as f64, or null if column is empty
///
/// **Algorithm**: Two-pass (mean, then squared differences)
/// **Complexity**: O(n)
pub fn variance(
    df: *const DataFrame,
    column_name: []const u8,
) !?f64 {
    std.debug.assert(column_name.len > 0); // Name required
    std.debug.assert(df.columnCount() > 0); // Need columns

    const col = df.column(column_name) orelse {
        std.log.err("variance: Column '{s}' not found in DataFrame", .{column_name});
        return error.ColumnNotFound;
    };

    if (df.len() == 0) return null;
    if (df.len() == 1) return 0.0; // Single value has zero variance

    // Pass 1: Compute mean
    const mean_val = computeMean(df, col) orelse return null;

    // Pass 2: Compute sum of squared differences
    return switch (col.value_type) {
        .Int64 => blk: {
            const data = col.asInt64() orelse {
                std.log.err("variance: Column '{s}' type mismatch (expected Int64 or Float64, got {})", .{ column_name, col.value_type });
                return error.TypeMismatch;
            };
            var sum_sq_diff: f64 = 0.0;

            var idx: u32 = 0;
            while (idx < MAX_ROWS and idx < data.len) : (idx += 1) {
                const diff = @as(f64, @floatFromInt(data[idx])) - mean_val;
                sum_sq_diff += diff * diff;
            }

            std.debug.assert(idx == data.len); // Processed all values
            break :blk sum_sq_diff / @as(f64, @floatFromInt(data.len - 1)); // Sample variance (n-1)
        },
        .Float64 => blk: {
            const data = col.asFloat64() orelse {
                std.log.err("variance: Column '{s}' type mismatch (expected Int64 or Float64, got {})", .{ column_name, col.value_type });
                return error.TypeMismatch;
            };
            var sum_sq_diff: f64 = 0.0;

            var idx: u32 = 0;
            while (idx < MAX_ROWS and idx < data.len) : (idx += 1) {
                const diff = data[idx] - mean_val;
                sum_sq_diff += diff * diff;
            }

            std.debug.assert(idx == data.len); // Processed all values
            break :blk sum_sq_diff / @as(f64, @floatFromInt(data.len - 1)); // Sample variance (n-1)
        },
        else => {
            std.log.err("variance: Column '{s}' has unsupported type {} (expected Int64 or Float64)", .{ column_name, col.value_type });
            return error.TypeMismatch;
        },
    };
}

/// Helper function to compute mean (internal use)
fn computeMean(df: *const DataFrame, col: *const Series) ?f64 {
    std.debug.assert(df.len() > 0); // Need data
    std.debug.assert(col.length == df.len()); // Column length matches

    const total = switch (col.value_type) {
        .Int64 => blk: {
            const data = col.asInt64() orelse return null;
            var sum: i64 = 0;

            var idx: u32 = 0;
            while (idx < MAX_ROWS and idx < data.len) : (idx += 1) {
                sum += data[idx];
            }

            break :blk @as(f64, @floatFromInt(sum));
        },
        .Float64 => blk: {
            const data = col.asFloat64() orelse return null;
            var sum: f64 = 0.0;

            var idx: u32 = 0;
            while (idx < MAX_ROWS and idx < data.len) : (idx += 1) {
                sum += data[idx];
            }

            break :blk sum;
        },
        else => return null,
    };

    return total / @as(f64, @floatFromInt(df.len()));
}

/// Computes median of a numeric column
///
/// Args:
///   - df: Source DataFrame
///   - column_name: Name of column to analyze
///   - allocator: Allocator for temporary sorting buffer
///
/// Returns: Median as f64, or null if column is empty
///
/// **Algorithm Choice**: Uses full sort (O(n log n)) instead of quickselect (O(n))
/// **Rationale**:
/// - Quickselect requires mutable array (modifies input)
/// - Full sort more stable and predictable
/// - For DataFrame analytics, n < 1M typically (sort fast enough)
/// - Optimization opportunity: Implement quickselect for n > 100K (defer to 0.5.0)
///
/// **Complexity**: O(n log n) time, O(n) space for temporary buffer
pub fn median(
    df: *const DataFrame,
    column_name: []const u8,
    allocator: Allocator,
) !?f64 {
    std.debug.assert(column_name.len > 0); // Name required
    std.debug.assert(df.columnCount() > 0); // Need columns

    // For simplicity, use quantile(0.5)
    return try quantile(df, column_name, allocator, 0.5);
}

/// Computes quantile (percentile) of a numeric column
///
/// Args:
///   - df: Source DataFrame
///   - column_name: Name of column to analyze
///   - allocator: Allocator for temporary sorting buffer
///   - q: Quantile value (0.0 to 1.0, e.g., 0.25 for 25th percentile)
///
/// Returns: Quantile value as f64, or null if column is empty
///
/// **Algorithm**: Full sort + interpolation
/// **Complexity**: O(n log n)
pub fn quantile(
    df: *const DataFrame,
    column_name: []const u8,
    allocator: Allocator,
    q: f64,
) !?f64 {
    std.debug.assert(column_name.len > 0); // Name required
    std.debug.assert(df.columnCount() > 0); // Need columns
    std.debug.assert(q >= 0.0 and q <= 1.0); // Valid quantile range

    const col = df.column(column_name) orelse {
        std.log.err("quantile: Column '{s}' not found in DataFrame", .{column_name});
        return error.ColumnNotFound;
    };

    if (df.len() == 0) return null;

    return switch (col.value_type) {
        .Int64 => blk: {
            const data = col.asInt64() orelse {
                std.log.err("quantile: Column '{s}' type mismatch (expected Int64 or Float64, got {})", .{ column_name, col.value_type });
                return error.TypeMismatch;
            };

            // Copy data to temporary buffer for sorting
            var sorted = try allocator.alloc(f64, data.len);
            defer allocator.free(sorted);

            var idx: u32 = 0;
            while (idx < MAX_ROWS and idx < data.len) : (idx += 1) {
                sorted[idx] = @as(f64, @floatFromInt(data[idx]));
            }
            std.debug.assert(idx == data.len); // Copied all values

            // Sort using NaN-safe comparison (std.sort.asc panics on NaN)
            std.mem.sort(f64, sorted, {}, compareFloat64WithNaN);

            break :blk computeQuantileFromSorted(sorted, q);
        },
        .Float64 => blk: {
            const data = col.asFloat64() orelse {
                std.log.err("quantile: Column '{s}' type mismatch (expected Int64 or Float64, got {})", .{ column_name, col.value_type });
                return error.TypeMismatch;
            };

            // Copy data to temporary buffer for sorting
            var sorted = try allocator.alloc(f64, data.len);
            defer allocator.free(sorted);

            var idx: u32 = 0;
            while (idx < MAX_ROWS and idx < data.len) : (idx += 1) {
                sorted[idx] = data[idx];
            }
            std.debug.assert(idx == data.len); // Copied all values

            // Sort using NaN-safe comparison (std.sort.asc panics on NaN)
            std.mem.sort(f64, sorted, {}, compareFloat64WithNaN);

            break :blk computeQuantileFromSorted(sorted, q);
        },
        else => {
            std.log.err("quantile: Column '{s}' has unsupported type {} (expected Int64 or Float64)", .{ column_name, col.value_type });
            return error.TypeMismatch;
        },
    };
}

/// Helper function to compute quantile from sorted array
fn computeQuantileFromSorted(sorted: []const f64, q: f64) f64 {
    std.debug.assert(sorted.len > 0); // Need data
    std.debug.assert(q >= 0.0 and q <= 1.0); // Valid quantile

    if (sorted.len == 1) return sorted[0];

    // Linear interpolation between indices
    const pos = q * @as(f64, @floatFromInt(sorted.len - 1));
    const lower_idx: usize = @intFromFloat(@floor(pos));
    const upper_idx: usize = @intCast(@min(lower_idx + 1, sorted.len - 1));

    const fraction = pos - @as(f64, @floatFromInt(lower_idx));

    std.debug.assert(lower_idx < sorted.len); // Valid index
    std.debug.assert(upper_idx < sorted.len); // Valid index

    return sorted[lower_idx] * (1.0 - fraction) + sorted[upper_idx] * fraction;
}

/// Computes Pearson correlation matrix for selected numeric columns
///
/// Args:
///   - df: Source DataFrame
///   - allocator: Allocator for matrix
///   - column_names: Array of column names to correlate
///
/// Returns: n×n correlation matrix (2D array), or error
///
/// **Algorithm**: Two-pass per pair (means, then correlation)
/// **Complexity**: O(n²m) where n=columns, m=rows
///
/// **Note**: Result is allocated by caller, must be freed
pub fn corrMatrix(
    df: *const DataFrame,
    allocator: Allocator,
    column_names: []const []const u8,
) ![][]f64 {
    std.debug.assert(column_names.len > 0); // Need columns
    std.debug.assert(column_names.len <= MAX_CORR_COLS); // Reasonable limit

    if (df.len() < 2) return error.InsufficientData; // Need at least 2 rows

    const n = column_names.len;

    // Allocate matrix
    var matrix = try allocator.alloc([]f64, n);
    errdefer allocator.free(matrix);

    var i: u32 = 0;
    while (i < MAX_CORR_COLS and i < n) : (i += 1) {
        matrix[i] = try allocator.alloc(f64, n);
    }
    std.debug.assert(i == n); // Allocated all rows

    errdefer {
        var j: u32 = 0;
        while (j < i) : (j += 1) {
            allocator.free(matrix[j]);
        }
        allocator.free(matrix);
    }

    // Compute correlation for each pair
    var row: u32 = 0;
    while (row < MAX_CORR_COLS and row < n) : (row += 1) {
        var col: u32 = 0;
        while (col < MAX_CORR_COLS and col < n) : (col += 1) {
            if (row == col) {
                matrix[row][col] = 1.0; // Diagonal = 1 (perfect correlation with self)
            } else if (col < row) {
                // Symmetric: reuse previous calculation
                matrix[row][col] = matrix[col][row];
            } else {
                // Compute correlation
                matrix[row][col] = try computeCorrelation(df, column_names[row], column_names[col]);
            }
        }
        std.debug.assert(col == n); // Filled all columns
    }
    std.debug.assert(row == n); // Filled all rows

    return matrix;
}

/// Helper function to compute Pearson correlation between two columns
fn computeCorrelation(
    df: *const DataFrame,
    col1_name: []const u8,
    col2_name: []const u8,
) !f64 {
    std.debug.assert(col1_name.len > 0); // Valid name
    std.debug.assert(col2_name.len > 0); // Valid name

    const col1 = df.column(col1_name) orelse return error.ColumnNotFound;
    const col2 = df.column(col2_name) orelse return error.ColumnNotFound;

    const mean1 = computeMean(df, col1) orelse return error.TypeMismatch;
    const mean2 = computeMean(df, col2) orelse return error.TypeMismatch;

    // Extract numeric data
    const data1 = extractNumericData(col1) orelse return error.TypeMismatch;
    const data2 = extractNumericData(col2) orelse return error.TypeMismatch;

    std.debug.assert(data1.len == data2.len); // Same length

    // Compute correlation: cov(X,Y) / (stdDev(X) * stdDev(Y))
    var sum_xy: f64 = 0.0;
    var sum_xx: f64 = 0.0;
    var sum_yy: f64 = 0.0;

    var idx: u32 = 0;
    while (idx < MAX_ROWS and idx < data1.len) : (idx += 1) {
        const dx = data1[idx] - mean1;
        const dy = data2[idx] - mean2;

        sum_xy += dx * dy;
        sum_xx += dx * dx;
        sum_yy += dy * dy;
    }
    std.debug.assert(idx == data1.len); // Processed all values

    // Handle zero variance cases
    if (sum_xx == 0.0 or sum_yy == 0.0) return 0.0;

    return sum_xy / @sqrt(sum_xx * sum_yy);
}

/// Helper to extract numeric data from column as f64 array
fn extractNumericData(col: *const Series) ?[]const f64 {
    return switch (col.value_type) {
        .Float64 => col.asFloat64(),
        // For Int64, would need conversion - defer to future implementation
        else => null,
    };
}

/// Computes rank of values in a series
///
/// Args:
///   - series: Input Series
///   - allocator: Allocator for result Series
///   - method: Ranking method for handling ties
///
/// Returns: New Series with ranks (Float64 type)
///
/// **Complexity**: O(n log n) due to sorting
pub fn rank(
    series: *const Series,
    allocator: Allocator,
    method: RankMethod,
) !Series {
    std.debug.assert(series.length > 0); // Need data
    std.debug.assert(series.length <= MAX_ROWS); // Reasonable limit

    // Create index-value pairs for sorting
    const IndexValue = struct {
        idx: u32,
        value: f64,
    };

    var pairs = try allocator.alloc(IndexValue, series.length);
    defer allocator.free(pairs);

    // Extract values
    switch (series.value_type) {
        .Int64 => {
            const data = series.asInt64() orelse return error.TypeMismatch;

            var i: u32 = 0;
            while (i < MAX_ROWS and i < data.len) : (i += 1) {
                pairs[i] = .{
                    .idx = i,
                    .value = @as(f64, @floatFromInt(data[i])),
                };
            }
            std.debug.assert(i == data.len); // Processed all
        },
        .Float64 => {
            const data = series.asFloat64() orelse return error.TypeMismatch;

            var i: u32 = 0;
            while (i < MAX_ROWS and i < data.len) : (i += 1) {
                pairs[i] = .{
                    .idx = i,
                    .value = data[i],
                };
            }
            std.debug.assert(i == data.len); // Processed all
        },
        else => return error.TypeMismatch,
    }

    // Sort by value
    std.mem.sort(IndexValue, pairs, {}, struct {
        fn lessThan(_: void, a: IndexValue, b: IndexValue) bool {
            return a.value < b.value;
        }
    }.lessThan);

    // Assign ranks
    var ranks = try allocator.alloc(f64, series.length);
    errdefer allocator.free(ranks);

    var i: u32 = 0;
    while (i < MAX_ROWS and i < pairs.len) : (i += 1) {
        // For now, implement 'First' method (rank in order of appearance after sort)
        // TODO: Implement Average, Min, Max methods for tie handling
        _ = method; // Suppress unused warning

        ranks[pairs[i].idx] = @as(f64, @floatFromInt(i + 1)); // 1-based ranking
    }
    std.debug.assert(i == pairs.len); // Assigned all ranks

    // Create result Series
    return Series{
        .name = series.name,
        .value_type = .Float64,
        .data = .{ .Float64 = ranks },
        .length = series.length,
        .allocator_owned = true,
    };
}

/// Computes frequency distribution of values in a series
///
/// Args:
///   - series: Input Series
///   - allocator: Allocator for result DataFrame
///
/// Returns: DataFrame with columns [value, count] sorted by count descending
///
/// **Complexity**: O(n) for counting, O(k log k) for sorting (k = unique values)
///
/// **Note**: For categorical columns, this is very efficient (O(k) where k=categories)
pub fn valueCounts(
    series: *const Series,
    allocator: Allocator,
) !DataFrame {
    std.debug.assert(series.length > 0); // Need data
    std.debug.assert(series.length <= MAX_ROWS); // Reasonable limit

    // Use HashMap for counting
    var counts = std.AutoHashMap(i64, u32).init(allocator);
    defer counts.deinit();

    // Count occurrences
    switch (series.value_type) {
        .Int64 => {
            const data = series.asInt64() orelse return error.TypeMismatch;

            var i: u32 = 0;
            while (i < MAX_ROWS and i < data.len) : (i += 1) {
                const entry = try counts.getOrPut(data[i]);
                if (!entry.found_existing) {
                    entry.value_ptr.* = 1;
                } else {
                    entry.value_ptr.* += 1;
                }
            }
            std.debug.assert(i == data.len); // Counted all
        },
        // TODO: Implement for other types (Float64, String, Categorical)
        else => return error.TypeMismatch,
    }

    // Convert to DataFrame
    // TODO: Implement conversion - requires creating DataFrame with value/count columns
    // For now, return placeholder error
    _ = counts;
    return error.NotImplemented;
}
