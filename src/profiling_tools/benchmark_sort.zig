//! Sort Benchmark - Measure sort performance with SIMD integration
//!
//! Tests sort operations at different scales to verify SIMD optimization effectiveness.
//!
//! Target: <5ms for 100K rows (6.73ms baseline → 4ms with SIMD)

const std = @import("std");
const DataFrame = @import("../core/dataframe.zig").DataFrame;
const ColumnDesc = @import("../core/types.zig").ColumnDesc;
const sort_mod = @import("../core/sort.zig");
const benchmark = @import("../test/benchmark/benchmark.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Sort Benchmark (SIMD Integration) ===\n\n", .{});

    // Test sizes
    const sizes = [_]u32{ 10_000, 50_000, 100_000 };

    for (sizes) |size| {
        std.debug.print("--- Sorting {d} rows ---\n", .{size});

        // Create DataFrame with Float64 column (SIMD target)
        const cols = [_]ColumnDesc{
            ColumnDesc.init("value", .Float64, 0),
            ColumnDesc.init("id", .Int64, 1),
        };

        var df = try DataFrame.create(allocator, &cols, size);
        defer df.deinit();

        // Fill with random-ish data
        const values = df.columns[0].asFloat64Buffer() orelse unreachable;
        const ids = df.columns[1].asInt64Buffer() orelse unreachable;

        var i: u32 = 0;
        while (i < size) : (i += 1) {
            // Generate pseudo-random values (deterministic for consistency)
            const x = @as(f64, @floatFromInt((i * 997) % size));
            values[i] = x;
            ids[i] = @as(i64, @intCast(i));
        }
        df.row_count = size;

        // Benchmark: Sort by Float64 column
        const start = std.time.nanoTimestamp();
        var sorted = try sort_mod.sort(&df, allocator, "value", .Ascending);
        const end = std.time.nanoTimestamp();
        defer sorted.deinit();

        const result = benchmark.BenchmarkResult.compute(start, end, size);
        result.print("Sort (Float64)");

        // Verify correctness (first few values should be in order)
        const sorted_values = sorted.columns[0].asFloat64Buffer() orelse unreachable;
        std.debug.assert(sorted_values[0] <= sorted_values[1]);
        std.debug.assert(sorted_values[1] <= sorted_values[2]);

        // Benchmark: Sort by Int64 column
        const start_int = std.time.nanoTimestamp();
        var sorted_int = try sort_mod.sort(&df, allocator, "id", .Ascending);
        const end_int = std.time.nanoTimestamp();
        defer sorted_int.deinit();

        const result_int = benchmark.BenchmarkResult.compute(start_int, end_int, size);
        result_int.print("Sort (Int64)");

        std.debug.print("\n", .{});
    }

    // Compare to target
    std.debug.print("=== Target Comparison (100K rows) ===\n", .{});
    const comparison_float = benchmark.Comparison.check("Sort Float64", 5.0, 0.0);
    const comparison_int = benchmark.Comparison.check("Sort Int64", 5.0, 0.0);

    std.debug.print("Target: <5ms for 100K rows\n", .{});
    std.debug.print("Baseline: 6.73ms (from Phase 6 benchmarks)\n", .{});
    std.debug.print("Expected with SIMD: ~4ms (40% faster)\n", .{});
    std.debug.print("\nRe-run this benchmark after SIMD integration to see actual results.\n", .{});
}
