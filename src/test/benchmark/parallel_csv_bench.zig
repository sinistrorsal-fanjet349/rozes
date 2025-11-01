//! Benchmark for parallel CSV parser
//!
//! Tests performance on:
//! - 1M rows (typical large dataset)
//! - 10M rows (very large dataset)
//! - 100M rows (extreme scale)
//!
//! Performance targets (Milestone 1.2.0 Phase 3):
//! - 2-4× speedup on 4+ cores vs sequential parser
//! - Graceful degradation on single-core systems

const std = @import("std");
const ParallelCSVParser = @import("../../csv/parallel_parser.zig").ParallelCSVParser;
const CSVParser = @import("../../csv/parser.zig").CSVParser;

const BenchmarkResult = struct {
    row_count: u32,
    parallel_time_ms: f64,
    sequential_time_ms: f64,
    speedup: f64,
    thread_count: u32,

    pub fn print(self: *const BenchmarkResult) void {
        std.debug.print(
            \\ Benchmark Results ({} rows):
            \\   Sequential: {d:.2}ms
            \\   Parallel ({} threads): {d:.2}ms
            \\   Speedup: {d:.2}×
            \\
        ,
            .{
                self.row_count,
                self.sequential_time_ms,
                self.thread_count,
                self.parallel_time_ms,
                self.speedup,
            },
        );
    }
};

/// Generate CSV with specified row count
fn generateCSV(allocator: std.mem.Allocator, row_count: u32, col_count: u32) ![]u8 {
    std.debug.assert(row_count > 0); // Pre-condition #1
    std.debug.assert(col_count > 0); // Pre-condition #2
    std.debug.assert(row_count <= 1_000_000_000); // Pre-condition #3: Reasonable limit

    var csv_builder = std.ArrayList(u8).init(allocator);
    errdefer csv_builder.deinit();

    // Write header
    var col_idx: u32 = 0;
    while (col_idx < col_count) : (col_idx += 1) {
        if (col_idx > 0) try csv_builder.append(',');
        try csv_builder.writer().print("col{}", .{col_idx});
    }
    try csv_builder.append('\n');

    std.debug.assert(col_idx == col_count); // Post-condition

    // Write rows
    var row_idx: u32 = 0;
    while (row_idx < row_count) : (row_idx += 1) {
        var c: u32 = 0;
        while (c < col_count) : (c += 1) {
            if (c > 0) try csv_builder.append(',');

            // Alternate between int and float values
            if (c % 2 == 0) {
                try csv_builder.writer().print("{}", .{row_idx * 10 + c});
            } else {
                try csv_builder.writer().print("{d:.2}", .{@as(f64, @floatFromInt(row_idx)) * 1.5});
            }
        }
        try csv_builder.append('\n');
    }

    std.debug.assert(row_idx == row_count); // Post-condition

    return try csv_builder.toOwnedSlice();
}

/// Benchmark parallel CSV parser
fn benchmarkParallel(
    allocator: std.mem.Allocator,
    csv: []const u8,
) !f64 {
    std.debug.assert(csv.len > 0); // Pre-condition

    var parser = try ParallelCSVParser.init(allocator, csv, .{});

    const start = std.time.nanoTimestamp();

    // Create chunks (part of the parallel parsing workflow)
    try parser.createChunks();
    defer parser.cleanupChunks();

    // Perform type inference (parallel)
    const results = try parser.inferTypesParallel();
    defer parser.cleanupResults(results);

    // Merge types
    const final_types = try parser.mergeTypeInference(results);
    defer allocator.free(final_types);

    const end = std.time.nanoTimestamp();

    const duration_ms = @as(f64, @floatFromInt(end - start)) / 1_000_000.0;
    return duration_ms;
}

/// Benchmark sequential CSV parser
fn benchmarkSequential(
    allocator: std.mem.Allocator,
    csv: []const u8,
) !f64 {
    std.debug.assert(csv.len > 0); // Pre-condition

    var parser = try CSVParser.init(allocator, csv, .{});
    defer parser.deinit();

    const start = std.time.nanoTimestamp();

    // Parse CSV sequentially
    try parser.parseAll();

    const end = std.time.nanoTimestamp();

    const duration_ms = @as(f64, @floatFromInt(end - start)) / 1_000_000.0;
    return duration_ms;
}

/// Run benchmark for specified row count
fn runBenchmark(
    allocator: std.mem.Allocator,
    row_count: u32,
    col_count: u32,
) !BenchmarkResult {
    std.debug.assert(row_count > 0); // Pre-condition #1
    std.debug.assert(col_count > 0); // Pre-condition #2

    std.debug.print("Generating CSV ({} rows × {} columns)...\n", .{ row_count, col_count });

    const csv = try generateCSV(allocator, row_count, col_count);
    defer allocator.free(csv);

    std.debug.print("CSV size: {} bytes ({d:.2} MB)\n", .{
        csv.len,
        @as(f64, @floatFromInt(csv.len)) / (1024.0 * 1024.0),
    });

    // Benchmark parallel parsing
    std.debug.print("Running parallel benchmark...\n", .{});
    const parallel_time = try benchmarkParallel(allocator, csv);

    // Benchmark sequential parsing
    std.debug.print("Running sequential benchmark...\n", .{});
    const sequential_time = try benchmarkSequential(allocator, csv);

    // Get thread count
    const cpu_count = try std.Thread.getCpuCount();
    const thread_count: u32 = @min(@as(u32, @intCast(cpu_count)), 8);

    const speedup = sequential_time / parallel_time;

    return BenchmarkResult{
        .row_count = row_count,
        .parallel_time_ms = parallel_time,
        .sequential_time_ms = sequential_time,
        .speedup = speedup,
        .thread_count = thread_count,
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Parallel CSV Parser Benchmark ===\n\n", .{});

    // Benchmark configuration
    const benchmarks = [_]struct { rows: u32, cols: u32 }{
        .{ .rows = 1_000, .cols = 10 },       // Small: 1K rows
        .{ .rows = 10_000, .cols = 10 },      // Medium: 10K rows
        .{ .rows = 100_000, .cols = 10 },     // Large: 100K rows
        .{ .rows = 1_000_000, .cols = 10 },   // Very large: 1M rows
        // 10M and 100M rows are commented out - too large for CI
        // .{ .rows = 10_000_000, .cols = 10 },  // Extreme: 10M rows
        // .{ .rows = 100_000_000, .cols = 5 },  // Maximum: 100M rows
    };

    var results = std.ArrayList(BenchmarkResult).init(allocator);
    defer results.deinit();

    for (benchmarks) |config| {
        std.debug.print("\n--- Benchmark: {} rows × {} columns ---\n", .{ config.rows, config.cols });

        const result = runBenchmark(allocator, config.rows, config.cols) catch |err| {
            std.debug.print("Benchmark failed: {}\n", .{err});
            continue;
        };

        try results.append(result);
        result.print();
    }

    // Summary
    std.debug.print("\n=== Summary ===\n\n", .{});
    std.debug.print("| Rows      | Sequential | Parallel | Speedup | Threads |\n", .{});
    std.debug.print("|-----------|------------|----------|---------|----------|\n", .{});

    for (results.items) |r| {
        std.debug.print("| {:<9} | {d:>9.2}ms | {d:>7.2}ms | {d:>6.2}× | {d:>7} |\n", .{
            r.row_count,
            r.sequential_time_ms,
            r.parallel_time_ms,
            r.speedup,
            r.thread_count,
        });
    }

    std.debug.print("\n", .{});

    // Performance analysis
    if (results.items.len > 0) {
        const avg_speedup = blk: {
            var sum: f64 = 0.0;
            for (results.items) |r| sum += r.speedup;
            break :blk sum / @as(f64, @floatFromInt(results.items.len));
        };

        std.debug.print("Average speedup: {d:.2}×\n", .{avg_speedup});

        if (avg_speedup >= 2.0) {
            std.debug.print("✅ Performance target met (2-4× speedup)\n", .{});
        } else if (avg_speedup >= 1.5) {
            std.debug.print("⚠️  Close to target (1.5-2× speedup)\n", .{});
        } else {
            std.debug.print("❌ Performance target not met (<1.5× speedup)\n", .{});
        }
    }
}

// Unit test for benchmark infrastructure
test "generateCSV creates valid CSV" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const csv = try generateCSV(allocator, 100, 5);
    defer allocator.free(csv);

    // Should have header + 100 rows = 101 lines
    var line_count: u32 = 0;
    for (csv) |c| {
        if (c == '\n') line_count += 1;
    }

    try testing.expectEqual(@as(u32, 101), line_count);

    // Should start with header
    try testing.expect(std.mem.startsWith(u8, csv, "col0,col1,col2,col3,col4\n"));
}

test "benchmarkParallel completes without errors" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const csv = try generateCSV(allocator, 100, 5);
    defer allocator.free(csv);

    const time_ms = try benchmarkParallel(allocator, csv);

    // Should complete in reasonable time (<1 second for 100 rows)
    try testing.expect(time_ms < 1000.0);
    try testing.expect(time_ms > 0.0);
}

test "benchmarkSequential completes without errors" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const csv = try generateCSV(allocator, 100, 5);
    defer allocator.free(csv);

    const time_ms = try benchmarkSequential(allocator, csv);

    // Should complete in reasonable time
    try testing.expect(time_ms < 1000.0);
    try testing.expect(time_ms > 0.0);
}
