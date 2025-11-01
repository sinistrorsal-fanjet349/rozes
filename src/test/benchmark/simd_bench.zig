//! SIMD Aggregations Benchmark
//!
//! Benchmarks SIMD-accelerated aggregation functions (Milestone 1.2.0 Phase 1)
//! Target: 30%+ speedup over scalar implementation
//! Configuration: 200K rows × 100K iterations for accurate microsecond timings

const std = @import("std");
const rozes = @import("rozes");
const simd = rozes.simd;
const bench = @import("benchmark.zig");

/// Benchmark SIMD sum operation
pub fn benchmarkSIMDSum(allocator: std.mem.Allocator, row_count: u32) !bench.BenchmarkResult {
    std.debug.assert(row_count > 0);
    std.debug.assert(row_count <= 1_000_000);

    // Generate test data
    const data = try allocator.alloc(f64, row_count);
    defer allocator.free(data);

    var i: u32 = 0;
    while (i < row_count) : (i += 1) {
        data[i] = @as(f64, @floatFromInt(i + 1));
    }

    // Warm-up
    var warmup: u32 = 0;
    while (warmup < 10) : (warmup += 1) {
        _ = simd.sumFloat64(data);
    }

    // Benchmark (100K iterations for microsecond resolution on 200K rows)
    const iterations: u32 = 100000;
    const start = std.time.nanoTimestamp();

    var result_sum: f64 = 0;
    var iter: u32 = 0;
    while (iter < iterations) : (iter += 1) {
        result_sum += simd.sumFloat64(data);
    }

    const end = std.time.nanoTimestamp();

    // Prevent dead code elimination
    std.mem.doNotOptimizeAway(&result_sum);
    const total_ns: u64 = @intCast(end - start);

    // Use floating point to preserve fractional nanoseconds
    const avg_ns_f64 = @as(f64, @floatFromInt(total_ns)) / @as(f64, @floatFromInt(iterations));
    const duration_ms = avg_ns_f64 / 1_000_000.0;
    const duration_sec = duration_ms / 1000.0;
    const throughput = @as(f64, @floatFromInt(row_count)) / duration_sec;

    return bench.BenchmarkResult{
        .duration_ns = @intFromFloat(@round(avg_ns_f64)),
        .duration_ms = duration_ms,
        .throughput = throughput,
        .row_count = row_count,
    };
}

/// Benchmark SIMD mean operation
pub fn benchmarkSIMDMean(allocator: std.mem.Allocator, row_count: u32) !bench.BenchmarkResult {
    std.debug.assert(row_count > 0);
    std.debug.assert(row_count <= 1_000_000);

    const data = try allocator.alloc(f64, row_count);
    defer allocator.free(data);

    var i: u32 = 0;
    while (i < row_count) : (i += 1) {
        data[i] = @as(f64, @floatFromInt(i + 1));
    }

    var warmup: u32 = 0;
    while (warmup < 10) : (warmup += 1) {
        _ = simd.meanFloat64(data);
    }

    const iterations: u32 = 100000;
    const start = std.time.nanoTimestamp();

    var result_sum: f64 = 0;
    var iter: u32 = 0;
    while (iter < iterations) : (iter += 1) {
        result_sum += simd.meanFloat64(data) orelse 0;
    }

    const end = std.time.nanoTimestamp();

    // Prevent dead code elimination
    std.mem.doNotOptimizeAway(&result_sum);
    const total_ns: u64 = @intCast(end - start);

    // Use floating point to preserve fractional nanoseconds
    const avg_ns_f64 = @as(f64, @floatFromInt(total_ns)) / @as(f64, @floatFromInt(iterations));
    const duration_ms = avg_ns_f64 / 1_000_000.0;
    const duration_sec = duration_ms / 1000.0;
    const throughput = @as(f64, @floatFromInt(row_count)) / duration_sec;

    return bench.BenchmarkResult{
        .duration_ns = @intFromFloat(@round(avg_ns_f64)),
        .duration_ms = duration_ms,
        .throughput = throughput,
        .row_count = row_count,
    };
}

/// Benchmark SIMD min operation
pub fn benchmarkSIMDMin(allocator: std.mem.Allocator, row_count: u32) !bench.BenchmarkResult {
    std.debug.assert(row_count > 0);
    std.debug.assert(row_count <= 1_000_000);

    const data = try allocator.alloc(f64, row_count);
    defer allocator.free(data);

    var i: u32 = 0;
    while (i < row_count) : (i += 1) {
        data[i] = @as(f64, @floatFromInt(i + 1));
    }

    var warmup: u32 = 0;
    while (warmup < 10) : (warmup += 1) {
        _ = simd.minFloat64(data);
    }

    const iterations: u32 = 100000;
    const start = std.time.nanoTimestamp();

    var result_sum: f64 = 0;
    var iter: u32 = 0;
    while (iter < iterations) : (iter += 1) {
        // Prevent CSE: modify one element to make each iteration unique
        data[iter % row_count] += @as(f64, @floatFromInt(iter));
        result_sum += simd.minFloat64(data) orelse 0;
        // Restore original value
        data[iter % row_count] = @as(f64, @floatFromInt((iter % row_count) + 1));
    }

    const end = std.time.nanoTimestamp();

    // Prevent dead code elimination
    std.mem.doNotOptimizeAway(&result_sum);
    const total_ns: u64 = @intCast(end - start);

    // Use floating point to preserve fractional nanoseconds
    const avg_ns_f64 = @as(f64, @floatFromInt(total_ns)) / @as(f64, @floatFromInt(iterations));
    const duration_ms = avg_ns_f64 / 1_000_000.0;
    const duration_sec = duration_ms / 1000.0;
    const throughput = @as(f64, @floatFromInt(row_count)) / duration_sec;

    return bench.BenchmarkResult{
        .duration_ns = @intFromFloat(@round(avg_ns_f64)),
        .duration_ms = duration_ms,
        .throughput = throughput,
        .row_count = row_count,
    };
}

/// Benchmark SIMD max operation
pub fn benchmarkSIMDMax(allocator: std.mem.Allocator, row_count: u32) !bench.BenchmarkResult {
    std.debug.assert(row_count > 0);
    std.debug.assert(row_count <= 1_000_000);

    const data = try allocator.alloc(f64, row_count);
    defer allocator.free(data);

    var i: u32 = 0;
    while (i < row_count) : (i += 1) {
        data[i] = @as(f64, @floatFromInt(i + 1));
    }

    var warmup: u32 = 0;
    while (warmup < 10) : (warmup += 1) {
        _ = simd.maxFloat64(data);
    }

    const iterations: u32 = 100000;
    const start = std.time.nanoTimestamp();

    var result_sum: f64 = 0;
    var iter: u32 = 0;
    while (iter < iterations) : (iter += 1) {
        // Prevent CSE: modify one element to make each iteration unique
        data[iter % row_count] += @as(f64, @floatFromInt(iter));
        result_sum += simd.maxFloat64(data) orelse 0;
        // Restore original value
        data[iter % row_count] = @as(f64, @floatFromInt((iter % row_count) + 1));
    }

    const end = std.time.nanoTimestamp();

    // Prevent dead code elimination
    std.mem.doNotOptimizeAway(&result_sum);
    const total_ns: u64 = @intCast(end - start);

    // Use floating point to preserve fractional nanoseconds
    const avg_ns_f64 = @as(f64, @floatFromInt(total_ns)) / @as(f64, @floatFromInt(iterations));
    const duration_ms = avg_ns_f64 / 1_000_000.0;
    const duration_sec = duration_ms / 1000.0;
    const throughput = @as(f64, @floatFromInt(row_count)) / duration_sec;

    return bench.BenchmarkResult{
        .duration_ns = @intFromFloat(@round(avg_ns_f64)),
        .duration_ms = duration_ms,
        .throughput = throughput,
        .row_count = row_count,
    };
}

/// Benchmark SIMD variance operation
pub fn benchmarkSIMDVariance(allocator: std.mem.Allocator, row_count: u32) !bench.BenchmarkResult {
    std.debug.assert(row_count > 0);
    std.debug.assert(row_count <= 1_000_000);

    const data = try allocator.alloc(f64, row_count);
    defer allocator.free(data);

    var i: u32 = 0;
    while (i < row_count) : (i += 1) {
        data[i] = @as(f64, @floatFromInt(i + 1));
    }

    var warmup: u32 = 0;
    while (warmup < 10) : (warmup += 1) {
        _ = simd.varianceFloat64(data);
    }

    const iterations: u32 = 100000;
    const start = std.time.nanoTimestamp();

    var result_sum: f64 = 0;
    var iter: u32 = 0;
    while (iter < iterations) : (iter += 1) {
        // Prevent CSE: modify one element to make each iteration unique
        data[iter % row_count] += @as(f64, @floatFromInt(iter));
        result_sum += simd.varianceFloat64(data) orelse 0;
        // Restore original value
        data[iter % row_count] = @as(f64, @floatFromInt((iter % row_count) + 1));
    }

    const end = std.time.nanoTimestamp();

    // Prevent dead code elimination
    std.mem.doNotOptimizeAway(&result_sum);
    const total_ns: u64 = @intCast(end - start);

    // Use floating point to preserve fractional nanoseconds
    const avg_ns_f64 = @as(f64, @floatFromInt(total_ns)) / @as(f64, @floatFromInt(iterations));
    const duration_ms = avg_ns_f64 / 1_000_000.0;
    const duration_sec = duration_ms / 1000.0;
    const throughput = @as(f64, @floatFromInt(row_count)) / duration_sec;

    return bench.BenchmarkResult{
        .duration_ns = @intFromFloat(@round(avg_ns_f64)),
        .duration_ms = duration_ms,
        .throughput = throughput,
        .row_count = row_count,
    };
}

/// Benchmark SIMD standard deviation operation
pub fn benchmarkSIMDStdDev(allocator: std.mem.Allocator, row_count: u32) !bench.BenchmarkResult {
    std.debug.assert(row_count > 0);
    std.debug.assert(row_count <= 1_000_000);

    const data = try allocator.alloc(f64, row_count);
    defer allocator.free(data);

    var i: u32 = 0;
    while (i < row_count) : (i += 1) {
        data[i] = @as(f64, @floatFromInt(i + 1));
    }

    var warmup: u32 = 0;
    while (warmup < 10) : (warmup += 1) {
        _ = simd.stdDevFloat64(data);
    }

    const iterations: u32 = 100000;
    const start = std.time.nanoTimestamp();

    var result_sum: f64 = 0;
    var iter: u32 = 0;
    while (iter < iterations) : (iter += 1) {
        // Prevent CSE: modify one element to make each iteration unique
        data[iter % row_count] += @as(f64, @floatFromInt(iter));
        result_sum += simd.stdDevFloat64(data) orelse 0;
        // Restore original value
        data[iter % row_count] = @as(f64, @floatFromInt((iter % row_count) + 1));
    }

    const end = std.time.nanoTimestamp();

    // Prevent dead code elimination
    std.mem.doNotOptimizeAway(&result_sum);
    const total_ns: u64 = @intCast(end - start);

    // Use floating point to preserve fractional nanoseconds
    const avg_ns_f64 = @as(f64, @floatFromInt(total_ns)) / @as(f64, @floatFromInt(iterations));
    const duration_ms = avg_ns_f64 / 1_000_000.0;
    const duration_sec = duration_ms / 1000.0;
    const throughput = @as(f64, @floatFromInt(row_count)) / duration_sec;

    return bench.BenchmarkResult{
        .duration_ns = @intFromFloat(@round(avg_ns_f64)),
        .duration_ms = duration_ms,
        .throughput = throughput,
        .row_count = row_count,
    };
}
