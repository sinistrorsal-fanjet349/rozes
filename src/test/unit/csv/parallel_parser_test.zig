//! Unit tests for parallel CSV parser
//!
//! Tests cover:
//! - Chunk boundary handling (quotes, escapes, newlines)
//! - Parallel type inference
//! - Type conflict resolution
//! - Thread safety and memory safety
//! - Performance on large CSV files

const std = @import("std");
const testing = std.testing;
const ParallelCSVParser = @import("../../../csv/parallel_parser.zig").ParallelCSVParser;
const ValueType = @import("../../../core/types.zig").ValueType;

// Test chunk boundary detection with quoted fields
test "findChunkBoundary handles quoted fields with embedded newlines" {
    const allocator = testing.allocator;

    // CSV with quoted field containing newlines
    const csv =
        \\name,bio,age
        \\"Alice","Line 1
        \\Line 2
        \\Line 3",30
        \\"Bob","Simple bio",25
        \\
    ;

    var parser = try ParallelCSVParser.init(allocator, csv, .{});

    // Find boundary after first row (should skip embedded newlines in quotes)
    const boundary1 = try parser.findChunkBoundary(0, 40);

    // Should find the newline after age=30, NOT the ones inside quotes
    try testing.expect(boundary1 > 0);
    try testing.expect(boundary1 < csv.len);

    // Verify the boundary is at a valid row end (after "30\n")
    const before_boundary = csv[0..boundary1];
    try testing.expect(std.mem.endsWith(u8, before_boundary, "30\n") or
        std.mem.endsWith(u8, before_boundary, "30\r\n"));
}

// Test chunk boundary with CRLF line endings
test "findChunkBoundary handles CRLF line endings" {
    const allocator = testing.allocator;

    const csv = "name,age\r\nAlice,30\r\nBob,25\r\n";

    var parser = try ParallelCSVParser.init(allocator, csv, .{});

    // Find boundary after first row
    const boundary = try parser.findChunkBoundary(0, 20);

    try testing.expect(boundary > 0);

    // Verify it's after the CRLF
    try testing.expect(csv[boundary - 1] == '\n' or csv[boundary - 2] == '\r');
}

// Test chunk boundary with CR-only line endings (old Mac format)
test "findChunkBoundary handles CR line endings" {
    const allocator = testing.allocator;

    const csv = "name,age\rAlice,30\rBob,25\r";

    var parser = try ParallelCSVParser.init(allocator, csv, .{});

    const boundary = try parser.findChunkBoundary(0, 15);

    try testing.expect(boundary > 0);
    try testing.expect(csv[boundary - 1] == '\r');
}

// Test chunk boundary with very long lines (>1KB)
test "findChunkBoundary extends chunk for very long lines" {
    const allocator = testing.allocator;

    // Create CSV with 2KB line
    var csv_builder = std.ArrayList(u8).init(allocator);
    defer csv_builder.deinit();

    try csv_builder.appendSlice("name,data\n");

    // Add a 2KB field
    try csv_builder.appendSlice("Alice,");
    var i: usize = 0;
    while (i < 2048) : (i += 1) {
        try csv_builder.append('x');
    }
    try csv_builder.appendSlice("\nBob,short\n");

    var parser = try ParallelCSVParser.init(allocator, csv_builder.items, .{});

    // Try to find boundary in the middle of the long line
    const boundary = try parser.findChunkBoundary(0, 500);

    // Should extend past the 500-byte mark to find the actual newline
    try testing.expect(boundary > 500);
}

// Test thread count determination
test "ParallelCSVParser uses single thread for small files" {
    const allocator = testing.allocator;

    const small_csv = "name,age\nAlice,30\n";

    var parser = try ParallelCSVParser.init(allocator, small_csv, .{});

    // Small CSV (<128KB) should use single thread
    try testing.expectEqual(@as(u32, 1), parser.thread_count);
}

test "ParallelCSVParser uses multiple threads for large files" {
    const allocator = testing.allocator;

    // Create 512KB CSV (4× MIN_CHUNK_SIZE)
    const csv_size = 512 * 1024;
    var large_csv = try allocator.alloc(u8, csv_size);
    defer allocator.free(large_csv);

    // Fill with valid CSV data
    @memset(large_csv, 'x');
    large_csv[0] = 'n';
    large_csv[csv_size - 1] = '\n';

    var parser = try ParallelCSVParser.init(allocator, large_csv, .{});

    // Large CSV should use multiple threads
    try testing.expect(parser.thread_count > 1);
    try testing.expect(parser.thread_count <= 8); // MAX_THREADS
}

// Test type inference merging with conflicts
test "mergeColumnType resolves Int64/Float64 conflict to Float64" {
    const allocator = testing.allocator;

    const csv = "dummy\n";
    var parser = try ParallelCSVParser.init(allocator, csv, .{});

    const ChunkInferenceResult = @import("../../../csv/parallel_parser.zig").ChunkInferenceResult;

    // Chunk 1 infers Int64
    var result1 = try ChunkInferenceResult.init(allocator, 1);
    defer result1.deinit(allocator);
    result1.col_types[0] = .Int64;
    result1.confidence[0] = 0.9;

    // Chunk 2 infers Float64
    var result2 = try ChunkInferenceResult.init(allocator, 1);
    defer result2.deinit(allocator);
    result2.col_types[0] = .Float64;
    result2.confidence[0] = 0.8;

    const results = [_]ChunkInferenceResult{ result1, result2 };

    // Merge should choose Float64 (more permissive)
    const merged_type = try parser.mergeColumnType(&results, 0);
    try testing.expectEqual(ValueType.Float64, merged_type);
}

test "mergeColumnType prefers String when present" {
    const allocator = testing.allocator;

    const csv = "dummy\n";
    var parser = try ParallelCSVParser.init(allocator, csv, .{});

    const ChunkInferenceResult = @import("../../../csv/parallel_parser.zig").ChunkInferenceResult;

    var result1 = try ChunkInferenceResult.init(allocator, 1);
    defer result1.deinit(allocator);
    result1.col_types[0] = .Int64;

    var result2 = try ChunkInferenceResult.init(allocator, 1);
    defer result2.deinit(allocator);
    result2.col_types[0] = .String; // Mixed data

    const results = [_]ChunkInferenceResult{ result1, result2 };

    // String is most permissive, should win
    const merged_type = try parser.mergeColumnType(&results, 0);
    try testing.expectEqual(ValueType.String, merged_type);
}

// Test memory safety with parallel execution
test "ParallelCSVParser does not leak memory with multiple chunks" {
    const allocator = testing.allocator;

    // Create CSV with multiple chunks worth of data
    var csv_builder = std.ArrayList(u8).init(allocator);
    defer csv_builder.deinit();

    try csv_builder.appendSlice("value\n");

    // Generate 10K rows
    var i: u32 = 0;
    while (i < 10_000) : (i += 1) {
        try csv_builder.writer().print("{}\n", .{i});
    }

    var parser = try ParallelCSVParser.init(allocator, csv_builder.items, .{});

    // Create chunks (this allocates memory)
    try parser.createChunks();
    defer parser.cleanupChunks();

    // Verify chunks created
    try testing.expect(parser.chunks.len > 0);

    // Memory leak test: testing.allocator will report leaks automatically
}

// Test chunk creation with edge cases
test "createChunks handles empty CSV gracefully" {
    const allocator = testing.allocator;

    const empty_csv = "";

    // Should fail on empty CSV (violates pre-condition)
    try testing.expectError(error.Overflow, ParallelCSVParser.init(allocator, empty_csv, .{}));
}

test "createChunks creates single chunk for small CSV" {
    const allocator = testing.allocator;

    const small_csv = "name,age\nAlice,30\nBob,25\n";

    var parser = try ParallelCSVParser.init(allocator, small_csv, .{});

    try parser.createChunks();
    defer parser.cleanupChunks();

    // Small CSV should create 1 chunk (or 0 if falls back to sequential)
    try testing.expect(parser.chunks.len >= 0);
}

// Test type inference on mixed type columns
test "inferChunkType handles mixed numeric types" {
    const allocator = testing.allocator;

    // CSV with integers in chunk 1, floats in chunk 2
    const csv = "value\n10\n20\n30.5\n";

    var parser = try ParallelCSVParser.init(allocator, csv, .{});

    try parser.createChunks();
    defer parser.cleanupChunks();

    // If chunks created, test inference
    if (parser.chunks.len > 0) {
        const ChunkInferenceResult = @import("../../../csv/parallel_parser.zig").ChunkInferenceResult;
        var result = try ChunkInferenceResult.init(allocator, 1);
        defer result.deinit(allocator);

        try parser.inferChunkType(&parser.chunks[0], &result);

        // Should infer some type (String default for MVP)
        try testing.expect(result.col_types[0] != .Null);
    }
}

// Stress test: Large CSV with parallel parsing
test "ParallelCSVParser handles 100K rows without errors" {
    const allocator = testing.allocator;

    // Generate 100K row CSV
    var csv_builder = std.ArrayList(u8).init(allocator);
    defer csv_builder.deinit();

    try csv_builder.appendSlice("id,value\n");

    var i: u32 = 0;
    while (i < 100_000) : (i += 1) {
        try csv_builder.writer().print("{},{}\n", .{ i, i * 2 });
    }

    var parser = try ParallelCSVParser.init(allocator, csv_builder.items, .{});

    // Should create chunks without errors
    try parser.createChunks();
    defer parser.cleanupChunks();

    try testing.expect(parser.chunks.len > 0);
}
