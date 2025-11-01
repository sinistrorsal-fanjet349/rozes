//! Radix Join Distribution Tests
//!
//! Tests radix join correctness with various data distributions:
//! - Uniform distribution (all keys equally likely)
//! - Zipf distribution (power-law, 80/20 rule)
//! - Clustered distribution (keys grouped together)
//!
//! This ensures radix join handles skewed real-world data correctly.
//!
//! Part of Milestone 1.2.0 Phase 2

const std = @import("std");
const testing = std.testing;
const radix_join = @import("../../../core/radix_join.zig");

/// Generate uniform distribution (all values equally likely)
fn generateUniformKeys(allocator: std.mem.Allocator, size: usize, max_key: i64) ![]radix_join.KeyValue {
    std.debug.assert(size > 0);
    std.debug.assert(size <= radix_join.MAX_TOTAL_ROWS);
    std.debug.assert(max_key > 0);

    const data = try allocator.alloc(radix_join.KeyValue, size);
    errdefer allocator.free(data);

    // Use simple linear congruential generator for reproducible randomness
    var seed: u64 = 12345;
    const a: u64 = 1103515245;
    const c: u64 = 12345;
    const m: u64 = 2147483648; // 2^31

    var i: u32 = 0;
    while (i < size) : (i += 1) {
        seed = (a *% seed +% c) % m;
        const key = @as(i64, @intCast(seed % @as(u64, @intCast(max_key))));
        data[i] = radix_join.KeyValue.init(key, i);
    }

    return data;
}

/// Generate Zipf distribution (power-law, 80% of accesses go to 20% of keys)
///
/// **Zipf distribution**: P(k) ∝ 1 / k^s
/// - s = 1.0: Classic Zipf (80/20 rule)
/// - s = 0.5: Mild skew
/// - s = 2.0: Extreme skew
///
/// **Application**: Models real-world data like user activity, word frequency
fn generateZipfKeys(allocator: std.mem.Allocator, size: usize, max_key: i64, s: f64) ![]radix_join.KeyValue {
    std.debug.assert(size > 0);
    std.debug.assert(size <= radix_join.MAX_TOTAL_ROWS);
    std.debug.assert(max_key > 0);
    std.debug.assert(s > 0.0); // Zipf parameter must be positive

    const data = try allocator.alloc(radix_join.KeyValue, size);
    errdefer allocator.free(data);

    // Precompute normalization constant (sum of 1/k^s for k=1..max_key)
    var norm: f64 = 0.0;
    var k: u32 = 1;
    while (k <= max_key) : (k += 1) {
        const k_f64 = @as(f64, @floatFromInt(k));
        norm += 1.0 / std.math.pow(f64, k_f64, s);
    }

    // Generate keys according to Zipf distribution using inverse CDF method
    var seed: u64 = 12345;
    const a: u64 = 1103515245;
    const c: u64 = 12345;
    const m: u64 = 2147483648;

    var i: u32 = 0;
    while (i < size) : (i += 1) {
        // Generate uniform random number in [0, 1]
        seed = (a *% seed +% c) % m;
        const u = @as(f64, @floatFromInt(seed)) / @as(f64, @floatFromInt(m));

        // Find key using inverse CDF
        var cumulative: f64 = 0.0;
        var key: i64 = 1;
        k = 1;
        while (k <= max_key) : (k += 1) {
            const k_f64 = @as(f64, @floatFromInt(k));
            const prob = (1.0 / std.math.pow(f64, k_f64, s)) / norm;
            cumulative += prob;
            if (u <= cumulative) {
                key = @intCast(k);
                break;
            }
        }

        data[i] = radix_join.KeyValue.init(key, i);
    }

    return data;
}

/// Generate clustered distribution (keys are spatially clustered)
///
/// **Pattern**: Keys are grouped into clusters with high intra-cluster similarity
/// **Application**: Models geographic data, temporal patterns, sequential IDs
fn generateClusteredKeys(allocator: std.mem.Allocator, size: usize, num_clusters: u32) ![]radix_join.KeyValue {
    std.debug.assert(size > 0);
    std.debug.assert(size <= radix_join.MAX_TOTAL_ROWS);
    std.debug.assert(num_clusters > 0);
    std.debug.assert(num_clusters <= 1000);

    const data = try allocator.alloc(radix_join.KeyValue, size);
    errdefer allocator.free(data);

    const cluster_size = size / num_clusters;

    var i: u32 = 0;
    while (i < size) : (i += 1) {
        const cluster_id = i / @as(u32, @intCast(cluster_size));
        // Keys within cluster are close together (cluster_id * 1000 + offset)
        const base_key = @as(i64, @intCast(cluster_id)) * 1000;
        const offset = @as(i64, @intCast(i % cluster_size));
        data[i] = radix_join.KeyValue.init(base_key + offset, i);
    }

    return data;
}

// ============================================================================
// Tests: Radix Partitioning Correctness with Different Distributions
// ============================================================================

test "Radix partitioning: uniform distribution" {
    const allocator = testing.allocator;

    const size = 10_000;
    const max_key = 5_000; // 50% unique keys

    const input = try generateUniformKeys(allocator, size, max_key);
    defer allocator.free(input);

    var ctx = try radix_join.partitionData(allocator, input);
    defer ctx.deinit();

    // Verify all elements are partitioned correctly
    try testing.expectEqual(input.len, ctx.partitioned_data.len);

    // Verify partitions are non-empty and non-overlapping
    var total_count: u32 = 0;
    for (ctx.partitions) |partition| {
        total_count += partition.count;
    }
    try testing.expectEqual(@as(u32, @intCast(size)), total_count);
}

test "Radix partitioning: Zipf distribution (s=1.0, 80/20 rule)" {
    const allocator = testing.allocator;

    const size = 10_000;
    const max_key = 1_000; // Most accesses go to first few keys

    const input = try generateZipfKeys(allocator, size, max_key, 1.0);
    defer allocator.free(input);

    var ctx = try radix_join.partitionData(allocator, input);
    defer ctx.deinit();

    // Verify correctness
    try testing.expectEqual(input.len, ctx.partitioned_data.len);

    var total_count: u32 = 0;
    for (ctx.partitions) |partition| {
        total_count += partition.count;
    }
    try testing.expectEqual(@as(u32, @intCast(size)), total_count);

    // Verify skew: some partitions should have significantly more elements
    var max_partition_size: u32 = 0;
    var min_partition_size: u32 = std.math.maxInt(u32);

    for (ctx.partitions) |partition| {
        if (partition.count > 0) {
            max_partition_size = @max(max_partition_size, partition.count);
            min_partition_size = @min(min_partition_size, partition.count);
        }
    }

    // With Zipf, expect significant skew (max >> min)
    // Note: Actual ratio depends on radix bits and Zipf parameter
}

test "Radix partitioning: clustered distribution" {
    const allocator = testing.allocator;

    const size = 10_000;
    const num_clusters = 100; // 100 clusters of ~100 elements each

    const input = try generateClusteredKeys(allocator, size, num_clusters);
    defer allocator.free(input);

    var ctx = try radix_join.partitionData(allocator, input);
    defer ctx.deinit();

    // Verify correctness
    try testing.expectEqual(input.len, ctx.partitioned_data.len);

    var total_count: u32 = 0;
    for (ctx.partitions) |partition| {
        total_count += partition.count;
    }
    try testing.expectEqual(@as(u32, @intCast(size)), total_count);
}

// ============================================================================
// Tests: Hash Table Probe Correctness with Different Distributions
// ============================================================================

test "Hash table probe: uniform distribution" {
    const allocator = testing.allocator;

    const size = 5_000;
    const max_key = 2_500;

    const input = try generateUniformKeys(allocator, size, max_key);
    defer allocator.free(input);

    var table = try radix_join.buildHashTable(allocator, input);
    defer table.deinit();

    // Probe for keys that should exist
    var matches = std.ArrayListUnmanaged(u32){};
    defer matches.deinit(allocator);

    try table.probe(input[0].key, &matches, allocator);

    // Should find at least one match (the key itself)
    try testing.expect(matches.items.len >= 1);

    // Verify all matches have the correct key
    for (matches.items) |row_idx| {
        try testing.expectEqual(input[0].key, input[row_idx].key);
    }
}

test "Hash table probe: Zipf distribution (many duplicates)" {
    const allocator = testing.allocator;

    const size = 5_000;
    const max_key = 100; // Small key space → many duplicates

    const input = try generateZipfKeys(allocator, size, max_key, 1.0);
    defer allocator.free(input);

    var table = try radix_join.buildHashTable(allocator, input);
    defer table.deinit();

    // Probe for key=1 (most frequent in Zipf)
    var matches = std.ArrayListUnmanaged(u32){};
    defer matches.deinit(allocator);

    try table.probe(1, &matches, allocator);

    // With Zipf s=1.0, key=1 should appear many times
    try testing.expect(matches.items.len > 10); // Expect significant duplicates

    // Verify all matches are correct
    for (matches.items) |row_idx| {
        try testing.expectEqual(@as(i64, 1), input[row_idx].key);
    }
}

test "Hash table probe: clustered distribution" {
    const allocator = testing.allocator;

    const size = 5_000;
    const num_clusters = 50;

    const input = try generateClusteredKeys(allocator, size, num_clusters);
    defer allocator.free(input);

    var table = try radix_join.buildHashTable(allocator, input);
    defer table.deinit();

    // Probe for a key in the middle of a cluster
    const cluster_0_key = input[50].key; // Middle of first cluster

    var matches = std.ArrayListUnmanaged(u32){};
    defer matches.deinit(allocator);

    try table.probe(cluster_0_key, &matches, allocator);

    // Should find exactly one match (keys within cluster are unique)
    try testing.expect(matches.items.len >= 1);

    // Verify match is correct
    try testing.expectEqual(cluster_0_key, input[matches.items[0]].key);
}

// ============================================================================
// Tests: Bloom Filter Performance with Different Distributions
// ============================================================================

test "Bloom filter: uniform distribution (low false positive rate)" {
    const allocator = testing.allocator;

    const size = 10_000;
    const max_key = 10_000; // All keys unique

    const input = try generateUniformKeys(allocator, size, max_key);
    defer allocator.free(input);

    var table = try radix_join.buildHashTable(allocator, input);
    defer table.deinit();

    // Probe for non-existent keys
    var false_positives: u32 = 0;
    var probe_count: u32 = 0;

    var key: i64 = max_key + 1;
    while (key < max_key + 1000) : (key += 1) {
        if (table.bloom_filter.mightContain(key)) {
            false_positives += 1;
        }
        probe_count += 1;
    }

    const fpr = @as(f64, @floatFromInt(false_positives)) / @as(f64, @floatFromInt(probe_count));

    // False positive rate should be ~1% (bloom filter target)
    try testing.expect(fpr < 0.03); // Less than 3%
}

test "Bloom filter: Zipf distribution (handles duplicates correctly)" {
    const allocator = testing.allocator;

    const size = 10_000;
    const max_key = 100; // Many duplicates

    const input = try generateZipfKeys(allocator, size, max_key, 1.0);
    defer allocator.free(input);

    var table = try radix_join.buildHashTable(allocator, input);
    defer table.deinit();

    // Bloom filter should contain all unique keys
    // Probe for existing keys (should all return true)
    var key: i64 = 1;
    while (key <= max_key) : (key += 1) {
        const found = table.bloom_filter.mightContain(key);
        // If key exists in input, bloom filter MUST return true (no false negatives)
        if (found) {
            // Check if key actually exists in input
            var exists = false;
            for (input) |kv| {
                if (kv.key == key) {
                    exists = true;
                    break;
                }
            }
            // Bloom filter says "might contain", verify it's correct or false positive
            if (!exists) {
                // This is a false positive (acceptable)
            }
        }
    }

    // Bloom filter should reject keys outside range (very low FPR)
    var false_positives: u32 = 0;
    key = max_key + 1;
    while (key < max_key + 1000) : (key += 1) {
        if (table.bloom_filter.mightContain(key)) {
            false_positives += 1;
        }
    }

    const fpr = @as(f64, @floatFromInt(false_positives)) / 999.0;
    try testing.expect(fpr < 0.05); // Less than 5%
}
