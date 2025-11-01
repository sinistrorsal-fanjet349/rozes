const std = @import("std");
const testing = std.testing;
const radix_join = @import("../../../core/radix_join.zig");

const KeyValue = radix_join.KeyValue;
const Partition = radix_join.Partition;
const RadixJoinContext = radix_join.RadixJoinContext;
const PARTITION_COUNT = radix_join.PARTITION_COUNT;

// ============================================================================
// Basic Functionality Tests
// ============================================================================

test "KeyValue.init creates correct key-value pair" {
    const kv = KeyValue.init(42, 10);
    try testing.expectEqual(@as(i64, 42), kv.key);
    try testing.expectEqual(@as(u32, 10), kv.row_index);
}

test "Partition.init creates empty partition" {
    const p = Partition.init(5);
    try testing.expectEqual(@as(u32, 5), p.index);
    try testing.expectEqual(@as(u32, 0), p.start);
    try testing.expectEqual(@as(u32, 0), p.count);
    try testing.expect(p.isEmpty());
}

test "Partition.isEmpty returns correct status" {
    var p = Partition.init(0);
    try testing.expect(p.isEmpty());

    p.count = 10;
    try testing.expect(!p.isEmpty());
}

// ============================================================================
// RadixJoinContext Tests
// ============================================================================

test "RadixJoinContext.init allocates correct sizes" {
    const allocator = testing.allocator;

    var ctx = try RadixJoinContext.init(allocator, 5000);
    defer ctx.deinit();

    try testing.expectEqual(PARTITION_COUNT, ctx.histogram.len);
    try testing.expectEqual(PARTITION_COUNT, ctx.partitions.len);
    try testing.expectEqual(@as(usize, 5000), ctx.partitioned_data.len);

    // Verify histogram is zeroed
    for (ctx.histogram) |count| {
        try testing.expectEqual(@as(u32, 0), count);
    }

    // Verify partitions are initialized
    for (ctx.partitions, 0..) |p, i| {
        try testing.expectEqual(@as(u32, i), p.index);
        try testing.expect(p.isEmpty());
    }
}

test "RadixJoinContext memory is freed correctly" {
    const allocator = testing.allocator;

    // Create and destroy 1000 times to check for leaks
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        var ctx = try RadixJoinContext.init(allocator, 100);
        ctx.deinit();
    }
}

// ============================================================================
// Histogram Tests
// ============================================================================

test "buildHistogram with single element" {
    const allocator = testing.allocator;

    const input = [_]KeyValue{
        KeyValue.init(42, 0),
    };

    var ctx = try RadixJoinContext.init(allocator, input.len);
    defer ctx.deinit();

    radix_join.buildHistogram(&ctx, &input, 0);

    // Verify total count
    var total: u32 = 0;
    for (ctx.histogram) |count| {
        total += count;
    }
    try testing.expectEqual(@as(u32, 1), total);
}

test "buildHistogram with identical keys" {
    const allocator = testing.allocator;

    // All same key should go to same partition
    const input = [_]KeyValue{
        KeyValue.init(1000, 0),
        KeyValue.init(1000, 1),
        KeyValue.init(1000, 2),
        KeyValue.init(1000, 3),
        KeyValue.init(1000, 4),
    };

    var ctx = try RadixJoinContext.init(allocator, input.len);
    defer ctx.deinit();

    radix_join.buildHistogram(&ctx, &input, 0);

    // All should be in one partition
    var partitions_used: u32 = 0;
    for (ctx.histogram) |count| {
        if (count > 0) {
            partitions_used += 1;
            try testing.expectEqual(@as(u32, 5), count);
        }
    }
    try testing.expectEqual(@as(u32, 1), partitions_used);
}

test "buildHistogram with diverse keys" {
    const allocator = testing.allocator;

    // Keys with different high bits should go to different partitions
    const input = [_]KeyValue{
        KeyValue.init(std.math.minInt(i64), 0), // Very negative
        KeyValue.init(-1000, 1),
        KeyValue.init(0, 2),
        KeyValue.init(1000, 3),
        KeyValue.init(std.math.maxInt(i64), 4), // Very positive
    };

    var ctx = try RadixJoinContext.init(allocator, input.len);
    defer ctx.deinit();

    radix_join.buildHistogram(&ctx, &input, 0);

    // Verify total
    var total: u32 = 0;
    for (ctx.histogram) |count| {
        total += count;
    }
    try testing.expectEqual(@as(u32, input.len), total);
}

test "buildHistogram with 1000 elements" {
    const allocator = testing.allocator;

    // Generate 1000 sequential keys
    var input = try allocator.alloc(KeyValue, 1000);
    defer allocator.free(input);

    for (input, 0..) |*kv, i| {
        kv.* = KeyValue.init(@intCast(i * 1000), @intCast(i));
    }

    var ctx = try RadixJoinContext.init(allocator, @intCast(input.len));
    defer ctx.deinit();

    radix_join.buildHistogram(&ctx, input, 0);

    // Verify total
    var total: u32 = 0;
    for (ctx.histogram) |count| {
        total += count;
    }
    try testing.expectEqual(@as(u32, 1000), total);
}

// ============================================================================
// Prefix Sum Tests
// ============================================================================

test "computePrefixSum with empty partitions" {
    const allocator = testing.allocator;

    var ctx = try RadixJoinContext.init(allocator, 0);
    defer ctx.deinit();

    @memset(ctx.histogram, 0);
    radix_join.computePrefixSum(&ctx);

    // All partitions should have start=0, count=0
    for (ctx.partitions) |p| {
        try testing.expectEqual(@as(u32, 0), p.start);
        try testing.expectEqual(@as(u32, 0), p.count);
    }
}

test "computePrefixSum with simple distribution" {
    const allocator = testing.allocator;

    var ctx = try RadixJoinContext.init(allocator, 15);
    defer ctx.deinit();

    @memset(ctx.histogram, 0);
    ctx.histogram[0] = 5;
    ctx.histogram[1] = 3;
    ctx.histogram[2] = 7;

    radix_join.computePrefixSum(&ctx);

    // Partition 0: [0, 5)
    try testing.expectEqual(@as(u32, 0), ctx.partitions[0].start);
    try testing.expectEqual(@as(u32, 5), ctx.partitions[0].count);

    // Partition 1: [5, 8)
    try testing.expectEqual(@as(u32, 5), ctx.partitions[1].start);
    try testing.expectEqual(@as(u32, 3), ctx.partitions[1].count);

    // Partition 2: [8, 15)
    try testing.expectEqual(@as(u32, 8), ctx.partitions[2].start);
    try testing.expectEqual(@as(u32, 7), ctx.partitions[2].count);

    // Rest should be empty
    for (ctx.partitions[3..]) |p| {
        try testing.expect(p.isEmpty());
    }
}

test "computePrefixSum non-overlapping partitions" {
    const allocator = testing.allocator;

    var ctx = try RadixJoinContext.init(allocator, 256);
    defer ctx.deinit();

    // Set each partition to have 1 element
    for (ctx.histogram) |*h| {
        h.* = 1;
    }

    radix_join.computePrefixSum(&ctx);

    // Verify no overlaps
    var i: u32 = 0;
    while (i < PARTITION_COUNT - 1) : (i += 1) {
        const curr_end = ctx.partitions[i].start + ctx.partitions[i].count;
        try testing.expectEqual(curr_end, ctx.partitions[i + 1].start);
    }
}

// ============================================================================
// Scatter Tests
// ============================================================================

test "scatterToPartitions with single element" {
    const allocator = testing.allocator;

    const input = [_]KeyValue{
        KeyValue.init(42, 99),
    };

    var ctx = try RadixJoinContext.init(allocator, input.len);
    defer ctx.deinit();

    radix_join.buildHistogram(&ctx, &input, 0);
    radix_join.computePrefixSum(&ctx);
    radix_join.scatterToPartitions(&ctx, &input, 0);

    // Verify element is in partitioned data
    var found = false;
    for (ctx.partitioned_data) |kv| {
        if (kv.key == 42 and kv.row_index == 99) {
            found = true;
            break;
        }
    }
    try testing.expect(found);
}

test "scatterToPartitions preserves all elements" {
    const allocator = testing.allocator;

    const input = [_]KeyValue{
        KeyValue.init(100, 0),
        KeyValue.init(200, 1),
        KeyValue.init(150, 2),
        KeyValue.init(250, 3),
        KeyValue.init(100, 4), // Duplicate key
    };

    var ctx = try RadixJoinContext.init(allocator, input.len);
    defer ctx.deinit();

    radix_join.buildHistogram(&ctx, &input, 0);
    radix_join.computePrefixSum(&ctx);
    radix_join.scatterToPartitions(&ctx, &input, 0);

    // Verify all input elements exist in output
    for (input) |in_kv| {
        var found = false;
        for (ctx.partitioned_data) |out_kv| {
            if (out_kv.key == in_kv.key and
                out_kv.row_index == in_kv.row_index)
            {
                found = true;
                break;
            }
        }
        try testing.expect(found);
    }
}

test "scatterToPartitions groups identical keys" {
    const allocator = testing.allocator;

    // All same key should be in same partition
    const input = [_]KeyValue{
        KeyValue.init(1000, 0),
        KeyValue.init(1000, 1),
        KeyValue.init(1000, 2),
    };

    var ctx = try RadixJoinContext.init(allocator, input.len);
    defer ctx.deinit();

    radix_join.buildHistogram(&ctx, &input, 0);
    radix_join.computePrefixSum(&ctx);
    radix_join.scatterToPartitions(&ctx, &input, 0);

    // Find which partition has elements
    var partition_with_data: ?u32 = null;
    for (ctx.partitions, 0..) |p, i| {
        if (!p.isEmpty()) {
            partition_with_data = @intCast(i);
            try testing.expectEqual(@as(u32, 3), p.count);
            break;
        }
    }

    try testing.expect(partition_with_data != null);
}

// ============================================================================
// End-to-End Tests
// ============================================================================

test "partitionData with small dataset" {
    const allocator = testing.allocator;

    const input = [_]KeyValue{
        KeyValue.init(10, 0),
        KeyValue.init(20, 1),
        KeyValue.init(30, 2),
    };

    var ctx = try radix_join.partitionData(allocator, &input);
    defer ctx.deinit();

    try testing.expectEqual(input.len, ctx.partitioned_data.len);

    // Verify all elements present
    for (input) |in_kv| {
        var found = false;
        for (ctx.partitioned_data) |out_kv| {
            if (out_kv.key == in_kv.key and
                out_kv.row_index == in_kv.row_index)
            {
                found = true;
                break;
            }
        }
        try testing.expect(found);
    }
}

test "partitionData with 10K elements" {
    const allocator = testing.allocator;

    // Generate 10K random keys
    var input = try allocator.alloc(KeyValue, 10_000);
    defer allocator.free(input);

    var prng = std.rand.DefaultPrng.init(12345);
    const random = prng.random();

    for (input, 0..) |*kv, i| {
        const key = random.int(i64);
        kv.* = KeyValue.init(key, @intCast(i));
    }

    var ctx = try radix_join.partitionData(allocator, input);
    defer ctx.deinit();

    try testing.expectEqual(input.len, ctx.partitioned_data.len);

    // Verify total count across partitions
    var total: u32 = 0;
    for (ctx.partitions) |p| {
        total += p.count;
    }
    try testing.expectEqual(@as(u32, 10_000), total);
}

test "partitionData partitions are non-overlapping" {
    const allocator = testing.allocator;

    var input = try allocator.alloc(KeyValue, 1000);
    defer allocator.free(input);

    for (input, 0..) |*kv, i| {
        kv.* = KeyValue.init(@intCast(i), @intCast(i));
    }

    var ctx = try radix_join.partitionData(allocator, input);
    defer ctx.deinit();

    // Verify partitions don't overlap
    var i: u32 = 0;
    while (i < PARTITION_COUNT - 1) : (i += 1) {
        if (ctx.partitions[i].count > 0 and
            ctx.partitions[i + 1].count > 0)
        {
            const curr_end = ctx.partitions[i].start +
                           ctx.partitions[i].count;
            try testing.expect(curr_end <= ctx.partitions[i + 1].start);
        }
    }
}

test "partitionData memory safety - no leaks" {
    const allocator = testing.allocator;

    // Run 1000 iterations to detect leaks
    var iter: u32 = 0;
    while (iter < 1000) : (iter += 1) {
        const input = [_]KeyValue{
            KeyValue.init(100, 0),
            KeyValue.init(200, 1),
            KeyValue.init(300, 2),
        };

        var ctx = try radix_join.partitionData(allocator, &input);
        ctx.deinit();
    }
}

// ============================================================================
// Edge Case Tests
// ============================================================================

test "partitionData with extreme values" {
    const allocator = testing.allocator;

    const input = [_]KeyValue{
        KeyValue.init(std.math.minInt(i64), 0),
        KeyValue.init(std.math.maxInt(i64), 1),
        KeyValue.init(0, 2),
        KeyValue.init(-1, 3),
        KeyValue.init(1, 4),
    };

    var ctx = try radix_join.partitionData(allocator, &input);
    defer ctx.deinit();

    // All elements should be preserved
    try testing.expectEqual(input.len, ctx.partitioned_data.len);
}

test "partitionData with duplicate keys preserves row indices" {
    const allocator = testing.allocator;

    const input = [_]KeyValue{
        KeyValue.init(42, 10),
        KeyValue.init(42, 20),
        KeyValue.init(42, 30),
    };

    var ctx = try radix_join.partitionData(allocator, &input);
    defer ctx.deinit();

    // Verify all row indices present
    const row_indices = [_]u32{ 10, 20, 30 };
    for (row_indices) |expected_idx| {
        var found = false;
        for (ctx.partitioned_data) |kv| {
            if (kv.key == 42 and kv.row_index == expected_idx) {
                found = true;
                break;
            }
        }
        try testing.expect(found);
    }
}

// ============================================================================
// Hash Table Tests
// ============================================================================

const HashTable = radix_join.HashTable;
const HashEntry = radix_join.HashEntry;

test "HashEntry.init creates empty entry" {
    const entry = HashEntry.init();
    try testing.expectEqual(false, entry.occupied);
}

test "HashEntry.set marks entry as occupied" {
    var entry = HashEntry.init();
    entry.set(42, 10);

    try testing.expectEqual(true, entry.occupied);
    try testing.expectEqual(@as(i64, 42), entry.key);
    try testing.expectEqual(@as(u32, 10), entry.row_index);
}

test "HashTable.init allocates correct capacity" {
    const allocator = testing.allocator;

    var table = try HashTable.init(allocator, 100);
    defer table.deinit();

    // Capacity should be rounded up to power of 2
    try testing.expect(std.math.isPowerOfTwo(table.capacity));
    try testing.expect(table.capacity >= 100);
    try testing.expectEqual(@as(u32, 0), table.count);
}

test "HashTable.insert adds entry" {
    const allocator = testing.allocator;

    var table = try HashTable.init(allocator, 10);
    defer table.deinit();

    try table.insert(42, 100);

    try testing.expectEqual(@as(u32, 1), table.count);
}

test "HashTable.insert with collisions" {
    const allocator = testing.allocator;

    var table = try HashTable.init(allocator, 8);
    defer table.deinit();

    // Insert multiple keys (some will collide)
    try table.insert(1, 10);
    try table.insert(2, 20);
    try table.insert(3, 30);
    try table.insert(4, 40);

    try testing.expectEqual(@as(u32, 4), table.count);
}

test "HashTable.probe finds inserted key" {
    const allocator = testing.allocator;

    var table = try HashTable.init(allocator, 10);
    defer table.deinit();

    try table.insert(42, 100);

    var results = std.ArrayList(u32).init(allocator);
    defer results.deinit();

    try table.probe(42, &results);

    try testing.expectEqual(@as(usize, 1), results.items.len);
    try testing.expectEqual(@as(u32, 100), results.items[0]);
}

test "HashTable.probe returns empty for missing key" {
    const allocator = testing.allocator;

    var table = try HashTable.init(allocator, 10);
    defer table.deinit();

    try table.insert(42, 100);

    var results = std.ArrayList(u32).init(allocator);
    defer results.deinit();

    try table.probe(99, &results);

    try testing.expectEqual(@as(usize, 0), results.items.len);
}

test "HashTable.probe finds multiple matching keys" {
    const allocator = testing.allocator;

    var table = try HashTable.init(allocator, 20);
    defer table.deinit();

    // Insert duplicate keys with different row indices
    try table.insert(42, 10);
    try table.insert(42, 20);
    try table.insert(42, 30);
    try table.insert(99, 100);

    var results = std.ArrayList(u32).init(allocator);
    defer results.deinit();

    try table.probe(42, &results);

    // Should find all 3 entries with key=42
    try testing.expectEqual(@as(usize, 3), results.items.len);

    // Verify all row indices present
    var found_10 = false;
    var found_20 = false;
    var found_30 = false;

    for (results.items) |idx| {
        if (idx == 10) found_10 = true;
        if (idx == 20) found_20 = true;
        if (idx == 30) found_30 = true;
    }

    try testing.expect(found_10);
    try testing.expect(found_20);
    try testing.expect(found_30);
}

test "buildHashTable creates table from partition" {
    const allocator = testing.allocator;

    const data = [_]KeyValue{
        KeyValue.init(10, 0),
        KeyValue.init(20, 1),
        KeyValue.init(30, 2),
        KeyValue.init(10, 3), // Duplicate key
    };

    var table = try radix_join.buildHashTable(allocator, &data);
    defer table.deinit();

    try testing.expectEqual(@as(u32, 4), table.count);

    // Verify all entries can be probed
    var results = std.ArrayList(u32).init(allocator);
    defer results.deinit();

    try table.probe(10, &results);
    try testing.expectEqual(@as(usize, 2), results.items.len);

    results.clearRetainingCapacity();
    try table.probe(20, &results);
    try testing.expectEqual(@as(usize, 1), results.items.len);
}

test "buildHashTable with 1000 entries" {
    const allocator = testing.allocator;

    var data = try allocator.alloc(KeyValue, 1000);
    defer allocator.free(data);

    for (data, 0..) |*kv, i| {
        kv.* = KeyValue.init(@intCast(i * 10), @intCast(i));
    }

    var table = try radix_join.buildHashTable(allocator, data);
    defer table.deinit();

    try testing.expectEqual(@as(u32, 1000), table.count);

    // Verify random lookups
    var results = std.ArrayList(u32).init(allocator);
    defer results.deinit();

    try table.probe(500 * 10, &results);
    try testing.expectEqual(@as(usize, 1), results.items.len);
    try testing.expectEqual(@as(u32, 500), results.items[0]);
}

test "HashTable memory safety - no leaks" {
    const allocator = testing.allocator;

    var iter: u32 = 0;
    while (iter < 1000) : (iter += 1) {
        var table = try HashTable.init(allocator, 10);
        try table.insert(42, 100);
        table.deinit();
    }
}
