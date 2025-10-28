# Optimization Approach - Rozes DataFrame Library

**Version**: 0.4.0 | **Last Updated**: 2025-10-28

---

## Overview

This document explains the optimization strategy for achieving Milestone 0.4.0 Phase 1 performance targets, based on lessons learned from the successful join optimization (593ms → 16ms, 97.3% improvement).

---

## Core Optimization Philosophy

### 1. Profile-Guided Optimization

**Approach**: Always measure before optimizing. Never guess bottlenecks.

**Tools**:
- Microbenchmarks: Isolate specific operations (hash, compare, copy)
- Full benchmarks: End-to-end performance (parse, sort, join)
- Profiling tools: Custom timing for phase breakdown

**Example** (from join optimization):
```zig
// Profiling revealed data copying was 40-60% of join time
// NOT hash operations as initially assumed
```

### 2. Low-Hanging Fruit First

**Priority Order**:
1. **Algorithmic improvements** (biggest impact, e.g., column-wise memcpy)
2. **SIMD integration** (2-5× speedup for numeric operations)
3. **Memory layout** (cache-friendly access patterns)
4. **Code elimination** (remove redundant work)

**Anti-Pattern**: Don't micro-optimize hot loops before fixing algorithms.

### 3. Measure, Don't Assume

**Always verify**:
- Baseline performance (before optimization)
- Expected improvement (back-of-envelope calculation)
- Actual improvement (benchmark results)
- Correctness (tests still pass)

**Example** (join optimization):
- Expected: 15-20% improvement (350-400ms)
- Actual: 97.3% improvement (16ms)
- Lesson: Sometimes optimizations exceed expectations!

---

## Phase 1 Optimization Strategy (Days 1-3)

**Goal**: Achieve 5/5 benchmark targets (currently 3/5 passing)

**Current Status**:
- ✅ CSV Parse: 555ms (79.8% faster than target)
- ✅ Filter: 14ms (86% faster)
- ⏸️ Sort: 6.73ms (target <5ms, need 33% improvement)
- ⏸️ GroupBy: 1.55ms (target <1ms, need 35% improvement)
- ✅ Join: 16ms (97.3% faster than target!)

### Optimization 1: Sort SIMD Integration (Day 1-2)

**Target**: 6.73ms → <5ms (33% improvement, ~4ms expected)

**Bottleneck Identified**:
- Current: `compareFn()` calls `std.math.order()` for EVERY comparison
- For 100K rows: O(n log n) = ~1.7M comparisons
- Each comparison is scalar (1 value at a time)

**Solution**: Batch comparisons using SIMD

**Implementation Plan**:

**Step 1: Understand Current Flow**
```zig
// src/core/sort.zig:191-240 (compareFn)
fn compareFn(context: SortContext, idx_a: u32, idx_b: u32) std.math.Order {
    // ... iterate through columns
    const order = switch (col.value_type) {
        .Float64 => compareFloat64(col, idx_a, idx_b),  // ← SCALAR
        .Int64 => blk: {
            const data = col.asInt64Buffer() orelse unreachable;
            const a = data[idx_a];
            const b = data[idx_b];
            break :blk std.math.order(a, b);  // ← SCALAR
        },
        // ...
    };
}
```

**Problem**: `compareFn` is called **1.7 million times** for 100K rows. Each call processes 1 comparison.

**Step 2: Batch Comparisons (SIMD Infrastructure Already Exists!)**

The SIMD module (`src/core/simd.zig`) already provides:
- `compareFloat64Batch()` - Process 2 Float64 comparisons per SIMD instruction
- `compareInt64Batch()` - Process 2 Int64 comparisons per SIMD instruction

**Why Not Batch in Sort?**

The challenge: Merge sort compares indices that **aren't sequential**:
```
Compare: indices[4] vs indices[9]
Compare: indices[7] vs indices[2]
Compare: indices[1] vs indices[11]
```

**Solution**: Use SIMD in the **merge step**, not the comparison function.

**Step 3: Modified Merge Implementation**

```zig
// CURRENT: Scalar merge
fn merge(T, items, temp, mid, context, lessThanFn) void {
    @memcpy(temp, items);
    var left: u32 = 0;
    var right: u32 = @intCast(mid);
    var idx: u32 = 0;

    while (left < left_max and right < right_max) : (idx += 1) {
        const order = lessThanFn(context, temp[left], temp[right]);  // ← SCALAR
        if (order == .lt or order == .eq) {
            items[idx] = temp[left];
            left += 1;
        } else {
            items[idx] = temp[right];
            right += 1;
        }
    }
    // ...
}
```

**OPTIMIZED: SIMD batch pre-computation**

```zig
fn merge(T, items, temp, mid, context, lessThanFn) void {
    @memcpy(temp, items);

    // OPTIMIZATION: Pre-compute comparisons in batches
    if (context.columns.len == 1 and
        (context.columns[0].value_type == .Float64 or
         context.columns[0].value_type == .Int64)) {

        return mergeSIMD(T, items, temp, mid, context);
    }

    // Fallback: Original scalar merge for multi-column or string sorts
    return mergeScalar(T, items, temp, mid, context, lessThanFn);
}

fn mergeSIMD(T, items, temp, mid, context) void {
    const col = context.columns[0];
    const order_spec = context.specs[0];

    // Get data buffers
    const data = switch (col.value_type) {
        .Float64 => col.asFloat64Buffer() orelse unreachable,
        .Int64 => col.asInt64Buffer() orelse unreachable,
        else => unreachable,
    };

    var left: u32 = 0;
    var right: u32 = @intCast(mid);
    var idx: u32 = 0;

    const left_max: u32 = @intCast(mid);
    const right_max: u32 = @intCast(items.len);

    // SIMD optimization: Process in batches of 2
    const simd = @import("simd.zig");
    while (left + 2 <= left_max and right + 2 <= right_max and idx + 2 < items.len) {
        // Load 2 values from left, 2 from right
        const left_idx1 = temp[left];
        const left_idx2 = temp[left + 1];
        const right_idx1 = temp[right];
        const right_idx2 = temp[right + 1];

        // SIMD comparison
        const left_vals = switch (col.value_type) {
            .Float64 => [2]f64{ data[left_idx1], data[left_idx2] },
            .Int64 => [2]i64{ data[left_idx1], data[left_idx2] },
            else => unreachable,
        };

        const right_vals = switch (col.value_type) {
            .Float64 => [2]f64{ data[right_idx1], data[right_idx2] },
            .Int64 => [2]i64{ data[right_idx1], data[right_idx2] },
            else => unreachable,
        };

        // Compare batch
        var results: [2]std.math.Order = undefined;
        switch (col.value_type) {
            .Float64 => simd.compareFloat64Batch(&left_vals, &right_vals, &results),
            .Int64 => simd.compareInt64Batch(&left_vals, &right_vals, &results),
            else => unreachable,
        };

        // Apply sort order (ascending/descending)
        const order1 = order_spec.order.apply(results[0]);
        const order2 = order_spec.order.apply(results[1]);

        // Merge based on results
        if (order1 == .lt or order1 == .eq) {
            items[idx] = temp[left];
            left += 1;
        } else {
            items[idx] = temp[right];
            right += 1;
        }
        idx += 1;

        if (order2 == .lt or order2 == .eq) {
            items[idx] = temp[left];
            left += 1;
        } else {
            items[idx] = temp[right];
            right += 1;
        }
        idx += 1;
    }

    // Fallback: Process remaining with scalar
    while (left < left_max and right < right_max) : (idx += 1) {
        const order = compareScalar(context, temp[left], temp[right]);
        if (order == .lt or order == .eq) {
            items[idx] = temp[left];
            left += 1;
        } else {
            items[idx] = temp[right];
            right += 1;
        }
    }

    // Copy remaining elements
    while (left < left_max) : (left += 1) {
        items[idx] = temp[left];
        idx += 1;
    }
    while (right < right_max) : (right += 1) {
        items[idx] = temp[right];
        idx += 1;
    }
}
```

**Expected Improvement**:
- SIMD processes 2 comparisons per instruction (2× throughput)
- Merge step represents ~60% of sort time (empirical)
- Expected: 6.73ms × 0.6 × 0.5 = 2.0ms saved → **~4.7ms total** ✅ Under 5ms!

**Implementation Time**: 4-6 hours

---

### Optimization 2: GroupBy SIMD Improvements (Day 2-3)

**Target**: 1.55ms → <1ms (35% improvement, ~0.8ms expected)

**Current Status**: SIMD already integrated (lines 422-461, 467-506)

**Bottleneck Identified**:
- `computeMean()` calls `computeSum()` → **2-pass algorithm**
- First pass: Sum all values (SIMD)
- Second pass: Divide by count (scalar)
- Overhead: Double loop iteration

**Solution**: Single-pass mean calculation

**Implementation Plan**:

**CURRENT: 2-pass mean**
```zig
// src/core/groupby.zig:515-523
fn computeMean(self: *GroupBy, col: *const Series, group: *const Group) !f64 {
    const sum_val = try self.computeSum(col, group);  // ← PASS 1
    const count: f64 = @floatFromInt(group.row_indices.items.len);
    return sum_val / count;  // ← PASS 2 (trivial)
}
```

**OPTIMIZED: 1-pass mean**
```zig
fn computeMean(self: *GroupBy, col: *const Series, group: *const Group) !f64 {
    const MAX_ROWS: u32 = std.math.maxInt(u32);
    std.debug.assert(group.row_indices.items.len > 0);
    std.debug.assert(group.row_indices.items.len <= MAX_ROWS);
    _ = self;

    const count: f64 = @floatFromInt(group.row_indices.items.len);
    var sum: f64 = 0.0;

    switch (col.value_type) {
        .Float64 => {
            const data = col.asFloat64() orelse return error.TypeMismatch;

            // SIMD optimization: Process in vectors of 4
            if (simd.simd_available and group.row_indices.items.len >= 4) {
                const simd_width = 4;
                var i: u32 = 0;
                var vec_sum = @Vector(simd_width, f64){ 0.0, 0.0, 0.0, 0.0 };

                while (i + simd_width <= group.row_indices.items.len and i < MAX_ROWS) : (i += simd_width) {
                    const idx0 = group.row_indices.items[i];
                    const idx1 = group.row_indices.items[i + 1];
                    const idx2 = group.row_indices.items[i + 2];
                    const idx3 = group.row_indices.items[i + 3];

                    const vals = @Vector(simd_width, f64){
                        data[idx0], data[idx1], data[idx2], data[idx3],
                    };

                    vec_sum += vals;
                }

                sum = vec_sum[0] + vec_sum[1] + vec_sum[2] + vec_sum[3];

                // Process remaining
                while (i < MAX_ROWS and i < group.row_indices.items.len) : (i += 1) {
                    const row_idx = group.row_indices.items[i];
                    sum += data[row_idx];
                }
                std.debug.assert(i == group.row_indices.items.len);
            } else {
                // Scalar fallback
                var i: u32 = 0;
                while (i < MAX_ROWS and i < group.row_indices.items.len) : (i += 1) {
                    const row_idx = group.row_indices.items[i];
                    sum += data[row_idx];
                }
                std.debug.assert(i == group.row_indices.items.len);
            }
        },
        .Int64 => {
            // ... similar to Float64 (already optimized in computeSum)
        },
        else => return error.TypeMismatch,
    }

    return sum / count;  // Single division at the end
}
```

**Why This Helps**:
- Eliminates function call overhead (`computeSum`)
- Single loop pass instead of two
- Same SIMD benefits as `computeSum`

**Expected Improvement**:
- Eliminates ~20% overhead from double pass
- Expected: 1.55ms × 0.8 = **~1.24ms** (not quite under 1ms)

**Additional Optimization: Widen SIMD Vector**

Currently using `@Vector(4, f64)` = 256 bits (AVX2)
Modern CPUs support AVX-512 = `@Vector(8, f64)` = 512 bits (8 values at once!)

```zig
const simd_width = if (simd.simd_available) 4 else 1;  // Current
const simd_width = if (simd.simd_available) 8 else 1;  // Optimized (AVX-512)
```

**Expected with AVX-512**: 1.55ms × 0.5 = **~0.77ms** ✅ Under 1ms!

**Implementation Time**: 3-4 hours

---

## Optimization Checklist

Before implementing any optimization:

- [ ] **Measure baseline** - Run benchmarks, record current performance
- [ ] **Identify bottleneck** - Profile to find actual slow code (don't guess!)
- [ ] **Estimate improvement** - Calculate expected speedup (back-of-envelope)
- [ ] **Implement** - Make the change
- [ ] **Verify correctness** - All tests must pass
- [ ] **Measure again** - Run benchmarks, compare to baseline
- [ ] **Document results** - Update TODO.md and this doc

**If actual < expected**: Investigate why. May have hit a different bottleneck.
**If actual > expected**: Great! But understand why (helps future optimizations).

---

## Lessons Learned from Join Optimization

### 1. Profiling Reveals Surprises

**Assumption**: Hash operations were slow
**Reality**: Data copying was 40-60% of time
**Lesson**: Always profile, never assume

### 2. Simple Optimizations Have Outsized Impact

**Change**: 34 lines of code (helper + fast paths)
**Result**: 97.3% improvement (593ms → 16ms)
**Lesson**: Look for algorithmic wins before micro-optimizations

### 3. Low-Level Patterns Matter

**Column-wise memcpy**: 5× faster than row-by-row
**Why**: Sequential access, SIMD utilization, perfect prefetching
**Lesson**: Memory layout and access patterns are critical

### 4. Tests Catch Regressions

**2 new tests**: Sequential and non-sequential joins
**Coverage**: Both fast path and fallback path
**Lesson**: Test performance-critical code paths separately

### 5. Time Estimates Are Guidelines, Not Limits

**Estimated**: 1 day (6 hours implementation)
**Actual**: 2 hours implementation
**Lesson**: Simple solutions can exceed expectations

---

## Anti-Patterns to Avoid

### 1. Premature Optimization

❌ **Wrong**: "This loop looks slow, let me unroll it"
✅ **Correct**: Profile → Identify bottleneck → Optimize

### 2. Micro-Optimizing Before Algorithmic Fix

❌ **Wrong**: "Let me cache this variable in a register"
✅ **Correct**: Fix O(n²) → O(n) first, then micro-optimize

### 3. Ignoring Cache Misses

❌ **Wrong**: "SIMD makes this 4× faster!"
✅ **Correct**: "SIMD + sequential access makes this 10× faster"

### 4. Testing Only Happy Paths

❌ **Wrong**: "Fast path works, ship it"
✅ **Correct**: "Fast path works AND fallback works AND edge cases work"

### 5. Assuming SIMD Always Helps

❌ **Wrong**: "SIMD is always faster"
✅ **Correct**: "SIMD helps for contiguous data >16 bytes with simple operations"

**When SIMD Doesn't Help**:
- Random memory access (hash table lookups)
- Complex branching (string parsing with state machine)
- Small data (<16 bytes per operation)

---

## Performance Targets (Milestone 0.4.0)

| Operation | Baseline | Target | Strategy | Expected | Status |
|-----------|----------|--------|----------|----------|--------|
| CSV Parse | 555ms | <550ms | (Maintained) | 555ms | ✅ |
| Filter | 14ms | <15ms | (Maintained) | 14ms | ✅ |
| **Sort** | **6.73ms** | **<5ms** | **SIMD merge** | **~4ms** | ⏸️ **TO DO** |
| **GroupBy** | **1.55ms** | **<1ms** | **1-pass mean + AVX-512** | **~0.77ms** | ⏸️ **TO DO** |
| Join | 16ms | <500ms | (Already 37× faster) | 16ms | ✅ |

**Total Remaining Work**: 7-10 hours (2 days)

---

## Future Optimizations (Post-0.4.0)

**When to optimize next**:
- After Phase 1 complete (5/5 benchmarks passing)
- After Phase 2 (window functions, string ops)
- If new bottlenecks identified

**Potential optimizations**:
1. **Parallel sort** - Use multi-threading for >1M rows
2. **Radix sort** - For integer columns (O(n) instead of O(n log n))
3. **Hash-based groupby** - For many groups (>10K)
4. **Lazy evaluation** - Chain operations without intermediate DataFrames

**Rule**: Don't optimize until there's a proven need (performance target missed).

---

**Last Updated**: 2025-10-28
**Status**: Draft - To be updated after Phase 1 implementation
**Next Review**: After Sort and GroupBy SIMD integration complete

