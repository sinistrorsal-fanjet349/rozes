# Profiling Tools

**Purpose**: Performance analysis and benchmarking tools for Rozes DataFrame library

**Location**: `src/profiling_tools/`

---

## Available Tools

### 1. `benchmark_join.zig` - Join Performance Benchmark

**Purpose**: Measure join performance at various scales with column-wise memcpy optimization

**Usage**:
```bash
zig build-exe src/profiling_tools/benchmark_join.zig -O ReleaseFast
./benchmark_join
```

**Output**:
```
=== Join Performance Benchmark ===
Test: 10K × 10K join
  Duration: 1.84ms
  Result rows: 10000

Test: 50K × 50K join
  Duration: 8.27ms
  Result rows: 50000

Test: 100K × 100K join
  Duration: 16.07ms
  Result rows: 100000
  Baseline: 593ms
  Target: <500ms
  ✓ PASS (97.3% faster than baseline)
```

**Features**:
- Tests at 10K, 50K, 100K scales
- Measures sequential join performance
- Compares to baseline and target
- Validates data integrity

**Related Docs**:
- `docs/join_optimization_results.md`
- `docs/join_optimization_recommendation.md`

---

### 2. `run_join_profile.zig` - Join Phase Profiling

**Purpose**: Break down join performance by phase (build, probe, copy)

**Usage**:
```bash
zig build-exe src/profiling_tools/run_join_profile.zig -O ReleaseFast
./run_join_profile
```

**Output**:
```
=== Join Performance Profiling ===

Test 1: Hash Table Build (10K rows)
  Average build time: 0.01ms
  Throughput: 760M rows/sec

Test 2: Hash Map Probe (10K lookups)
  Average probe time: 0.07ms
  Throughput: 136M probes/sec

Test 3: Data Copying (100K rows, 4 columns)
  Row-by-row: 0.24ms
  Column memcpy: 0.05ms
  Speedup: 5.0x faster

Test 4: Full Join Breakdown (10K × 10K)
  Total join time: 0.87ms
  ✓ PASS
```

**Features**:
- Measures hash table build time
- Measures probe performance
- **Compares row-by-row vs column-wise memcpy** (5× speedup)
- Full join with timing breakdown

**Related Docs**: `docs/join_profiling_analysis.md`

---

### 3. `run_benchmark.zig` - General Benchmark Runner (Placeholder)

**Status**: Placeholder for future benchmarks

**Future Features**:
- CSV parsing benchmarks
- Filter operation benchmarks
- Sort operation benchmarks
- GroupBy operation benchmarks
- Aggregate operation benchmarks

---

## Development Guidelines

### Adding New Profiling Tools

1. **Create tool**: `src/profiling_tools/your_tool.zig`
2. **Document**: Add section to this README
3. **Compile**: `zig build-exe src/profiling_tools/your_tool.zig -O ReleaseFast`
4. **Run**: `./your_tool`

### Tool Requirements

**Must Have**:
- Clear output format
- Timing measurements in ms (not ns)
- Comparison to baseline/target
- Pass/fail indication

**Nice to Have**:
- Multiple test sizes
- Throughput metrics (rows/sec)
- Memory usage statistics
- Breakdown by component

### Example Template

```zig
const std = @import("std");
const DataFrame = @import("../core/dataframe.zig").DataFrame;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Your Benchmark Name ===\n\n", .{});

    // Run benchmarks
    try benchmarkOperation(allocator, 1000);
    try benchmarkOperation(allocator, 10_000);
    try benchmarkOperation(allocator, 100_000);

    std.debug.print("\n=== Benchmark Complete ===\n", .{});
}

fn benchmarkOperation(allocator: std.mem.Allocator, row_count: u32) !void {
    // Setup
    var df = try DataFrame.create(allocator, &cols, row_count);
    defer df.deinit();

    // Measure
    const start = std.time.nanoTimestamp();
    // ... operation here
    const end = std.time.nanoTimestamp();

    const duration_ms = @as(f64, @floatFromInt(end - start)) / 1_000_000.0;

    // Report
    std.debug.print("Test {} rows:\n", .{row_count});
    std.debug.print("  Duration: {d:.2}ms\n", .{duration_ms});
}
```

---

## Profiling Methodology

### Best Practices

1. **Use ReleaseFast**: `-O ReleaseFast` for realistic timings
2. **Warm up**: Run once before measurement to warm caches
3. **Multiple iterations**: Average over 5-10 runs for stability
4. **Isolate components**: Test individual operations, not full workflows
5. **Document baseline**: Record "before optimization" timings

### Common Pitfalls

❌ **Don't**:
- Measure in Debug mode (10-100× slower)
- Include setup/teardown in timing
- Use nanoseconds for user-facing output
- Optimize without profiling first

✅ **Do**:
- Use ReleaseFast for benchmarks
- Measure only the operation
- Report in milliseconds
- Profile before optimizing

### Interpreting Results

**Speedup Calculation**:
```
speedup = baseline_time / optimized_time
improvement_pct = (baseline_time - optimized_time) / baseline_time * 100
```

**Example**:
- Baseline: 593ms
- Optimized: 16ms
- Speedup: 593 / 16 = 37× faster
- Improvement: (593 - 16) / 593 * 100 = 97.3% faster

---

## Historical Results

### Join Optimization (2025-10-28)

**Baseline**: 593ms for 100K × 100K join
**Target**: <500ms
**Result**: 16ms (97.3% faster)

**Key Insight**: Column-wise memcpy is 5× faster than row-by-row copying

**Tools Used**:
- `run_join_profile.zig` - Identified bottleneck
- `benchmark_join.zig` - Validated improvement

**See**: `docs/join_optimization_results.md`

---

## Future Profiling Needs

### Planned Tools

1. **SIMD Benchmark** (Phase 1 Days 1-2)
   - Compare scalar vs SIMD sort
   - Compare scalar vs SIMD aggregations
   - Target: sort <5ms, groupby <1ms

2. **String Operations Benchmark** (Phase 2)
   - Case conversion performance
   - Split/join performance
   - Pattern matching performance

3. **CSV Parsing Benchmark** (ongoing)
   - Throughput by file size
   - Memory usage profiling
   - Comparison to Papa Parse

---

## Contributing

When adding profiling tools:

1. **Document results**: Update this README + relevant docs
2. **Clean output**: Use consistent formatting
3. **Validate correctness**: Ensure operations are correct, not just fast
4. **Compare fairly**: Use same hardware, same compiler flags

---

**Last Updated**: 2025-10-28
**Tools**: 2 active, 1 placeholder
**Latest Results**: Join optimization (97.3% faster)
