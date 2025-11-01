# Rozes DataFrame Library - Development TODO

**Version**: 1.3.0 (planned) | **Last Updated**: 2025-11-01

---

## Milestone 1.2.0: Advanced Optimizations ✅ **COMPLETED** (2025-11-01)

**Duration**: 4-6 weeks | **Goal**: 2-10× performance improvements through SIMD, parallelism, and query optimization

### Completion Summary

**Status**: **COMPLETE** - 11/12 benchmarks passed performance targets (92% pass rate)

**What Was Achieved**:

- ✅ Phase 1: SIMD Aggregations (0.04-0.09ms, 2-6B rows/sec, 95-97% faster than targets)
- ✅ Phase 2: Radix Hash Join (1.65× speedup on 100K×100K, bloom filters 97% faster)
- ✅ Phase 3: Parallel CSV Parsing (578ms for 1M rows, 81% faster than 3s target)
- ✅ Phase 4: Parallel DataFrame Operations (13ms filter, 6ms sort, 1.76ms groupby - all exceed targets)
- ✅ Phase 5: Apache Arrow Compatibility (schema mapping complete, IPC API implemented)
- ✅ Phase 6: Lazy Evaluation & Query Optimization (predicate/projection pushdown, 18 tests)

**Performance Results**:

- SIMD: 95-97% faster than targets (billions of rows/sec)
- Parallel CSV: 81% faster than target (1.7M rows/sec)
- Filter: 87% faster (76M rows/sec)
- Sort: 94% faster (16M rows/sec)
- GroupBy: 99% faster (57M rows/sec)
- Radix Join: 1.65× speedup vs standard hash

**Known Issues**:

- Full join pipeline 18% slower than target (588ms vs 500ms) due to CSV parsing overhead
- Pure join algorithm meets target (0.44ms, 96% faster)

### Overview

This milestone focused on performance optimizations leveraging modern CPU features and parallel processing. All optimizations maintain Tiger Style compliance and backward compatibility.

**Performance Targets**:

- SIMD aggregations: 30% speedup for groupBy operations ✅ **EXCEEDED**
- Radix hash join: 2-3× speedup for integer key joins ⚠️ **1.65× achieved**
- Parallel CSV parsing: 2-4× faster type inference ✅ **EXCEEDED**
- Parallel operations: 2-6× speedup on datasets >100K rows ✅ **EXCEEDED**
- Lazy evaluation: 2-10× improvement for chained operations ✅ **IMPLEMENTED**
- Apache Arrow: Zero-copy interop with Arrow ecosystem ✅ **IMPLEMENTED**

---

## Milestone 1.3.0: WebGPU Acceleration + Package Architecture

**Duration**: 5-6 weeks | **Goal**: WebGPU browser acceleration + environment-optimized package exports

### Overview

This milestone adds WebGPU acceleration for browser environments and implements a clean package architecture with multiple entry points (`rozes`, `rozes/web`, `rozes/node`, `rozes/csv`). All optimizations maintain Tiger Style compliance and backward compatibility.

**Key Features**:

- Environment-optimized exports (universal, web, node, csv-only)
- WebGPU acceleration for browser (2-10× speedup on >100K rows)
- Automatic CPU fallback when WebGPU unavailable
- Bundle size optimization (40 KB to 180 KB depending on use case)
- Maintain single npm package with subpath exports

**Performance Targets**:

- WebGPU aggregations: 2-5× speedup on >100K rows
- WebGPU filter: 3-5× speedup on >100K rows
- WebGPU groupBy: 3-6× speedup on >100K rows (stretch goal)
- Bundle sizes: 40 KB (csv) → 120 KB (universal) → 180 KB (web)

---

### Phase 0: Package Architecture (Week 0)

**Goal**: Implement environment-optimized subpath exports

#### Tasks:

1. **Package.json Subpath Exports** (1-2 days)

   - [ ] Add `exports` field with 4 entry points:
     - `"."` → Universal (CSV + DF + CPU SIMD) ~120 KB
     - `"./web"` → Web-optimized (+ WebGPU) ~180 KB
     - `"./node"` → Node.js-optimized (CPU) ~120 KB
     - `"./csv"` → CSV-only ~40 KB
   - [ ] Add conditional exports (import/require/browser/node)
   - [ ] Update TypeScript definitions for all exports
   - [ ] Add `package.json` to exports for tooling

2. **Build System Updates** (2-3 days)

   - [ ] Update `build.zig` for 4 WASM targets:
     - `rozes.wasm` - Universal build
     - `rozes-web.wasm` - Web build (with WebGPU)
     - `rozes-node.wasm` - Node.js build
     - `csv.wasm` - CSV-only build
   - [ ] Create separate entry points:
     - `src/rozes.zig` - Universal entry
     - `src/rozes_web.zig` - Web entry (imports GPU)
     - `src/rozes_node.zig` - Node entry
     - `src/csv_only.zig` - CSV parser only
   - [ ] Configure optimization levels per target

3. **JavaScript Wrappers** (1-2 days)

   - [ ] Create `js/rozes.js` - Universal wrapper
   - [ ] Create `js/web.js` - Web wrapper (GPU detection)
   - [ ] Create `js/node.js` - Node.js wrapper
   - [ ] Create `js/csv.js` - CSV-only wrapper
   - [ ] Add bundle size tests (<40/120/180 KB limits)

4. **Documentation** (1 day)
   - [ ] Create `docs/PACKAGES.md` - Export guide
   - [ ] Document bundle sizes and use cases
   - [ ] Add quick decision guide
   - [ ] Document SSR/isomorphic app patterns
   - [ ] Update README.md with installation options

**Acceptance Criteria**:

- ✅ All 4 exports work correctly in Node.js and browser
- ✅ Bundle sizes meet targets (±10%)
- ✅ TypeScript autocomplete works for all exports
- ✅ Documentation explains when to use each export
- ✅ Backward compatible (existing imports still work)

---

### Phase 1: WebGPU Infrastructure (Week 1)

**Goal**: Set up WebGPU bindings, detection, and fallback mechanisms

#### Tasks:

1. **WebGPU Bindings Integration** (2-3 days)

   - [ ] Add `wgpu_native_zig` to build.zig.zon dependencies
   - [ ] Test compilation for `wasm32-freestanding` target
   - [ ] Verify browser WebGPU API compatibility
   - [ ] Document WebGPU backend selection (Dawn vs wgpu-native)

2. **WebGPU Abstraction Layer** (2-3 days)

   - [ ] Create `src/gpu/webgpu.zig`
   - [ ] Implement device initialization
   - [ ] Add compute pipeline creation helpers
   - [ ] Implement buffer management (CPU ↔ GPU transfer)
   - [ ] Add WGSL shader compilation utilities
   - [ ] Create GPU memory pool for reuse

3. **Feature Detection & Fallback** (1-2 days)

   - [ ] Runtime WebGPU availability check
   - [ ] Automatic fallback to CPU SIMD if unavailable
   - [ ] Dataset size threshold (GPU only if >100K rows)
   - [ ] Export `hasWebGPU()` utility for user detection
   - [ ] Add configuration option to disable GPU

4. **Testing & Validation** (1-2 days)
   - [ ] Unit test: simple compute shader (array addition)
   - [ ] Unit test: CPU↔GPU memory transfer
   - [ ] Unit test: fallback behavior
   - [ ] Integration test: GPU vs CPU results match
   - [ ] Benchmark: GPU transfer overhead

**Acceptance Criteria**:

- ✅ WebGPU device initializes on Chrome 113+, Firefox 141+, Safari 26+
- ✅ Graceful fallback to CPU on non-WebGPU browsers
- ✅ Buffer transfer overhead <5% of compute time
- ✅ Tiger Style: bounded buffer sizes (MAX_GPU_BUFFER_SIZE)
- ✅ Explicit error handling for GPU initialization failures

---

### Phase 2: GPU Aggregations (Week 2)

**Goal**: Implement parallel reduction for sum, mean, min, max

#### Tasks:

1. **WGSL Reduction Shaders** (2-3 days)

   - [ ] Create `src/gpu/shaders/reduction.wgsl`
   - [ ] Implement parallel reduction (sum) with workgroup reduction
   - [ ] Handle non-power-of-2 array sizes
   - [ ] Add horizontal reduction for final result
   - [ ] Create variants for Int32, Int64, Float32, Float64
   - [ ] Implement min/max with vector comparisons

2. **GPU Aggregation API** (2-3 days)

   - [ ] Implement `Series.sum_gpu()` - parallel reduction
   - [ ] Implement `Series.mean_gpu()` - sum + division
   - [ ] Implement `Series.min_gpu()` / `Series.max_gpu()`
   - [ ] Add variance/stddev GPU implementations
   - [ ] Auto-dispatch: GPU if >100K rows, else CPU
   - [ ] Integrate with existing stats.zig API

3. **Testing & Validation** (2-3 days)
   - [ ] Unit tests: GPU results match CPU bit-for-bit (integers)
   - [ ] Unit tests: GPU results match CPU within 1e-6 (floats)
   - [ ] Benchmark: GPU vs CPU SIMD (expect 2-5× on 1M rows)
   - [ ] Test edge cases: empty arrays, single element, NaN
   - [ ] Memory leak tests (1000 iterations)
   - [ ] Cross-browser testing (Chrome, Firefox, Safari)

**Acceptance Criteria**:

- ✅ 2-5× speedup on aggregations for >100K rows
- ✅ Results match CPU implementation (correctness)
- ✅ Overhead <10% on 100K rows (breakeven threshold)
- ✅ Tiger Style: bounded workgroup sizes (MAX_WORKGROUP_SIZE)
- ✅ All functions have 2+ assertions

---

### Phase 3: GPU Filter & Map (Week 3)

**Goal**: Parallel filtering and element-wise transformations

#### Tasks:

1. **Filter Shader** (2-3 days)

   - [ ] Create `src/gpu/shaders/filter.wgsl`
   - [ ] Implement parallel predicate evaluation
   - [ ] Add stream compaction for result array
   - [ ] Handle variable-length output
   - [ ] Support comparison operators (>, <, ==, !=, >=, <=)
   - [ ] Support logical operators (AND, OR, NOT)

2. **Map Shader** (1-2 days)

   - [ ] Create `src/gpu/shaders/map.wgsl`
   - [ ] Implement element-wise transformations
   - [ ] Support arithmetic operations (+, -, \*, /)
   - [ ] Support Int32, Int64, Float32, Float64 types
   - [ ] Add type conversion operations

3. **API Implementation** (2-3 days)

   - [ ] Implement `DataFrame.filter_gpu(predicate)`
   - [ ] Implement `Series.map_gpu(fn)`
   - [ ] Compile simple predicates to WGSL
   - [ ] Handle complex predicates (fall back to CPU)
   - [ ] Auto-dispatch based on dataset size

4. **Testing & Benchmarking** (2-3 days)
   - [ ] Correctness tests: GPU vs CPU implementation
   - [ ] Benchmark: filter 1M rows (expect 3-5× speedup)
   - [ ] Test different selectivity (10%, 50%, 90% pass rate)
   - [ ] Test map operations with various functions
   - [ ] Cross-browser compatibility tests

**Acceptance Criteria**:

- ✅ 3-5× speedup on filter for >100K rows
- ✅ Correctly handles variable-length output arrays
- ✅ Supports simple predicates (comparisons, logical ops)
- ✅ Memory usage <2× input size during operation
- ✅ Tiger Style: bounded predicate complexity

---

### Phase 4: GPU GroupBy (Week 4, Optional/Stretch Goal)

**Goal**: Parallel groupBy aggregations

#### Tasks:

1. **Hash-Based Grouping** (3-4 days)

   - [ ] Create `src/gpu/shaders/groupby.wgsl`
   - [ ] Implement parallel hash computation
   - [ ] Build hash table on GPU
   - [ ] Handle hash collisions (linear probing)
   - [ ] Parallel aggregation per group

2. **API Implementation** (2-3 days)

   - [ ] Implement `DataFrame.groupBy_gpu(column).sum()`
   - [ ] Support integer keys only (MVP)
   - [ ] Add mean, min, max, count aggregations
   - [ ] Limit to 100K unique groups (MAX_GROUPS)

3. **Testing & Benchmarking** (1-2 days)
   - [ ] Correctness: GPU vs CPU groupBy results
   - [ ] Benchmark: 1M rows, 1K groups (expect 3-6×)
   - [ ] Test with different cardinalities (10, 100, 1K, 10K groups)
   - [ ] Test hash collision handling

**Acceptance Criteria**:

- ✅ 3-6× speedup on groupBy for >100K rows
- ✅ Supports integer keys (defer string keys to future)
- ✅ Handles up to 100K unique groups
- ✅ Correct handling of hash collisions
- ✅ Tiger Style: bounded group count

**Note**: This phase is optional. If Phases 1-3 take longer than expected, defer to Milestone 1.4.0.

---

### Phase 5: Performance Tuning & Documentation (Week 5)

**Goal**: Optimize performance and document usage

#### Tasks:

1. **Performance Optimization** (2-3 days)

   - [ ] Tune workgroup sizes (test 64, 128, 256, 512)
   - [ ] Minimize CPU↔GPU transfer overhead
   - [ ] Implement shader compilation caching
   - [ ] Add adaptive thresholds for GPU dispatch
   - [ ] Profile and optimize hot paths
   - [ ] Benchmark against CPU baseline

2. **Documentation** (2-3 days)

   - [ ] Create `docs/WEBGPU.md` - WebGPU guide
     - Architecture overview
     - Browser compatibility matrix
     - How to enable/disable GPU
     - Performance characteristics
     - Troubleshooting guide
   - [ ] Update `docs/PACKAGES.md` with GPU info
   - [ ] Add WebGPU examples to README.md
   - [ ] Document performance benchmarks

3. **JavaScript Integration** (1-2 days)
   - [ ] Add GPU-specific examples to `js/rozes.js`
   - [ ] Implement browser detection helper
   - [ ] Create performance comparison demo
   - [ ] Add TypeScript definitions for GPU APIs
   - [ ] Update interactive browser tests

**Acceptance Criteria**:

- ✅ Performance targets met (see Success Metrics)
- ✅ Documentation explains when GPU provides benefit
- ✅ Examples demonstrate GPU usage patterns
- ✅ Browser compatibility clearly documented
- ✅ Troubleshooting guide covers common issues

---

### Cross-Phase Requirements

#### Browser Compatibility:

- [ ] Chrome 113+ (primary target, stable since April 2023)
- [ ] Firefox 141+ (secondary, stable since July 2025)
- [ ] Safari 26+ (tertiary, stable since June 2025)
- [ ] Graceful degradation on older browsers (CPU fallback)
- [ ] Test on mobile browsers (iOS Safari, Chrome Android)

#### Performance Targets:

- [ ] Aggregations: 2-5× speedup on 1M rows (GPU vs CPU SIMD)
- [ ] Filter: 3-5× speedup on 1M rows
- [ ] Map: 2-4× speedup on 1M rows
- [ ] GroupBy: 3-6× speedup on 1M rows (if implemented)
- [ ] Breakeven: <10% overhead on 100K rows
- [ ] GPU transfer: <5% of total compute time

#### Bundle Size Targets:

- [ ] `rozes/csv`: ≤50 KB (target 40 KB)
- [ ] `rozes` (universal): ≤130 KB (target 120 KB)
- [ ] `rozes/node`: ≤130 KB (target 120 KB)
- [ ] `rozes/web`: ≤190 KB (target 180 KB)
- [ ] Verify with bundle analyzer in CI

#### Quality Assurance:

- [ ] All GPU functions have CPU fallback
- [ ] 100% correctness vs CPU implementation
- [ ] Memory leak tests (1000 iterations, GPU on/off)
- [ ] Tiger Style compliance (assertions, bounded loops)
- [ ] Cross-browser integration tests

#### Integration:

- [ ] Node.js native addon stays CPU-only
- [ ] Browser WASM gets WebGPU acceleration
- [ ] User can disable GPU: `{useGPU: false}` option
- [ ] SSR/isomorphic apps work correctly
- [ ] All exports tested in real projects

---

### Risks & Mitigations

**Risk 1**: WebGPU browser support is incomplete

- **Mitigation**: Mandatory CPU fallback, test on all major browsers

**Risk 2**: GPU overhead negates benefits on datasets <1M rows

- **Mitigation**: Adaptive thresholds, extensive benchmarking

**Risk 3**: WebGPU API changes break compatibility

- **Mitigation**: Pin to specific wgpu-native version, version detection

**Risk 4**: Shader compilation complexity

- **Mitigation**: Start with simple shaders, defer complex ops to CPU

**Risk 5**: Mobile browser GPU support is poor

- **Mitigation**: Desktop-first strategy, CPU fallback for mobile

**Risk 6**: Package architecture breaks existing users

- **Mitigation**: Main export stays default, backward compatible

---

### Success Metrics

**Performance**:

- ✅ GPU: 2-10× speedup on operations >100K rows
- ✅ Breakeven: <10% overhead on 100K rows
- ✅ CPU fallback: 0% performance degradation vs CPU-only

**Bundle Sizes**:

- ✅ CSV-only: 40 KB (3× smaller than full)
- ✅ Universal: 120 KB (no GPU bloat)
- ✅ Web: 180 KB (60 KB GPU overhead acceptable)

**Quality**:

- ✅ 100% correctness vs CPU implementation
- ✅ No memory leaks (1000-iteration tests)
- ✅ 100% Tiger Style compliance
- ✅ 100% test coverage for GPU code paths

**Compatibility**:

- ✅ Works on Chrome 113+, Firefox 141+, Safari 26+
- ✅ Graceful fallback on older browsers
- ✅ Mobile browsers work (CPU fallback)
- ✅ SSR/isomorphic apps work correctly

**Developer Experience**:

- ✅ Clear documentation for all exports
- ✅ TypeScript autocomplete works
- ✅ Easy to choose right export for use case
- ✅ Migration guide for existing users

---

**Estimated Completion**: 5-6 weeks from start
**Dependencies**: Milestone 1.2.0 (SIMD infrastructure) recommended but not required

---

## Milestone 1.4.0: Node.js API Completion

**Duration**: 2-3 weeks | **Goal**: Expose all Zig DataFrame operations to Node.js/TypeScript API

### Overview

This milestone focuses on completing the Node.js/TypeScript bindings to expose the full Zig DataFrame API that's already implemented (50+ operations). Currently, many advanced operations are available in Zig but not exposed to JavaScript users.

**Key Features**:

- CSV export (`toCSV()`, `toCSVFile()`)
- Advanced DataFrame operations (`filter()`, `groupBy()`, `join()`)
- Lazy evaluation API (expose LazyDataFrame to Node.js)
- Multi-column sort
- All remaining Zig operations exposed to JavaScript

### Phase 1: CSV Export Bindings (Week 1)

**Goal**: Expose CSV export functionality to Node.js/TypeScript

#### Tasks:

1. **WASM Export Bindings** (2-3 days)
   - [ ] Implement `dataframe_to_csv()` WASM binding
   - [ ] Implement `dataframe_to_csv_file()` WASM binding (Node.js only)
   - [ ] Handle CSV export options (delimiter, quote char, header, etc.)
   - [ ] Memory management for output buffer
   - [ ] Error handling for I/O operations

2. **JavaScript Wrapper** (1-2 days)
   - [ ] Add `DataFrame.toCSV(options?)` method
   - [ ] Add `DataFrame.toCSVFile(path, options?)` method (Node.js only)
   - [ ] TypeScript definitions for export options
   - [ ] Update API documentation

3. **Testing** (1-2 days)
   - [ ] Unit tests: round-trip (parse → export → parse)
   - [ ] Test all export options (delimiters, quotes, headers)
   - [ ] Test large datasets (1M rows)
   - [ ] Cross-platform file I/O tests (Node.js)

**Acceptance Criteria**:
- ✅ Round-trip correctness (parse → export → parse produces identical data)
- ✅ All CSV export options work correctly
- ✅ Performance: Export 1M rows in <500ms
- ✅ Tiger Style compliant

---

### Phase 2: Filter, GroupBy, Join Bindings (Week 2)

**Goal**: Expose advanced DataFrame operations to Node.js/TypeScript

#### Tasks:

1. **Filter Operation** (2-3 days)
   - [ ] Design JavaScript predicate callback interface
   - [ ] Implement WASM binding for filter with JS callbacks
   - [ ] Handle memory management for filtered results
   - [ ] Add TypeScript definitions
   - [ ] Test with various predicates (numeric, string, boolean)

2. **GroupBy Aggregations** (2-3 days)
   - [ ] Implement `DataFrame.groupBy(column)` binding
   - [ ] Expose aggregation methods: `sum()`, `mean()`, `min()`, `max()`, `count()`
   - [ ] Handle multiple aggregations on same grouping
   - [ ] Add TypeScript definitions for GroupBy result
   - [ ] Test with different cardinalities

3. **Join Operations** (2-3 days)
   - [ ] Implement `DataFrame.join(other, leftKey, rightKey, type)` binding
   - [ ] Support all join types: inner, left, right, outer, cross
   - [ ] Handle memory for joined results
   - [ ] Add TypeScript definitions
   - [ ] Test all join types with various datasets

4. **Testing & Documentation** (1-2 days)
   - [ ] Integration tests: filter → groupBy → join chains
   - [ ] Performance benchmarks vs pure Zig API
   - [ ] Update API documentation
   - [ ] Add examples to README

**Acceptance Criteria**:
- ✅ All operations work correctly (match Zig API behavior)
- ✅ Memory safe (no leaks in 1000-iteration tests)
- ✅ Performance overhead <10% vs pure Zig
- ✅ TypeScript autocomplete works

---

### Phase 3: Lazy Evaluation & Multi-Column Sort (Week 3)

**Goal**: Expose remaining advanced features

#### Tasks:

1. **Lazy Evaluation Bindings** (2-3 days)
   - [ ] Implement `DataFrame.lazy()` to create LazyDataFrame
   - [ ] Expose lazy operations: `select()`, `filter()`, `groupBy()`, `join()`
   - [ ] Implement `.collect()` to execute query plan
   - [ ] Add TypeScript definitions for LazyDataFrame
   - [ ] Test query optimization (predicate/projection pushdown)

2. **Multi-Column Sort** (1-2 days)
   - [ ] Update `DataFrame.sort()` to accept array of columns
   - [ ] Support per-column sort order (ascending/descending)
   - [ ] Add TypeScript definitions
   - [ ] Test with 2-5 column sorts

3. **Additional Operations** (2-3 days)
   - [ ] Expose `fillna()`, `dropna()`, `isNull()`
   - [ ] Expose window operations (`rolling()`, `expanding()`)
   - [ ] Expose string operations (`strUpper()`, `strLower()`, etc.)
   - [ ] Expose reshape operations (`pivot()`, `melt()`, `transpose()`)
   - [ ] Add TypeScript definitions for all

4. **Testing & Documentation** (1-2 days)
   - [ ] Comprehensive integration tests
   - [ ] Update `docs/NODEJS_API.md` with all new operations
   - [ ] Update README.md feature matrix
   - [ ] Add migration examples from pandas/Polars

**Acceptance Criteria**:
- ✅ LazyDataFrame API fully functional (2-10× speedup for chained ops)
- ✅ Multi-column sort works correctly
- ✅ All major Zig operations exposed to JavaScript
- ✅ Documentation complete and accurate
- ✅ Example code for all new features

---

### Success Metrics

**API Completeness**:
- ✅ 100% of core Zig operations exposed to Node.js/TypeScript
- ✅ Feature parity between Zig and JavaScript APIs

**Performance**:
- ✅ JavaScript API overhead <10% vs pure Zig
- ✅ Lazy evaluation: 2-10× speedup for chained operations

**Quality**:
- ✅ No memory leaks (1000-iteration tests)
- ✅ 100% Tiger Style compliance
- ✅ 100% test coverage for new bindings
- ✅ TypeScript definitions accurate and complete

**Developer Experience**:
- ✅ Clear documentation for all new operations
- ✅ Migration guide from pandas/Polars updated
- ✅ Examples demonstrate real-world usage
- ✅ README.md "Known Limitations" section removed

---

**Estimated Completion**: 2-3 weeks from start
**Dependencies**: Milestone 1.3.0 (package architecture) optional

---

## Code Quality Standards

**Tiger Style Compliance** (MANDATORY):

- ✅ 2+ assertions per function
- ✅ Bounded loops with explicit MAX constants
- ✅ Functions ≤70 lines
- ✅ Explicit types (u32, not usize)
- ✅ Explicit error handling (no silent failures)

**Testing Requirements**:

- ✅ Unit tests for every public function
- ✅ Error case tests (bounds, invalid input)
- ✅ Memory leak tests (1000 iterations)
- ✅ Integration tests (end-to-end workflows)

---

**Last Updated**: 2025-11-01
**Next Review**: When Milestone 1.2.0 Phase 2 begins
