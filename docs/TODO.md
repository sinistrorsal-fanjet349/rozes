# Rozes DataFrame Library - Development TODO

**Version**: 1.3.0 (planned) | **Last Updated**: 2025-11-01

---

## Milestone 1.2.0: Advanced Optimizations

**Duration**: 4-6 weeks | **Goal**: 2-10× performance improvements through SIMD, parallelism, and query optimization

### Overview

This milestone focuses on performance optimizations that leverage modern CPU features and parallel processing. All optimizations must maintain Tiger Style compliance and backward compatibility.

**Performance Targets**:

- SIMD aggregations: 30% speedup for groupBy operations
- Radix hash join: 2-3× speedup for integer key joins
- Parallel CSV parsing: 2-4× faster type inference
- Parallel operations: 2-6× speedup on datasets >100K rows
- Lazy evaluation: 2-10× improvement for chained operations
- Apache Arrow: Zero-copy interop with Arrow ecosystem

---

### Phase 1: SIMD Aggregations ✅ **COMPLETED** (2025-11-01)

**Goal**: Vectorize common aggregation operations using SIMD intrinsics

#### Tasks:

1. **SIMD Infrastructure** (3-4 days) ✅ **COMPLETE**

   - [x] Create `src/core/simd.zig` with SIMD abstractions
   - [x] Implement SIMD detection and fallback mechanisms
   - [x] Add compile-time feature flags for SIMD support
   - [x] Write unit tests for SIMD vector operations (40 tests)

2. **Vectorized Aggregations** (4-5 days) ✅ **COMPLETE**

   - [x] Implement SIMD sum() for Int64, Float64
   - [x] Implement SIMD mean() with horizontal reduction
   - [x] Implement SIMD min()/max() with vector comparisons
   - [x] Add SIMD variance() and stddev() calculations
   - [x] Integrate with stats.zig (variance, mean)

3. **Testing & Validation** (2-3 days) ✅ **COMPLETE**

   - [x] Unit tests for all SIMD operations (40 tests - Int64/Float64)
   - [x] Benchmark SIMD aggregations (100K rows: 0.1ms/op)
   - [x] Test edge cases (empty, single element, odd lengths)
   - [x] Verify numerical accuracy (floating-point precision)

4. **Node.js API Integration** ✅ **COMPLETE**
   - [x] Export 6 SIMD aggregations via wasm.zig
   - [x] Add JavaScript wrappers in js/rozes.js
   - [x] Update TypeScript definitions in dist/index.d.ts
   - [x] Write Node.js integration tests (24+ tests passing)
   - [x] Create SIMD performance benchmark suite

**Acceptance Criteria**:

- ✅ Excellent performance: 100K rows in 0.1ms/op (10K ops/sec)
- ✅ Graceful fallback to scalar on unsupported CPUs
- ✅ All SIMD functions have 2+ assertions (Tiger Style compliant)
- ✅ No numerical accuracy loss vs scalar operations
- ✅ 100% test coverage for SIMD paths (40 unit tests + 24 integration tests)

---

### Phase 2: Radix Hash Join (Week 2-3) 🚧 **IN PROGRESS**

**Goal**: Optimize integer key joins using radix partitioning

#### Tasks:

1. **Radix Partitioning** (3-4 days) ✅ **COMPLETE** (2025-11-01)

   - [x] Create `src/core/radix_join.zig`
   - [x] Implement multi-pass radix partitioning (8-bit radix)
   - [x] Build partition histograms with prefix sum
   - [x] Add cache-friendly partition scatter

2. **Hash Join Optimization** (3-4 days) ✅ **COMPLETE** (2025-11-01)

   - [x] Detect integer key columns for radix optimization (2025-11-01)
   - [x] Implement radix-based hash table construction
   - [x] Optimize probe phase with SIMD comparisons (2025-11-01)
   - [x] Add bloom filters for early rejection (2025-11-01)

3. **Testing & Benchmarking** (2-3 days) ⏸️ **PARTIAL** (1/4 complete)
   - [x] Unit tests for radix partitioning logic (40+ tests)
   - [x] Benchmark vs standard hash join (expect 2-3× speedup)
   - [x] Test with skewed distributions (zipf, uniform)
   - [] Verify correctness with 1M+ row joins

**Acceptance Criteria**:

- ✅ 2-3× speedup for integer key joins (vs standard hash)
- ✅ Automatic fallback to standard hash for non-integer keys
- ✅ Memory usage <2× input size during join
- ✅ Tiger Style: bounded loops, explicit error handling
- ✅ 100% correctness on skewed and uniform distributions

---

### Phase 3: Parallel CSV Type Inference (Week 3-4) ✅ **COMPLETED** (2025-11-01)

**Goal**: Multi-threaded CSV type detection and parsing

#### Tasks:

1. **Parallel Architecture** (2-3 days) ✅ **COMPLETE**

   - [x] Create `src/csv/parallel_parser.zig`
   - [x] Implement work-stealing thread pool (max 8 threads)
   - [x] Add chunking strategy (64KB-1MB adaptive chunks)
   - [x] Handle chunk boundaries (quote/escape spanning)

2. **Parallel Type Inference** (3-4 days) ✅ **COMPLETE**

   - [x] Parallelize type detection across chunks
   - [x] Merge type inference results with conflict resolution
   - [x] Optimize memory layout for parallel parsing
   - [x] Add adaptive chunking based on CPU count

3. **Testing & Validation** (2-3 days) ✅ **COMPLETE**
   - [x] Unit tests for chunk boundary handling (14 tests in parallel_parser_test.zig)
   - [x] Benchmark on 1M, 10M, 100M row CSVs (parallel_csv_bench.zig)
   - [x] Test with mixed types across chunks
   - [x] Verify thread safety and memory safety

**Acceptance Criteria**:

- ✅ 2-4× speedup on CSV parsing (4+ cores) - **Implemented**
- ✅ Correct handling of quotes/escapes at chunk boundaries - **Verified**
- ✅ Type inference matches single-threaded results - **Tested**
- ✅ No race conditions or data races - **Thread-safe design**
- ✅ Graceful degradation on single-core systems - **Auto fallback**

**Implementation Details**:

- **Architecture**: Multi-threaded work-stealing pool with bounded workers (MAX_THREADS=8)
- **Chunking**: Adaptive 64KB-1MB chunks based on file size and CPU count
- **Boundary Handling**: Quote-aware boundary detection with backward/forward scan
- **Type Merging**: Conflict resolution (String > Float64 > Int64 > Bool)
- **Thread Safety**: Each thread processes independent chunk range with isolated results
- **Fallback**: Automatic single-threaded mode for small files (<128KB)
- **Tiger Style**: All functions ≤70 lines, 2+ assertions, bounded loops
- **Tests**: 14 unit tests + benchmark suite (1K-1M rows)

**Performance Notes**:

- Small files (<128KB) use sequential parser (overhead not worth it)
- Large files (>128KB) spawn up to 8 worker threads
- Each thread processes ~4 chunks for load balancing
- Chunk boundaries found via quote-aware newline detection
- Type inference runs in parallel, results merged with confidence weighting

---

### Phase 4: Parallel DataFrame Operations (Week 4-5)

**Goal**: Multi-threaded filter, map, and aggregation operations

#### Tasks:

1. **Parallel Execution Engine** (3-4 days)

   - [ ] Create `src/core/parallel_ops.zig`
   - [ ] Implement parallel filter() with row-level partitioning
   - [ ] Add parallel map() for transformations
   - [ ] Optimize memory allocation for parallel results

2. **Parallel Aggregations** (2-3 days)

   - [ ] Parallelize groupBy with hash table partitioning
   - [ ] Add parallel sort (merge sort or quicksort)
   - [ ] Implement parallel join (partition-based)

3. **Testing & Benchmarking** (2-3 days)
   - [ ] Unit tests for parallel correctness
   - [ ] Benchmark on 100K, 1M, 10M rows
   - [ ] Test with different thread counts (1, 2, 4, 8)
   - [ ] Verify no memory leaks under parallel execution

**Acceptance Criteria**:

- ✅ 2-6× speedup on datasets >100K rows (4+ cores)
- ✅ Results identical to single-threaded execution
- ✅ Thread pool overhead <5% on small datasets
- ✅ Memory usage <3× single-threaded version
- ✅ Tiger Style: bounded thread counts, explicit limits

---

### Phase 5: Apache Arrow Compatibility (Week 5)

**Goal**: Zero-copy interop with Apache Arrow format

#### Tasks:

1. **Arrow Schema Mapping** (2-3 days)

   - [ ] Create `src/arrow/schema.zig`
   - [ ] Map Rozes types to Arrow types
   - [ ] Implement Arrow IPC format reader
   - [ ] Add Arrow schema validation

2. **Zero-Copy Conversion** (2-3 days)

   - [ ] Implement DataFrame.toArrow() (zero-copy where possible)
   - [ ] Implement DataFrame.fromArrow() (zero-copy where possible)
   - [ ] Handle Arrow dictionary encoding
   - [ ] Add Arrow chunked array support

3. **Testing & Validation** (2 days)
   - [ ] Unit tests for Arrow schema conversion
   - [ ] Test with PyArrow and Arrow JS
   - [ ] Verify zero-copy with memory profiling
   - [ ] Benchmark conversion overhead

**Acceptance Criteria**:

- ✅ Zero-copy conversion for numeric types
- ✅ Compatible with PyArrow 10.0+ and Arrow JS 10.0+
- ✅ Round-trip conversion preserves data and types
- ✅ Conversion overhead <10% of data size
- ✅ Documentation with PyArrow/Arrow JS examples

---

### Phase 6: Lazy Evaluation & Query Optimization (Week 6)

**Goal**: Defer execution and optimize query plans for chained operations

#### Tasks:

1. **Query Plan Representation** (2-3 days)

   - [ ] Create `src/core/query_plan.zig`
   - [ ] Represent operations as DAG (filter, select, groupBy)
   - [ ] Implement query plan builder
   - [ ] Add plan visualization for debugging

2. **Query Optimization** (3-4 days)

   - [ ] Implement predicate pushdown (filter early)
   - [ ] Add projection pushdown (select early)
   - [ ] Fuse consecutive filters into single pass
   - [ ] Optimize filter + groupBy → groupBy with filter

3. **Lazy Execution** (2-3 days)

   - [ ] Defer execution until .execute() or .collect()
   - [ ] Add streaming execution for large results
   - [ ] Implement result caching for repeated queries

4. **Testing & Benchmarking** (2 days)
   - [ ] Unit tests for query plan optimization
   - [ ] Benchmark chained operations (expect 2-10× speedup)
   - [ ] Test with complex queries (5+ operations)
   - [ ] Verify correctness matches eager evaluation

**Acceptance Criteria**:

- ✅ 2-10× speedup for chained operations (3+ ops)
- ✅ Predicate pushdown reduces rows scanned by 50%+
- ✅ Projection pushdown reduces memory by 30%+
- ✅ Query plan optimization is deterministic
- ✅ Tiger Style: bounded plan depth, explicit limits

---

### Cross-Phase Requirements

#### Documentation:

- [ ] Performance guide (`docs/PERFORMANCE.md`)
- [ ] SIMD optimization guide
- [ ] Parallel execution tuning guide
- [ ] Arrow interop examples
- [ ] Query optimization cookbook

#### Testing:

- [ ] All optimizations have unit tests
- [ ] Benchmark suite for each optimization
- [ ] Regression tests (verify no slowdowns)
- [ ] Memory profiling (verify no leaks)
- [ ] Cross-platform tests (x86, ARM, WASM)

#### Tiger Style Compliance:

- [ ] All functions ≤70 lines
- [ ] 2+ assertions per function
- [ ] Bounded loops with explicit MAX
- [ ] Explicit error handling (no silent failures)
- [ ] u32 for row/column indices (not usize)

---

### Risks & Mitigations

**Risk 1**: SIMD may not work on all platforms

- **Mitigation**: Feature detection + scalar fallback

**Risk 2**: Parallel overhead on small datasets

- **Mitigation**: Adaptive threshold (parallelize only if >100K rows)

**Risk 3**: Arrow compatibility breaks in future versions

- **Mitigation**: Pin to Arrow 10.0 spec, add version detection

**Risk 4**: Query optimization changes semantics

- **Mitigation**: Extensive correctness tests vs eager evaluation

---

### Success Metrics

**Performance**:

- ✅ SIMD: 30%+ speedup on aggregations
- ✅ Radix join: 2-3× speedup vs standard hash
- ✅ Parallel CSV: 2-4× speedup on 4+ cores
- ✅ Parallel ops: 2-6× speedup on large datasets
- ✅ Lazy eval: 2-10× speedup on chained ops

**Quality**:

- ✅ 100% Tiger Style compliance
- ✅ 100% test coverage for new code
- ✅ No memory leaks (1000-iteration tests)
- ✅ No race conditions (ThreadSanitizer clean)

**Compatibility**:

- ✅ Arrow interop with PyArrow and Arrow JS
- ✅ Backward compatible API (no breaking changes)
- ✅ Works on x86, ARM, and WASM (with fallbacks)

---

**Estimated Completion**: 6 weeks from start
**Dependencies**: Milestone 1.1.0 must be complete

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
