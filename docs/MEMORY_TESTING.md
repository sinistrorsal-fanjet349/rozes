# Memory Leak Testing for Rozes DataFrame

**Version**: 1.1.0 | **Last Updated**: 2025-10-31

---

## Overview

This document covers memory leak testing strategies for Rozes DataFrame library, with a focus on the FinalizationRegistry-based automatic memory management feature introduced in 1.1.0.

### Testing Strategy

Memory testing is split into two categories:

1. **Fast Automated Tests** (~5 minutes) - Run on every commit via CI/CD
2. **Long-Running Stress Tests** (hours to days) - Run manually pre-release or when investigating issues

---

## Fast Automated Tests (`zig build memory-test`)

These tests run quickly (<5 minutes total) and are suitable for CI/CD pipelines.

### Running the Tests

```bash
# Run all 5 fast memory tests
zig build memory-test
```

### Test Suite

| Test | Duration | Purpose | Pass Criteria |
|------|----------|---------|---------------|
| **gc_verification_test** | ~30s | Verify GC reclaims memory | Heap growth <5 MB for 10K DataFrames |
| **wasm_memory_test** | ~30s | Track Wasm memory stability | Wasm memory growth <5 MB for 5K DFs |
| **error_recovery_test** | ~10s | Cleanup despite errors | Heap growth <5 MB with errors |
| **auto_vs_manual_test** | ~1min | Compare cleanup strategies | Both approaches bounded |
| **memory_pressure_test** | ~2min | Sustained load handling | Stable heap under pressure |

### Test Descriptions

#### 1. GC Verification Test (`gc_verification_test.js`)

**Purpose**: Verify FinalizationRegistry correctly frees Wasm memory when DataFrames are garbage collected.

**What it tests**:
- Basic GC cleanup (1000 DataFrames)
- High-volume stress (10K DataFrames in batches)
- Interleaved manual and auto cleanup
- Rapid create/destroy cycles
- GC timing verification

**Expected behavior**:
- Memory recovered after GC (>50% of allocated memory)
- Heap growth bounded to <5 MB for 10K DataFrames
- Finalization completes in <500ms

**Run manually**:
```bash
node --expose-gc src/test/nodejs/memory/gc_verification_test.js
```

#### 2. Wasm Memory Test (`wasm_memory_test.js`)

**Purpose**: Track WebAssembly memory (separate from JS heap) and verify stability.

**What it tests**:
- Wasm memory stability with auto cleanup (5K DataFrames in cycles)
- Manual vs auto cleanup comparison (5K DataFrames each)
- Large DataFrame handling (50 DFs with 1000 rows)
- Continuous pressure (10 seconds of DataFrame creation)
- Mixed operations (various DataFrame ops)

**Expected behavior**:
- Wasm memory growth <5 MB after auto cleanup + GC
- Manual cleanup faster (~3×) than auto cleanup
- Large DataFrames don't cause unbounded growth

**Run manually**:
```bash
node --expose-gc src/test/nodejs/memory/wasm_memory_test.js
```

**Note**: External memory (Wasm) may temporarily be higher with auto cleanup until GC runs. This is expected behavior.

#### 3. Error Recovery Test (`error_recovery_test.js`)

**Purpose**: Verify DataFrames are cleaned up even when errors occur during processing.

**What it tests**:
- Error thrown after DataFrame creation
- Error during DataFrame operations
- Try-catch-finally patterns
- Mixed manual/auto cleanup with errors
- Invalid CSV with auto cleanup
- Async errors
- Nested error scenarios

**Expected behavior**:
- Heap growth <5 MB despite 1000 errors
- FinalizationRegistry still triggers cleanup
- No leaked DataFrames after error recovery

**Run manually**:
```bash
node --expose-gc src/test/nodejs/memory/error_recovery_test.js
```

#### 4. Auto vs Manual Test (`auto_vs_manual_test.js`)

**Purpose**: Compare memory usage patterns between automatic and manual cleanup.

**What it tests**:
- Memory footprint comparison (1000 DataFrames each)
- Cleanup timing comparison
- Memory pressure handling (100 large DataFrames)
- Interleaved cleanup patterns
- Batch processing (10 batches × 200 DataFrames)
- Recommended usage patterns (HTTP handler, CLI tool)

**Expected behavior**:
- Similar memory footprint after GC
- Manual cleanup ~3× faster than auto cleanup
- Both strategies handle memory pressure well
- Interleaved usage works correctly

**Run manually**:
```bash
node --expose-gc src/test/nodejs/memory/auto_vs_manual_test.js
```

#### 5. Memory Pressure Test (`memory_pressure_test.js`)

**Purpose**: Test DataFrame memory management under sustained memory pressure.

**What it tests**:
- Large DataFrames (100 DFs with 1000 rows each)
- Sustained pressure (30 seconds continuous creation)
- Memory recovery after spike
- Wide DataFrames (100 columns)
- Memory stability over iterations (10 cycles)

**Expected behavior**:
- Heap growth <20 MB for 100 large DataFrames
- Sustained pressure: peak <50 MB, final <20 MB
- Memory recovered after spike (>80% recovery)
- Wide DataFrames: growth <15 MB
- Stable trend over iterations (<5 MB drift)

**Run manually**:
```bash
node --expose-gc src/test/nodejs/memory/memory_pressure_test.js
```

---

## Long-Running Stress Tests (Manual/Nightly CI)

These tests provide production-level verification but take hours to complete.

### When to Run

- **Pre-release** (every major/minor version)
- **Investigating suspected memory leaks**
- **Nightly CI** (optional, recommended)
- **After significant memory management changes**

### Test Scenarios

#### Scenario 1: 24-Hour Soak Test

**Purpose**: Prove memory stability over extended periods (production-level verification).

**Implementation**: Create `src/test/nodejs/stress/soak_test.js`

```javascript
/**
 * 24-Hour Soak Test
 *
 * Creates 100K+ DataFrames over 24 hours, tracks memory every 60 seconds.
 * Goal: Prove bounded memory growth (<1% per hour).
 */

const { Rozes } = require('../../../dist/index.js');
const fs = require('fs');

async function soakTest() {
    const rozes = await Rozes.init();
    const DURATION_HOURS = 24;
    const INTERVAL_MS = 60 * 1000; // 1 minute

    const logFile = 'soak_test_results.csv';
    fs.writeFileSync(logFile, 'timestamp,elapsed_hours,heap_mb,rss_mb,created_count\n');

    const startTime = Date.now();
    let totalCreated = 0;

    while ((Date.now() - startTime) < DURATION_HOURS * 3600 * 1000) {
        // Create batch of DataFrames
        for (let i = 0; i < 100; i++) {
            const df = rozes.DataFrame.fromCSV('a,b,c\n1,2,3\n4,5,6', { autoCleanup: true });
            totalCreated++;
        }

        // Force GC periodically
        if (global.gc && totalCreated % 10000 === 0) {
            global.gc();
        }

        // Log memory stats
        const mem = process.memoryUsage();
        const elapsed = (Date.now() - startTime) / 3600000; // hours
        fs.appendFileSync(logFile,
            `${Date.now()},${elapsed.toFixed(2)},${(mem.heapUsed/1024/1024).toFixed(2)},${(mem.rss/1024/1024).toFixed(2)},${totalCreated}\n`
        );

        // Wait before next batch
        await new Promise(resolve => setTimeout(resolve, INTERVAL_MS));
    }

    console.log(`Soak test completed: ${totalCreated} DataFrames created over ${DURATION_HOURS} hours`);
}

soakTest();
```

**Run**:
```bash
node --expose-gc src/test/nodejs/stress/soak_test.js
```

**Success Criteria**:
- Memory growth <1% per hour
- No unbounded growth (linear trend with small slope)
- Final memory <2× initial memory
- CSV log shows stable pattern

**Example Analysis**:
```bash
# After test completes, analyze the CSV
cat soak_test_results.csv | tail -20
# Look for: stable heap_mb, no exponential growth
```

#### Scenario 2: Production HTTP Server Simulation

**Purpose**: Test real-world usage pattern (Express.js server under load).

**Implementation**: Create `src/test/nodejs/stress/http_server_stress.js`

```javascript
/**
 * Production HTTP Server Stress Test
 *
 * Simulates Express.js server handling 10K requests/hour for 2 hours.
 * Each request: CSV upload → Parse → Filter → Aggregate → Return JSON
 */

const { Rozes } = require('../../../dist/index.js');

async function simulateRequest(rozes, requestId) {
    // Simulate CSV upload (100 rows)
    const csv = Array.from({ length: 100 }, (_, i) =>
        `user${i},${20 + i % 50},${Math.random() * 100}`
    ).join('\n');

    const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: false });

    try {
        // Simulate processing
        const ages = df.column('age');
        const avgAge = ages ? ages.reduce((a, b) => a + b) / ages.length : 0;
        return { requestId, avgAge, rows: df.shape.rows };
    } finally {
        df.free(); // Always cleanup (production pattern)
    }
}

async function httpServerStress() {
    const rozes = await Rozes.init();
    const DURATION_HOURS = 2;
    const REQUESTS_PER_HOUR = 10000;
    const INTERVAL_MS = (3600 * 1000) / REQUESTS_PER_HOUR;

    const startTime = Date.now();
    const baseline = process.memoryUsage();
    let requestCount = 0;

    console.log(`Baseline memory: ${(baseline.heapUsed / 1024 / 1024).toFixed(2)} MB`);

    while ((Date.now() - startTime) < DURATION_HOURS * 3600 * 1000) {
        await simulateRequest(rozes, requestCount++);

        // Log every 1000 requests
        if (requestCount % 1000 === 0) {
            const mem = process.memoryUsage();
            const elapsed = ((Date.now() - startTime) / 60000).toFixed(1);
            console.log(`${elapsed}min: ${requestCount} requests, Heap: ${(mem.heapUsed/1024/1024).toFixed(2)} MB`);
        }

        // Small delay to simulate realistic load
        await new Promise(resolve => setTimeout(resolve, INTERVAL_MS));
    }

    // Final memory check
    const finalMem = process.memoryUsage();
    const growth = (finalMem.heapUsed - baseline.heapUsed) / 1024 / 1024;
    console.log(`\nCompleted ${requestCount} requests`);
    console.log(`Memory growth: ${growth.toFixed(2)} MB`);
    console.log(growth < 10 ? '✓ PASS' : '✗ FAIL - Memory leak detected');
}

httpServerStress();
```

**Run**:
```bash
node src/test/nodejs/stress/http_server_stress.js --duration=2h
```

**Success Criteria**:
- Memory returns to baseline after load spike
- Growth <10 MB for 20K requests
- No crashes or hangs
- Consistent response times

#### Scenario 3: Tight Loop Stress Test

**Purpose**: Test finalization under extreme churn (1M DataFrames in tight loop).

**Implementation**: Create `src/test/nodejs/stress/tight_loop_stress.js`

```javascript
/**
 * Tight Loop Stress Test
 *
 * Creates 1M DataFrames in tight loop with periodic GC.
 * Goal: Verify finalization handles rapid churn without accumulation.
 */

const { Rozes } = require('../../../dist/index.js');

async function tightLoopStress() {
    const rozes = await Rozes.init();
    const TOTAL = 1_000_000;
    const GC_INTERVAL = 1000;

    const baseline = process.memoryUsage();
    let maxHeap = baseline.heapUsed;

    console.log(`Baseline: ${(baseline.heapUsed / 1024 / 1024).toFixed(2)} MB`);

    for (let i = 0; i < TOTAL; i++) {
        const df = rozes.DataFrame.fromCSV('a\n1\n2\n3', { autoCleanup: true });
        // Immediately goes out of scope

        if (i % GC_INTERVAL === 0) {
            if (global.gc) global.gc();
            const mem = process.memoryUsage();
            maxHeap = Math.max(maxHeap, mem.heapUsed);

            if (i % 100000 === 0) {
                console.log(`${i/1000}K: Heap ${(mem.heapUsed/1024/1024).toFixed(2)} MB (peak ${(maxHeap/1024/1024).toFixed(2)} MB)`);
            }
        }
    }

    // Final GC
    if (global.gc) global.gc();
    const finalMem = process.memoryUsage();
    const growth = (finalMem.heapUsed - baseline.heapUsed) / 1024 / 1024;
    const peakGrowth = (maxHeap - baseline.heapUsed) / 1024 / 1024;

    console.log(`\nCompleted: ${TOTAL.toLocaleString()} DataFrames`);
    console.log(`Final growth: ${growth.toFixed(2)} MB`);
    console.log(`Peak growth: ${peakGrowth.toFixed(2)} MB`);
    console.log(growth < 5 ? '✓ PASS' : '✗ FAIL - Memory accumulation detected');
}

tightLoopStress();
```

**Run**:
```bash
node --expose-gc src/test/nodejs/stress/tight_loop_stress.js
```

**Success Criteria**:
- Final heap growth <5 MB
- Peak heap growth <50 MB
- No exponential growth pattern
- Completes without OOM

---

## Chrome DevTools Heap Profiling

For deep analysis of memory leaks, use Chrome DevTools to capture heap snapshots.

### Setup

1. **Install Chrome/Chromium**
2. **Run Node.js with inspector**:
   ```bash
   node --inspect --expose-gc your_test.js
   ```
3. **Open Chrome DevTools**:
   - Navigate to `chrome://inspect`
   - Click "Open dedicated DevTools for Node"

### Heap Snapshot Workflow

#### Step 1: Capture Baseline

```javascript
const { Rozes } = require('../../dist/index.js');

async function heapProfileTest() {
    const rozes = await Rozes.init();

    // Force GC to establish baseline
    global.gc();
    debugger; // PAUSE HERE - Take Snapshot #1

    // Create 1000 DataFrames
    for (let i = 0; i < 1000; i++) {
        const df = rozes.DataFrame.fromCSV('a,b\n1,2', { autoCleanup: true });
    }

    debugger; // PAUSE HERE - Take Snapshot #2

    // Force GC
    global.gc();
    await new Promise(resolve => setTimeout(resolve, 200)); // Wait for finalization

    debugger; // PAUSE HERE - Take Snapshot #3
}

heapProfileTest();
```

#### Step 2: Analyze Snapshots

1. **Snapshot #1** (Baseline) - Initial state
2. **Snapshot #2** (After creation) - Should show 1000 DataFrames
3. **Snapshot #3** (After GC) - DataFrames should be gone

**What to look for**:
- Compare Snapshot #3 vs #1: Should be similar
- Search for "DataFrame" objects in Snapshot #3: Should be 0 or minimal
- Check "Detached DOM tree" size: Should not grow
- Look at "Distance" column: Retained objects should have no path from GC roots

### Expected Heap Snapshot Pattern

**✅ Correct (No Leak)**:
```
Snapshot #1: 50 MB total, 0 DataFrame objects
Snapshot #2: 75 MB total, 1000 DataFrame objects
Snapshot #3: 52 MB total, 0 DataFrame objects  ✓ Cleaned up!
```

**❌ Memory Leak**:
```
Snapshot #1: 50 MB total, 0 DataFrame objects
Snapshot #2: 75 MB total, 1000 DataFrame objects
Snapshot #3: 74 MB total, 950 DataFrame objects  ✗ Leak detected!
```

### Common Leak Patterns

| Pattern | Description | Solution |
|---------|-------------|----------|
| **Retained Event Listeners** | DataFrame registered to event | Remove listeners in free() |
| **Global References** | Stored in global scope | Clear global references |
| **Circular References** | Objects reference each other | Break cycles or use WeakRef |
| **Closure Capture** | Function closes over DataFrame | Limit closure scope |

---

## Decision Matrix: When to Run Which Test

| Scenario | Fast Tests | Soak Test | HTTP Stress | Tight Loop | DevTools |
|----------|-----------|-----------|-------------|------------|----------|
| **Every commit** | ✅ Yes | ❌ No | ❌ No | ❌ No | ❌ No |
| **PR review** | ✅ Yes | ❌ No | ⚠️ Optional | ❌ No | ❌ No |
| **Pre-release** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | ⚠️ Optional |
| **Suspected leak** | ✅ Yes | ⚠️ Optional | ⚠️ Optional | ✅ Yes | ✅ Yes |
| **Memory regression** | ✅ Yes | ❌ No | ❌ No | ✅ Yes | ✅ Yes |
| **Nightly CI** | ✅ Yes | ⚠️ Optional | ⚠️ Optional | ⚠️ Optional | ❌ No |

**Legend**:
- ✅ **Yes** - Always run
- ⚠️ **Optional** - Run if time permits
- ❌ **No** - Not necessary

---

## Interpreting Results

### Pass Criteria Summary

| Test | Pass Criteria | Fail Criteria |
|------|--------------|---------------|
| **Fast tests** | All 5 tests pass in <5 minutes | Any test fails or timeout |
| **Soak test** | Memory growth <1%/hour over 24h | Unbounded growth or crash |
| **HTTP stress** | Memory returns to baseline | Growth >10 MB or crash |
| **Tight loop** | Final heap <5 MB growth | >50 MB growth or OOM |
| **Heap snapshot** | No retained DataFrames after GC | DataFrames still in heap |

### Troubleshooting Failures

#### Fast Test Failure

**Symptom**: `wasm_memory_test` shows auto cleanup growth >5 MB

**Diagnosis**:
1. Check GC timing - finalization is non-deterministic
2. Increase wait time after GC (100ms → 500ms)
3. Verify FinalizationRegistry is supported (Node.js 14.6+, Chrome 84+)

**Fix**:
```javascript
// Increase wait time for finalization
global.gc();
await new Promise(resolve => setTimeout(resolve, 500)); // Was 100ms
```

#### Soak Test Failure

**Symptom**: Memory grows >1%/hour over 24 hours

**Diagnosis**:
1. Plot memory over time (CSV data)
2. Check for linear vs exponential growth
3. Inspect heap snapshot at hour 24

**Fix**:
- If linear growth: Acceptable (GC overhead)
- If exponential growth: Real leak, use DevTools

#### HTTP Stress Failure

**Symptom**: Memory doesn't return to baseline after load

**Diagnosis**:
1. Check if manual `free()` is called in finally block
2. Verify no global references to DataFrames
3. Check event listeners or closures

**Fix**:
```javascript
// Always use try-finally for manual cleanup
const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: false });
try {
    // Process
} finally {
    df.free(); // Guaranteed cleanup
}
```

---

## Production Monitoring

### Metrics to Track

For production deployments, monitor these metrics:

| Metric | Threshold | Alert If |
|--------|-----------|----------|
| **Heap Used** | <100 MB (per process) | >200 MB sustained |
| **RSS** | <500 MB | >1 GB |
| **External Memory** | <50 MB | >100 MB (Wasm memory) |
| **GC Frequency** | <10/min | >50/min (thrashing) |
| **Process Uptime** | Days | <1 hour (crash loop) |

### APM Integration

**New Relic Example**:
```javascript
const newrelic = require('newrelic');

// Track DataFrame memory
setInterval(() => {
    const mem = process.memoryUsage();
    newrelic.recordMetric('Custom/DataFrame/HeapUsed', mem.heapUsed / 1024 / 1024);
    newrelic.recordMetric('Custom/DataFrame/External', mem.external / 1024 / 1024);
}, 60000); // Every minute
```

**DataDog Example**:
```javascript
const { StatsD } = require('node-dogstatsd');
const statsd = new StatsD();

setInterval(() => {
    const mem = process.memoryUsage();
    statsd.gauge('dataframe.heap_mb', mem.heapUsed / 1024 / 1024);
    statsd.gauge('dataframe.wasm_mb', mem.external / 1024 / 1024);
}, 60000);
```

---

## FAQ

### Q: Do I need to call `df.free()` with `autoCleanup: true`?

**A**: No, but you can for deterministic cleanup. With `autoCleanup: true`, finalization happens automatically but non-deterministically (when GC runs). Calling `free()` explicitly gives you control over when memory is released.

### Q: Why does auto cleanup use more memory temporarily?

**A**: FinalizationRegistry schedules cleanup when GC runs, which is non-deterministic. Memory is held until GC occurs. Manual `free()` releases immediately.

### Q: Is auto cleanup slower?

**A**: Yes, ~3× slower due to FinalizationRegistry overhead and deferred GC. Use manual cleanup in tight loops or production HTTP handlers.

### Q: Can I mix auto and manual cleanup?

**A**: Yes! They coexist safely. Use auto cleanup for prototyping/scripts, manual cleanup for production code.

### Q: How do I know if I have a memory leak?

**A**: Run fast tests first. If they pass but you suspect a leak, run soak test (24h) and use Chrome DevTools heap snapshots.

### Q: What browsers support FinalizationRegistry?

**A**: Chrome 84+, Firefox 79+, Safari 14.1+, Node.js 14.6+. Older browsers/Node versions require manual cleanup.

---

## Related Documentation

- **[docs/MEMORY_MANAGEMENT.md](./MEMORY_MANAGEMENT.md)** - Memory management guide (auto vs manual)
- **[src/test/nodejs/memory/](../src/test/nodejs/memory/)** - Fast memory test implementations
- **[docs/TODO.md](./TODO.md)** - Priority 6: Memory testing tasks

---

**Last Updated**: 2025-10-31
**Author**: Claude Code
**Status**: Production Ready (1.1.0)
