# Memory Management Guide - Rozes DataFrame

**Version**: 1.1.0 | **Last Updated**: 2025-10-31

---

## Overview

Rozes DataFrame runs on WebAssembly, which has a **separate memory space** from JavaScript's heap. This means JavaScript's garbage collector **cannot automatically free** WebAssembly memory.

**Solution**: Rozes offers **two memory management approaches**:

1. **Automatic Cleanup** (default) - FinalizationRegistry handles cleanup automatically
2. **Manual Cleanup** (opt-out) - You call `df.free()` explicitly for better performance

---

## Quick Start

### Automatic Cleanup (Default - Convenient)

```javascript
const rozes = await Rozes.init();
const df = rozes.DataFrame.fromCSV(csvData);

// Use DataFrame
console.log(df.shape);

// Memory freed automatically on GC
// Can still call df.free() for immediate cleanup (recommended)
df.free(); // Optional but recommended
```

**TypeScript**:
```typescript
import { Rozes, DataFrame } from 'rozes';

const rozes = await Rozes.init();
const df: DataFrame = rozes.DataFrame.fromCSV(csvData);

// Use DataFrame - type-safe!
console.log(df.shape);

// No free() needed, but recommended for immediate cleanup
df.free(); // Optional but recommended
```

### Manual Cleanup (Opt-Out - Performance)

```javascript
const rozes = await Rozes.init();
const df = rozes.DataFrame.fromCSV(csvData, { autoCleanup: false });

try {
  // Use DataFrame
  console.log(df.shape);
} finally {
  df.free(); // MUST call this
}
```

**TypeScript**:
```typescript
import { Rozes, DataFrame, CSVOptions } from 'rozes';

const rozes = await Rozes.init();
const options: CSVOptions = { autoCleanup: false };
const df: DataFrame = rozes.DataFrame.fromCSV(csvData, options);

try {
  // Use DataFrame
  console.log(df.shape);
} finally {
  df.free(); // Type-safe and required
}
```

---

## Automatic Memory Management

### Overview

**Default behavior**: `autoCleanup: true`

Memory is freed automatically when DataFrame is garbage collected. Calling `df.free()` is **optional** but **recommended** for immediate cleanup.

### ✅ Advantages

- **Convenient**: No need to call `df.free()`
- **Safety net**: Prevents memory leaks if you forget to free
- **Less boilerplate**: No try/finally blocks needed
- **Great for scripts**: Perfect for quick prototypes and exploration

### ⚠️ Disadvantages

- **Non-deterministic**: GC decides when to free (could be seconds or minutes)
- **Memory pressure**: 1000 DataFrames in a loop → memory grows until GC runs
- **GC pauses**: Can be 10-100ms when cleaning up Wasm memory
- **Debugging harder**: Memory leaks less obvious (is it leaked or waiting for GC?)

### Best Practices

#### Pattern 1: Try-Finally (Recommended)

```javascript
const df = rozes.DataFrame.fromCSV(csvData);

try {
  // Use DataFrame
  const ages = df.column('age');
  console.log(ages);
} finally {
  df.free(); // Always runs, even on error
}
```

#### Pattern 2: Multiple DataFrames

```javascript
const df1 = rozes.DataFrame.fromCSV(csv1);
const df2 = rozes.DataFrame.fromCSV(csv2);

try {
  // Use both DataFrames
  console.log(df1.shape, df2.shape);
} finally {
  df1.free();
  df2.free();
}
```

#### Pattern 3: Batch Processing

```javascript
// Process 1000 files with manual cleanup
for (let i = 0; i < 1000; i++) {
  const df = rozes.DataFrame.fromCSV(csvData);

  // Process data
  const result = processDataFrame(df);

  // Free immediately (deterministic)
  df.free();
}
// Memory usage stays flat!
```

### Performance

**Manual cleanup is ~3× faster** in tight loops because:
- No FinalizationRegistry overhead
- No waiting for GC
- Immediate memory reclamation

**Benchmark (1000 DataFrames)**:
- Manual cleanup: ~60ms (0.06ms each)
- Auto cleanup: ~200ms (0.20ms each)

---

## Automatic Memory Management

### Overview

**Opt-in behavior**: `autoCleanup: true`

Memory freed automatically when DataFrame is garbage collected.

### ✅ Advantages

- **Convenient**: No need to call `df.free()`
- **Safety net**: Prevents memory leaks if you forget to free
- **Less boilerplate**: No try/finally blocks needed
- **Great for scripts**: Perfect for quick prototypes and exploration

### ⚠️ Disadvantages

- **Non-deterministic**: GC decides when to free (could be seconds or minutes)
- **Memory pressure**: 1000 DataFrames in a loop → memory grows until GC runs
- **GC pauses**: Can be 10-100ms when cleaning up Wasm memory
- **Debugging harder**: Memory leaks less obvious (is it leaked or waiting for GC?)

### How It Works

Uses **FinalizationRegistry** (ES2021 feature):

```javascript
const finalizationRegistry = new FinalizationRegistry((handle) => {
  // Called when DataFrame is garbage collected
  wasm.instance.exports.rozes_free(handle);
});

class DataFrame {
  constructor(handle, wasm, autoCleanup = false) {
    if (autoCleanup) {
      // Register for auto cleanup
      finalizationRegistry.register(this, handle, this);
    }
  }

  free() {
    if (this._autoCleanup) {
      // Unregister (prevents double-free)
      finalizationRegistry.unregister(this);
    }
    // Free immediately
    wasm.instance.exports.rozes_free(this._handle);
  }
}
```

### When GC Runs

JavaScript's GC runs when:
- Heap memory is low
- Event loop is idle
- `global.gc()` is called (Node.js with `--expose-gc`)

**You cannot control when GC runs** - this is the key tradeoff.

### Best Practices

#### Pattern 1: Simple Scripts

```javascript
// Quick data analysis
const df = rozes.DataFrame.fromCSV(csvData, { autoCleanup: true });

console.log('Total rows:', df.shape.rows);
const ages = df.column('age');
console.log('Average age:', ages.reduce((a, b) => a + b) / ages.length);

// No free() needed - convenient!
```

#### Pattern 2: Hybrid Approach (Recommended for Scripts)

```javascript
// Enable auto cleanup as safety net
const df = rozes.DataFrame.fromCSV(csvData, { autoCleanup: true });

try {
  // Use DataFrame
  const result = processDataFrame(df);

  // Still call free() for deterministic cleanup
  df.free();
} catch (err) {
  console.error(err);
  // If error occurs, auto cleanup ensures no leak
}
```

#### Pattern 3: Avoid in Tight Loops

```javascript
// ❌ BAD: Memory grows until GC runs
for (let i = 0; i < 1000; i++) {
  const df = rozes.DataFrame.fromCSV(csvData, { autoCleanup: true });
  processDataFrame(df);
  // Memory not freed until GC decides to run
}

// ✅ GOOD: Deterministic cleanup
for (let i = 0; i < 1000; i++) {
  const df = rozes.DataFrame.fromCSV(csvData);
  processDataFrame(df);
  df.free(); // Free immediately
}
```

---

## Browser Compatibility

**FinalizationRegistry** is supported in:

| Browser        | Version | Support |
| -------------- | ------- | ------- |
| Chrome         | 84+     | ✅      |
| Firefox        | 79+     | ✅      |
| Safari         | 14.1+   | ✅      |
| Edge           | 84+     | ✅      |
| Node.js        | 14.6+   | ✅      |
| Chrome Android | 84+     | ✅      |
| Safari iOS     | 14.5+   | ✅      |

**IE 11**: ❌ Not supported (no WebAssembly)

**Fallback**: If FinalizationRegistry is not available, you **must** use manual cleanup.

---

## Decision Matrix

### When to Use Auto Cleanup (Default)

✅ **Quick scripts and prototypes**
✅ **Interactive data exploration**
✅ **Jupyter-style notebooks**
✅ **Most applications** (unless performance-critical)
✅ **Short-lived processes**
✅ **Learning and experimentation**
✅ **Convenience > performance**

### When to Use Manual Cleanup (Opt-Out)

✅ **Performance-critical code** (~3× faster in loops)
✅ **Tight loops processing many DataFrames**
✅ **Long-running servers** (predictable memory)
✅ **Production applications** (deterministic behavior)
✅ **When you need predictable memory usage**
✅ **When debugging memory issues**

---

## Common Patterns

### Pattern 1: HTTP Request Handler (Manual)

```javascript
app.post('/analyze', async (req, res) => {
  const df = rozes.DataFrame.fromCSV(req.body.csv);

  try {
    const result = analyzeData(df);
    res.json(result);
  } finally {
    df.free(); // Must free on every request
  }
});
```

### Pattern 2: CLI Tool (Auto)

```javascript
// cli.js - one-shot script
const rozes = await Rozes.init();
const df = rozes.DataFrame.fromCSVFile(process.argv[2], { autoCleanup: true });

console.log(`Rows: ${df.shape.rows}`);
console.log(`Columns: ${df.columns.join(', ')}`);

// Process exits - no need to free
```

### Pattern 3: Data Pipeline (Manual)

```javascript
async function processPipeline(files) {
  const results = [];

  for (const file of files) {
    const df = rozes.DataFrame.fromCSVFile(file);

    try {
      const processed = transform(df);
      results.push(processed);
    } finally {
      df.free(); // Free after each file
    }
  }

  return results;
}
```

### Pattern 4: Interactive REPL (Auto)

```javascript
// repl.js
const rozes = await Rozes.init();

repl.defineCommand('load', {
  action(filename) {
    // Auto cleanup - convenient for exploration
    global.df = rozes.DataFrame.fromCSVFile(filename, { autoCleanup: true });
    console.log(`Loaded: ${global.df.shape.rows} rows`);
  }
});
```

---

## Debugging Memory Issues

### Check for Memory Leaks (Manual Cleanup)

```javascript
// Run this 1000 times - memory should stay flat
for (let i = 0; i < 1000; i++) {
  const df = rozes.DataFrame.fromCSV(csvData);
  df.free();

  if (i % 100 === 0) {
    const mem = process.memoryUsage();
    console.log(`Iteration ${i}: ${(mem.heapUsed / 1024 / 1024).toFixed(2)} MB`);
  }
}
// If memory grows, you have a leak!
```

### Test Auto Cleanup (with GC)

```bash
# Run with --expose-gc to trigger GC manually
node --expose-gc your_script.js
```

```javascript
// Force GC if available
if (global.gc) {
  console.log('Before GC:', process.memoryUsage().heapUsed);
  global.gc();
  global.gc(); // Run twice for thorough cleanup
  console.log('After GC:', process.memoryUsage().heapUsed);
}
```

### Monitor Memory Pressure

```javascript
// Track memory growth
const startMem = process.memoryUsage().heapUsed;

for (let i = 0; i < 100; i++) {
  const df = rozes.DataFrame.fromCSV(csvData, { autoCleanup: true });
  // Use df
}

const endMem = process.memoryUsage().heapUsed;
const growth = (endMem - startMem) / 1024 / 1024;
console.log(`Memory growth: ${growth.toFixed(2)} MB`);

// With auto cleanup, growth depends on GC timing
// With manual cleanup, growth should be ~0 MB
```

---

## FAQ

### Q: Can I use both manual and auto cleanup in the same app?

**A**: Yes! You can mix both approaches:

```javascript
// Some DataFrames with auto cleanup
const df1 = rozes.DataFrame.fromCSV(csv1, { autoCleanup: true });

// Some with manual cleanup
const df2 = rozes.DataFrame.fromCSV(csv2);
df2.free();
```

### Q: What happens if I call `free()` twice?

**A**: Safe - the `free()` method is idempotent:

```javascript
df.free();
df.free(); // No-op, safe to call
```

### Q: Can I still call `free()` with autoCleanup: true?

**A**: Yes! It's actually **recommended** for deterministic cleanup:

```javascript
const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });
// Use df
df.free(); // Immediate cleanup (recommended)
// If you forget, auto cleanup is a safety net
```

### Q: How much memory does a DataFrame use?

**A**: Approximately **8 bytes per cell** (columnar storage):

```javascript
// 1000 rows × 10 columns × 8 bytes = 80 KB
const df = rozes.DataFrame.fromCSV(csv);
console.log('Estimated memory:', df.shape.rows * df.shape.cols * 8, 'bytes');
```

### Q: Does auto cleanup work in browsers?

**A**: Yes! FinalizationRegistry works in all modern browsers (Chrome 84+, Firefox 79+, Safari 14.1+).

### Q: What's the performance impact of autoCleanup?

**A**: ~3× slower in tight loops (200ms vs 60ms for 1000 DataFrames). For single DataFrames, the difference is negligible (<1ms).

---

## Summary

### Auto Cleanup (Default)

```javascript
const df = rozes.DataFrame.fromCSV(csv);
// use df
df.free(); // Optional but recommended
```

- ✅ Convenient
- ✅ No boilerplate required
- ✅ Safety net
- ⚠️ Non-deterministic timing
- ⚠️ Memory pressure in loops

### Manual Cleanup (Opt-Out)

```javascript
const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: false });
try { /* use */ } finally { df.free(); }
```

- ✅ Deterministic
- ✅ ~3× faster
- ✅ Production-ready
- ⚠️ Manual work required

### Hybrid (Recommended)

```javascript
const df = rozes.DataFrame.fromCSV(csv); // autoCleanup: true by default
try { /* use */ } finally { df.free(); } // Still call free() for immediate cleanup
```

- ✅ Deterministic cleanup
- ✅ Safety net if forgotten
- ✅ Best of both worlds

---

## See Also

- [README.md](../README.md) - Getting started guide
- [API Documentation](./API.md) - Complete API reference
- [Examples](../examples/) - Code examples
- [Tests](../src/test/nodejs/) - Test cases

---

**Last Updated**: 2025-10-31
**Version**: 1.1.0
