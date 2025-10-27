# Rozes DataFrame - JavaScript Wrapper

High-performance DataFrame library for JavaScript, powered by WebAssembly.

## Features

- ✅ **High Performance**: WebAssembly-powered CSV parsing
- ✅ **Zero-Copy**: Direct TypedArray access to columnar data
- ✅ **Small Bundle**: ~47KB Wasm module
- ✅ **Type Safe**: Full TypeScript definitions
- ✅ **Memory Efficient**: Columnar storage with manual memory management

## Quick Start

### Browser (ES Modules)

```html
<!DOCTYPE html>
<html>
<head>
    <script type="module">
        import { Rozes } from './rozes.js';

        // Initialize Rozes
        const rozes = await Rozes.init('./rozes.wasm');

        // Parse CSV
        const csv = `age,score
30,95.5
25,87.3
35,91.0`;

        const df = rozes.DataFrame.fromCSV(csv);

        console.log(df.shape);  // { rows: 3, cols: 2 }
        console.log(df.columns); // ['age', 'score']

        // Zero-copy column access
        const ages = df.column('age');  // Float64Array [30, 25, 35]
        const scores = df.column('score'); // Float64Array [95.5, 87.3, 91.0]

        // Always free when done
        df.free();
    </script>
</head>
</html>
```

### Node.js

```javascript
const { Rozes } = require('./rozes.js');
const fs = require('fs');

(async () => {
    // Load Wasm module
    const wasmBuffer = fs.readFileSync('./rozes.wasm');
    const rozes = await Rozes.init(wasmBuffer);

    // Parse CSV
    const csv = fs.readFileSync('data.csv', 'utf-8');
    const df = rozes.DataFrame.fromCSV(csv);

    // Process data
    console.log(`Loaded ${df.shape.rows} rows`);

    // Clean up
    df.free();
})();
```

## API Reference

### `Rozes.init(wasmPath)`

Initialize the Rozes library by loading the WebAssembly module.

**Parameters:**
- `wasmPath` (string | URL): Path to `rozes.wasm` file

**Returns:** `Promise<Rozes>`

**Example:**
```javascript
const rozes = await Rozes.init('./rozes.wasm');
```

---

### `DataFrame.fromCSV(csvText, options)`

Parse CSV string into a DataFrame.

**Parameters:**
- `csvText` (string): CSV data as text
- `options` (object, optional):
  - `delimiter` (string): Field delimiter (default: `','`)
  - `has_headers` (boolean): First row contains headers (default: `true`)
  - `skip_blank_lines` (boolean): Skip blank lines (default: `true`)
  - `trim_whitespace` (boolean): Trim whitespace (default: `false`)

**Returns:** `DataFrame`

**Example:**
```javascript
const df = rozes.DataFrame.fromCSV(csv, {
    delimiter: '\t',  // Tab-separated
    has_headers: true
});
```

---

### `df.shape`

Get DataFrame dimensions.

**Returns:** `{ rows: number, cols: number }`

**Example:**
```javascript
console.log(df.shape); // { rows: 100, cols: 5 }
```

---

### `df.columns`

Get column names.

**Returns:** `string[]`

**Example:**
```javascript
console.log(df.columns); // ['age', 'score', 'height']
```

---

### `df.column(name)`

Get column data as TypedArray (zero-copy).

**Parameters:**
- `name` (string): Column name

**Returns:** `Float64Array | BigInt64Array | null`

**Example:**
```javascript
const ages = df.column('age');  // Float64Array
const count = ages.length;
const sum = ages.reduce((a, b) => a + b, 0);
const mean = sum / count;
```

---

### `df.free()`

Free DataFrame memory. **Must** be called when done to prevent memory leaks.

**Example:**
```javascript
df.free();
// df is now invalid - don't use after freeing!
```

---

### `df.toString()`

Get DataFrame summary as string.

**Returns:** `string`

**Example:**
```javascript
console.log(df.toString());
// DataFrame(3 rows × 2 cols)
// Columns: age, score
```

## Memory Management

⚠️ **Important**: Unlike JavaScript objects, DataFrames must be manually freed.

```javascript
// ✅ Correct
const df = rozes.DataFrame.fromCSV(csv);
// ... use df ...
df.free();

// ❌ Wrong - memory leak!
const df = rozes.DataFrame.fromCSV(csv);
// ... use df ...
// (never freed)
```

### Automatic Cleanup Pattern

```javascript
function processCSV(csv) {
    const df = rozes.DataFrame.fromCSV(csv);
    try {
        // Use DataFrame
        const result = df.column('age');
        return Array.from(result);
    } finally {
        // Always free, even if error thrown
        df.free();
    }
}
```

## Performance

**Parsing Benchmark** (100K rows × 10 cols):

| Library | Time | Memory |
|---------|------|--------|
| Rozes (Wasm) | **~150ms** | ~8MB |
| Papa Parse | ~450ms | ~50MB |
| CSV Parse | ~380ms | ~40MB |

*Benchmarks run on Chrome 120, M1 Mac*

## Type Conversion

Rozes automatically infers column types:

| CSV Value | Detected Type | JavaScript Type |
|-----------|---------------|-----------------|
| `42` | Int64 | `BigInt64Array` |
| `3.14` | Float64 | `Float64Array` |
| `"text"` | String | (0.2.0) |
| (empty) | Null | `0` or `0n` |

## Error Handling

```javascript
try {
    const df = rozes.DataFrame.fromCSV(invalidCSV);
} catch (error) {
    if (error instanceof RozesError) {
        console.error(`Error code: ${error.code}`);
        console.error(`Message: ${error.message}`);
    }
}
```

**Error Codes:**

- `OutOfMemory` (-1): Insufficient memory
- `InvalidFormat` (-2): Malformed CSV
- `ColumnNotFound` (-4): Column doesn't exist
- `TypeMismatch` (-5): Column type doesn't match
- `TooManyDataFrames` (-7): Max 1000 DataFrames

## Browser Support

- ✅ Chrome 90+
- ✅ Firefox 88+
- ✅ Safari 14+
- ✅ Edge 90+
- ❌ Internet Explorer (no WebAssembly support)

## Building from Source

```bash
# Build Wasm module
zig build wasm

# Output: zig-out/bin/rozes.wasm (47KB)
```

## License

See main project LICENSE

## Links

- [Full Documentation](../docs/RFC.md)
- [Examples](./examples/)
- [GitHub Issues](https://github.com/yourusername/rozes/issues)
