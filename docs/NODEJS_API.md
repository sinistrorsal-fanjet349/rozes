# Rozes Node.js API Reference

**Version**: 1.0.0
**Last Updated**: 2025-10-31

High-performance DataFrame library for Node.js powered by WebAssembly. 3-10× faster than Papa Parse and csv-parse.

---

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Initialization](#initialization)
4. [DataFrame Creation](#dataframe-creation)
5. [DataFrame Properties](#dataframe-properties)
6. [DataFrame Methods](#dataframe-methods)
7. [CSV Options](#csv-options)
8. [TypeScript Types](#typescript-types)
9. [Error Handling](#error-handling)
10. [Memory Management](#memory-management)
11. [Performance Tips](#performance-tips)
12. [Complete Examples](#complete-examples)

---

## Installation

```bash
npm install rozes
```

**Requirements**:
- Node.js 14+ (LTS versions recommended)
- No native dependencies (pure WASM)

---

## Quick Start

### CommonJS

```javascript
const { Rozes } = require('rozes');

async function main() {
  const rozes = await Rozes.init();
  const df = rozes.DataFrame.fromCSV("age,score\n30,95.5\n25,87.3");

  console.log(df.shape); // { rows: 2, cols: 2 }
  console.log(df.columns); // ['age', 'score']

  const ages = df.column('age'); // Float64Array [30, 25]
  console.log(Array.from(ages));

  // Memory freed automatically! (Can still call df.free() for immediate cleanup)
}

main();
```

### ES Modules

```javascript
import { Rozes } from 'rozes';

const rozes = await Rozes.init();
const df = rozes.DataFrame.fromCSV("age,score\n30,95.5\n25,87.3");

console.log(df.shape); // { rows: 2, cols: 2 }
df.free();
```

### TypeScript

```typescript
import { Rozes, DataFrame } from 'rozes';

const rozes: Rozes = await Rozes.init();
const df: DataFrame = rozes.DataFrame.fromCSV("age,score\n30,95.5\n25,87.3");

// Full autocomplete support
const shape = df.shape; // { rows: number, cols: number }
const columns = df.columns; // string[]
const ages = df.column('age'); // Float64Array | Int32Array | BigInt64Array | null

df.free();
```

---

## Initialization

### `Rozes.init(wasmPath?: string): Promise<Rozes>`

Initialize the Rozes library by loading the WebAssembly module.

**Parameters**:
- `wasmPath` (optional): Custom path to WASM file. Defaults to bundled `rozes.wasm`.

**Returns**: Promise resolving to initialized `Rozes` instance

**Example**:

```javascript
// Use bundled WASM (default)
const rozes = await Rozes.init();

// Use custom WASM path
const rozes = await Rozes.init('./custom-path/rozes.wasm');

// Access version
console.log(rozes.version); // "1.0.0"
```

**Notes**:
- Must be called before any DataFrame operations
- Only needs to be called once per application
- WASM module is ~62KB (35KB gzipped)

---

## DataFrame Creation

### `DataFrame.fromCSV(csvText: string, options?: CSVOptions): DataFrame`

Parse CSV string into DataFrame.

**Parameters**:
- `csvText`: CSV data as string
- `options` (optional): Parsing options (see [CSV Options](#csv-options))

**Returns**: New `DataFrame` instance

**Example**:

```javascript
const csv = `name,age,score
Alice,30,95.5
Bob,25,87.3
Charlie,35,91.0`;

const df = rozes.DataFrame.fromCSV(csv);
console.log(df.shape); // { rows: 3, cols: 3 }
```

**Performance**: Parses 1M rows in ~570ms (1.75M rows/sec)

---

### `DataFrame.fromCSVFile(filePath: string, options?: CSVOptions): DataFrame`

Load CSV from file (Node.js only).

**Parameters**:
- `filePath`: Path to CSV file (absolute or relative)
- `options` (optional): Parsing options (see [CSV Options](#csv-options))

**Returns**: New `DataFrame` instance

**Example**:

```javascript
const df = rozes.DataFrame.fromCSVFile('data.csv');
console.log(`Loaded ${df.shape.rows} rows`);
df.free();
```

**Notes**:
- File is read synchronously (async version planned for 1.1.0)
- Works with relative and absolute paths
- Supports large files (tested up to 1M rows)

---

## DataFrame Properties

### `df.shape: DataFrameShape`

Get DataFrame dimensions.

```javascript
const df = rozes.DataFrame.fromCSV(csvText);
console.log(df.shape); // { rows: 1000, cols: 5 }
```

**Type**:
```typescript
interface DataFrameShape {
  rows: number; // Number of rows
  cols: number; // Number of columns
}
```

---

### `df.columns: string[]`

Get column names.

```javascript
const df = rozes.DataFrame.fromCSV(csvText);
console.log(df.columns); // ['name', 'age', 'score']
```

**Returns**: Array of column names in order

---

### `df.length: number`

Get number of rows (same as `df.shape.rows`).

```javascript
const df = rozes.DataFrame.fromCSV(csvText);
console.log(df.length); // 1000
```

---

## DataFrame Methods

### `df.column(name: string): TypedArray | null`

Get column data as typed array (zero-copy access).

**Parameters**:
- `name`: Column name

**Returns**:
- `Float64Array` for Float64 columns
- `Int32Array` for Int32 columns
- `BigInt64Array` for Int64 columns
- `null` if column not found

**Example**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);

// Numeric columns
const ages = df.column('age'); // Float64Array
const ids = df.column('id');   // BigInt64Array

// Check if column exists
if (ages) {
  const avgAge = ages.reduce((a, b) => a + b) / ages.length;
  console.log(`Average age: ${avgAge}`);
}

// Handle BigInt columns
if (ids) {
  for (const id of ids) {
    console.log(Number(id)); // Convert BigInt to Number if needed
  }
}
```

**Performance**: Zero-copy access - no data copying, just TypedArray view

**Notes**:
- Returns reference to internal data (modifications affect DataFrame)
- String columns not yet supported in 1.0.0 (planned for 1.1.0)
- Boolean columns not yet supported in 1.0.0 (planned for 1.1.0)

---

### `df.filter(columnName: string, operator: string, value: number): DataFrame`

Filter DataFrame rows by numeric condition.

**Parameters**:
- `columnName`: Column to filter on (must be numeric)
- `operator`: Comparison operator: `'=='`, `'!='`, `'>'`, `'<'`, `'>='`, `'<='`
- `value`: Value to compare against

**Returns**: New filtered DataFrame

**Example**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);

// Filter: age >= 30
const adults = df.filter('age', '>=', 30);
console.log(`${adults.shape.rows} adults`);
adults.free();

// Filter: salary > 100000
const highEarners = df.filter('salary', '>', 100000);
console.log(`${highEarners.shape.rows} high earners`);
highEarners.free();

// Filter: years_experience == 5
const fiveYears = df.filter('years_experience', '==', 5);
fiveYears.free();
```

**Notes**:
- Only works on numeric columns (Int32, Int64, Float64)
- String column filtering not supported in 1.0.0 (planned for 1.1.0)
- Returns new DataFrame - remember to call `.free()` when done

---

### `df.select(columnNames: string[]): DataFrame`

Select specific columns from DataFrame.

**Parameters**:
- `columnNames`: Array of column names to select

**Returns**: New DataFrame with selected columns only

**Example**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);

// Select specific columns
const subset = df.select(['name', 'age', 'salary']);
console.log(subset.columns); // ['name', 'age', 'salary']
subset.free();

// Select numeric columns only
const numeric = df.select(['age', 'salary', 'years_experience']);
numeric.free();
```

**Notes**:
- Column order in result matches order in `columnNames` array
- Non-existent columns are ignored
- Returns new DataFrame - remember to call `.free()` when done

---

### `df.head(n: number): DataFrame`

Get first n rows of DataFrame.

**Parameters**:
- `n`: Number of rows to return

**Returns**: New DataFrame with first n rows

**Example**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);

// Get first 10 rows
const top10 = df.head(10);
console.log(`First ${top10.shape.rows} rows`);
top10.free();

// Get first 5 rows
const preview = df.head(5);
preview.free();
```

**Notes**:
- If `n` is greater than row count, returns all rows
- Returns new DataFrame - remember to call `.free()` when done

---

### `df.tail(n: number): DataFrame`

Get last n rows of DataFrame.

**Parameters**:
- `n`: Number of rows to return

**Returns**: New DataFrame with last n rows

**Example**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);

// Get last 10 rows
const bottom10 = df.tail(10);
console.log(`Last ${bottom10.shape.rows} rows`);
bottom10.free();

// Get last 5 rows
const recent = df.tail(5);
recent.free();
```

**Notes**:
- If `n` is greater than row count, returns all rows
- Returns new DataFrame - remember to call `.free()` when done

---

### `df.sort(columnName: string, descending?: boolean): DataFrame`

Sort DataFrame by column.

**Parameters**:
- `columnName`: Column to sort by (must be numeric in 1.0.0)
- `descending`: Sort in descending order (default: `false` for ascending)

**Returns**: New sorted DataFrame

**Example**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);

// Sort by age (ascending)
const byAge = df.sort('age');
const ages = byAge.column('age');
console.log(Array.from(ages)); // [25, 28, 30, 35, 40, ...]
byAge.free();

// Sort by salary (descending)
const bySalary = df.sort('salary', true);
const salaries = bySalary.column('salary');
console.log(Array.from(salaries)); // [125000, 115000, 110000, ...]
bySalary.free();
```

**Notes**:
- Only works on numeric columns in 1.0.0 (Int32, Int64, Float64)
- String column sorting planned for 1.1.0
- Returns new DataFrame - remember to call `.free()` when done
- Stable sort (preserves relative order of equal elements)

---

### `df.free(): void`

Release DataFrame memory.

**By default** (autoCleanup: true): Memory is freed automatically when the DataFrame is garbage collected. Calling `free()` is **optional** but **recommended** for immediate cleanup.

**Manual mode** (autoCleanup: false): You **must** call `free()` when done to prevent memory leaks.

**Example (default - auto cleanup)**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);
// ... use df
// Memory freed automatically on GC
// Can still call df.free() for immediate cleanup (recommended)
df.free(); // Optional but recommended
```

**Example (manual mode - production)**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText, { autoCleanup: false });
try {
  // Use df
  const ages = df.column('age');
  console.log(ages);
} finally {
  df.free(); // MUST call - always runs, even if error
}
```

**Why call free() even with auto cleanup?**
- Immediate memory release (no waiting for GC)
- ~3× faster in tight loops
- Predictable performance

---

## CSV Options

Configure CSV parsing behavior:

```typescript
interface CSVOptions {
  delimiter?: string;           // Field delimiter (default: ',')
  has_headers?: boolean;        // First row contains headers (default: true)
  skip_blank_lines?: boolean;   // Skip blank lines (default: true)
  trim_whitespace?: boolean;    // Trim whitespace from fields (default: false)
  autoCleanup?: boolean;        // Automatically free memory on GC (default: true)
}
```

### Examples

**Tab-separated values (TSV)**:

```javascript
const tsv = "name\tage\tscorer\nAlice\t30\t95.5";
const df = rozes.DataFrame.fromCSV(tsv, { delimiter: '\t' });
```

**No headers**:

```javascript
const noHeaders = "30,95.5\n25,87.3";
const df = rozes.DataFrame.fromCSV(noHeaders, { has_headers: false });
console.log(df.columns); // ['column_0', 'column_1']
```

**Trim whitespace**:

```javascript
const csv = "name, age, score\nAlice , 30 , 95.5";
const df = rozes.DataFrame.fromCSV(csv, { trim_whitespace: true });
// Fields trimmed: "Alice ", " 30 " → "Alice", "30"
```

**Custom delimiter**:

```javascript
const pipeSeparated = "name|age|score\nAlice|30|95.5";
const df = rozes.DataFrame.fromCSV(pipeSeparated, { delimiter: '|' });
```

---

## TypeScript Types

Full TypeScript support with autocomplete:

### Rozes Class

```typescript
class Rozes {
  static init(wasmPath?: string): Promise<Rozes>;
  readonly DataFrame: typeof DataFrame;
  readonly version: string;
}
```

### DataFrame Class

```typescript
class DataFrame {
  static fromCSV(csvText: string, options?: CSVOptions): DataFrame;
  static fromCSVFile(filePath: string, options?: CSVOptions): DataFrame;

  readonly shape: DataFrameShape;
  readonly columns: string[];
  readonly length: number;

  column(name: string): Float64Array | Int32Array | BigInt64Array | null;

  // DataFrame operations (1.0.0)
  filter(columnName: string, operator: '==' | '!=' | '>' | '<' | '>=' | '<=', value: number): DataFrame;
  select(columnNames: string[]): DataFrame;
  head(n: number): DataFrame;
  tail(n: number): DataFrame;
  sort(columnName: string, descending?: boolean): DataFrame;

  free(): void;
}
```

### Interfaces

```typescript
interface CSVOptions {
  delimiter?: string;
  has_headers?: boolean;
  skip_blank_lines?: boolean;
  trim_whitespace?: boolean;
}

interface DataFrameShape {
  rows: number;
  cols: number;
}
```

### Error Types

```typescript
enum ErrorCode {
  Success = 0,
  OutOfMemory = -1,
  InvalidFormat = -2,
  InvalidHandle = -3,
  ColumnNotFound = -4,
  TypeMismatch = -5,
  IndexOutOfBounds = -6,
  TooManyDataFrames = -7,
  InvalidOptions = -8,
}

class RozesError extends Error {
  readonly code: ErrorCode;
}
```

---

## Error Handling

All operations can throw `RozesError`:

```javascript
const { Rozes, RozesError, ErrorCode } = require('rozes');

try {
  const rozes = await Rozes.init();
  const df = rozes.DataFrame.fromCSV(malformedCSV);
  df.free();
} catch (err) {
  if (err instanceof RozesError) {
    console.error(`Rozes error ${err.code}: ${err.message}`);

    switch (err.code) {
      case ErrorCode.InvalidFormat:
        console.error('CSV format is invalid');
        break;
      case ErrorCode.OutOfMemory:
        console.error('Out of memory');
        break;
      default:
        console.error('Unknown error');
    }
  } else {
    console.error('Unexpected error:', err);
  }
}
```

**Common Errors**:
- `InvalidFormat` (-2): Malformed CSV
- `OutOfMemory` (-1): Allocation failed
- `ColumnNotFound` (-4): Column name doesn't exist
- `TypeMismatch` (-5): Incompatible types

---

## Memory Management

### Automatic vs Manual Cleanup

**Default (autoCleanup: true)**: Memory is freed automatically when DataFrame is garbage collected. No need to call `df.free()`, but it's still recommended for immediate cleanup.

**Manual Mode (autoCleanup: false)**: You must explicitly call `df.free()` to release WASM memory.

### Best Practices

**1. Default mode - optional free (recommended for most cases)**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);
// Use df
const result = processData(df);
df.free(); // Optional but recommended for immediate cleanup
```

**2. Manual mode - required free (production/performance-critical)**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText, { autoCleanup: false });
try {
  // Use df
  const result = processData(df);
  return result;
} finally {
  df.free(); // MUST call - always runs
}
```

**2. Free DataFrames in reverse order**:

```javascript
const df1 = rozes.DataFrame.fromCSV(csv1);
const df2 = rozes.DataFrame.fromCSV(csv2);

try {
  // Use df1, df2
} finally {
  df2.free(); // Free in reverse order
  df1.free();
}
```

**3. Extract data before freeing**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);
const ages = df.column('age');

// Copy if you need to keep data after free
const agesCopy = new Float64Array(ages);

df.free(); // Safe - agesCopy is independent

console.log(agesCopy); // Still works
```

**4. Don't use after free**:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);
df.free();

// ❌ BAD - df is invalid after free
console.log(df.shape); // May crash or return garbage
```

---

## Performance Tips

### 1. Zero-Copy Access

Use `column()` for maximum performance:

```javascript
const df = rozes.DataFrame.fromCSV(csvText);

// ✅ FAST - Zero-copy TypedArray access
const ages = df.column('age'); // Direct memory view
const sum = ages.reduce((a, b) => a + b, 0);

// ❌ SLOW - Would require serialization
// (Not available in 1.0.0 anyway)
```

### 2. Batch Operations

Process data in bulk:

```javascript
const df = rozes.DataFrame.fromCSV(largeCSV);

// ✅ FAST - Single TypedArray access
const prices = df.column('price');
const quantities = df.column('quantity');

let total = 0;
for (let i = 0; i < prices.length; i++) {
  total += prices[i] * Number(quantities[i]);
}

df.free();
```

### 3. Reuse Rozes Instance

Initialize once, use many times:

```javascript
// ✅ GOOD - Initialize once
const rozes = await Rozes.init();

function processFile(path) {
  const df = rozes.DataFrame.fromCSVFile(path);
  // ... process
  df.free();
}

// ❌ BAD - Re-initializing is wasteful
async function processFile(path) {
  const rozes = await Rozes.init(); // Loads WASM every time
  // ...
}
```

### 4. Large Files

For large files, consider chunking (stream API coming in 1.1.0):

```javascript
// Current: Load entire file at once
const df = rozes.DataFrame.fromCSVFile('large.csv'); // ~570ms for 1M rows

// Future (1.1.0): Stream processing
// const stream = rozes.DataFrame.fromCSVStream('large.csv', { chunkSize: 10000 });
```

---

## Complete Examples

### Basic Usage

```javascript
const { Rozes } = require('rozes');

async function main() {
  const rozes = await Rozes.init();

  const csv = `name,age,score
Alice,30,95.5
Bob,25,87.3
Charlie,35,91.0`;

  const df = rozes.DataFrame.fromCSV(csv);

  console.log(df.shape); // { rows: 3, cols: 3 }
  console.log(df.columns); // ['name', 'age', 'score']

  const ages = df.column('age');
  console.log(Array.from(ages)); // [30, 25, 35]

  df.free();
}

main();
```

### DataFrame Operations & Chaining

```javascript
const { Rozes } = require('rozes');

async function main() {
  const rozes = await Rozes.init();

  const csv = `name,age,department,salary,years_experience
Alice,30,Engineering,95000,5
Bob,25,Sales,67000,2
Charlie,35,Engineering,110000,10
Diana,28,Marketing,72000,4
Eve,45,Engineering,125000,18
Frank,32,Sales,78000,7`;

  const df = rozes.DataFrame.fromCSV(csv);

  // Example 1: Filter operation
  const adults = df.filter('age', '>=', 30);
  console.log(`${adults.shape.rows} employees age >= 30`);
  adults.free();

  // Example 2: Select specific columns
  const subset = df.select(['age', 'salary', 'years_experience']);
  console.log(`Selected ${subset.shape.cols} columns`);
  subset.free();

  // Example 3: Sort by salary (descending)
  const bySalary = df.sort('salary', true);
  const topSalary = bySalary.column('salary');
  console.log(`Top salary: $${topSalary[0]}`);
  bySalary.free();

  // Example 4: Head and tail
  const top3 = df.head(3);
  const bottom3 = df.tail(3);
  console.log(`First 3 rows: ${top3.shape.rows}`);
  console.log(`Last 3 rows: ${bottom3.shape.rows}`);
  top3.free();
  bottom3.free();

  // Example 5: Operation chaining
  // Goal: Find top 3 highest-paid employees over 30
  const over30 = df.filter('age', '>', 30);
  const highEarners = over30.filter('salary', '>', 70000);
  const sorted = highEarners.sort('salary', true);
  const top3HighEarners = sorted.head(3);

  const salaries = top3HighEarners.column('salary');
  const ages = top3HighEarners.column('age');
  console.log('\nTop 3 highest-paid employees over 30:');
  for (let i = 0; i < salaries.length; i++) {
    console.log(`  ${i + 1}. Age ${ages[i]}, $${salaries[i].toLocaleString()}/year`);
  }

  // Clean up (in reverse order)
  top3HighEarners.free();
  sorted.free();
  highEarners.free();
  over30.free();
  df.free();
}

main();
```

### File I/O

```javascript
const { Rozes } = require('rozes');
const fs = require('fs');

async function main() {
  const rozes = await Rozes.init();

  // Load from file
  const df = rozes.DataFrame.fromCSVFile('data.csv');

  // Access data
  const prices = df.column('price');
  const quantities = df.column('quantity');

  // Calculate total value
  let totalValue = 0;
  for (let i = 0; i < prices.length; i++) {
    totalValue += prices[i] * Number(quantities[i]);
  }

  console.log(`Total: $${totalValue.toFixed(2)}`);

  df.free();
}

main();
```

### Error Handling

```javascript
const { Rozes, RozesError, ErrorCode } = require('rozes');

async function safeParseCSV(csvText) {
  const rozes = await Rozes.init();

  try {
    const df = rozes.DataFrame.fromCSV(csvText);

    // Process data
    const result = {
      rows: df.shape.rows,
      cols: df.shape.cols,
      columns: df.columns
    };

    df.free();
    return { success: true, data: result };

  } catch (err) {
    if (err instanceof RozesError) {
      return {
        success: false,
        error: {
          code: err.code,
          message: err.message
        }
      };
    }
    throw err; // Re-throw unexpected errors
  }
}
```

### TypeScript

```typescript
import { Rozes, DataFrame, CSVOptions, DataFrameShape } from 'rozes';

async function analyzeCSV(filePath: string): Promise<DataFrameShape> {
  const rozes: Rozes = await Rozes.init();

  const options: CSVOptions = {
    delimiter: ',',
    has_headers: true,
    trim_whitespace: true
  };

  const df: DataFrame = rozes.DataFrame.fromCSVFile(filePath, options);

  try {
    const shape: DataFrameShape = df.shape;

    // TypeScript knows column() can return null
    const ages = df.column('age');
    if (ages) {
      const avgAge = ages.reduce((a, b) => a + b) / ages.length;
      console.log(`Average age: ${avgAge}`);
    }

    return shape;
  } finally {
    df.free();
  }
}
```

---

## Known Limitations (1.0.0)

**Features Deferred to 1.1.0**:
- CSV export (`toCSV()`, `toCSVFile()`) - WASM export not yet implemented
- String column access - only numeric columns via `column()`
- Boolean column access - only numeric columns via `column()`
- String/Boolean column filtering/sorting - only numeric operations supported
- GroupBy and Join operations - planned for 1.1.0
- Stream API for large files (>1GB)

**Currently Available (1.0.0)**:
- ✅ Numeric filtering (`filter()` with ==, !=, >, <, >=, <=)
- ✅ Column selection (`select()`)
- ✅ Sorting by numeric columns (`sort()`)
- ✅ Head and tail operations (`head()`, `tail()`)
- ✅ Operation chaining (combine multiple operations)

**Workarounds**:
- For string data: Use Zig API (see [ZIG_API.md](./ZIG_API.md))
- For CSV export: Manually reconstruct from column data
- For GroupBy/Join: Wait for 1.1.0 or use Zig API

---

## Next Steps

- **Migration Guide**: See [MIGRATION.md](./MIGRATION.md) for migrating from Papa Parse, csv-parse, Danfo.js
- **Zig API**: See [ZIG_API.md](./ZIG_API.md) for full DataFrame operations
- **Performance**: See [BENCHMARK_BASELINE_REPORT.md](./BENCHMARK_BASELINE_REPORT.md) for detailed benchmarks
- **Examples**: Browse `examples/node/` directory

---

**Version**: 1.0.0
**Last Updated**: 2025-10-31
**License**: MIT
