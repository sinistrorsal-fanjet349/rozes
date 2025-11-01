# Rozes Node.js Examples

Real-world examples demonstrating Rozes DataFrame capabilities in Node.js environments.

## Examples

### 1. Sales Analytics Dashboard

**Path**: `01-sales-analytics/`

**Demonstrates**:
- Large dataset analysis (10,000 sales records)
- GroupBy aggregations (revenue by region, product, salesperson)
- Filtering and sorting (high-value orders)
- Derived columns (monthly trends)
- Summary statistics (total revenue, profit margins)
- Performance benchmarking

**Use Case**: Business intelligence, sales reporting, KPI dashboards

**Run**:
```bash
cd 01-sales-analytics
npm install
npm run generate-data
npm start
```

**Dataset**: 10,000 sales records across 2024 with order details, regional data, and financial metrics.

**Performance**: ~150ms total analysis time on 10K rows with SIMD/parallel optimizations.

---

### 2. Time Series Analysis

**Path**: `02-time-series/`

**Demonstrates**:
- High-frequency time series data (15-minute intervals)
- Rolling window aggregations (moving averages)
- Resampling (minute → hour → day)
- Anomaly detection (statistical outlier detection)
- Correlation analysis (multi-variate relationships)
- IoT sensor data processing

**Use Case**: Environmental monitoring, industrial IoT, smart buildings, predictive maintenance

**Run**:
```bash
cd 02-time-series
npm install
npm run generate-data
npm start
```

**Dataset**: 30 days of sensor data from 5 IoT devices (14,400 readings) with temperature, humidity, pressure, light, and CO2 metrics.

**Performance**: ~200ms total analysis time including resampling, rolling windows, and anomaly detection.

---

### 3. ETL Data Processing Pipeline

**Path**: `03-data-pipeline/`

**Demonstrates**:
- Multi-table joins (customers ⋈ products ⋈ transactions)
- Data quality assessment and reporting
- Data cleaning (filter invalid, impute missing values)
- Complex transformations (derived columns)
- Business analytics (customer LTV, product performance)
- Export pipeline (multiple output formats)
- Lazy evaluation and query optimization

**Use Case**: E-commerce analytics, business intelligence, data warehousing, API data processing

**Run**:
```bash
cd 03-data-pipeline
npm install
npm run generate-data
npm start
```

**Dataset**:
- 1,000 customers
- 200 products
- 5,000 transactions (with ~5% quality issues)

**Performance**: ~350ms end-to-end pipeline including joins, transformations, analytics, and exports.

---

## Installation

Each example is self-contained with its own `package.json`. Install dependencies per example:

```bash
cd <example-directory>
npm install
```

All examples use a local reference to the Rozes package (`"rozes": "file:../../.."`), so ensure you've built Rozes first:

```bash
# From the rozes root directory
zig build -Dtarget=wasm32-freestanding -Doptimize=ReleaseSmall
```

## Quick Start

### Run All Examples

```bash
# From examples/nodejs/
for dir in 01-* 02-* 03-*; do
  echo "Running $dir..."
  cd $dir
  npm install
  npm run generate-data
  npm start
  cd ..
  echo ""
done
```

### Run Single Example

```bash
cd 01-sales-analytics
npm install
npm run generate-data
npm start
```

## Common Patterns

### Loading CSV Data

```javascript
import Rozes from 'rozes';
import fs from 'fs';

const csv = fs.readFileSync('data.csv', 'utf8');
const df = Rozes.fromCSV(csv);
```

### GroupBy Aggregations

```javascript
const stats = df
  .groupBy('category')
  .agg({
    total: { column: 'amount', fn: 'sum' },
    average: { column: 'amount', fn: 'mean' },
    count: 'count'
  })
  .sortBy('total', 'desc');
```

### Filtering

```javascript
const filtered = df.filter(row => row.get('amount') > 1000);
```

### Derived Columns

```javascript
const withProfit = df.withColumn('profit', row => {
  const revenue = row.get('revenue');
  const cost = row.get('cost');
  return revenue - cost;
});
```

### Joins

```javascript
const joined = df1.join(df2, 'key_column', 'key_column', 'inner');
```

### Export

```javascript
const csv = df.toCSV();
fs.writeFileSync('output.csv', csv);
```

## Performance Features

All examples demonstrate Rozes' performance optimizations:

- **SIMD Aggregations**: 2-10× speedup on sum, mean, min, max operations
- **Parallel CSV Parsing**: 2-4× faster type inference on large files
- **Parallel Operations**: 2-6× speedup on filter, sort, groupBy for >100K rows
- **Lazy Evaluation**: Query optimization with predicate/projection pushdown
- **Radix Hash Join**: 1.5-3× speedup on integer key joins

Expected performance (with optimizations):
- CSV Parsing: ~500K-1M rows/sec
- Filter: ~50-100M rows/sec
- Sort: ~10-20M rows/sec
- GroupBy: ~20-50M rows/sec
- Joins: ~100K-1M rows/sec

## Requirements

- **Node.js**: 18+ (ESM support)
- **Rozes**: Built for `wasm32-freestanding` target
- **Zig**: 0.15+ (for building Rozes)

## Troubleshooting

### "Cannot find module 'rozes'"

Ensure you've built the Rozes WASM module:

```bash
cd ../../..  # Back to rozes root
zig build -Dtarget=wasm32-freestanding -Doptimize=ReleaseSmall
```

### "File not found" errors

Generate sample data first:

```bash
npm run generate-data
```

### Slow performance

1. Ensure you're using `ReleaseSmall` or `ReleaseFast` build mode
2. Check that SIMD/parallel features are enabled (look for ✨ messages in output)
3. Test with larger datasets (optimizations shine on >100K rows)

## License

MIT

---

**Contributing**: Found a bug or have a feature request? Open an issue at [https://github.com/yourusername/rozes](https://github.com/yourusername/rozes)
