# Sales Analytics Dashboard Example

Demonstrates Rozes DataFrame capabilities for real-world sales analytics:

- **GroupBy Aggregations**: Revenue by region, product, salesperson
- **Filtering**: High-value orders (>$10K)
- **Sorting**: Ranking by revenue, profit
- **Derived Columns**: Monthly trends from dates
- **Summary Statistics**: Total revenue, profit margins, averages
- **Performance**: Benchmarking with SIMD/parallel optimizations

## Dataset

10,000 sales records with:
- Order details (ID, date, product, quantity, pricing)
- Geographic data (region)
- Sales attribution (salesperson)
- Financial metrics (revenue, cost, profit)

## Installation

```bash
npm install
```

**Note**: This example uses the published `rozes` npm package. Make sure Rozes is published to npm before running.

## Usage

### 1. Generate Sample Data

```bash
npm run generate-data
```

This creates `sales_data.csv` with 10,000 sales records spanning 2024.

### 2. Run Analytics

```bash
npm start
```

### Expected Output

```
=== Rozes Sales Analytics Dashboard ===

📊 Loading sales data...
✓ Loaded 10,000 records in 45.23ms
  Columns: order_id, date, product, region, salesperson, quantity, unit_price, revenue, cost, profit

1️⃣  Revenue by Region
────────────────────────────────────────────────────────────
region  revenue      profit       orders
East    $5,234,567   $1,890,234   2,045
North   $4,987,654   $1,765,432   1,987
...

⚡ Computed in 12.34ms
```

## Key Techniques

### GroupBy Aggregations

```javascript
const byRegion = df
  .groupBy('region')
  .agg({
    revenue: 'sum',
    profit: 'sum',
    orders: 'count'
  })
  .sortBy('revenue', 'desc');
```

### Filtering with Predicates

```javascript
const highValue = df
  .filter(row => row.get('revenue') > 10000)
  .sortBy('revenue', 'desc');
```

### Derived Columns

```javascript
const withMonth = df.withColumn('month', row => {
  const date = row.get('date');
  return date.substring(0, 7); // Extract YYYY-MM
});
```

### Column Statistics

```javascript
const totalRevenue = df.column('revenue').sum();
const avgOrderValue = df.column('revenue').mean();
const profitMargin = (totalProfit / totalRevenue) * 100;
```

## Performance Notes

- **CSV Parsing**: ~45ms for 10K rows (SIMD-optimized)
- **GroupBy**: ~10-15ms per aggregation (parallel-optimized)
- **Filter**: ~5ms for predicate evaluation (SIMD-optimized)
- **Total Analysis**: ~100-150ms for complete dashboard

On larger datasets (100K+ rows), expect 2-10× speedup with SIMD/parallel optimizations.

## License

MIT
