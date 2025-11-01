# ETL Data Processing Pipeline Example

Demonstrates Rozes DataFrame capabilities for production-grade ETL workflows:

- **Multi-Table Joins**: Customers ⋈ Products ⋈ Transactions
- **Data Quality Assessment**: Automated validation and reporting
- **Data Cleaning**: Filter invalid records, impute missing values
- **Complex Transformations**: Derived columns, aggregations, rankings
- **Business Analytics**: Customer LTV, product performance, geographic insights
- **Export Pipeline**: Generate multiple output reports
- **Performance Optimization**: Lazy evaluation, predicate pushdown, SIMD/parallel

## Dataset

Three related tables simulating an e-commerce database:

1. **Customers** (1,000 records)
   - Demographics (name, email, country)
   - Segmentation (Premium/Standard/Basic)
   - Status (active/inactive)

2. **Products** (200 records)
   - Catalog info (name, category, brand)
   - Pricing (price, cost)
   - Inventory (stock, availability)

3. **Transactions** (5,000 records)
   - Order details (customer, product, quantity)
   - Financial (amount, calculated profit)
   - Status (completed/pending/cancelled)
   - **Note**: ~5% have data quality issues

## Installation

```bash
npm install
```

## Usage

### 1. Generate Sample Data

```bash
npm run generate-data
```

This creates:
- `customers.csv` (1,000 customers)
- `products.csv` (200 products)
- `transactions.csv` (5,000 transactions with ~5% quality issues)

### 2. Run ETL Pipeline

```bash
npm start
```

### Expected Output

```
=== Rozes ETL Data Processing Pipeline ===

📊 Loading datasets...
✓ Loaded in 68.42ms:
  - Customers: 1,000 records
  - Products: 200 records
  - Transactions: 5,000 records

📊 Phase 1: Data Quality Assessment

📋 Data Quality Report - Transactions
────────────────────────────────────────────────────────────────────────────────
Total Records: 5,000
Columns: 7

⚠️  Data Quality Issues:
  customer_id: 125 issues (2.50%) - 125 nulls, 0 invalid
  quantity: 123 issues (2.46%) - 0 nulls, 123 invalid
  amount: 1,002 issues (20.04%) - 1,002 nulls, 0 invalid

🔄 Phase 2: Data Cleaning & Transformation

🧹 Cleaning Transactions Data...
  Step 1: Filtered to 4,652 valid transactions
  Step 2: Joined with products (4,652 records)
✓ Cleaning complete in 45.23ms

📈 Phase 3: Analytics

💰 Customer Lifetime Value Analysis
────────────────────────────────────────────────────────────────────────────────
customer_id  full_name  country   segment   total_spent  total_profit  transaction_count  avg_order_value
523          Alice      USA       Premium   $45,678.90   $12,345.67    89                $513.13
...

⚡ Computed in 32.45ms
```

## Key Techniques

### Multi-Table Joins

```javascript
// Inner join transactions with products
const withProducts = transactions.join(
  products,
  'product_id',
  'product_id',
  'inner'
);

// Join with customers for enrichment
const enriched = withProducts.join(
  customers,
  'customer_id',
  'customer_id',
  'inner'
);
```

### Data Quality Assessment

```javascript
function assessDataQuality(df, name) {
  const issues = [];

  for (const col of df.columnNames()) {
    const values = df.column(col).toArray();
    const nullCount = values.filter(v => v === null || v === '').length;
    const invalidCount = values.filter(v =>
      typeof v === 'number' && (v < 0 || isNaN(v))
    ).length;

    if (nullCount > 0 || invalidCount > 0) {
      issues.push({ column: col, nulls: nullCount, invalid: invalidCount });
    }
  }

  return issues;
}
```

### Data Cleaning & Transformation

```javascript
// Filter invalid records
const valid = df.filter(row =>
  row.get('customer_id') !== '' &&
  row.get('quantity') > 0 &&
  row.get('status') === 'completed'
);

// Impute missing values
const withAmount = df.withColumn('final_amount', row => {
  const existing = row.get('amount');
  if (existing && existing !== '') return parseFloat(existing);

  // Calculate from price × quantity
  return row.get('price') * row.get('quantity');
});

// Calculate derived metrics
const withProfit = df.withColumn('profit', row =>
  row.get('final_amount') - (row.get('cost') * row.get('quantity'))
);
```

### Customer Lifetime Value

```javascript
const ltv = transactions
  .join(customers, 'customer_id', 'customer_id', 'inner')
  .groupBy('customer_id')
  .agg({
    total_spent: { column: 'amount', fn: 'sum' },
    total_profit: { column: 'profit', fn: 'sum' },
    transaction_count: 'count',
    avg_order_value: { column: 'amount', fn: 'mean' }
  })
  .sortBy('total_profit', 'desc');
```

### Export Pipeline

```javascript
// Export top customers
const csv = ltv.head(100).toCSV();
fs.writeFileSync('output_top_customers.csv', csv);

// Export product performance
const productCSV = productStats.toCSV();
fs.writeFileSync('output_top_products.csv', productCSV);
```

## Performance Notes

- **Data Loading**: ~70ms for 6,200 total records
- **Data Cleaning**: ~45ms (filter + join + transform)
- **Joins**: ~15-20ms per join operation (inner join)
- **GroupBy Aggregations**: ~30ms (with lazy evaluation)
- **CSV Export**: ~10ms per report
- **Total Pipeline**: ~250-350ms end-to-end

With **lazy evaluation** enabled:
- Predicate pushdown reduces rows processed
- Projection pushdown skips unused columns
- Query optimization combines operations

Expected 2-10× speedup on larger datasets (100K+ rows) with SIMD/parallel.

## Output Files

After running, you'll have:

1. **output_top_customers.csv** - Top 10 customers by LTV
2. **output_top_products.csv** - Top 10 products by profit
3. **output_geographic_analysis.csv** - Revenue by country

## Use Cases

- **E-commerce Analytics**: Customer segmentation, product recommendations
- **Business Intelligence**: Sales reporting, performance dashboards
- **Data Warehousing**: ETL for OLAP systems
- **Data Migration**: Transform and clean legacy data
- **API Data Processing**: Aggregate and join API responses

## Advanced Features Demonstrated

- ✅ Multi-table joins (inner, left, right)
- ✅ Data validation and quality checks
- ✅ Missing value imputation
- ✅ Derived column calculations
- ✅ Complex aggregations (sum, mean, count_distinct)
- ✅ Sorting and ranking
- ✅ CSV export with formatting
- ✅ Lazy evaluation and query optimization
- ✅ SIMD/parallel processing

## License

MIT
