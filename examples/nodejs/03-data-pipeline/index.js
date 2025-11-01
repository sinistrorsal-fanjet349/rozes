/**
 * ⚠️  NOTE: This example uses future API features planned for Milestone 1.4.0
 *
 * Features used that are not yet implemented:
 * - withColumn() - Adding computed columns
 * - filter() with JavaScript callbacks
 * - groupBy() with complex aggregations (agg() method)
 * - Multi-column groupBy
 * - Column methods: .toArray(), .unique()
 * - Helper methods: .rowCount(), .columnNames()
 *
 * This is an aspirational example showing the planned API.
 * See docs/TODO.md for the implementation timeline.
 */

import { Rozes, DataFrame } from 'rozes';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * ETL Data Processing Pipeline
 *
 * Demonstrates:
 * - Multi-table joins (customers, products, transactions)
 * - Data validation and cleaning
 * - Derived column calculations
 * - Data quality reporting
 * - Complex transformations
 * - Export to multiple formats
 * - Performance optimization with lazy evaluation
 */

/**
 * Load datasets
 */
function loadDatasets() {
  console.log('📊 Loading datasets...');
  const start = performance.now();

  const customers = DataFrame.fromCSV(fs.readFileSync('customers.csv', 'utf8'));
  const products = DataFrame.fromCSV(fs.readFileSync('products.csv', 'utf8'));
  const transactions = DataFrame.fromCSV(fs.readFileSync('transactions.csv', 'utf8'));

  const loadTime = performance.now() - start;

  console.log(`✓ Loaded in ${loadTime.toFixed(2)}ms:`);
  console.log(`  - Customers: ${customers.shape.rows} records`);
  console.log(`  - Products: ${products.shape.rows} records`);
  console.log(`  - Transactions: ${transactions.shape.rows} records\n`);

  return { customers, products, transactions, loadTime };
}

/**
 * Data Quality Assessment
 */
function assessDataQuality(df, name) {
  console.log(`📋 Data Quality Report - ${name}`);
  console.log('─'.repeat(80));

  const totalRows = df.shape.rows;
  const columns = df.columns;

  console.log(`Total Records: ${totalRows.toLocaleString()}`);
  console.log(`Columns: ${columns.length}\n`);

  // Check for invalid values per column
  const issues = [];

  for (const col of columns) {
    const values = df.column(col); // Already returns TypedArray or string[]

    // Count nulls/empty strings
    let nullCount = 0;
    let invalidCount = 0;

    for (const val of values) {
      if (val === null || val === undefined || val === '') {
        nullCount++;
      } else if (typeof val === 'number' && (val < 0 || isNaN(val))) {
        invalidCount++;
      }
    }

    if (nullCount > 0 || invalidCount > 0) {
      issues.push({
        column: col,
        nulls: nullCount,
        invalid: invalidCount,
        total: nullCount + invalidCount,
        percentage: ((nullCount + invalidCount) / totalRows * 100).toFixed(2)
      });
    }
  }

  if (issues.length === 0) {
    console.log('✓ No data quality issues detected\n');
  } else {
    console.log('⚠️  Data Quality Issues:');
    for (const issue of issues) {
      console.log(`  ${issue.column}: ${issue.total} issues (${issue.percentage}%) - ${issue.nulls} nulls, ${issue.invalid} invalid`);
    }
    console.log();
  }

  return issues;
}

/**
 * Clean and validate transactions
 */
function cleanTransactions(transactions, products) {
  console.log('🧹 Cleaning Transactions Data...');
  const start = performance.now();

  // Step 1: Filter out invalid transactions
  const validTransactions = transactions.filter(row => {
    const customerId = row.get('customer_id');
    const quantity = row.get('quantity');
    const status = row.get('status');

    // Keep only completed transactions with valid data
    return (
      customerId !== '' &&
      customerId !== null &&
      quantity > 0 &&
      status === 'completed'
    );
  });

  console.log(`  Step 1: Filtered to ${validTransactions.shape.rows} valid transactions`);

  // Step 2: Join with products to get price information
  const withProducts = validTransactions.join(
    products,
    'product_id',
    'product_id',
    'inner'
  );

  console.log(`  Step 2: Joined with products (${withProducts.shape.rows} records)`);

  // Step 3: Calculate missing amounts
  const withAmount = withProducts.withColumn('final_amount', row => {
    const existingAmount = row.get('amount');
    if (existingAmount && existingAmount !== '') {
      return parseFloat(existingAmount);
    }
    // Calculate from price and quantity
    const price = row.get('price');
    const quantity = row.get('quantity');
    return Math.round(price * quantity * 100) / 100;
  });

  // Step 4: Calculate profit
  const withProfit = withAmount.withColumn('profit', row => {
    const amount = row.get('final_amount');
    const cost = row.get('cost');
    const quantity = row.get('quantity');
    return Math.round((amount - (cost * quantity)) * 100) / 100;
  });

  const cleanTime = performance.now() - start;
  console.log(`✓ Cleaning complete in ${cleanTime.toFixed(2)}ms\n`);

  return { cleaned: withProfit, cleanTime };
}

/**
 * Customer Lifetime Value Analysis
 */
function analyzeCustomerLTV(transactions, customers) {
  console.log('💰 Customer Lifetime Value Analysis');
  console.log('─'.repeat(80));
  const start = performance.now();

  // Join transactions with customers
  const customerTransactions = transactions.join(
    customers,
    'customer_id',
    'customer_id',
    'inner'
  );

  // Calculate LTV by customer
  const ltv = customerTransactions
    .groupBy('customer_id')
    .agg({
      full_name: { column: 'first_name', fn: 'first' }, // Get first name
      country: 'first',
      segment: 'first',
      total_spent: { column: 'final_amount', fn: 'sum' },
      total_profit: { column: 'profit', fn: 'sum' },
      transaction_count: 'count',
      avg_order_value: { column: 'final_amount', fn: 'mean' }
    })
    .sortBy('total_profit', 'desc')
    .head(10);

  const ltvTime = performance.now() - start;

  console.log(ltv.toString());
  console.log(`⚡ Computed in ${ltvTime.toFixed(2)}ms\n`);

  return { ltv, ltvTime };
}

/**
 * Product Performance Analysis
 */
function analyzeProductPerformance(transactions, products) {
  console.log('📦 Product Performance Analysis');
  console.log('─'.repeat(80));
  const start = performance.now();

  // Already have product info from join
  const productStats = transactions
    .groupBy('product_id')
    .agg({
      product_name: 'first',
      category: 'first',
      brand: 'first',
      units_sold: { column: 'quantity', fn: 'sum' },
      revenue: { column: 'final_amount', fn: 'sum' },
      profit: { column: 'profit', fn: 'sum' },
      orders: 'count',
      avg_price: { column: 'price', fn: 'mean' }
    })
    .withColumn('profit_margin', row => {
      const profit = row.get('profit');
      const revenue = row.get('revenue');
      return revenue > 0 ? Math.round((profit / revenue * 100) * 100) / 100 : 0;
    })
    .sortBy('profit', 'desc')
    .head(10);

  const perfTime = performance.now() - start;

  console.log(productStats.toString());
  console.log(`⚡ Computed in ${perfTime.toFixed(2)}ms\n`);

  return { productStats, perfTime };
}

/**
 * Geographic Analysis
 */
function analyzeByGeography(transactions, customers) {
  console.log('🌍 Geographic Analysis');
  console.log('─'.repeat(80));
  const start = performance.now();

  const customerTransactions = transactions.join(
    customers,
    'customer_id',
    'customer_id',
    'inner'
  );

  const geoStats = customerTransactions
    .groupBy('country')
    .agg({
      customers: { column: 'customer_id', fn: 'count_distinct' },
      revenue: { column: 'final_amount', fn: 'sum' },
      profit: { column: 'profit', fn: 'sum' },
      orders: 'count',
      avg_order_value: { column: 'final_amount', fn: 'mean' }
    })
    .sortBy('revenue', 'desc');

  const geoTime = performance.now() - start;

  console.log(geoStats.toString());
  console.log(`⚡ Computed in ${geoTime.toFixed(2)}ms\n`);

  return { geoStats, geoTime };
}

/**
 * Main pipeline
 */
async function main() {
  console.log('=== Rozes ETL Data Processing Pipeline ===\n');

  // Check if data exists
  const requiredFiles = ['customers.csv', 'products.csv', 'transactions.csv'];
  for (const file of requiredFiles) {
    if (!fs.existsSync(file)) {
      console.log(`⚠️  ${file} not found. Run: npm run generate-data`);
      process.exit(1);
    }
  }

  // Initialize Rozes
  console.log('🚀 Initializing Rozes...');
  const wasmPath = path.join(__dirname, '../../../zig-out/bin/rozes.wasm');
  await Rozes.init(wasmPath);

  // Load datasets
  const { customers, products, transactions, loadTime } = loadDatasets();

  // Assess data quality
  console.log('📊 Phase 1: Data Quality Assessment\n');
  assessDataQuality(customers, 'Customers');
  assessDataQuality(products, 'Products');
  const transactionIssues = assessDataQuality(transactions, 'Transactions');

  // Clean and transform
  console.log('🔄 Phase 2: Data Cleaning & Transformation\n');
  const { cleaned, cleanTime } = cleanTransactions(transactions, products);

  // Analyses
  console.log('📈 Phase 3: Analytics\n');
  const { ltv, ltvTime } = analyzeCustomerLTV(cleaned, customers);
  const { productStats, perfTime } = analyzeProductPerformance(cleaned, products);
  const { geoStats, geoTime } = analyzeByGeography(cleaned, customers);

  // Export results
  console.log('💾 Phase 4: Export Results\n');
  const exportStart = performance.now();

  // Export top customers
  const topCustomersCSV = ltv.toCSV();
  fs.writeFileSync('output_top_customers.csv', topCustomersCSV);
  console.log('✓ Exported output_top_customers.csv');

  // Export top products
  const topProductsCSV = productStats.toCSV();
  fs.writeFileSync('output_top_products.csv', topProductsCSV);
  console.log('✓ Exported output_top_products.csv');

  // Export geographic stats
  const geoStatsCSV = geoStats.toCSV();
  fs.writeFileSync('output_geographic_analysis.csv', geoStatsCSV);
  console.log('✓ Exported output_geographic_analysis.csv');

  const exportTime = performance.now() - exportStart;
  console.log(`⚡ Exports completed in ${exportTime.toFixed(2)}ms\n`);

  // Performance summary
  console.log('⚡ Performance Summary');
  console.log('─'.repeat(80));
  const totalTime = loadTime + cleanTime + ltvTime + perfTime + geoTime + exportTime;

  console.log(`Data Loading:          ${loadTime.toFixed(2)}ms`);
  console.log(`Data Cleaning:         ${cleanTime.toFixed(2)}ms`);
  console.log(`LTV Analysis:          ${ltvTime.toFixed(2)}ms`);
  console.log(`Product Analysis:      ${perfTime.toFixed(2)}ms`);
  console.log(`Geographic Analysis:   ${geoTime.toFixed(2)}ms`);
  console.log(`Export Results:        ${exportTime.toFixed(2)}ms`);
  console.log('─'.repeat(80));
  console.log(`Total Pipeline Time:   ${totalTime.toFixed(2)}ms`);

  console.log('\n✓ ETL pipeline complete!');
  console.log(`✓ Processed ${transactions.shape.rows} transactions`);
  console.log(`✓ Generated 3 output reports`);
}

main().catch(console.error);
