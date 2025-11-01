import { Rozes, DataFrame } from 'rozes';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Sales Analytics Dashboard
 *
 * Demonstrates:
 * - CSV parsing with large datasets
 * - GroupBy aggregations (revenue by region, product, salesperson)
 * - Filtering (high-value orders)
 * - Sorting and ranking
 * - Summary statistics (sum, mean, min, max)
 * - Performance benchmarking with SIMD optimizations
 */

async function main() {
  console.log('=== Rozes Sales Analytics Dashboard ===\n');

  // Initialize Rozes
  console.log('🚀 Initializing Rozes...');
  await Rozes.init(); // Auto-detects WASM path

  // Check if data exists
  const dataPath = path.join(__dirname, 'sales_data.csv');
  if (!fs.existsSync(dataPath)) {
    console.log('⚠️  sales_data.csv not found. Run: npm run generate-data');
    process.exit(1);
  }

  // Load sales data
  console.log('📊 Loading sales data...');
  const startLoad = performance.now();
  const csv = fs.readFileSync(dataPath, 'utf8');
  const df = DataFrame.fromCSV(csv);
  const loadTime = performance.now() - startLoad;

  console.log(`✓ Loaded ${df.shape.rows} records in ${loadTime.toFixed(2)}ms`);
  try {
    console.log(`  Columns: ${df.columns.join(', ')}\n`);
  } catch (e) {
    console.log(`  [Column names unavailable: ${e.message}]\n`);
  }

  // 1. Revenue by Region (GroupBy aggregation)
  console.log('1️⃣  Revenue by Region');
  console.log('─'.repeat(60));
  const startRegion = performance.now();
  const byRegion = df.groupBy('region', 'revenue', 'sum');
  const regionTime = performance.now() - startRegion;

  console.log(byRegion.toString());
  const regions = byRegion.column('region');
  const revenues = byRegion.column('revenue');
  console.log('\nRevenue by Region:');
  if (regions && revenues) {
    for (let i = 0; i < Math.min(5, regions.length); i++) {
      const regionName = typeof regions[i] === 'string' ? regions[i] : String.fromCharCode(...new Uint8Array(revenues.buffer, i * 8, 8));
      const revenueVal = typeof revenues[i] === 'number' ? revenues[i] : revenues[i];
      console.log(`  ${regionName}: $${Number(revenueVal).toFixed(2)}`);
    }
  }
  console.log(`⚡ Computed in ${regionTime.toFixed(2)}ms\n`);

  // 2. High-Value Orders (>$10,000)
  console.log('2️⃣  High-Value Orders (Revenue > $10,000)');
  console.log('─'.repeat(60));
  const startFilter = performance.now();
  const highValue = df.filter('revenue', '>', 10000);
  const filterTime = performance.now() - startFilter;

  console.log(highValue.toString());
  console.log(`⚡ Filtered in ${filterTime.toFixed(2)}ms`);
  console.log(`  Found ${highValue.shape.rows} total high-value orders\n`);

  // 3. Top Products by Revenue
  console.log('3️⃣  Products by Revenue');
  console.log('─'.repeat(60));
  const startProduct = performance.now();
  const byProduct = df.groupBy('product', 'revenue', 'sum');
  const productTime = performance.now() - startProduct;

  console.log(byProduct.toString());
  const products = byProduct.column('product');
  const productRevenues = byProduct.column('revenue');
  console.log('\nProducts:');
  if (products && productRevenues) {
    for (let i = 0; i < Math.min(5, products.length); i++) {
      if (products[i] && productRevenues[i]) {
        console.log(`  ${products[i]}: $${Number(productRevenues[i]).toFixed(2)}`);
      }
    }
  }
  console.log(`⚡ Computed in ${productTime.toFixed(2)}ms\n`);

  // 4. Summary Statistics (SIMD-accelerated)
  console.log('4️⃣  Overall Summary Statistics');
  console.log('─'.repeat(60));
  const startStats = performance.now();

  const totalRevenue = df.sum('revenue');
  const totalProfit = df.sum('profit');
  const avgRevenue = df.mean('revenue');
  const minRevenue = df.min('revenue');
  const maxRevenue = df.max('revenue');
  const stdRevenue = df.stddev('revenue');

  const statsTime = performance.now() - startStats;

  console.log(`Total Revenue:     $${totalRevenue.toLocaleString('en-US', { minimumFractionDigits: 2 })}`);
  console.log(`Total Profit:      $${totalProfit.toLocaleString('en-US', { minimumFractionDigits: 2 })}`);
  console.log(`Total Orders:      ${df.shape.rows.toLocaleString('en-US')}`);
  console.log(`Avg Revenue:       $${avgRevenue.toFixed(2)}`);
  console.log(`Min Revenue:       $${minRevenue.toFixed(2)}`);
  console.log(`Max Revenue:       $${maxRevenue.toFixed(2)}`);
  console.log(`Std Dev Revenue:   $${stdRevenue.toFixed(2)}`);
  console.log(`Profit Margin:     ${((totalProfit / totalRevenue) * 100).toFixed(2)}%`);
  console.log(`⚡ Computed in ${statsTime.toFixed(2)}ms (SIMD-accelerated)\n`);

  // 5. Performance Summary
  console.log('5️⃣  Performance Summary');
  console.log('─'.repeat(60));
  const totalAnalysisTime = regionTime + productTime + filterTime + statsTime;

  console.log(`CSV Parsing:           ${loadTime.toFixed(2)}ms`);
  console.log(`Region GroupBy:        ${regionTime.toFixed(2)}ms`);
  console.log(`Product GroupBy:       ${productTime.toFixed(2)}ms`);
  console.log(`High-Value Filter:     ${filterTime.toFixed(2)}ms`);
  console.log(`Summary Stats:         ${statsTime.toFixed(2)}ms`);
  console.log('─'.repeat(60));
  console.log(`Total Analysis Time:   ${totalAnalysisTime.toFixed(2)}ms`);
  console.log(`Total Time (w/ load):  ${(loadTime + totalAnalysisTime).toFixed(2)}ms`);

  console.log('\n✨ Features enabled:');
  console.log('  • SIMD aggregations (sum, mean, min, max, stddev)');
  console.log('  • Parallel CSV parsing');
  console.log('  • Radix hash joins');
  console.log('  • Query optimization');

  console.log('\n✓ Analysis complete!');

  // Cleanup
  df.free();
  byRegion.free();
  highValue.free();
  byProduct.free();
}

main().catch(console.error);
