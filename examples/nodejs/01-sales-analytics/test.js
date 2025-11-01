import { Rozes, DataFrame } from 'rozes';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import assert from 'assert';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Tests for Sales Analytics Example
 *
 * Validates:
 * - CSV parsing performance
 * - GroupBy aggregations
 * - Filtering operations
 * - Summary statistics (SIMD-accelerated)
 * - Memory management
 */

async function runTests() {
  console.log('=== Rozes Sales Analytics Tests ===\n');

  let passedTests = 0;
  let failedTests = 0;

  // Initialize Rozes
  console.log('🚀 Initializing Rozes...');
  await Rozes.init();

  const dataPath = path.join(__dirname, 'sales_data.csv');
  if (!fs.existsSync(dataPath)) {
    console.error('⚠️  sales_data.csv not found. Run: npm run generate-data');
    process.exit(1);
  }

  const csv = fs.readFileSync(dataPath, 'utf8');

  // Test 1: CSV Parsing
  console.log('\n1️⃣  Test: CSV Parsing');
  try {
    const df = DataFrame.fromCSV(csv);
    assert.ok(df.shape.rows === 10000, `Expected 10000 rows, got ${df.shape.rows}`);
    assert.ok(df.shape.cols === 10, `Expected 10 columns, got ${df.shape.cols}`);
    console.log('✅ PASSED: CSV parsed correctly (10000 rows × 10 cols)');
    passedTests++;
    df.free();
  } catch (e) {
    console.error(`❌ FAILED: ${e.message}`);
    failedTests++;
  }

  // Test 2: Column Names
  console.log('\n2️⃣  Test: Column Names');
  try {
    const df = DataFrame.fromCSV(csv);
    const columns = df.columns;
    const expectedColumns = ['order_id', 'date', 'product', 'region', 'salesperson', 'quantity', 'unit_price', 'revenue', 'cost', 'profit'];
    assert.deepStrictEqual(columns, expectedColumns, 'Column names mismatch');
    console.log('✅ PASSED: All column names correct');
    passedTests++;
    df.free();
  } catch (e) {
    console.error(`❌ FAILED: ${e.message}`);
    failedTests++;
  }

  // Test 3: GroupBy Aggregation (Region)
  console.log('\n3️⃣  Test: GroupBy Aggregation (Region)');
  try {
    const df = DataFrame.fromCSV(csv);
    const byRegion = df.groupBy('region', 'revenue', 'sum');

    assert.ok(byRegion.shape.rows > 0, 'GroupBy should return rows');
    assert.ok(byRegion.shape.cols === 2, `Expected 2 columns, got ${byRegion.shape.cols}`);

    const regions = byRegion.column('region');
    const revenues = byRegion.column('revenue_Sum'); // Note: Column name is revenue_Sum after aggregation

    assert.ok(regions !== null, 'Region column should exist');
    assert.ok(revenues !== null, 'Revenue_Sum column should exist');
    assert.ok(revenues.length === regions.length, 'Columns should have same length');

    console.log(`✅ PASSED: GroupBy produced ${byRegion.shape.rows} regions`);
    passedTests++;
    df.free();
    byRegion.free();
  } catch (e) {
    console.error(`❌ FAILED: ${e.message}`);
    failedTests++;
  }

  // Test 4: Filter Operation
  console.log('\n4️⃣  Test: Filter (Revenue > $10,000)');
  try {
    const df = DataFrame.fromCSV(csv);
    const filtered = df.filter('revenue', '>', 10000);

    assert.ok(filtered.shape.rows > 0, 'Filter should return some rows');
    assert.ok(filtered.shape.rows < df.shape.rows, 'Filter should reduce row count');
    assert.ok(filtered.shape.cols === df.shape.cols, 'Filter should preserve all columns');

    // Verify all filtered rows have revenue > 10000
    const revenues = filtered.column('revenue');
    for (let i = 0; i < Math.min(100, revenues.length); i++) {
      assert.ok(revenues[i] > 10000, `Row ${i} has revenue ${revenues[i]} <= 10000`);
    }

    console.log(`✅ PASSED: Filtered to ${filtered.shape.rows} high-value orders`);
    passedTests++;
    df.free();
    filtered.free();
  } catch (e) {
    console.error(`❌ FAILED: ${e.message}`);
    failedTests++;
  }

  // Test 5: SIMD Aggregations (sum)
  console.log('\n5️⃣  Test: SIMD Sum Aggregation');
  try {
    const df = DataFrame.fromCSV(csv);
    const totalRevenue = df.sum('revenue');
    const totalProfit = df.sum('profit');

    assert.ok(totalRevenue > 0, 'Total revenue should be positive');
    assert.ok(totalProfit > 0, 'Total profit should be positive');
    assert.ok(totalProfit < totalRevenue, 'Profit should be less than revenue');

    console.log(`✅ PASSED: Sum computed (Revenue: $${totalRevenue.toFixed(2)})`);
    passedTests++;
    df.free();
  } catch (e) {
    console.error(`❌ FAILED: ${e.message}`);
    failedTests++;
  }

  // Test 6: SIMD Aggregations (mean, min, max)
  console.log('\n6️⃣  Test: SIMD Statistical Aggregations');
  try {
    const df = DataFrame.fromCSV(csv);
    const meanRevenue = df.mean('revenue');
    const minRevenue = df.min('revenue');
    const maxRevenue = df.max('revenue');
    const stdRevenue = df.stddev('revenue');

    assert.ok(meanRevenue > 0, 'Mean revenue should be positive');
    assert.ok(minRevenue > 0, 'Min revenue should be positive');
    assert.ok(maxRevenue > minRevenue, 'Max should be greater than min');
    assert.ok(meanRevenue >= minRevenue && meanRevenue <= maxRevenue, 'Mean should be between min and max');
    assert.ok(stdRevenue > 0, 'Std dev should be positive');

    console.log(`✅ PASSED: Stats computed (Mean: $${meanRevenue.toFixed(2)}, Range: $${minRevenue.toFixed(2)}-$${maxRevenue.toFixed(2)})`);
    passedTests++;
    df.free();
  } catch (e) {
    console.error(`❌ FAILED: ${e.message}`);
    failedTests++;
  }

  // Test 7: CSV Export (toCSV)
  console.log('\n7️⃣  Test: CSV Export');
  try {
    const df = DataFrame.fromCSV('name,age,score\nAlice,30,95.5\nBob,25,87.3\n');
    const csvOut = df.toCSV();

    assert.ok(csvOut.includes('name'), 'CSV should include headers');
    assert.ok(csvOut.includes('Alice'), 'CSV should include data');
    assert.ok(csvOut.includes('30'), 'CSV should include numeric data');

    // Parse it back
    const df2 = DataFrame.fromCSV(csvOut);
    assert.ok(df2.shape.rows === df.shape.rows, 'Round-trip should preserve row count');
    assert.ok(df2.shape.cols === df.shape.cols, 'Round-trip should preserve column count');

    console.log('✅ PASSED: CSV export and round-trip successful');
    passedTests++;
    df.free();
    df2.free();
  } catch (e) {
    console.error(`❌ FAILED: ${e.message}`);
    failedTests++;
  }

  // Test 8: Memory Management (free)
  console.log('\n8️⃣  Test: Memory Management');
  try {
    const df = DataFrame.fromCSV('x,y\n1,2\n3,4\n');
    df.free();

    // Trying to use freed DataFrame should throw
    let threw = false;
    try {
      df.shape; // Should throw "DataFrame has been freed"
    } catch (e) {
      threw = true;
      assert.ok(e.message.includes('freed'), 'Should throw "freed" error');
    }

    assert.ok(threw, 'Accessing freed DataFrame should throw');
    console.log('✅ PASSED: Memory management works correctly');
    passedTests++;
  } catch (e) {
    console.error(`❌ FAILED: ${e.message}`);
    failedTests++;
  }

  // Test 9: Performance Benchmark (CSV Parsing)
  console.log('\n9️⃣  Test: Performance Benchmark (CSV Parsing)');
  try {
    const start = performance.now();
    const df = DataFrame.fromCSV(csv);
    const duration = performance.now() - start;

    // Should parse 10K rows in < 100ms
    assert.ok(duration < 100, `Parsing took ${duration.toFixed(2)}ms, expected < 100ms`);

    console.log(`✅ PASSED: Parsed 10K rows in ${duration.toFixed(2)}ms`);
    passedTests++;
    df.free();
  } catch (e) {
    console.error(`❌ FAILED: ${e.message}`);
    failedTests++;
  }

  // Test 10: Performance Benchmark (Aggregations)
  console.log('\n🔟 Test: Performance Benchmark (SIMD Aggregations)');
  try {
    const df = DataFrame.fromCSV(csv);

    const start = performance.now();
    df.sum('revenue');
    df.mean('revenue');
    df.min('revenue');
    df.max('revenue');
    df.stddev('revenue');
    const duration = performance.now() - start;

    // 5 aggregations on 10K rows should be < 5ms
    assert.ok(duration < 5, `Aggregations took ${duration.toFixed(2)}ms, expected < 5ms`);

    console.log(`✅ PASSED: 5 SIMD aggregations in ${duration.toFixed(2)}ms`);
    passedTests++;
    df.free();
  } catch (e) {
    console.error(`❌ FAILED: ${e.message}`);
    failedTests++;
  }

  // Summary
  console.log('\n' + '='.repeat(60));
  console.log(`Test Results: ${passedTests} passed, ${failedTests} failed`);
  console.log('='.repeat(60));

  if (failedTests > 0) {
    process.exit(1);
  }
}

runTests().catch((e) => {
  console.error('Test suite failed:', e);
  process.exit(1);
});
