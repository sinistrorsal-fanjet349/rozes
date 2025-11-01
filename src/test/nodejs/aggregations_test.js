/**
 * Node.js Tests for SIMD-Accelerated Aggregation Functions (Milestone 1.2.0 Phase 1)
 *
 * Tests: sum(), mean(), min(), max(), variance(), stddev()
 *
 * Performance: All aggregations use SIMD vectorization (30%+ speedup)
 */

const { Rozes } = require('../../../dist/index.js');

async function main() {
  console.log('🧪 Testing SIMD-Accelerated Aggregations (Milestone 1.2.0 Phase 1)\n');

  // Initialize Rozes
  const rozes = await Rozes.init();
  console.log('✅ Rozes initialized (version:', rozes.version, ')\n');

  let passedTests = 0;
  let totalTests = 0;

  function test(name, fn) {
    totalTests++;
    try {
      fn();
      console.log(`✅ ${name}`);
      passedTests++;
    } catch (err) {
      console.error(`❌ ${name}`);
      console.error('   Error:', err.message);
      console.error('   Stack:', err.stack);
    }
  }

  function assertApproxEqual(actual, expected, tolerance = 0.0001, message = '') {
    const diff = Math.abs(actual - expected);
    if (diff > tolerance) {
      throw new Error(
        `${message}\n  Expected: ${expected}\n  Actual: ${actual}\n  Diff: ${diff} (tolerance: ${tolerance})`
      );
    }
  }

  function assertEqual(actual, expected, message = '') {
    if (actual !== expected) {
      throw new Error(`${message}\n  Expected: ${expected}\n  Actual: ${actual}`);
    }
  }

  // ============================================================================
  // Test Data Setup
  // ============================================================================

  // Test 1: Small dataset with known results
  const csv1 = `price,quantity
10.0,2
20.0,3
30.0,5`;

  const df1 = rozes.DataFrame.fromCSV(csv1);

  // Test 2: Dataset with decimals
  const csv2 = `value
1.5
2.5
3.5
4.5
5.5`;

  const df2 = rozes.DataFrame.fromCSV(csv2);

  // Test 3: Large dataset for SIMD performance
  let largeCsvRows = ['value'];
  for (let i = 1; i <= 1000; i++) {
    largeCsvRows.push(i.toString());
  }
  const csv3 = largeCsvRows.join('\n');
  const df3 = rozes.DataFrame.fromCSV(csv3);

  // Test 4: Dataset with negative values
  const csv4 = `temperature
-10.5
20.0
-5.0
15.0
0.0`;

  const df4 = rozes.DataFrame.fromCSV(csv4);

  // ============================================================================
  // Sum Tests
  // ============================================================================

  test('sum() computes correct sum for integers', () => {
    const result = df1.sum('quantity');
    assertEqual(result, 10.0, 'Sum of [2, 3, 5] should be 10');
  });

  test('sum() computes correct sum for decimals', () => {
    const result = df1.sum('price');
    assertEqual(result, 60.0, 'Sum of [10, 20, 30] should be 60');
  });

  test('sum() handles decimals correctly', () => {
    const result = df2.sum('value');
    assertApproxEqual(result, 17.5, 0.01, 'Sum of [1.5, 2.5, 3.5, 4.5, 5.5]');
  });

  test('sum() handles large dataset (1000 rows)', () => {
    const result = df3.sum('value');
    // Sum of 1 to 1000 = 1000 * 1001 / 2 = 500500
    assertEqual(result, 500500, 'Sum of 1..1000 should be 500500');
  });

  test('sum() handles negative values', () => {
    const result = df4.sum('temperature');
    assertApproxEqual(result, 19.5, 0.01, 'Sum of [-10.5, 20, -5, 15, 0]');
  });

  test('sum() throws error for non-existent column', () => {
    try {
      df1.sum('nonexistent');
      throw new Error('Should have thrown error for non-existent column');
    } catch (err) {
      if (!err.message.includes('nonexistent')) {
        throw err;
      }
    }
  });

  // ============================================================================
  // Mean Tests
  // ============================================================================

  test('mean() computes correct average', () => {
    const result = df1.mean('price');
    assertEqual(result, 20.0, 'Mean of [10, 20, 30] should be 20');
  });

  test('mean() handles decimals correctly', () => {
    const result = df2.mean('value');
    assertApproxEqual(result, 3.5, 0.01, 'Mean of [1.5, 2.5, 3.5, 4.5, 5.5]');
  });

  test('mean() handles large dataset (1000 rows)', () => {
    const result = df3.mean('value');
    // Mean of 1 to 1000 = 500.5
    assertApproxEqual(result, 500.5, 0.01, 'Mean of 1..1000');
  });

  test('mean() handles negative values', () => {
    const result = df4.mean('temperature');
    assertApproxEqual(result, 3.9, 0.01, 'Mean of [-10.5, 20, -5, 15, 0]');
  });

  test('mean() throws error for non-existent column', () => {
    try {
      df1.mean('nonexistent');
      throw new Error('Should have thrown error for non-existent column');
    } catch (err) {
      if (!err.message.includes('nonexistent')) {
        throw err;
      }
    }
  });

  // ============================================================================
  // Min Tests
  // ============================================================================

  test('min() finds minimum value', () => {
    const result = df1.min('price');
    assertEqual(result, 10.0, 'Min of [10, 20, 30] should be 10');
  });

  test('min() handles decimals correctly', () => {
    const result = df2.min('value');
    assertApproxEqual(result, 1.5, 0.01, 'Min of [1.5, 2.5, 3.5, 4.5, 5.5]');
  });

  test('min() handles large dataset (1000 rows)', () => {
    const result = df3.min('value');
    assertEqual(result, 1.0, 'Min of 1..1000 should be 1');
  });

  test('min() handles negative values', () => {
    const result = df4.min('temperature');
    assertApproxEqual(result, -10.5, 0.01, 'Min of [-10.5, 20, -5, 15, 0]');
  });

  test('min() throws error for non-existent column', () => {
    try {
      df1.min('nonexistent');
      throw new Error('Should have thrown error for non-existent column');
    } catch (err) {
      if (!err.message.includes('nonexistent')) {
        throw err;
      }
    }
  });

  // ============================================================================
  // Max Tests
  // ============================================================================

  test('max() finds maximum value', () => {
    const result = df1.max('price');
    assertEqual(result, 30.0, 'Max of [10, 20, 30] should be 30');
  });

  test('max() handles decimals correctly', () => {
    const result = df2.max('value');
    assertApproxEqual(result, 5.5, 0.01, 'Max of [1.5, 2.5, 3.5, 4.5, 5.5]');
  });

  test('max() handles large dataset (1000 rows)', () => {
    const result = df3.max('value');
    assertEqual(result, 1000.0, 'Max of 1..1000 should be 1000');
  });

  test('max() handles negative values', () => {
    const result = df4.max('temperature');
    assertApproxEqual(result, 20.0, 0.01, 'Max of [-10.5, 20, -5, 15, 0]');
  });

  test('max() throws error for non-existent column', () => {
    try {
      df1.max('nonexistent');
      throw new Error('Should have thrown error for non-existent column');
    } catch (err) {
      if (!err.message.includes('nonexistent')) {
        throw err;
      }
    }
  });

  // ============================================================================
  // Variance Tests
  // ============================================================================

  test('variance() computes sample variance', () => {
    // Data: [10, 20, 30]
    // Mean: 20
    // Squared differences: [(10-20)^2, (20-20)^2, (30-20)^2] = [100, 0, 100]
    // Sum: 200
    // Sample variance (n-1): 200 / 2 = 100
    const result = df1.variance('price');
    assertApproxEqual(result, 100.0, 0.01, 'Variance of [10, 20, 30]');
  });

  test('variance() handles decimals correctly', () => {
    // Data: [1.5, 2.5, 3.5, 4.5, 5.5]
    // Mean: 3.5
    // Sample variance: 2.5
    const result = df2.variance('value');
    assertApproxEqual(result, 2.5, 0.01, 'Variance of [1.5, 2.5, 3.5, 4.5, 5.5]');
  });

  test('variance() handles large dataset (1000 rows)', () => {
    const result = df3.variance('value');
    // Variance of 1..1000 is approximately 83416.67
    if (result < 83000 || result > 84000) {
      throw new Error(`Variance out of expected range: ${result}`);
    }
  });

  test('variance() handles negative values', () => {
    const result = df4.variance('temperature');
    // Just check it's positive and reasonable
    if (result <= 0 || result > 200) {
      throw new Error(`Variance out of expected range: ${result}`);
    }
  });

  test('variance() throws error for non-existent column', () => {
    try {
      df1.variance('nonexistent');
      throw new Error('Should have thrown error for non-existent column');
    } catch (err) {
      if (!err.message.includes('nonexistent')) {
        throw err;
      }
    }
  });

  // ============================================================================
  // Standard Deviation Tests
  // ============================================================================

  test('stddev() computes sample standard deviation', () => {
    // Data: [10, 20, 30]
    // Variance: 100
    // StdDev: sqrt(100) = 10
    const result = df1.stddev('price');
    assertApproxEqual(result, 10.0, 0.01, 'StdDev of [10, 20, 30]');
  });

  test('stddev() handles decimals correctly', () => {
    // Data: [1.5, 2.5, 3.5, 4.5, 5.5]
    // Variance: 2.5
    // StdDev: sqrt(2.5) ≈ 1.5811
    const result = df2.stddev('value');
    assertApproxEqual(result, Math.sqrt(2.5), 0.01, 'StdDev of [1.5, 2.5, 3.5, 4.5, 5.5]');
  });

  test('stddev() matches sqrt of variance', () => {
    const variance = df1.variance('price');
    const stddev = df1.stddev('price');
    assertApproxEqual(stddev, Math.sqrt(variance), 0.01, 'StdDev should equal sqrt(variance)');
  });

  test('stddev() handles large dataset (1000 rows)', () => {
    const result = df3.stddev('value');
    // StdDev of 1..1000 is approximately 288.82
    if (result < 288 || result > 290) {
      throw new Error(`StdDev out of expected range: ${result}`);
    }
  });

  test('stddev() handles negative values', () => {
    const result = df4.stddev('temperature');
    // Just check it's positive and reasonable
    if (result <= 0 || result > 20) {
      throw new Error(`StdDev out of expected range: ${result}`);
    }
  });

  test('stddev() throws error for non-existent column', () => {
    try {
      df1.stddev('nonexistent');
      throw new Error('Should have thrown error for non-existent column');
    } catch (err) {
      if (!err.message.includes('nonexistent')) {
        throw err;
      }
    }
  });

  // ============================================================================
  // Edge Cases
  // ============================================================================

  // Single value tests
  const csvSingle = `value\n42.0`;
  const dfSingle = rozes.DataFrame.fromCSV(csvSingle);

  test('sum() handles single value', () => {
    const result = dfSingle.sum('value');
    assertEqual(result, 42.0, 'Sum of [42] should be 42');
  });

  test('mean() handles single value', () => {
    const result = dfSingle.mean('value');
    assertEqual(result, 42.0, 'Mean of [42] should be 42');
  });

  test('min() handles single value', () => {
    const result = dfSingle.min('value');
    assertEqual(result, 42.0, 'Min of [42] should be 42');
  });

  test('max() handles single value', () => {
    const result = dfSingle.max('value');
    assertEqual(result, 42.0, 'Max of [42] should be 42');
  });

  test('variance() handles single value (returns 0)', () => {
    const result = dfSingle.variance('value');
    assertEqual(result, 0.0, 'Variance of single value should be 0');
  });

  test('stddev() handles single value (returns 0)', () => {
    const result = dfSingle.stddev('value');
    assertEqual(result, 0.0, 'StdDev of single value should be 0');
  });

  // ============================================================================
  // Cleanup
  // ============================================================================

  console.log('\n✨ Note: Run separate benchmark suite for performance measurements\n');

  df1.free();
  df2.free();
  df3.free();
  df4.free();
  dfSingle.free();

  // ============================================================================
  // Summary
  // ============================================================================

  console.log(`\n${'='.repeat(60)}`);
  console.log(`📊 Test Summary: ${passedTests}/${totalTests} tests passed`);
  console.log(`${'='.repeat(60)}\n`);

  if (passedTests === totalTests) {
    console.log('✅ All SIMD aggregation tests passed!\n');
    process.exit(0);
  } else {
    console.error(`❌ ${totalTests - passedTests} test(s) failed\n`);
    process.exit(1);
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
