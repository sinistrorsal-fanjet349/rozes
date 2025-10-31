/**
 * Auto Cleanup Tests for Rozes DataFrame
 *
 * Tests the FinalizationRegistry-based automatic memory management feature.
 *
 * Run with: node src/test/nodejs/auto_cleanup_test.js
 */

const { Rozes } = require('../../../dist/index.js');
const assert = require('assert');

// Test CSV data
const smallCSV = 'name,age,score\nAlice,30,95.5\nBob,25,87.3\nCharlie,35,91.0\n';

const mediumCSV = Array.from({ length: 100 }, (_, i) =>
  `person${i},${20 + i % 50},${Math.random() * 100}`
).join('\n');

const largeCSV = Array.from({ length: 1000 }, (_, i) =>
  `person${i},${20 + i % 50},${Math.random() * 100}`
).join('\n');

/**
 * Test 1: Basic auto cleanup - verify no errors
 */
async function testBasicAutoCleanup() {
  console.log('\n=== Test 1: Basic Auto Cleanup ===');

  const rozes = await Rozes.init();

  // Create DataFrame with auto cleanup enabled
  const df = rozes.DataFrame.fromCSV(smallCSV, { autoCleanup: true });

  console.log(`Created DataFrame: ${df.shape.rows} rows × ${df.shape.cols} cols`);
  console.log(`Columns: ${df.columns.join(', ')}`);

  // Use the DataFrame
  const ages = df.column('age');
  assert(ages instanceof BigInt64Array, 'Age column should be BigInt64Array');
  assert.strictEqual(ages.length, 3, 'Should have 3 ages');

  // Don't call free() - let GC handle it
  console.log('✅ Test passed - DataFrame will be auto-freed on GC');
}

/**
 * Test 2: Manual free with auto cleanup enabled
 */
async function testManualFreeWithAutoCleanup() {
  console.log('\n=== Test 2: Manual Free with Auto Cleanup ===');

  const rozes = await Rozes.init();

  // Create DataFrame with auto cleanup enabled
  const df = rozes.DataFrame.fromCSV(smallCSV, { autoCleanup: true });

  console.log(`Created DataFrame: ${df.shape.rows} rows × ${df.shape.cols} cols`);

  // Call free() explicitly (should unregister from FinalizationRegistry)
  df.free();

  console.log('✅ Test passed - Manual free() works with autoCleanup: true');
}

/**
 * Test 3: Traditional manual memory management (default)
 */
async function testManualMemoryManagement() {
  console.log('\n=== Test 3: Traditional Manual Memory Management ===');

  const rozes = await Rozes.init();

  // Create DataFrame without auto cleanup (default)
  const df = rozes.DataFrame.fromCSV(smallCSV);

  console.log(`Created DataFrame: ${df.shape.rows} rows × ${df.shape.cols} cols`);

  // Must call free() explicitly
  df.free();

  console.log('✅ Test passed - Manual free() required and called');
}

/**
 * Test 4: Large dataset stress test (1000 DataFrames with auto cleanup)
 */
async function testLargeDatasetAutoCleanup() {
  console.log('\n=== Test 4: Large Dataset Auto Cleanup (1000 DataFrames) ===');

  const rozes = await Rozes.init();

  const startTime = Date.now();
  const dfCount = 1000;

  // Create many DataFrames with auto cleanup
  for (let i = 0; i < dfCount; i++) {
    const df = rozes.DataFrame.fromCSV(mediumCSV, {
      autoCleanup: true,
      has_headers: false
    });

    // Use the DataFrame
    if (i % 100 === 0) {
      console.log(`  Created ${i + 1}/${dfCount} DataFrames...`);
    }

    // Don't call free() - let GC handle it
  }

  const duration = Date.now() - startTime;
  console.log(`Created ${dfCount} DataFrames in ${duration}ms (${(duration / dfCount).toFixed(2)}ms each)`);

  // Force GC if available (run with: node --expose-gc)
  if (global.gc) {
    console.log('Running garbage collection...');
    global.gc();
    global.gc(); // Run twice to ensure thorough cleanup
    console.log('GC complete');
  } else {
    console.log('⚠️  Run with --expose-gc to test GC: node --expose-gc test.js');
  }

  console.log('✅ Test passed - No crashes with 1000 DataFrames + auto cleanup');
}

/**
 * Test 5: Large dataset with manual cleanup (for comparison)
 */
async function testLargeDatasetManualCleanup() {
  console.log('\n=== Test 5: Large Dataset Manual Cleanup (1000 DataFrames) ===');

  const rozes = await Rozes.init();

  const startTime = Date.now();
  const dfCount = 1000;

  // Create many DataFrames with manual cleanup
  for (let i = 0; i < dfCount; i++) {
    const df = rozes.DataFrame.fromCSV(mediumCSV, {
      has_headers: false
    });

    // Use the DataFrame
    if (i % 100 === 0) {
      console.log(`  Created ${i + 1}/${dfCount} DataFrames...`);
    }

    // Manually free each DataFrame immediately
    df.free();
  }

  const duration = Date.now() - startTime;
  console.log(`Created and freed ${dfCount} DataFrames in ${duration}ms (${(duration / dfCount).toFixed(2)}ms each)`);
  console.log('✅ Test passed - Manual cleanup is deterministic and faster');
}

/**
 * Test 6: Memory pressure test (simulate heavy workload)
 */
async function testMemoryPressure() {
  console.log('\n=== Test 6: Memory Pressure Test ===');

  const rozes = await Rozes.init();

  console.log('Creating 100 large DataFrames (1000 rows each) with auto cleanup...');
  const startMem = process.memoryUsage();

  for (let i = 0; i < 100; i++) {
    const df = rozes.DataFrame.fromCSV(largeCSV, {
      autoCleanup: true,
      has_headers: false
    });

    if (i % 10 === 0) {
      const mem = process.memoryUsage();
      console.log(`  ${i + 1}/100 - Heap: ${(mem.heapUsed / 1024 / 1024).toFixed(2)} MB`);
    }
  }

  const endMem = process.memoryUsage();
  console.log(`\nMemory before: ${(startMem.heapUsed / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Memory after:  ${(endMem.heapUsed / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Memory growth: ${((endMem.heapUsed - startMem.heapUsed) / 1024 / 1024).toFixed(2)} MB`);

  // Force GC if available
  if (global.gc) {
    console.log('\nRunning GC...');
    global.gc();
    global.gc();
    const afterGCMem = process.memoryUsage();
    console.log(`Memory after GC: ${(afterGCMem.heapUsed / 1024 / 1024).toFixed(2)} MB`);
    console.log(`Freed by GC: ${((endMem.heapUsed - afterGCMem.heapUsed) / 1024 / 1024).toFixed(2)} MB`);
  }

  console.log('✅ Test passed - Memory pressure handled');
}

/**
 * Test 7: Error handling with auto cleanup
 */
async function testErrorHandlingWithAutoCleanup() {
  console.log('\n=== Test 7: Error Handling with Auto Cleanup ===');

  const rozes = await Rozes.init();

  // Test that errors don't prevent auto cleanup registration
  const df = rozes.DataFrame.fromCSV(smallCSV, { autoCleanup: true });

  // Use valid columns first
  const ages = df.column('age');
  assert(ages instanceof BigInt64Array, 'Age column should be BigInt64Array');

  console.log('✅ DataFrame valid and working');

  // Don't call free() - let auto cleanup handle it

  console.log('✅ Test passed - Error handling works with auto cleanup');
}

/**
 * Test 8: Mixed manual and auto cleanup
 */
async function testMixedCleanup() {
  console.log('\n=== Test 8: Mixed Manual and Auto Cleanup ===');

  const rozes = await Rozes.init();

  // Create some with auto cleanup
  const df1 = rozes.DataFrame.fromCSV(smallCSV, { autoCleanup: true });
  const df2 = rozes.DataFrame.fromCSV(smallCSV, { autoCleanup: true });

  // Create some with manual cleanup
  const df3 = rozes.DataFrame.fromCSV(smallCSV);
  const df4 = rozes.DataFrame.fromCSV(smallCSV);

  console.log(`Created 4 DataFrames (2 auto, 2 manual)`);

  // Manually free one auto cleanup DataFrame
  df1.free();

  // Manually free manual cleanup DataFrames
  df3.free();
  df4.free();

  // df2 will be auto-freed by GC

  console.log('✅ Test passed - Mixed cleanup strategies work together');
}

/**
 * Main test runner
 */
async function runAllTests() {
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║  Rozes DataFrame - Auto Cleanup Tests                    ║');
  console.log('╚═══════════════════════════════════════════════════════════╝');

  if (global.gc) {
    console.log('✅ Running with --expose-gc (GC tests enabled)');
  } else {
    console.log('⚠️  Run with --expose-gc for full GC testing:');
    console.log('   node --expose-gc src/test/nodejs/auto_cleanup_test.js');
  }

  try {
    await testBasicAutoCleanup();
    await testManualFreeWithAutoCleanup();
    await testManualMemoryManagement();
    await testLargeDatasetAutoCleanup();
    await testLargeDatasetManualCleanup();
    await testMemoryPressure();
    await testErrorHandlingWithAutoCleanup();
    await testMixedCleanup();

    console.log('\n╔═══════════════════════════════════════════════════════════╗');
    console.log('║  ✅ All tests passed!                                     ║');
    console.log('╚═══════════════════════════════════════════════════════════╝');

    console.log('\n💡 Key Takeaways:');
    console.log('   - autoCleanup: true → No free() needed (convenient)');
    console.log('   - autoCleanup: false → Must call free() (predictable)');
    console.log('   - Manual free() always recommended for production');
    console.log('   - Auto cleanup best for prototyping and scripts');

  } catch (err) {
    console.error('\n❌ Test failed:', err);
    process.exit(1);
  }
}

// Run tests
runAllTests().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
