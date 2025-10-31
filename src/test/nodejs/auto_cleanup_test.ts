/**
 * Auto Cleanup Tests for Rozes DataFrame (TypeScript)
 *
 * Type-safe tests for FinalizationRegistry-based automatic memory management.
 *
 * Compile: npx tsc src/test/nodejs/auto_cleanup_test.ts
 * Run: node src/test/nodejs/auto_cleanup_test.js
 * Or with GC: node --expose-gc src/test/nodejs/auto_cleanup_test.js
 */

import { Rozes, DataFrame, CSVOptions, RozesError } from '../../../dist/index';
import * as assert from 'assert';

// Test CSV data
const smallCSV = 'name,age,score\nAlice,30,95.5\nBob,25,87.3\nCharlie,35,91.0\n';

const mediumCSV = Array.from({ length: 100 }, (_, i) =>
  `person${i},${20 + i % 50},${Math.random() * 100}`
).join('\n');

const largeCSV = Array.from({ length: 1000 }, (_, i) =>
  `person${i},${20 + i % 50},${Math.random() * 100}`
).join('\n');

/**
 * Test 1: Type-safe auto cleanup
 */
async function testTypeSafeAutoCleanup(): Promise<void> {
  console.log('\n=== Test 1: Type-Safe Auto Cleanup ===');

  const rozes = await Rozes.init();

  // Type-safe options
  const options: CSVOptions = { autoCleanup: true };
  const df: DataFrame = rozes.DataFrame.fromCSV(smallCSV, options);

  console.log(`Created DataFrame: ${df.shape.rows} rows × ${df.shape.cols} cols`);

  // Type-safe column access
  const ages = df.column('age');
  assert.ok(ages instanceof BigInt64Array, 'Age column should be BigInt64Array');
  assert.strictEqual(ages.length, 3, 'Should have 3 ages');

  // Type-safe column names
  const columns: string[] = df.columns;
  assert.strictEqual(columns.length, 3, 'Should have 3 columns');

  console.log('✅ Test passed - Type-safe auto cleanup works');
}

/**
 * Test 2: CSVOptions interface validation
 */
async function testCSVOptionsInterface(): Promise<void> {
  console.log('\n=== Test 2: CSVOptions Interface Validation ===');

  const rozes = await Rozes.init();

  // All options with types
  const options: CSVOptions = {
    delimiter: ',',
    has_headers: true,
    skip_blank_lines: true,
    trim_whitespace: false,
    autoCleanup: true
  };

  const df = rozes.DataFrame.fromCSV(smallCSV, options);

  assert.strictEqual(df.shape.rows, 3, 'Should have 3 rows');
  assert.strictEqual(df.shape.cols, 3, 'Should have 3 columns');

  console.log('✅ Test passed - CSVOptions interface validated');
}

/**
 * Test 3: Type guards for column data
 */
async function testTypeGuards(): Promise<void> {
  console.log('\n=== Test 3: Type Guards for Column Data ===');

  const rozes = await Rozes.init();
  const df = rozes.DataFrame.fromCSV(smallCSV, { autoCleanup: true });

  // Test Float64Array type guard
  const scores = df.column('score');
  if (scores instanceof Float64Array) {
    assert.ok(true, 'Score is Float64Array');
    assert.strictEqual(scores.length, 3, 'Should have 3 scores');
  } else {
    assert.fail('Score should be Float64Array');
  }

  // Test BigInt64Array type guard
  const ages = df.column('age');
  if (ages instanceof BigInt64Array) {
    assert.ok(true, 'Age is BigInt64Array');
    assert.strictEqual(ages.length, 3, 'Should have 3 ages');
  } else {
    assert.fail('Age should be BigInt64Array');
  }

  console.log('✅ Test passed - Type guards work correctly');
}

/**
 * Test 4: Manual free with auto cleanup (typed)
 */
async function testManualFreeTyped(): Promise<void> {
  console.log('\n=== Test 4: Manual Free (Type-Safe) ===');

  const rozes = await Rozes.init();

  const options: CSVOptions = { autoCleanup: true };
  const df: DataFrame = rozes.DataFrame.fromCSV(smallCSV, options);

  // Verify DataFrame is valid
  assert.strictEqual(typeof df.shape.rows, 'number');
  assert.strictEqual(typeof df.shape.cols, 'number');

  // Call free() explicitly (type-safe)
  df.free(); // TypeScript knows this returns void

  console.log('✅ Test passed - Manual free is type-safe');
}

/**
 * Test 5: Batch processing with types
 */
async function testBatchProcessingTyped(): Promise<void> {
  console.log('\n=== Test 5: Batch Processing (Type-Safe) ===');

  const rozes = await Rozes.init();

  interface DataFrameStats {
    rows: number;
    cols: number;
    hasAge: boolean;
  }

  const stats: DataFrameStats[] = [];
  const count = 100;

  console.log(`Processing ${count} DataFrames...`);

  for (let i = 0; i < count; i++) {
    const options: CSVOptions = { autoCleanup: true, has_headers: false };
    const df: DataFrame = rozes.DataFrame.fromCSV(mediumCSV, options);

    // Collect typed stats
    stats.push({
      rows: df.shape.rows,
      cols: df.shape.cols,
      hasAge: df.columns.includes('age')
    });

    if ((i + 1) % 20 === 0) {
      console.log(`  Processed ${i + 1}/${count}...`);
    }
  }

  // Type-safe aggregation
  const totalRows: number = stats.reduce((sum, s) => sum + s.rows, 0);
  assert.strictEqual(stats.length, count, `Should have ${count} stats`);

  console.log(`✅ Test passed - Processed ${totalRows} rows (type-safe)`);
}

/**
 * Test 6: Error handling with types
 */
async function testErrorHandlingTyped(): Promise<void> {
  console.log('\n=== Test 6: Error Handling (Type-Safe) ===');

  const rozes = await Rozes.init();

  try {
    // This should succeed
    const df: DataFrame = rozes.DataFrame.fromCSV(smallCSV, { autoCleanup: true });

    // Verify types
    assert.ok(typeof df.shape.rows === 'number');
    assert.ok(Array.isArray(df.columns));

    console.log('✅ DataFrame created successfully');

  } catch (err) {
    // Type-safe error handling
    if (err instanceof Error) {
      console.error(`Caught typed error: ${err.message}`);
    }
    assert.fail('Should not throw error');
  }

  console.log('✅ Test passed - Error handling is type-safe');
}

/**
 * Test 7: Memory stress test with types
 */
async function testMemoryStressTyped(): Promise<void> {
  console.log('\n=== Test 7: Memory Stress Test (Type-Safe) ===');

  const rozes = await Rozes.init();

  const count = 500;
  const dataframes: DataFrame[] = [];

  console.log(`Creating ${count} DataFrames...`);

  // Create many DataFrames with auto cleanup
  for (let i = 0; i < count; i++) {
    const options: CSVOptions = {
      autoCleanup: true,
      has_headers: false
    };
    const df: DataFrame = rozes.DataFrame.fromCSV(mediumCSV, options);

    // Store references (prevents immediate GC)
    dataframes.push(df);

    if ((i + 1) % 100 === 0) {
      console.log(`  Created ${i + 1}/${count}...`);
    }
  }

  // Verify all DataFrames are valid
  assert.strictEqual(dataframes.length, count);
  dataframes.forEach((df, i) => {
    assert.ok(df.shape.rows > 0, `DataFrame ${i} should have rows`);
  });

  console.log(`Created ${count} DataFrames (type-safe)`);

  // Clear references to allow GC
  dataframes.length = 0;

  // Force GC if available
  if (global.gc) {
    console.log('Running GC...');
    global.gc();
  }

  console.log('✅ Test passed - Memory stress test completed (type-safe)');
}

/**
 * Test 8: Type-safe column iteration
 */
async function testColumnIteration(): Promise<void> {
  console.log('\n=== Test 8: Type-Safe Column Iteration ===');

  const rozes = await Rozes.init();
  const df = rozes.DataFrame.fromCSV(smallCSV, { autoCleanup: true });

  // Iterate over columns with types
  const columnNames: string[] = df.columns;

  for (const colName of columnNames) {
    const col = df.column(colName);

    if (col instanceof Float64Array) {
      console.log(`  ${colName}: Float64Array[${col.length}]`);
    } else if (col instanceof BigInt64Array) {
      console.log(`  ${colName}: BigInt64Array[${col.length}]`);
    } else if (col === null) {
      console.log(`  ${colName}: null`);
    }
  }

  assert.strictEqual(columnNames.length, 3, 'Should have 3 columns');

  console.log('✅ Test passed - Column iteration is type-safe');
}

/**
 * Test 9: DataFrame shape type checking
 */
async function testShapeTypes(): Promise<void> {
  console.log('\n=== Test 9: DataFrame Shape Type Checking ===');

  const rozes = await Rozes.init();
  const df = rozes.DataFrame.fromCSV(smallCSV, { autoCleanup: true });

  // Destructure with types
  const { rows, cols } = df.shape;

  // Type checks
  assert.strictEqual(typeof rows, 'number', 'rows should be number');
  assert.strictEqual(typeof cols, 'number', 'cols should be number');
  assert.ok(rows > 0, 'rows should be positive');
  assert.ok(cols > 0, 'cols should be positive');

  console.log(`Shape: ${rows} rows × ${cols} cols (typed)`);

  console.log('✅ Test passed - Shape types are correct');
}

/**
 * Test 10: fromCSVFile type safety
 */
async function testFromCSVFileTyped(): Promise<void> {
  console.log('\n=== Test 10: fromCSVFile Type Safety ===');

  const rozes = await Rozes.init();
  const fs = require('fs');

  const tempFile: string = '/tmp/rozes_test_ts.csv';
  fs.writeFileSync(tempFile, smallCSV);

  try {
    const options: CSVOptions = {
      has_headers: true,
      autoCleanup: true
    };

    const df: DataFrame = rozes.DataFrame.fromCSVFile(tempFile, options);

    // Type-safe access
    assert.strictEqual(typeof df.shape.rows, 'number');
    assert.ok(Array.isArray(df.columns));

    console.log(`Loaded from file: ${df.shape.rows} rows (type-safe)`);

    console.log('✅ Test passed - fromCSVFile is type-safe');

  } finally {
    fs.unlinkSync(tempFile);
  }
}

/**
 * Main test runner
 */
async function runAllTests(): Promise<void> {
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║  Rozes DataFrame - TypeScript Auto Cleanup Tests         ║');
  console.log('╚═══════════════════════════════════════════════════════════╝');

  if (global.gc) {
    console.log('✅ Running with --expose-gc (GC tests enabled)');
  } else {
    console.log('⚠️  Run with --expose-gc for full GC testing');
  }

  try {
    await testTypeSafeAutoCleanup();
    await testCSVOptionsInterface();
    await testTypeGuards();
    await testManualFreeTyped();
    await testBatchProcessingTyped();
    await testErrorHandlingTyped();
    await testMemoryStressTyped();
    await testColumnIteration();
    await testShapeTypes();
    await testFromCSVFileTyped();

    console.log('\n╔═══════════════════════════════════════════════════════════╗');
    console.log('║  ✅ All TypeScript tests passed!                          ║');
    console.log('╚═══════════════════════════════════════════════════════════╝');

    console.log('\n💡 TypeScript Benefits:');
    console.log('   - CSVOptions interface with IntelliSense');
    console.log('   - Compile-time type checking');
    console.log('   - Type guards for column data');
    console.log('   - Full IDE autocomplete support');

  } catch (err) {
    if (err instanceof Error) {
      console.error('\n❌ Test failed:', err.message);
    } else {
      console.error('\n❌ Test failed:', err);
    }
    process.exit(1);
  }
}

// Run tests
runAllTests().catch((err: Error) => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
