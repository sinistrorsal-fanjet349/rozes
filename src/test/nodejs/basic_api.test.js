/**
 * Basic Node.js API Integration Tests
 *
 * Tests fundamental DataFrame operations that should always work.
 * These are the basics - if these fail, examples won't work.
 */

import { Rozes, DataFrame } from '../../../js/rozes.js';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let testsRun = 0;
let testsPassed = 0;
let testsFailed = 0;

function assert(condition, message) {
  testsRun++;
  if (!condition) {
    testsFailed++;
    console.error(`❌ FAIL: ${message}`);
    throw new Error(message);
  } else {
    testsPassed++;
    console.log(`✓ ${message}`);
  }
}

function assertEquals(actual, expected, message) {
  testsRun++;
  // Use == for looser comparison (handles TypedArray number vs primitive)
  if (actual != expected) {
    testsFailed++;
    console.error(`❌ FAIL: ${message}`);
    console.error(`  Expected: ${expected} (type: ${typeof expected})`);
    console.error(`  Actual: ${actual} (type: ${typeof actual})`);
    throw new Error(message);
  } else {
    testsPassed++;
    console.log(`✓ ${message}`);
  }
}

async function runTests() {
  console.log('=== Basic Node.js API Tests ===\n');

  // Initialize Rozes (for Node.js, we need to read the file and pass buffer)
  const wasmPath = path.join(__dirname, '../../../zig-out/bin/rozes.wasm');
  const wasmBuffer = fs.readFileSync(wasmPath);

  // Create a custom init function for Node.js
  const wasi = {
    args_get: () => 0, args_sizes_get: () => 0,
    environ_get: () => 0, environ_sizes_get: () => 0,
    clock_res_get: () => 0, clock_time_get: () => 0,
    fd_advise: () => 0, fd_allocate: () => 0, fd_close: () => 0,
    fd_datasync: () => 0, fd_fdstat_get: () => 0, fd_fdstat_set_flags: () => 0,
    fd_fdstat_set_rights: () => 0, fd_filestat_get: () => 0,
    fd_filestat_set_size: () => 0, fd_filestat_set_times: () => 0,
    fd_pread: () => 0, fd_prestat_get: () => 0, fd_prestat_dir_name: () => 0,
    fd_pwrite: () => 0, fd_read: () => 0, fd_readdir: () => 0,
    fd_renumber: () => 0, fd_seek: () => 0, fd_sync: () => 0,
    fd_tell: () => 0, fd_write: () => 0,
    path_create_directory: () => 0, path_filestat_get: () => 0,
    path_filestat_set_times: () => 0, path_link: () => 0, path_open: () => 0,
    path_readlink: () => 0, path_remove_directory: () => 0, path_rename: () => 0,
    path_symlink: () => 0, path_unlink_file: () => 0,
    poll_oneoff: () => 0, proc_exit: () => {}, proc_raise: () => 0,
    random_get: () => 0, sched_yield: () => 0,
    sock_recv: () => 0, sock_send: () => 0, sock_shutdown: () => 0,
  };

  const result = await WebAssembly.instantiate(wasmBuffer, {
    wasi_snapshot_preview1: wasi,
  });

  const wasm = {
    instance: result.instance,
    memory: result.instance.exports.memory,
  };

  // Manually set up DataFrame._wasm since we're bypassing Rozes.init()
  DataFrame._wasm = wasm;
  console.log('✓ Initialized Rozes\n');

  // Test 1: Parse simple CSV
  console.log('Test 1: Parse simple CSV');
  const csv1 = 'name,age,score\nAlice,30,95.5\nBob,25,87.3\n';
  const df1 = DataFrame.fromCSV(csv1);
  assert(df1 !== null, 'DataFrame.fromCSV should return a DataFrame');
  assertEquals(df1.shape.rows, 2, 'Should have 2 rows');
  assertEquals(df1.shape.cols, 3, 'Should have 3 columns');
  df1.free();
  console.log('');

  // Test 2: Column names
  console.log('Test 2: Column names');
  const csv2 = 'name,age,score\nAlice,30,95.5\n';
  const df2 = DataFrame.fromCSV(csv2);
  assert(Array.isArray(df2.columns), 'df.columns should be an array');
  assertEquals(df2.columns.length, 3, 'Should have 3 column names');
  assertEquals(df2.columns[0], 'name', 'First column should be "name"');
  assertEquals(df2.columns[1], 'age', 'Second column should be "age"');
  assertEquals(df2.columns[2], 'score', 'Third column should be "score"');
  df2.free();
  console.log('');

  // Test 3: Access numeric column (Float64)
  console.log('Test 3: Access numeric column (Float64)');
  const csv3 = 'name,age,score\nAlice,30,95.5\nBob,25,87.3\n';
  const df3 = DataFrame.fromCSV(csv3);
  const scores = df3.column('score');
  assert(scores !== null, 'df.column("score") should return data');
  assert(scores instanceof Float64Array, 'Score column should be Float64Array');
  assertEquals(scores.length, 2, 'Should have 2 score values');
  assertEquals(scores[0], 95.5, 'First score should be 95.5');
  assertEquals(scores[1], 87.3, 'Second score should be 87.3');
  df3.free();
  console.log('');

  // Test 4: Access string column
  console.log('Test 4: Access string column');
  const csv4 = 'name,age,city\nAlice,30,NYC\nBob,25,LA\n';
  const df4 = DataFrame.fromCSV(csv4);
  const names = df4.column('name');
  assert(names !== null, 'df.column("name") should return data');
  assert(Array.isArray(names), 'Name column should be an array');
  assertEquals(names.length, 2, 'Should have 2 name values');
  assertEquals(names[0], 'Alice', 'First name should be "Alice"');
  assertEquals(names[1], 'Bob', 'Second name should be "Bob"');

  const cities = df4.column('city');
  assert(cities !== null, 'df.column("city") should return data');
  assert(Array.isArray(cities), 'City column should be an array');
  assertEquals(cities.length, 2, 'Should have 2 city values');
  assertEquals(cities[0], 'NYC', 'First city should be "NYC"');
  assertEquals(cities[1], 'LA', 'Second city should be "LA"');
  df4.free();
  console.log('');

  // Test 5: Mixed type columns
  console.log('Test 5: Mixed type columns (numeric + string)');
  const csv5 = 'customer_id,segment,revenue\n1,Premium,1500.50\n2,Basic,250.00\n3,Premium,3200.75\n';
  const df5 = DataFrame.fromCSV(csv5);

  // Test numeric column
  const revenue = df5.column('revenue');
  assert(revenue !== null, 'df.column("revenue") should return data');
  assert(revenue instanceof Float64Array, 'Revenue should be Float64Array');
  assertEquals(revenue.length, 3, 'Should have 3 revenue values');

  // Test string column - THIS IS THE KEY TEST
  const segments = df5.column('segment');
  assert(segments !== null, 'df.column("segment") should return data');
  assert(Array.isArray(segments), 'Segment should be string array');
  assertEquals(segments.length, 3, 'Should have 3 segment values');
  assertEquals(segments[0], 'Premium', 'First segment should be "Premium"');
  assertEquals(segments[1], 'Basic', 'Second segment should be "Basic"');
  assertEquals(segments[2], 'Premium', 'Third segment should be "Premium"');

  df5.free();
  console.log('');

  // Test 6: Non-existent column
  console.log('Test 6: Non-existent column returns null');
  const csv6 = 'name,age\nAlice,30\n';
  const df6 = DataFrame.fromCSV(csv6);
  const nonExistent = df6.column('doesnt_exist');
  assert(nonExistent === null, 'df.column("doesnt_exist") should return null');
  df6.free();
  console.log('');

  // Test 7: SIMD aggregations
  console.log('Test 7: SIMD aggregations');
  const csv7 = 'value\n10\n20\n30\n40\n50\n';
  const df7 = DataFrame.fromCSV(csv7);

  const sum = df7.sum('value');
  assertEquals(sum, 150, 'Sum should be 150');

  const mean = df7.mean('value');
  assertEquals(mean, 30, 'Mean should be 30');

  const min = df7.min('value');
  assertEquals(min, 10, 'Min should be 10');

  const max = df7.max('value');
  assertEquals(max, 50, 'Max should be 50');

  df7.free();
  console.log('');

  // Test 8: Select columns
  console.log('Test 8: Select columns');
  const csv8 = 'name,age,city,score\nAlice,30,NYC,95\nBob,25,LA,87\n';
  const df8 = DataFrame.fromCSV(csv8);
  const selected = df8.select(['name', 'age']);

  assertEquals(selected.shape.cols, 2, 'Selected should have 2 columns');
  assertEquals(selected.columns.length, 2, 'Should have 2 column names');
  assertEquals(selected.columns[0], 'name', 'First column should be "name"');
  assertEquals(selected.columns[1], 'age', 'Second column should be "age"');

  df8.free();
  selected.free();
  console.log('');

  // Test 9: Filter
  console.log('Test 9: Filter numeric');
  const csv9 = 'name,age\nAlice,30\nBob,25\nCharlie,35\n';
  const df9 = DataFrame.fromCSV(csv9);
  const filtered = df9.filter('age', '>=', 30);

  assertEquals(filtered.shape.rows, 2, 'Filtered should have 2 rows (age >= 30)');

  df9.free();
  filtered.free();
  console.log('');

  // Test 10: Sort
  console.log('Test 10: Sort');
  const csv10 = 'name,score\nAlice,95\nBob,87\nCharlie,92\n';
  const df10 = DataFrame.fromCSV(csv10);
  const sorted = df10.sort('score', true); // descending

  const sortedScores = sorted.column('score');
  assert(sortedScores[0] >= sortedScores[1], 'Scores should be in descending order (first >= second)');
  assert(sortedScores[1] >= sortedScores[2], 'Scores should be in descending order (second >= third)');
  assertEquals(sortedScores[0], 95, 'First score should be 95 (highest)');

  df10.free();
  sorted.free();
  console.log('');

  // Test 11: GroupBy (single aggregation)
  console.log('Test 11: GroupBy with single aggregation');
  const csv11 = 'city,sales\nNYC,100\nLA,200\nNYC,150\nLA,250\n';
  const df11 = DataFrame.fromCSV(csv11);
  const grouped = df11.groupBy('city', 'sales', 'sum');

  assertEquals(grouped.shape.rows, 2, 'Should have 2 groups');
  assert(grouped.columns.includes('city'), 'Should have city column');

  df11.free();
  grouped.free();
  console.log('');

  // Test 12: Join
  console.log('Test 12: Join DataFrames');
  const csvLeft = 'id,name\n1,Alice\n2,Bob\n';
  const csvRight = 'id,age\n1,30\n2,25\n';
  const dfLeft = DataFrame.fromCSV(csvLeft);
  const dfRight = DataFrame.fromCSV(csvRight);

  const joined = dfLeft.join(dfRight, 'id', 'inner');
  assertEquals(joined.shape.rows, 2, 'Joined should have 2 rows');
  assert(joined.columns.includes('name'), 'Should have name from left');
  assert(joined.columns.includes('age'), 'Should have age from right');

  dfLeft.free();
  dfRight.free();
  joined.free();
  console.log('');

  // Test 13: CSV export
  console.log('Test 13: Export to CSV');
  const csv13 = 'name,age\nAlice,30\nBob,25\n';
  const df13 = DataFrame.fromCSV(csv13);
  const exported = df13.toCSV();

  assert(typeof exported === 'string', 'toCSV should return string');
  assert(exported.includes('name,age'), 'CSV should have header');
  assert(exported.includes('Alice,30'), 'CSV should have data');

  df13.free();
  console.log('');

  // Summary
  console.log('='.repeat(60));
  console.log(`Tests run: ${testsRun}`);
  console.log(`Passed: ${testsPassed}`);
  console.log(`Failed: ${testsFailed}`);

  if (testsFailed === 0) {
    console.log('\n✅ All tests passed!');
    process.exit(0);
  } else {
    console.log(`\n❌ ${testsFailed} test(s) failed`);
    process.exit(1);
  }
}

runTests().catch(err => {
  console.error('\n❌ Test suite crashed:');
  console.error(err);
  process.exit(1);
});
