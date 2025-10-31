/**
 * Benchmarks for GroupBy and Join Operations (Priority 3 - Milestone 1.1.0)
 *
 * Measures performance of groupBy and join operations
 */

const { Rozes } = require('../../../dist/index.js');

// Helper to format duration
function formatDuration(ms) {
  if (ms < 1) return `${(ms * 1000).toFixed(2)}µs`;
  if (ms < 1000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

async function main() {
  console.log('📊 Benchmarking GroupBy and Join Operations (Priority 3)\n');

  // Initialize Rozes
  const rozes = await Rozes.init();
  console.log(`✅ Rozes initialized (version: ${rozes.version})\n`);

  // ============================================================================
  // Benchmark 1: GroupBy with Sum (10K rows, 100 groups)
  // ============================================================================

  console.log('1️⃣  GroupBy.sum() - 10K rows, 100 groups');

  // Generate test data: 10K rows with 100 unique regions
  const groupByData = [];
  groupByData.push('region,sales');
  for (let i = 0; i < 10000; i++) {
    const region = `Region${i % 100}`;
    const sales = Math.floor(Math.random() * 1000);
    groupByData.push(`${region},${sales}`);
  }
  const groupByCSV = groupByData.join('\n');

  const df1 = rozes.DataFrame.fromCSV(groupByCSV, { autoCleanup: false });

  const start1 = performance.now();
  const grouped = df1.groupBy('region', 'sales', 'sum');
  const end1 = performance.now();

  grouped.free();
  df1.free();

  const duration1 = end1 - start1;
  console.log(`   Duration: ${formatDuration(duration1)}`);
  console.log(`   Throughput: ${(10000 / duration1 * 1000).toFixed(0)} rows/sec\n`);

  // ============================================================================
  // Benchmark 2: GroupBy with Mean (10K rows, 100 groups)
  // ============================================================================

  console.log('2️⃣  GroupBy.mean() - 10K rows, 100 groups');

  const df2 = rozes.DataFrame.fromCSV(groupByCSV, { autoCleanup: false });

  const start2 = performance.now();
  const meanGrouped = df2.groupBy('region', 'sales', 'mean');
  const end2 = performance.now();

  meanGrouped.free();
  df2.free();

  const duration2 = end2 - start2;
  console.log(`   Duration: ${formatDuration(duration2)}`);
  console.log(`   Throughput: ${(10000 / duration2 * 1000).toFixed(0)} rows/sec\n`);

  // ============================================================================
  // Benchmark 3: Inner Join (1K × 1K rows, 80% match rate)
  // ============================================================================

  console.log('3️⃣  Join.inner() - 1K × 1K rows, 80% match rate');

  // Generate left DataFrame: 1K users
  const leftData = ['user_id,name,age'];
  for (let i = 0; i < 1000; i++) {
    leftData.push(`${i},User${i},${20 + (i % 50)}`);
  }
  const leftCSV = leftData.join('\n');

  // Generate right DataFrame: 1K orders (80% match with users)
  const rightData = ['order_id,user_id,amount'];
  for (let i = 0; i < 1000; i++) {
    const userId = i < 800 ? i : (i + 200); // 80% match, 20% non-matching
    const amount = Math.floor(Math.random() * 1000);
    rightData.push(`${i},${userId},${amount}`);
  }
  const rightCSV = rightData.join('\n');

  const leftDF = rozes.DataFrame.fromCSV(leftCSV, { autoCleanup: false });
  const rightDF = rozes.DataFrame.fromCSV(rightCSV, { autoCleanup: false });

  const start3 = performance.now();
  const joined = leftDF.join(rightDF, 'user_id', 'inner');
  const end3 = performance.now();

  joined.free();
  leftDF.free();
  rightDF.free();

  const duration3 = end3 - start3;
  console.log(`   Duration: ${formatDuration(duration3)}`);
  console.log(`   Throughput: ${(2000 / duration3 * 1000).toFixed(0)} total rows/sec\n`);

  // ============================================================================
  // Benchmark 4: Left Join (1K × 1K rows, 80% match rate)
  // ============================================================================

  console.log('4️⃣  Join.left() - 1K × 1K rows, 80% match rate');

  const leftDF2 = rozes.DataFrame.fromCSV(leftCSV, { autoCleanup: false });
  const rightDF2 = rozes.DataFrame.fromCSV(rightCSV, { autoCleanup: false });

  const start4 = performance.now();
  const leftJoined = leftDF2.join(rightDF2, 'user_id', 'left');
  const end4 = performance.now();

  leftJoined.free();
  leftDF2.free();
  rightDF2.free();

  const duration4 = end4 - start4;
  console.log(`   Duration: ${formatDuration(duration4)}`);
  console.log(`   Throughput: ${(2000 / duration4 * 1000).toFixed(0)} total rows/sec\n`);

  // ============================================================================
  // Benchmark 5: Chained Operations (filter → groupBy)
  // ============================================================================

  console.log('5️⃣  Chained: filter → groupBy - 10K rows');

  // Generate simpler numeric-only data for chaining
  const numericData = ['category,value'];
  for (let i = 0; i < 10000; i++) {
    const category = i % 100;
    const value = Math.floor(Math.random() * 1000);
    numericData.push(`${category},${value}`);
  }
  const numericCSV = numericData.join('\n');

  const df5 = rozes.DataFrame.fromCSV(numericCSV, { autoCleanup: false });

  const start5 = performance.now();
  const filtered = df5.filter('value', '>=', 500);
  const chainGrouped = filtered.groupBy('category', 'value', 'mean');
  const end5 = performance.now();

  chainGrouped.free();
  filtered.free();
  df5.free();

  const duration5 = end5 - start5;
  console.log(`   Duration: ${formatDuration(duration5)}`);
  console.log(`   Throughput: ${(10000 / duration5 * 1000).toFixed(0)} rows/sec\n`);

  // ============================================================================
  // Benchmark 6: Chained Operations (join → groupBy)
  // ============================================================================

  console.log('6️⃣  Chained: join → groupBy - 1K × 1K rows');

  // Generate numeric-only data for join
  const leftNumeric = ['id,value1'];
  for (let i = 0; i < 1000; i++) {
    leftNumeric.push(`${i},${20 + (i % 50)}`);
  }
  const leftNumCSV = leftNumeric.join('\n');

  const rightNumeric = ['order_id,id,value2'];
  for (let i = 0; i < 1000; i++) {
    const id = i < 800 ? i : (i + 200);
    const value = Math.floor(Math.random() * 1000);
    rightNumeric.push(`${i},${id},${value}`);
  }
  const rightNumCSV = rightNumeric.join('\n');

  const leftDF6 = rozes.DataFrame.fromCSV(leftNumCSV, { autoCleanup: false });
  const rightDF6 = rozes.DataFrame.fromCSV(rightNumCSV, { autoCleanup: false });

  const start6 = performance.now();
  const joined6 = leftDF6.join(rightDF6, 'id', 'inner');
  const grouped6 = joined6.groupBy('id', 'value2', 'sum');
  const end6 = performance.now();

  grouped6.free();
  joined6.free();
  leftDF6.free();
  rightDF6.free();

  const duration6 = end6 - start6;
  console.log(`   Duration: ${formatDuration(duration6)}`);
  console.log(`   Throughput: ${(2000 / duration6 * 1000).toFixed(0)} total rows/sec\n`);

  // ============================================================================
  // Summary
  // ============================================================================

  console.log(`${'='.repeat(60)}`);
  console.log('✅ All benchmarks completed');
  console.log(`${'='.repeat(60)}\n`);

  console.log('Performance Summary:');
  console.log(`  GroupBy.sum():         ${formatDuration(duration1)} (${(10000 / duration1 * 1000).toFixed(0)} rows/sec)`);
  console.log(`  GroupBy.mean():        ${formatDuration(duration2)} (${(10000 / duration2 * 1000).toFixed(0)} rows/sec)`);
  console.log(`  Join.inner():          ${formatDuration(duration3)} (${(2000 / duration3 * 1000).toFixed(0)} rows/sec)`);
  console.log(`  Join.left():           ${formatDuration(duration4)} (${(2000 / duration4 * 1000).toFixed(0)} rows/sec)`);
  console.log(`  Filter→GroupBy chain:  ${formatDuration(duration5)} (${(10000 / duration5 * 1000).toFixed(0)} rows/sec)`);
  console.log(`  Join→GroupBy chain:    ${formatDuration(duration6)} (${(2000 / duration6 * 1000).toFixed(0)} rows/sec)`);
  console.log();
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
