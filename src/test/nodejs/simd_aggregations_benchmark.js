/**
 * SIMD Aggregations Performance Benchmark (Milestone 1.2.0 Phase 1)
 *
 * Measures SIMD-accelerated aggregation performance
 * Target: 30%+ speedup over scalar implementation
 */

const { Rozes } = require('../../../dist/index.js');

async function main() {
  console.log('⚡ SIMD Aggregations Performance Benchmark\n');
  console.log('Milestone 1.2.0 Phase 1 - Target: 30%+ speedup\n');

  // Initialize Rozes
  const rozes = await Rozes.init();
  console.log('✅ Rozes initialized (version:', rozes.version, ')\n');

  // ============================================================================
  // Test Data Setup
  // ============================================================================

  console.log('📊 Creating test datasets...\n');

  // Small dataset: 1K rows
  let csv1K = ['value'];
  for (let i = 1; i <= 1000; i++) {
    csv1K.push(i.toString());
  }
  const df1K = rozes.DataFrame.fromCSV(csv1K.join('\n'));
  console.log('  ✅ Created 1K row dataset');

  // Medium dataset: 10K rows
  let csv10K = ['value'];
  for (let i = 1; i <= 10000; i++) {
    csv10K.push(i.toString());
  }
  const df10K = rozes.DataFrame.fromCSV(csv10K.join('\n'));
  console.log('  ✅ Created 10K row dataset');

  // Large dataset: 100K rows
  let csv100K = ['value'];
  for (let i = 1; i <= 100000; i++) {
    csv100K.push(i.toString());
  }
  const df100K = rozes.DataFrame.fromCSV(csv100K.join('\n'));
  console.log('  ✅ Created 100K row dataset\n');

  // ============================================================================
  // Benchmark Configuration
  // ============================================================================

  const datasets = [
    { name: '1K rows', df: df1K, iterations: 1000 },
    { name: '10K rows', df: df10K, iterations: 100 },
    { name: '100K rows', df: df100K, iterations: 10 },
  ];

  const operations = [
    { name: 'sum', fn: (df) => df.sum('value') },
    { name: 'mean', fn: (df) => df.mean('value') },
    { name: 'min', fn: (df) => df.min('value') },
    { name: 'max', fn: (df) => df.max('value') },
    { name: 'variance', fn: (df) => df.variance('value') },
    { name: 'stddev', fn: (df) => df.stddev('value') },
  ];

  // ============================================================================
  // Run Benchmarks
  // ============================================================================

  console.log('🚀 Running benchmarks...\n');
  console.log('='.repeat(70));

  const results = [];

  for (const dataset of datasets) {
    console.log(`\n${dataset.name} (${dataset.iterations} iterations):`);
    console.log('-'.repeat(70));

    for (const op of operations) {
      // Warm-up
      for (let i = 0; i < 5; i++) {
        op.fn(dataset.df);
      }

      // Benchmark
      const start = Date.now();
      for (let i = 0; i < dataset.iterations; i++) {
        op.fn(dataset.df);
      }
      const elapsed = Date.now() - start;

      const avgTime = elapsed / dataset.iterations;
      const throughput = dataset.iterations / (elapsed / 1000); // ops/sec

      console.log(
        `  ${op.name.padEnd(10)} | ${elapsed.toString().padStart(6)}ms total | ` +
        `${avgTime.toFixed(3).padStart(8)}ms/op | ${throughput.toFixed(1).padStart(8)} ops/sec`
      );

      results.push({
        operation: op.name,
        dataset: dataset.name,
        totalTime: elapsed,
        avgTime: avgTime,
        throughput: throughput,
      });
    }
  }

  console.log('\n' + '='.repeat(70));

  // ============================================================================
  // Summary Statistics
  // ============================================================================

  console.log('\n📊 Performance Summary:\n');

  // Group by operation
  const byOperation = {};
  for (const result of results) {
    if (!byOperation[result.operation]) {
      byOperation[result.operation] = [];
    }
    byOperation[result.operation].push(result);
  }

  for (const [opName, opResults] of Object.entries(byOperation)) {
    console.log(`${opName.toUpperCase()}:`);
    for (const result of opResults) {
      console.log(
        `  ${result.dataset.padEnd(10)} - ${result.avgTime.toFixed(3)}ms/op ` +
        `(${result.throughput.toFixed(1)} ops/sec)`
      );
    }
    console.log('');
  }

  // ============================================================================
  // SIMD Effectiveness Analysis
  // ============================================================================

  console.log('⚡ SIMD Effectiveness:\n');

  // Estimate SIMD speedup based on theoretical 4-wide SIMD operations
  // SIMD should be ~30%+ faster for aggregations (conservative estimate)
  console.log('  Expected speedup vs scalar: 30-40%');
  console.log('  SIMD vector width: 4 (256-bit registers)');
  console.log('  Operations: sum, mean, min, max benefit most');
  console.log('  Variance/stddev: 2-pass algorithm (mean + variance)');
  console.log('');

  // Show best performance
  const fastest = results.reduce((best, curr) =>
    curr.throughput > best.throughput ? curr : best
  );
  console.log(`  🏆 Fastest: ${fastest.operation} on ${fastest.dataset}`);
  console.log(`      ${fastest.throughput.toFixed(1)} ops/sec (${fastest.avgTime.toFixed(3)}ms/op)`);

  // ============================================================================
  // Cleanup
  // ============================================================================

  df1K.free();
  df10K.free();
  df100K.free();

  console.log('\n✅ Benchmark complete!\n');
  process.exit(0);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
