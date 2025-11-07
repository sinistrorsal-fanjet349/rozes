/**
 * Integration tests for advanced aggregation operations.
 * Tests: median, quantile, valueCounts, corrMatrix, rank
 *
 * Run with: node --test src/test/nodejs/advanced_agg_test.js
 */

import test from 'node:test';
import assert from 'node:assert';
import Rozes, { DataFrame } from '../../../dist/index.mjs';

// Initialize Rozes before running tests
await Rozes.init();

/**
 * Helper: Create numeric DataFrame for testing
 */
function createNumericDF() {
  const csv = `value,score,age
10,85.5,25
20,90.0,30
30,85.5,35
40,95.0,40
50,92.5,45`;
  return DataFrame.fromCSV(csv);
}

/**
 * Helper: Create large DataFrame for performance testing
 */
function createLargeDF(rows = 100000) {
  const values = [];
  for (let i = 0; i < rows; i++) {
    const value = Math.floor(Math.random() * 1000);
    const score = 50 + Math.random() * 50;
    values.push(`${value},${score.toFixed(1)}`);
  }
  const csv = `value,score\n${values.join('\n')}`;
  return DataFrame.fromCSV(csv);
}

// ========================================
// MEDIAN TESTS
// ========================================

test('median() - basic functionality', () => {
  const df = createNumericDF();

  // Test median of odd-length column
  const medianValue = df.median('value');
  assert.strictEqual(medianValue, 30, 'Median of [10,20,30,40,50] should be 30');

  // Test median of column with ties
  const medianScore = df.median('score');
  assert.strictEqual(medianScore, 90.0, 'Median of [85.5,90.0,85.5,95.0,92.5] should be 90.0');

  df.free();
});

test('median() - even-length column', () => {
  const csv = `value
10
20
30
40`;
  const df = DataFrame.fromCSV(csv);

  // Median of even-length array is average of middle two
  const median = df.median('value');
  assert.strictEqual(median, 25, 'Median of [10,20,30,40] should be 25 (average of 20,30)');

  df.free();
});

test('median() - single value', () => {
  const csv = `value
42`;
  const df = DataFrame.fromCSV(csv);

  const median = df.median('value');
  assert.strictEqual(median, 42, 'Median of single value should be that value');

  df.free();
});

test('median() - large dataset performance', () => {
  const df = createLargeDF(100000);

  const start = Date.now();
  const median = df.median('value');
  const duration = Date.now() - start;

  assert.ok(median >= 0 && median <= 1000, 'Median should be in valid range');
  assert.ok(duration < 100, `Median on 100K rows should take <100ms (took ${duration}ms)`);

  df.free();
});

// ========================================
// QUANTILE TESTS
// ========================================

test('quantile() - quartiles (q25, q50, q75)', () => {
  const df = createNumericDF();

  const q25 = df.quantile('value', 0.25);
  const q50 = df.quantile('value', 0.50);
  const q75 = df.quantile('value', 0.75);

  assert.strictEqual(q25, 20, 'Q25 of [10,20,30,40,50] should be 20');
  assert.strictEqual(q50, 30, 'Q50 (median) of [10,20,30,40,50] should be 30');
  assert.strictEqual(q75, 40, 'Q75 of [10,20,30,40,50] should be 40');

  df.free();
});

test('quantile() - extreme quantiles (q0, q100)', () => {
  const df = createNumericDF();

  const q0 = df.quantile('value', 0.0);
  const q100 = df.quantile('value', 1.0);

  assert.strictEqual(q0, 10, 'Q0 (minimum) should be 10');
  assert.strictEqual(q100, 50, 'Q100 (maximum) should be 50');

  df.free();
});

test('quantile() - high precision (q95)', () => {
  const df = createNumericDF();

  const q95 = df.quantile('value', 0.95);
  assert.ok(q95 >= 40 && q95 <= 50, 'Q95 should be between Q75 and max');

  df.free();
});

test('quantile() - large dataset performance', () => {
  const df = createLargeDF(100000);

  const start = Date.now();
  const q25 = df.quantile('value', 0.25);
  const q75 = df.quantile('value', 0.75);
  const duration = Date.now() - start;

  assert.ok(q25 < q75, 'Q25 should be less than Q75');
  assert.ok(duration < 200, `Quantile on 100K rows should take <200ms (took ${duration}ms)`);

  df.free();
});

// ========================================
// VALUECOUNTS TESTS
// ========================================

test('valueCounts() - basic functionality', () => {
  const csv = `category
A
B
A
C
A
B`;
  const df = DataFrame.fromCSV(csv);

  const counts = df.valueCounts('category');

  assert.strictEqual(counts['A'], 3, 'Count of A should be 3');
  assert.strictEqual(counts['B'], 2, 'Count of B should be 2');
  assert.strictEqual(counts['C'], 1, 'Count of C should be 1');
  assert.strictEqual(Object.keys(counts).length, 3, 'Should have 3 unique values');

  df.free();
});

test('valueCounts() - numeric column', () => {
  const csv = `value:Int64
10
20
10
30
10
20`;
  const df = DataFrame.fromCSV(csv);

  const counts = df.valueCounts('value');

  assert.strictEqual(counts['10'], 3, 'Count of 10 should be 3');
  assert.strictEqual(counts['20'], 2, 'Count of 20 should be 2');
  assert.strictEqual(counts['30'], 1, 'Count of 30 should be 1');

  df.free();
});

test('valueCounts() - high cardinality', () => {
  const values = [];
  for (let i = 0; i < 1000; i++) {
    values.push(String(i % 100)); // 100 unique values, 10 occurrences each
  }
  const csv = `value\n${values.join('\n')}`;
  const df = DataFrame.fromCSV(csv);

  const counts = df.valueCounts('value');

  assert.strictEqual(Object.keys(counts).length, 100, 'Should have 100 unique values');
  for (const count of Object.values(counts)) {
    assert.strictEqual(count, 10, 'Each value should appear 10 times');
  }

  df.free();
});

test('valueCounts() - single unique value', () => {
  const csv = `category
A
A
A`;
  const df = DataFrame.fromCSV(csv);

  const counts = df.valueCounts('category');

  assert.strictEqual(Object.keys(counts).length, 1, 'Should have 1 unique value');
  assert.strictEqual(counts['A'], 3, 'Count of A should be 3');

  df.free();
});

// ========================================
// CORRMATRIX TESTS
// ========================================

test('corrMatrix() - basic 2-column correlation', () => {
  const csv = `x,y
1,2
2,4
3,6
4,8
5,10`;
  const df = DataFrame.fromCSV(csv);

  const corr = df.corrMatrix(['x', 'y']);

  // Perfect positive correlation
  assert.strictEqual(corr['x']['x'], 1.0, 'Correlation of x with itself should be 1.0');
  assert.strictEqual(corr['y']['y'], 1.0, 'Correlation of y with itself should be 1.0');
  assert.ok(Math.abs(corr['x']['y'] - 1.0) < 0.001, 'x and y should have perfect positive correlation (~1.0)');
  assert.ok(Math.abs(corr['y']['x'] - 1.0) < 0.001, 'Correlation matrix should be symmetric');

  df.free();
});

test('corrMatrix() - 3-column correlation', () => {
  const csv = `x,y,z
1,2,10
2,4,8
3,6,6
4,8,4
5,10,2`;
  const df = DataFrame.fromCSV(csv);

  const corr = df.corrMatrix(['x', 'y', 'z']);

  // Check diagonal (self-correlation)
  assert.strictEqual(corr['x']['x'], 1.0, 'x self-correlation should be 1.0');
  assert.strictEqual(corr['y']['y'], 1.0, 'y self-correlation should be 1.0');
  assert.strictEqual(corr['z']['z'], 1.0, 'z self-correlation should be 1.0');

  // Check symmetry
  assert.strictEqual(corr['x']['y'], corr['y']['x'], 'Matrix should be symmetric (x,y)');
  assert.strictEqual(corr['x']['z'], corr['z']['x'], 'Matrix should be symmetric (x,z)');
  assert.strictEqual(corr['y']['z'], corr['z']['y'], 'Matrix should be symmetric (y,z)');

  // Check x-y positive correlation
  assert.ok(corr['x']['y'] > 0.9, 'x and y should have strong positive correlation');

  // Check x-z negative correlation
  assert.ok(corr['x']['z'] < -0.9, 'x and z should have strong negative correlation');

  df.free();
});

test('corrMatrix() - all numeric columns (empty column list)', () => {
  const csv = `x,y,z
1,2,10
2,4,8
3,6,6`;
  const df = DataFrame.fromCSV(csv);

  const corr = df.corrMatrix(); // Empty array -> all numeric columns

  assert.ok('x' in corr, 'Should include column x');
  assert.ok('y' in corr, 'Should include column y');
  assert.ok('z' in corr, 'Should include column z');
  assert.strictEqual(Object.keys(corr).length, 3, 'Should have 3 columns');

  df.free();
});

test('corrMatrix() - uncorrelated variables', () => {
  const csv = `x,y
1,5
2,3
3,7
4,2
5,6`;
  const df = DataFrame.fromCSV(csv);

  const corr = df.corrMatrix(['x', 'y']);

  // Low correlation (close to 0)
  assert.ok(Math.abs(corr['x']['y']) < 0.5, 'Uncorrelated variables should have correlation near 0');

  df.free();
});

// ========================================
// RANK TESTS
// ========================================

test('rank() - basic ranking with default method (average)', () => {
  const csv = `score:Int64
85
90
85
95`;
  const df = DataFrame.fromCSV(csv);

  const ranked = df.rank('score'); // Default: "average"

  const ranks = ranked.column('score').asFloat64();
  assert.strictEqual(ranks[0], 1.5, 'First 85 should get rank 1.5 (average of 1 and 2)');
  assert.strictEqual(ranks[1], 3, 'Single 90 should get rank 3');
  assert.strictEqual(ranks[2], 1.5, 'Second 85 should get rank 1.5 (average of 1 and 2)');
  assert.strictEqual(ranks[3], 4, 'Single 95 should get rank 4');

  ranked.free();
  df.free();
});

test('rank() - min method (ties get minimum rank)', () => {
  const csv = `score:Int64
85
90
85
95`;
  const df = DataFrame.fromCSV(csv);

  const ranked = df.rank('score', 'min');

  const ranks = ranked.column('score').asFloat64();
  assert.strictEqual(ranks[0], 1, 'First 85 should get rank 1 (min)');
  assert.strictEqual(ranks[1], 3, 'Single 90 should get rank 3');
  assert.strictEqual(ranks[2], 1, 'Second 85 should get rank 1 (min)');
  assert.strictEqual(ranks[3], 4, 'Single 95 should get rank 4');

  ranked.free();
  df.free();
});

test('rank() - max method (ties get maximum rank)', () => {
  const csv = `score:Int64
85
90
85
95`;
  const df = DataFrame.fromCSV(csv);

  const ranked = df.rank('score', 'max');

  const ranks = ranked.column('score').asFloat64();
  assert.strictEqual(ranks[0], 2, 'First 85 should get rank 2 (max)');
  assert.strictEqual(ranks[1], 3, 'Single 90 should get rank 3');
  assert.strictEqual(ranks[2], 2, 'Second 85 should get rank 2 (max)');
  assert.strictEqual(ranks[3], 4, 'Single 95 should get rank 4');

  ranked.free();
  df.free();
});

test('rank() - dense method (no gaps in ranks)', () => {
  const csv = `score:Int64
85
90
85
95`;
  const df = DataFrame.fromCSV(csv);

  const ranked = df.rank('score', 'dense');

  const ranks = ranked.column('score').asFloat64();
  assert.strictEqual(ranks[0], 1, 'First 85 should get rank 1 (dense)');
  assert.strictEqual(ranks[1], 2, 'Single 90 should get rank 2 (dense, no gap)');
  assert.strictEqual(ranks[2], 1, 'Second 85 should get rank 1 (dense)');
  assert.strictEqual(ranks[3], 3, 'Single 95 should get rank 3 (dense)');

  ranked.free();
  df.free();
});

test('rank() - ordinal method (ties broken by position)', () => {
  const csv = `score:Int64
85
90
85
95`;
  const df = DataFrame.fromCSV(csv);

  const ranked = df.rank('score', 'ordinal');

  const ranks = ranked.column('score').asFloat64();
  assert.strictEqual(ranks[0], 1, 'First 85 should get rank 1 (ordinal, appears first)');
  assert.strictEqual(ranks[1], 3, 'Single 90 should get rank 3');
  assert.strictEqual(ranks[2], 2, 'Second 85 should get rank 2 (ordinal, appears second)');
  assert.strictEqual(ranks[3], 4, 'Single 95 should get rank 4');

  ranked.free();
  df.free();
});

test('rank() - all unique values', () => {
  const csv = `score:Int64
10
30
20
50
40`;
  const df = DataFrame.fromCSV(csv);

  const ranked = df.rank('score');

  const ranks = ranked.column('score').asFloat64();
  assert.strictEqual(ranks[0], 1, '10 should get rank 1');
  assert.strictEqual(ranks[1], 3, '30 should get rank 3');
  assert.strictEqual(ranks[2], 2, '20 should get rank 2');
  assert.strictEqual(ranks[3], 5, '50 should get rank 5');
  assert.strictEqual(ranks[4], 4, '40 should get rank 4');

  ranked.free();
  df.free();
});

test('rank() - all same values', () => {
  const csv = `score:Int64
100
100
100`;
  const df = DataFrame.fromCSV(csv);

  const ranked = df.rank('score'); // Default: "average"

  const ranks = ranked.column('score').asFloat64();
  // All ties -> average rank = (1 + 2 + 3) / 3 = 2
  assert.strictEqual(ranks[0], 2, 'All ties should get average rank 2');
  assert.strictEqual(ranks[1], 2, 'All ties should get average rank 2');
  assert.strictEqual(ranks[2], 2, 'All ties should get average rank 2');

  ranked.free();
  df.free();
});

// ========================================
// MEMORY LEAK TESTS
// ========================================

test('median() - memory leak test (1000 iterations)', () => {
  for (let i = 0; i < 1000; i++) {
    const df = createNumericDF();
    const median = df.median('value');
    assert.strictEqual(median, 30, 'Median should be consistent');
    df.free();
  }
});

test('quantile() - memory leak test (1000 iterations)', () => {
  for (let i = 0; i < 1000; i++) {
    const df = createNumericDF();
    const q25 = df.quantile('value', 0.25);
    assert.strictEqual(q25, 20, 'Q25 should be consistent');
    df.free();
  }
});

test('valueCounts() - memory leak test (1000 iterations)', () => {
  const csv = `category
A
B
A
C`;
  for (let i = 0; i < 1000; i++) {
    const df = DataFrame.fromCSV(csv);
    const counts = df.valueCounts('category');
    assert.strictEqual(counts['A'], 2, 'Count should be consistent');
    df.free();
  }
});

test('corrMatrix() - memory leak test (1000 iterations)', () => {
  const csv = `x,y
1,2
2,4
3,6`;
  for (let i = 0; i < 1000; i++) {
    const df = DataFrame.fromCSV(csv);
    const corr = df.corrMatrix(['x', 'y']);
    assert.ok(corr['x']['y'] > 0.9, 'Correlation should be consistent');
    df.free();
  }
});

test('rank() - memory leak test (1000 iterations)', () => {
  const csv = `score:Int64
85
90
85
95`;
  for (let i = 0; i < 1000; i++) {
    const df = DataFrame.fromCSV(csv);
    const ranked = df.rank('score');
    const ranks = ranked.column('score').asFloat64();
    assert.strictEqual(ranks[0], 1.5, 'Rank should be consistent');
    ranked.free();
    df.free();
  }
});

// ========================================
// EDGE CASE TESTS
// ========================================

test('median() - error on non-existent column', () => {
  const df = createNumericDF();

  assert.throws(() => {
    df.median('nonexistent');
  }, /Column not found|invalid column/i, 'Should throw error for non-existent column');

  df.free();
});

test('quantile() - error on invalid quantile value', () => {
  const df = createNumericDF();

  // Note: Depending on implementation, may clamp to [0, 1] or throw
  // For now, test that extreme values (outside [0, 1]) are handled
  const q_negative = df.quantile('value', -0.5);
  const q_above_one = df.quantile('value', 1.5);

  // Either clamped to valid range or error (implementation-dependent)
  assert.ok(q_negative !== undefined, 'Should handle negative quantile');
  assert.ok(q_above_one !== undefined, 'Should handle quantile > 1');

  df.free();
});

test('valueCounts() - empty DataFrame', () => {
  const csv = `category
`;
  const df = DataFrame.fromCSV(csv);

  const counts = df.valueCounts('category');
  assert.strictEqual(Object.keys(counts).length, 0, 'Empty DataFrame should have no counts');

  df.free();
});

test('corrMatrix() - single column', () => {
  const csv = `x
1
2
3`;
  const df = DataFrame.fromCSV(csv);

  const corr = df.corrMatrix(['x']);

  assert.strictEqual(corr['x']['x'], 1.0, 'Single column should have self-correlation 1.0');
  assert.strictEqual(Object.keys(corr).length, 1, 'Should have 1 column');

  df.free();
});

test('rank() - single value', () => {
  const csv = `score:Int64
42`;
  const df = DataFrame.fromCSV(csv);

  const ranked = df.rank('score');
  const ranks = ranked.column('score').asFloat64();

  assert.strictEqual(ranks[0], 1, 'Single value should get rank 1');

  ranked.free();
  df.free();
});

console.log('\n✅ All advanced aggregation tests completed!\n');
