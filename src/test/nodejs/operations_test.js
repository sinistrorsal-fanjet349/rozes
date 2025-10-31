/**
 * Node.js Tests for DataFrame Operations (Priority 2 - Milestone 1.1.0)
 *
 * Tests: select, head, tail, sort, filter
 */

const { Rozes } = require('../../../dist/index.js');

async function main() {
  console.log('🧪 Testing DataFrame Operations (Priority 2)\n');

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
    }
  }

  // Helper to convert typed array values to numbers
  function toNum(arr, idx) {
    return arr instanceof BigInt64Array ? Number(arr[idx]) : arr[idx];
  }

  // Test data
  const csv = `name,age,score
Alice,30,95.5
Bob,25,87.3
Charlie,35,91.0
Diana,28,92.5
Eve,22,88.0`;

  const df = rozes.DataFrame.fromCSV(csv);

  // ============================================================================
  // Test 1: Basic DataFrame properties
  // ============================================================================

  test('DataFrame.shape returns correct dimensions', () => {
    const shape = df.shape;
    if (shape.rows !== 5) throw new Error(`Expected 5 rows, got ${shape.rows}`);
    if (shape.cols !== 3) throw new Error(`Expected 3 cols, got ${shape.cols}`);
  });

  test('DataFrame.columns returns column names', () => {
    const cols = df.columns;
    if (cols.length !== 3) throw new Error(`Expected 3 columns, got ${cols.length}`);
    if (cols[0] !== 'name') throw new Error(`Expected 'name', got '${cols[0]}'`);
    if (cols[1] !== 'age') throw new Error(`Expected 'age', got '${cols[1]}'`);
    if (cols[2] !== 'score') throw new Error(`Expected 'score', got '${cols[2]}'`);
  });

  test('DataFrame.column() returns typed array', () => {
    const ages = df.column('age');
    if (!ages) throw new Error('Age column not found');
    if (!(ages instanceof BigInt64Array) && !(ages instanceof Float64Array)) {
      throw new Error(`Expected BigInt64Array or Float64Array, got ${ages.constructor.name}`);
    }
    if (ages.length !== 5) throw new Error(`Expected 5 values, got ${ages.length}`);
    
    if (toNum(ages, 0) !== 30) throw new Error(`Expected 30, got ${firstAge}`);
  });

  // ============================================================================
  // Test 2: Select operation
  // ============================================================================

  test('DataFrame.select() selects columns', () => {
    const selected = df.select(['name', 'age']);
    try {
      const shape = selected.shape;
      if (shape.rows !== 5) throw new Error(`Expected 5 rows, got ${shape.rows}`);
      if (shape.cols !== 2) throw new Error(`Expected 2 cols, got ${shape.cols}`);

      const cols = selected.columns;
      if (cols.length !== 2) throw new Error(`Expected 2 columns, got ${cols.length}`);
      if (!cols.includes('name')) throw new Error('Missing name column');
      if (!cols.includes('age')) throw new Error('Missing age column');
    } finally {
      selected.free();
    }
  });

  test('DataFrame.select() with single column', () => {
    const selected = df.select(['age']);
    try {
      const shape = selected.shape;
      if (shape.rows !== 5) throw new Error(`Expected 5 rows, got ${shape.rows}`);
      if (shape.cols !== 1) throw new Error(`Expected 1 col, got ${shape.cols}`);

      const ages = selected.column('age');
      if (!ages) throw new Error('Age column not found');
      
      if (toNum(ages, 0) !== 30) throw new Error(`Expected 30, got ${firstAge}`);
    } finally {
      selected.free();
    }
  });

  // ============================================================================
  // Test 3: Head operation
  // ============================================================================

  test('DataFrame.head() returns first n rows', () => {
    const head3 = df.head(3);
    try {
      const shape = head3.shape;
      if (shape.rows !== 3) throw new Error(`Expected 3 rows, got ${shape.rows}`);
      if (shape.cols !== 3) throw new Error(`Expected 3 cols, got ${shape.cols}`);

      const ages = head3.column('age');
      if (!ages) throw new Error('Age column not found');
      if (toNum(ages, 0) !== 30) throw new Error(`Expected 30, got ${ages[0]}`);
      if (toNum(ages, 1) !== 25) throw new Error(`Expected 25, got ${ages[1]}`);
      if (toNum(ages, 2) !== 35) throw new Error(`Expected 35, got ${ages[2]}`);
    } finally {
      head3.free();
    }
  });

  test('DataFrame.head(1) returns single row', () => {
    const head1 = df.head(1);
    try {
      const shape = head1.shape;
      if (shape.rows !== 1) throw new Error(`Expected 1 row, got ${shape.rows}`);

      const ages = head1.column('age');
      if (!ages) throw new Error('Age column not found');
      if (toNum(ages, 0) !== 30) throw new Error(`Expected 30, got ${ages[0]}`);
    } finally {
      head1.free();
    }
  });

  // ============================================================================
  // Test 4: Tail operation
  // ============================================================================

  test('DataFrame.tail() returns last n rows', () => {
    const tail2 = df.tail(2);
    try {
      const shape = tail2.shape;
      if (shape.rows !== 2) throw new Error(`Expected 2 rows, got ${shape.rows}`);
      if (shape.cols !== 3) throw new Error(`Expected 3 cols, got ${shape.cols}`);

      const ages = tail2.column('age');
      if (!ages) throw new Error('Age column not found');
      // Last 2 rows: Diana (28), Eve (22)
      if (toNum(ages, 0) !== 28) throw new Error(`Expected 28, got ${ages[0]}`);
      if (toNum(ages, 1) !== 22) throw new Error(`Expected 22, got ${ages[1]}`);
    } finally {
      tail2.free();
    }
  });

  // ============================================================================
  // Test 5: Sort operation
  // ============================================================================

  test('DataFrame.sort() ascending', () => {
    const sorted = df.sort('age', false);
    try {
      const ages = sorted.column('age');
      if (!ages) throw new Error('Age column not found');

      // Should be: 22, 25, 28, 30, 35
      if (toNum(ages, 0) !== 22) throw new Error(`Expected 22, got ${ages[0]}`);
      if (toNum(ages, 1) !== 25) throw new Error(`Expected 25, got ${ages[1]}`);
      if (toNum(ages, 2) !== 28) throw new Error(`Expected 28, got ${ages[2]}`);
      if (toNum(ages, 3) !== 30) throw new Error(`Expected 30, got ${ages[3]}`);
      if (toNum(ages, 4) !== 35) throw new Error(`Expected 35, got ${ages[4]}`);
    } finally {
      sorted.free();
    }
  });

  test('DataFrame.sort() descending', () => {
    const sorted = df.sort('age', true);
    try {
      const ages = sorted.column('age');
      if (!ages) throw new Error('Age column not found');

      // Should be: 35, 30, 28, 25, 22
      if (toNum(ages, 0) !== 35) throw new Error(`Expected 35, got ${ages[0]}`);
      if (toNum(ages, 1) !== 30) throw new Error(`Expected 30, got ${ages[1]}`);
      if (toNum(ages, 2) !== 28) throw new Error(`Expected 28, got ${ages[2]}`);
      if (toNum(ages, 3) !== 25) throw new Error(`Expected 25, got ${ages[3]}`);
      if (toNum(ages, 4) !== 22) throw new Error(`Expected 22, got ${ages[4]}`);
    } finally {
      sorted.free();
    }
  });

  test('DataFrame.sort() by score', () => {
    const sorted = df.sort('score', false);
    try {
      const scores = sorted.column('score');
      if (!scores) throw new Error('Score column not found');

      // Should be ascending: 87.3, 88.0, 91.0, 92.5, 95.5
      if (scores[0] !== 87.3) throw new Error(`Expected 87.3, got ${scores[0]}`);
      if (scores[4] !== 95.5) throw new Error(`Expected 95.5, got ${scores[4]}`);
    } finally {
      sorted.free();
    }
  });

  // ============================================================================
  // Test 6: Filter operation
  // ============================================================================

  test('DataFrame.filter() with >= operator', () => {
    const adults = df.filter('age', '>=', 30);
    try {
      const shape = adults.shape;
      // Alice (30), Charlie (35) = 2 rows
      if (shape.rows !== 2) throw new Error(`Expected 2 rows, got ${shape.rows}`);

      const ages = adults.column('age');
      if (!ages) throw new Error('Age column not found');
      if (toNum(ages, 0) !== 30) throw new Error(`Expected 30, got ${ages[0]}`);
      if (toNum(ages, 1) !== 35) throw new Error(`Expected 35, got ${ages[1]}`);
    } finally {
      adults.free();
    }
  });

  test('DataFrame.filter() with > operator', () => {
    const filtered = df.filter('age', '>', 28);
    try {
      const shape = filtered.shape;
      // Alice (30), Charlie (35) = 2 rows
      if (shape.rows !== 2) throw new Error(`Expected 2 rows, got ${shape.rows}`);
    } finally {
      filtered.free();
    }
  });

  test('DataFrame.filter() with < operator', () => {
    const young = df.filter('age', '<', 28);
    try {
      const shape = young.shape;
      // Bob (25), Eve (22) = 2 rows
      if (shape.rows !== 2) throw new Error(`Expected 2 rows, got ${shape.rows}`);

      const ages = young.column('age');
      if (!ages) throw new Error('Age column not found');
      if (toNum(ages, 0) !== 25) throw new Error(`Expected 25, got ${ages[0]}`);
      if (toNum(ages, 1) !== 22) throw new Error(`Expected 22, got ${ages[1]}`);
    } finally {
      young.free();
    }
  });

  test('DataFrame.filter() with == operator', () => {
    const exact = df.filter('age', '==', 30);
    try {
      const shape = exact.shape;
      // Only Alice (30)
      if (shape.rows !== 1) throw new Error(`Expected 1 row, got ${shape.rows}`);

      const ages = exact.column('age');
      if (!ages) throw new Error('Age column not found');
      if (toNum(ages, 0) !== 30) throw new Error(`Expected 30, got ${ages[0]}`);
    } finally {
      exact.free();
    }
  });

  test('DataFrame.filter() with != operator', () => {
    const notThirty = df.filter('age', '!=', 30);
    try {
      const shape = notThirty.shape;
      // All except Alice = 4 rows
      if (shape.rows !== 4) throw new Error(`Expected 4 rows, got ${shape.rows}`);
    } finally {
      notThirty.free();
    }
  });

  test('DataFrame.filter() on score column', () => {
    const highScorers = df.filter('score', '>=', 90.0);
    try {
      const shape = highScorers.shape;
      // Alice (95.5), Charlie (91.0), Diana (92.5) = 3 rows
      if (shape.rows !== 3) throw new Error(`Expected 3 rows, got ${shape.rows}`);

      const scores = highScorers.column('score');
      if (!scores) throw new Error('Score column not found');
      if (scores[0] !== 95.5) throw new Error(`Expected 95.5, got ${scores[0]}`);
    } finally {
      highScorers.free();
    }
  });

  // ============================================================================
  // Test 7: Chained operations
  // ============================================================================

  test('Chained: filter → select → head', () => {
    const result = df.filter('age', '>=', 25)
      .select(['name', 'age'])
      .head(2);

    try {
      const shape = result.shape;
      // Filter: 4 rows (age >= 25)
      // Select: 2 columns (name, age)
      // Head: 2 rows
      if (shape.rows !== 2) throw new Error(`Expected 2 rows, got ${shape.rows}`);
      if (shape.cols !== 2) throw new Error(`Expected 2 cols, got ${shape.cols}`);

      const cols = result.columns;
      if (!cols.includes('name')) throw new Error('Missing name column');
      if (!cols.includes('age')) throw new Error('Missing age column');
      if (cols.includes('score')) throw new Error('Should not have score column');
    } finally {
      result.free();
    }
  });

  test('Chained: select → sort → tail', () => {
    const result = df.select(['age', 'score'])
      .sort('age', false)
      .tail(3);

    try {
      const shape = result.shape;
      // Last 3 rows after sorting by age: 28, 30, 35
      if (shape.rows !== 3) throw new Error(`Expected 3 rows, got ${shape.rows}`);

      const ages = result.column('age');
      if (!ages) throw new Error('Age column not found');
      if (toNum(ages, 0) !== 28) throw new Error(`Expected 28, got ${ages[0]}`);
      if (toNum(ages, 1) !== 30) throw new Error(`Expected 30, got ${ages[1]}`);
      if (toNum(ages, 2) !== 35) throw new Error(`Expected 35, got ${ages[2]}`);
    } finally {
      result.free();
    }
  });

  // ============================================================================
  // Test 8: Error handling
  // ============================================================================

  test('select() throws on invalid column name', () => {
    let threw = false;
    try {
      const bad = df.select(['nonexistent']);
      bad.free();
    } catch (err) {
      threw = true;
    }
    if (!threw) throw new Error('Should have thrown error');
  });

  test('filter() throws on non-numeric column', () => {
    let threw = false;
    try {
      // 'name' is a string column, can't filter numerically
      const bad = df.filter('name', '>', 0);
      bad.free();
    } catch (err) {
      threw = true;
    }
    if (!threw) throw new Error('Should have thrown error');
  });

  // ============================================================================
  // Cleanup and report
  // ============================================================================

  df.free();

  console.log(`\n${'='.repeat(60)}`);
  console.log(`📊 Test Results: ${passedTests}/${totalTests} passed`);
  console.log(`${'='.repeat(60)}`);

  if (passedTests === totalTests) {
    console.log('✅ All tests passed!');
    process.exit(0);
  } else {
    console.log(`❌ ${totalTests - passedTests} test(s) failed`);
    process.exit(1);
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
