/**
 * Node.js Tests for GroupBy and Join Operations (Priority 3 - Milestone 1.1.0)
 *
 * Tests: groupBy (sum, mean, count, min, max), join (inner, left)
 */

const { Rozes } = require('../../../dist/index.js');

async function main() {
  console.log('🧪 Testing GroupBy and Join Operations (Priority 3)\n');

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

  // Helper to compare floats with tolerance
  function assertClose(actual, expected, tolerance = 0.01, msg = '') {
    const diff = Math.abs(actual - expected);
    if (diff > tolerance) {
      throw new Error(msg || `Expected ${expected}, got ${actual} (diff: ${diff})`);
    }
  }

  // ============================================================================
  // Test 1: GroupBy with Sum
  // ============================================================================

  const salesData = `region,product,sales
North,A,100
North,B,150
South,A,200
South,B,250
East,A,300
East,B,350`;

  const salesDF = rozes.DataFrame.fromCSV(salesData);

  test('GroupBy.sum() aggregates correctly', () => {
    const grouped = salesDF.groupBy('region', 'sales', 'sum');
    try {
      const shape = grouped.shape;
      if (shape.rows !== 3) throw new Error(`Expected 3 groups, got ${shape.rows}`);
      if (shape.cols !== 2) throw new Error(`Expected 2 cols, got ${shape.cols}`);

      // Check column names
      const cols = grouped.columns;
      if (!cols.includes('region')) throw new Error('Missing region column');
      if (!cols.includes('sales_Sum')) throw new Error('Missing sales_Sum column');

      // Check aggregated values
      const sums = grouped.column('sales_Sum');
      if (!sums) throw new Error('sales_Sum column not found');
      if (sums.length !== 3) throw new Error(`Expected 3 values, got ${sums.length}`);

      // Expected sums: North=250, South=450, East=650
      // Note: We can't read string column ('region') in Node.js yet (Priority 4)
      // So we'll verify numeric values are correct
      const sumValues = [];
      for (let i = 0; i < sums.length; i++) {
        const sum = toNum(sums, i);
        if (sum <= 0) throw new Error(`Expected positive sum, got ${sum}`);
        sumValues.push(sum);
      }

      // Check that we have the expected sum values (order may vary)
      sumValues.sort((a, b) => a - b);
      const expected = [250, 450, 650];
      for (let i = 0; i < expected.length; i++) {
        if (sumValues[i] !== expected[i]) {
          throw new Error(`Expected sum ${expected[i]}, got ${sumValues[i]}`);
        }
      }
    } finally {
      grouped.free();
    }
  });

  // ============================================================================
  // Test 2: GroupBy with Mean
  // ============================================================================

  const ageData = `city,age
NYC,30
NYC,40
LA,25
LA,35
SF,50`;

  const ageDF = rozes.DataFrame.fromCSV(ageData);

  test('GroupBy.mean() calculates average correctly', () => {
    const grouped = ageDF.groupBy('city', 'age', 'mean');
    try {
      const shape = grouped.shape;
      if (shape.rows !== 3) throw new Error(`Expected 3 cities, got ${shape.rows}`);
      if (shape.cols !== 2) throw new Error(`Expected 2 cols, got ${shape.cols}`);

      // Check column names
      const cols = grouped.columns;
      if (!cols.includes('city')) throw new Error('Missing city column');
      if (!cols.includes('age_Mean')) throw new Error('Missing age_Mean column');

      // Check mean values
      const means = grouped.column('age_Mean');
      if (!means) throw new Error('age_mean column not found');
      if (means.length !== 3) throw new Error(`Expected 3 values, got ${means.length}`);

      // Expected means: NYC=35, LA=30, SF=50
      // All means should be between 25 and 50
      for (let i = 0; i < means.length; i++) {
        const mean = toNum(means, i);
        if (mean < 25 || mean > 50) {
          throw new Error(`Mean out of expected range: ${mean}`);
        }
      }
    } finally {
      grouped.free();
    }
  });

  // ============================================================================
  // Test 3: GroupBy with Count
  // ============================================================================

  const categoryData = `category,item,price
Food,Apple,1.5
Food,Banana,0.8
Food,Orange,1.2
Electronics,Phone,500
Electronics,Laptop,1200
Clothing,Shirt,25`;

  const categoryDF = rozes.DataFrame.fromCSV(categoryData);

  test('GroupBy.count() counts rows per group', () => {
    const grouped = categoryDF.groupBy('category', 'item', 'count');
    try {
      const shape = grouped.shape;
      if (shape.rows !== 3) throw new Error(`Expected 3 categories, got ${shape.rows}`);

      // Check column names
      const cols = grouped.columns;
      if (!cols.includes('category')) throw new Error('Missing category column');
      if (!cols.includes('item_Count')) throw new Error('Missing item_Count column');

      // Check counts
      const counts = grouped.column('item_Count');
      if (!counts) throw new Error('item_count column not found');

      // Expected counts: Food=3, Electronics=2, Clothing=1
      let total = 0;
      for (let i = 0; i < counts.length; i++) {
        const count = toNum(counts, i);
        if (count < 1 || count > 3) {
          throw new Error(`Count out of expected range: ${count}`);
        }
        total += count;
      }
      if (total !== 6) throw new Error(`Expected total count of 6, got ${total}`);
    } finally {
      grouped.free();
    }
  });

  // ============================================================================
  // Test 4: GroupBy with Min and Max
  // ============================================================================

  const scoreData = `student,test_id,score
Alice,1,85
Alice,2,92
Bob,1,78
Bob,2,88
Charlie,1,95
Charlie,2,89`;

  const scoreDF = rozes.DataFrame.fromCSV(scoreData);

  test('GroupBy.min() finds minimum per group', () => {
    const grouped = scoreDF.groupBy('student', 'score', 'min');
    try {
      const shape = grouped.shape;
      if (shape.rows !== 3) throw new Error(`Expected 3 students, got ${shape.rows}`);

      const mins = grouped.column('score_Min');
      if (!mins) throw new Error('score_min column not found');

      // Expected mins: Alice=85, Bob=78, Charlie=89
      for (let i = 0; i < mins.length; i++) {
        const min = toNum(mins, i);
        if (min < 78 || min > 95) {
          throw new Error(`Min out of expected range: ${min}`);
        }
      }
    } finally {
      grouped.free();
    }
  });

  test('GroupBy.max() finds maximum per group', () => {
    const grouped = scoreDF.groupBy('student', 'score', 'max');
    try {
      const shape = grouped.shape;
      if (shape.rows !== 3) throw new Error(`Expected 3 students, got ${shape.rows}`);

      const maxs = grouped.column('score_Max');
      if (!maxs) throw new Error('score_max column not found');

      // Expected maxs: Alice=92, Bob=88, Charlie=95
      for (let i = 0; i < maxs.length; i++) {
        const max = toNum(maxs, i);
        if (max < 85 || max > 95) {
          throw new Error(`Max out of expected range: ${max}`);
        }
      }
    } finally {
      grouped.free();
    }
  });

  // ============================================================================
  // Test 5: Inner Join
  // ============================================================================

  const usersCSV = `user_id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35
4,Diana,28`;

  const ordersCSV = `order_id,user_id,amount
101,1,50
102,2,75
103,1,100
104,3,125`;

  const usersDF = rozes.DataFrame.fromCSV(usersCSV);
  const ordersDF = rozes.DataFrame.fromCSV(ordersCSV);

  test('Join.inner() joins on single column', () => {
    const joined = usersDF.join(ordersDF, 'user_id', 'inner');
    try {
      const shape = joined.shape;
      if (shape.rows !== 4) throw new Error(`Expected 4 joined rows, got ${shape.rows}`);

      // Check columns exist
      const cols = joined.columns;
      if (!cols.includes('user_id')) throw new Error('Missing user_id column');
      if (!cols.includes('name')) throw new Error('Missing name column');
      if (!cols.includes('age')) throw new Error('Missing age column');
      if (!cols.includes('order_id')) throw new Error('Missing order_id column');
      if (!cols.includes('amount')) throw new Error('Missing amount column');

      // Only users 1, 2, 3 should be in result (Diana has no orders)
      const userIds = joined.column('user_id');
      if (!userIds) throw new Error('user_id column not found');

      for (let i = 0; i < userIds.length; i++) {
        const userId = toNum(userIds, i);
        if (userId === 4) {
          throw new Error('Diana (user_id=4) should not be in inner join result');
        }
      }
    } finally {
      joined.free();
    }
  });

  test('Join.inner() with no parameters defaults to inner join', () => {
    const joined = usersDF.join(ordersDF, 'user_id'); // Default: inner
    try {
      const shape = joined.shape;
      if (shape.rows !== 4) throw new Error(`Expected 4 joined rows, got ${shape.rows}`);
    } finally {
      joined.free();
    }
  });

  // ============================================================================
  // Test 6: Left Join
  // ============================================================================

  test('Join.left() includes all left rows', () => {
    const joined = usersDF.join(ordersDF, 'user_id', 'left');
    try {
      const shape = joined.shape;
      if (shape.rows < 4) {
        throw new Error(`Expected at least 4 rows (all users), got ${shape.rows}`);
      }

      // All 4 users should be in result
      const userIds = joined.column('user_id');
      if (!userIds) throw new Error('user_id column not found');

      // Check that we have users 1, 2, 3, 4
      const userIdSet = new Set();
      for (let i = 0; i < userIds.length; i++) {
        userIdSet.add(toNum(userIds, i));
      }

      for (let expectedId = 1; expectedId <= 4; expectedId++) {
        if (!userIdSet.has(expectedId)) {
          throw new Error(`User ${expectedId} missing from left join`);
        }
      }
    } finally {
      joined.free();
    }
  });

  // ============================================================================
  // Test 7: Join on multiple columns
  // ============================================================================

  const locationsCSV = `city,state,population
NYC,NY,8000000
LA,CA,4000000`;

  const weatherCSV = `city,state,temp
NYC,NY,68
LA,CA,75`;

  const locDF = rozes.DataFrame.fromCSV(locationsCSV);
  const weatherDF = rozes.DataFrame.fromCSV(weatherCSV);

  test('Join on multiple columns', () => {
    const joined = locDF.join(weatherDF, ['city', 'state']);
    try {
      const shape = joined.shape;
      if (shape.rows !== 2) throw new Error(`Expected 2 rows, got ${shape.rows}`);

      // Should have columns from both DataFrames
      const cols = joined.columns;
      if (!cols.includes('city')) throw new Error('Missing city column');
      if (!cols.includes('state')) throw new Error('Missing state column');
      if (!cols.includes('population')) throw new Error('Missing population column');
      if (!cols.includes('temp')) throw new Error('Missing temp column');
    } finally {
      joined.free();
    }
  });

  // ============================================================================
  // Test 8: Error Cases
  // ============================================================================

  test('GroupBy throws on invalid aggregation function', () => {
    let errorThrown = false;
    try {
      const grouped = salesDF.groupBy('region', 'sales', 'invalid');
      grouped.free();
    } catch (err) {
      errorThrown = true;
      if (!err.message.includes('Invalid aggregation function')) {
        throw new Error(`Wrong error message: ${err.message}`);
      }
    }
    if (!errorThrown) throw new Error('Expected error for invalid aggregation function');
  });

  test('GroupBy throws on empty group column', () => {
    let errorThrown = false;
    try {
      const grouped = salesDF.groupBy('', 'sales', 'sum');
      grouped.free();
    } catch (err) {
      errorThrown = true;
    }
    if (!errorThrown) throw new Error('Expected error for empty group column');
  });

  test('Join throws on invalid join type', () => {
    let errorThrown = false;
    try {
      const joined = usersDF.join(ordersDF, 'user_id', 'invalid');
      joined.free();
    } catch (err) {
      errorThrown = true;
      if (!err.message.includes('Invalid join type')) {
        throw new Error(`Wrong error message: ${err.message}`);
      }
    }
    if (!errorThrown) throw new Error('Expected error for invalid join type');
  });

  test('Join throws on empty join columns', () => {
    let errorThrown = false;
    try {
      const joined = usersDF.join(ordersDF, []);
      joined.free();
    } catch (err) {
      errorThrown = true;
    }
    if (!errorThrown) throw new Error('Expected error for empty join columns');
  });

  // ============================================================================
  // Test 9: Operation Chaining
  // ============================================================================

  test('Chaining: filter → groupBy', () => {
    // Filter adults (age >= 25), then group by city
    const filtered = ageDF.filter('age', '>=', 25);
    try {
      const grouped = filtered.groupBy('city', 'age', 'mean');
      try {
        const shape = grouped.shape;
        if (shape.rows === 0) throw new Error('No groups found after chaining');
      } finally {
        grouped.free();
      }
    } finally {
      filtered.free();
    }
  });

  test('Chaining: join → groupBy', () => {
    // Join users and orders, then group by user_id and sum amounts
    const joined = usersDF.join(ordersDF, 'user_id', 'inner');
    try {
      const grouped = joined.groupBy('user_id', 'amount', 'sum');
      try {
        const shape = grouped.shape;
        // Should have 3 users (1, 2, 3)
        if (shape.rows !== 3) throw new Error(`Expected 3 users with orders, got ${shape.rows}`);

        // Check aggregated amounts
        const sums = grouped.column('amount_Sum');
        if (!sums) throw new Error('amount_sum column not found');

        // User 1 has orders 50 + 100 = 150
        // User 2 has order 75
        // User 3 has order 125
        let total = 0;
        for (let i = 0; i < sums.length; i++) {
          total += toNum(sums, i);
        }
        if (total !== 350) throw new Error(`Expected total of 350, got ${total}`);
      } finally {
        grouped.free();
      }
    } finally {
      joined.free();
    }
  });

  // ============================================================================
  // Test 10: Memory Management
  // ============================================================================

  test('Memory management: multiple groupBy operations', () => {
    for (let i = 0; i < 100; i++) {
      const grouped = salesDF.groupBy('region', 'sales', 'sum');
      grouped.free();
    }
    // If we get here without crashing or memory errors, test passes
  });

  test('Memory management: multiple join operations', () => {
    for (let i = 0; i < 100; i++) {
      const joined = usersDF.join(ordersDF, 'user_id');
      joined.free();
    }
    // If we get here without crashing or memory errors, test passes
  });

  // ============================================================================
  // Cleanup
  // ============================================================================

  salesDF.free();
  ageDF.free();
  categoryDF.free();
  scoreDF.free();
  usersDF.free();
  ordersDF.free();
  locDF.free();
  weatherDF.free();

  // ============================================================================
  // Summary
  // ============================================================================

  console.log(`\n${'='.repeat(60)}`);
  console.log(`Results: ${passedTests}/${totalTests} tests passed`);
  console.log(`${'='.repeat(60)}\n`);

  if (passedTests !== totalTests) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
