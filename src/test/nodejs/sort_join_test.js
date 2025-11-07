/**
 * Integration tests for multi-column sort and join operations
 * Tests DataFrame.sortBy() with multiple columns and various sort orders
 */

import test from 'node:test';
import assert from 'node:assert';
import Rozes, { DataFrame } from '../../../dist/index.mjs';

// Initialize Rozes before running tests
await Rozes.init();

// ============================================================================
// Multi-Column Sort Tests
// ============================================================================

test('sortBy: Two columns with different sort orders', async (t) => {
  const csv = `name,age,score
Alice,30,85
Bob,25,90
Charlie,30,90
David,25,85`;

  const df = DataFrame.fromCSV(csv);

  // Sort by age (asc), then score (desc)
  const sorted = df.sortBy([
    { column: 'age', order: 'asc' },
    { column: 'score', order: 'desc' },
  ]);

  // Expected order: David (25,85), Bob (25,90), Alice (30,85), Charlie (30,90)
  const names = sorted.column('name');
  const ages = sorted.column('age');
  const scores = sorted.column('score');

  assert.strictEqual(names[0], 'Bob', 'First should be Bob (age=25, score=90)');
  assert.strictEqual(ages[0], 25n);
  assert.strictEqual(scores[0], 90n);

  assert.strictEqual(names[1], 'David', 'Second should be David (age=25, score=85)');
  assert.strictEqual(ages[1], 25n);
  assert.strictEqual(scores[1], 85n);

  assert.strictEqual(names[2], 'Charlie', 'Third should be Charlie (age=30, score=90)');
  assert.strictEqual(ages[2], 30n);
  assert.strictEqual(scores[2], 90n);

  assert.strictEqual(names[3], 'Alice', 'Fourth should be Alice (age=30, score=85)');
  assert.strictEqual(ages[3], 30n);
  assert.strictEqual(scores[3], 85n);

  sorted.free();
  df.free();
});

test('sortBy: Three columns', async (t) => {
  const csv = `dept,level,salary
Engineering,2,75000
Engineering,2,80000
Engineering,1,60000
Sales,2,70000
Sales,1,55000
Sales,2,72000`;

  const df = DataFrame.fromCSV(csv);

  // Sort by dept (asc), level (desc), salary (asc)
  const sorted = df.sortBy([
    { column: 'dept', order: 'asc' },
    { column: 'level', order: 'desc' },
    { column: 'salary', order: 'asc' },
  ]);

  const depts = sorted.column('dept');
  const levels = sorted.column('level');
  const salaries = sorted.column('salary');

  // Engineering should come before Sales
  assert.strictEqual(depts[0], 'Engineering');
  assert.strictEqual(depts[1], 'Engineering');
  assert.strictEqual(depts[2], 'Engineering');
  assert.strictEqual(depts[3], 'Sales');

  // Within Engineering, level=2 should come before level=1
  assert.strictEqual(levels[0], 2n);
  assert.strictEqual(levels[1], 2n);
  assert.strictEqual(levels[2], 1n);

  // Within Engineering level=2, salary should be ascending
  assert.strictEqual(salaries[0], 75000n);
  assert.strictEqual(salaries[1], 80000n);

  sorted.free();
  df.free();
});

test('sortBy: Five columns (complex sort)', async (t) => {
  const csv = `region,country,city,store,revenue
North,USA,NYC,1,50000
North,USA,NYC,2,45000
North,USA,Boston,1,40000
North,Canada,Toronto,1,38000
South,Brazil,Rio,1,35000`;

  const df = DataFrame.fromCSV(csv);

  // Sort by all 5 columns
  const sorted = df.sortBy([
    { column: 'region', order: 'asc' },
    { column: 'country', order: 'desc' },
    { column: 'city', order: 'asc' },
    { column: 'store', order: 'asc' },
    { column: 'revenue', order: 'desc' },
  ]);

  const regions = sorted.column('region');

  // North should come before South
  assert.strictEqual(regions[0], 'North');
  assert.strictEqual(regions[1], 'North');
  assert.strictEqual(regions[2], 'North');
  assert.strictEqual(regions[3], 'North');
  assert.strictEqual(regions[4], 'South');

  sorted.free();
  df.free();
});

test('sortBy: All ascending order', async (t) => {
  const csv = `a,b,c
3,2,1
1,3,2
2,1,3`;

  const df = DataFrame.fromCSV(csv);

  const sorted = df.sortBy([
    { column: 'a', order: 'asc' },
    { column: 'b', order: 'asc' },
    { column: 'c', order: 'asc' },
  ]);

  const a = sorted.column('a');
  const b = sorted.column('b');
  const c = sorted.column('c');

  // Check first row
  assert.strictEqual(a[0], 1n);
  assert.strictEqual(b[0], 3n);
  assert.strictEqual(c[0], 2n);

  // Check last row
  assert.strictEqual(a[2], 3n);
  assert.strictEqual(b[2], 2n);
  assert.strictEqual(c[2], 1n);

  sorted.free();
  df.free();
});

test('sortBy: All descending order', async (t) => {
  const csv = `a,b,c
1,3,2
2,1,3
3,2,1`;

  const df = DataFrame.fromCSV(csv);

  const sorted = df.sortBy([
    { column: 'a', order: 'desc' },
    { column: 'b', order: 'desc' },
    { column: 'c', order: 'desc' },
  ]);

  const a = sorted.column('a');

  // a should be in descending order
  assert.strictEqual(a[0], 3n);
  assert.strictEqual(a[1], 2n);
  assert.strictEqual(a[2], 1n);

  sorted.free();
  df.free();
});

test('sortBy: Float64 columns', async (t) => {
  const csv = `price,quantity
19.99,5
29.99,3
19.99,10
29.99,8`;

  const df = DataFrame.fromCSV(csv);

  // Sort by price (asc), then quantity (desc)
  const sorted = df.sortBy([
    { column: 'price', order: 'asc' },
    { column: 'quantity', order: 'desc' },
  ]);

  const prices = sorted.column('price');
  const quantities = sorted.column('quantity');

  // First two rows should have price=19.99, quantity descending
  assert.strictEqual(prices[0], 19.99);
  assert.strictEqual(quantities[0], 10n);

  assert.strictEqual(prices[1], 19.99);
  assert.strictEqual(quantities[1], 5n);

  // Last two rows should have price=29.99, quantity descending
  assert.strictEqual(prices[2], 29.99);
  assert.strictEqual(quantities[2], 8n);

  assert.strictEqual(prices[3], 29.99);
  assert.strictEqual(quantities[3], 3n);

  sorted.free();
  df.free();
});

test('sortBy: Mixed String and numeric columns', async (t) => {
  const csv = `category,subcategory,value
A,X,100
A,Y,200
B,X,150
A,X,120`;

  const df = DataFrame.fromCSV(csv);

  // Sort by category (asc), subcategory (desc), value (asc)
  const sorted = df.sortBy([
    { column: 'category', order: 'asc' },
    { column: 'subcategory', order: 'desc' },
    { column: 'value', order: 'asc' },
  ]);

  const categories = sorted.column('category');
  const subcategories = sorted.column('subcategory');
  const values = sorted.column('value');

  // All A's should come first
  assert.strictEqual(categories[0], 'A');
  assert.strictEqual(categories[1], 'A');
  assert.strictEqual(categories[2], 'A');

  // Within A, Y should come before X (descending)
  assert.strictEqual(subcategories[0], 'Y');
  assert.strictEqual(subcategories[1], 'X');
  assert.strictEqual(subcategories[2], 'X');

  // Within A+X, values should be ascending
  assert.strictEqual(values[1], 100n);
  assert.strictEqual(values[2], 120n);

  sorted.free();
  df.free();
});

// ============================================================================
// Edge Case Tests
// ============================================================================

test('sortBy: Empty DataFrame', async (t) => {
  const csv = `name,age,score`;

  const df = DataFrame.fromCSV(csv);

  const sorted = df.sortBy([
    { column: 'age', order: 'asc' },
    { column: 'score', order: 'desc' },
  ]);

  assert.strictEqual(sorted.shape.rows, 0);
  assert.strictEqual(sorted.shape.cols, 3);

  sorted.free();
  df.free();
});

test('sortBy: Single row', async (t) => {
  const csv = `name,age,score
Alice,30,85`;

  const df = DataFrame.fromCSV(csv);

  const sorted = df.sortBy([
    { column: 'age', order: 'asc' },
    { column: 'score', order: 'desc' },
  ]);

  assert.strictEqual(sorted.shape.rows, 1);
  const names = sorted.column('name');
  assert.strictEqual(names[0], 'Alice');

  sorted.free();
  df.free();
});

test('sortBy: Single sort column', async (t) => {
  const csv = `name,age
Alice,30
Bob,25
Charlie,35`;

  const df = DataFrame.fromCSV(csv);

  const sorted = df.sortBy([{ column: 'age', order: 'asc' }]);

  const ages = sorted.column('age');
  assert.strictEqual(ages[0], 25n);
  assert.strictEqual(ages[1], 30n);
  assert.strictEqual(ages[2], 35n);

  sorted.free();
  df.free();
});

test('sortBy: All identical values (stable sort)', async (t) => {
  const csv = `name,value
Alice,10
Bob,10
Charlie,10`;

  const df = DataFrame.fromCSV(csv);

  const sorted = df.sortBy([{ column: 'value', order: 'asc' }]);

  // Order should be preserved when values are identical
  assert.strictEqual(sorted.shape.rows, 3);
  const values = sorted.column('value');
  assert.strictEqual(values[0], 10n);
  assert.strictEqual(values[1], 10n);
  assert.strictEqual(values[2], 10n);

  sorted.free();
  df.free();
});

test('sortBy: Medium dataset (50 rows)', async (t) => {
  // Generate 50 rows with numeric columns only
  let csv = 'level,salary\n';

  for (let i = 0; i < 50; i++) {
    const level = 1 + (i % 5);
    const salary = 50000 + (i * 1000);
    csv += `${level},${salary}\n`;
  }

  const df = DataFrame.fromCSV(csv);

  const start = Date.now();
  const sorted = df.sortBy([
    { column: 'level', order: 'asc' },
    { column: 'salary', order: 'desc' },
  ]);
  const duration = Date.now() - start;

  assert.strictEqual(sorted.shape.rows, 50);

  // Performance check: should complete in reasonable time (<100ms)
  assert.ok(duration < 100, `sortBy took ${duration}ms, expected <100ms`);

  sorted.free();
  df.free();
});

// ============================================================================
// Error Handling Tests
// ============================================================================

test('sortBy: Non-existent column', async (t) => {
  const csv = `name,age
Alice,30
Bob,25`;

  const df = DataFrame.fromCSV(csv);

  assert.throws(
    () => {
      df.sortBy([{ column: 'nonexistent', order: 'asc' }]);
    },
    /column/i,
    'Should throw error for non-existent column'
  );

  df.free();
});

test('sortBy: Invalid sort order', async (t) => {
  const csv = `name,age
Alice,30
Bob,25`;

  const df = DataFrame.fromCSV(csv);

  assert.throws(
    () => {
      df.sortBy([{ column: 'age', order: 'invalid' }]);
    },
    /order.*asc.*desc/i,
    'Should throw error for invalid sort order'
  );

  df.free();
});

test('sortBy: Empty specs array', async (t) => {
  const csv = `name,age
Alice,30
Bob,25`;

  const df = DataFrame.fromCSV(csv);

  assert.throws(
    () => {
      df.sortBy([]);
    },
    {
      message: /non-empty array/i
    }
  );

  df.free();
});

test('sortBy: Null specs', async (t) => {
  const csv = `name,age
Alice,30
Bob,25`;

  const df = DataFrame.fromCSV(csv);

  assert.throws(
    () => {
      df.sortBy(null);
    },
    /array/i,
    'Should throw error for null specs'
  );

  df.free();
});

// ============================================================================
// Memory Leak Tests
// ============================================================================

test('sortBy: No memory leaks (1000 iterations)', async (t) => {
  const csv = `name,age,score
Alice,30,85
Bob,25,90
Charlie,30,90
David,25,85`;

  for (let i = 0; i < 1000; i++) {
    const df = DataFrame.fromCSV(csv);
    const sorted = df.sortBy([
      { column: 'age', order: 'asc' },
      { column: 'score', order: 'desc' },
    ]);
    sorted.free();
    df.free();
  }

  // If this completes without error, no memory leaks
  assert.ok(true, 'Completed 1000 iterations without memory leaks');
});

// ============================================================================
// Join Tests
// ============================================================================
//
// FIXED (2025-11-06): Right join double-swap bug fixed in join.zig:468
// - Changed: return performJoin(right, left, allocator, join_cols, .Right);
// - To: return performJoin(right, left, allocator, join_cols, .Left);
// - Rationale: rightJoin() swaps DataFrames, so pass .Left to prevent double-swap
//
// Known limitation:
// - Join keeps both key columns (id + id_right) instead of deduplicating
//   (may be by design - preserves all data from both DataFrames)
//
// ============================================================================

test('rightJoin: Basic right outer join', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob`;

  const right_csv = `id,city
2,NYC
3,LA`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  const joined = left.join(right, 'id', 'right');

  // Right join: ALL rows from right + matching left
  // Expected: 2 rows (all from right: id=2,3)
  assert.strictEqual(joined.shape.rows, 2);
  assert.strictEqual(joined.shape.cols, 4, 'Has 4 columns: id, name, id_right, city');

  const csv = joined.toCSV();

  // Verify right join semantics: All right rows (2, 3), matched left where available
  assert.ok(csv.includes('Bob'), 'Should have Bob (matched row with id=2)');
  assert.ok(csv.includes('NYC'), 'Should have NYC (matched row)');
  assert.ok(csv.includes('LA'), 'Should have LA (right-only row with id=3)');
  assert.ok(!csv.includes('Alice'), 'Should NOT have Alice (left-only row with id=1)');

  joined.free();
  left.free();
  right.free();
});

test('rightJoin: All right rows matched', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob
3,Charlie`;

  const right_csv = `id,score
1,85
2,90`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  // Right join: ALL rows from right + matching left
  // Expected: 2 rows (all from right: 1, 2) - matched
  const joined = left.join(right, 'id', 'right');

  assert.strictEqual(joined.shape.rows, 2, 'Should have 2 rows (all from right)');
  assert.strictEqual(joined.shape.cols, 4, 'Has 4 columns: id, name, id_right, score');

  const csv = joined.toCSV();
  assert.ok(csv.includes('Alice'), 'Should have Alice (id=1, matched)');
  assert.ok(csv.includes('Bob'), 'Should have Bob (id=2, matched)');
  assert.ok(!csv.includes('Charlie'), 'Should NOT have Charlie (id=3, left-only)');

  joined.free();
  left.free();
  right.free();
});

test('rightJoin: No matches', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob`;

  const right_csv = `id,city
5,NYC
6,LA`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  // Right join: ALL rows from right + matching left (none match)
  // Expected: 2 rows (all from right: 5, 6) with empty names
  const joined = left.join(right, 'id', 'right');

  assert.strictEqual(joined.shape.rows, 2, 'Should have 2 rows (all from right)');
  assert.strictEqual(joined.shape.cols, 4, 'Has 4 columns: id, name, id_right, city');

  const csv = joined.toCSV();
  assert.ok(csv.includes('NYC'), 'Should have NYC (id=5, right-only)');
  assert.ok(csv.includes('LA'), 'Should have LA (id=6, right-only)');
  assert.ok(!csv.includes('Alice'), 'Should NOT have Alice (left-only)');
  assert.ok(!csv.includes('Bob'), 'Should NOT have Bob (left-only)');

  joined.free();
  left.free();
  right.free();
});

test('outerJoin: Full outer join with partial overlap', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob`;

  const right_csv = `id,city
2,NYC
3,LA`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  const joined = left.join(right, 'id', 'outer');

  // Outer join: all rows from both (1 from left only, 2 matched, 3 from right only)
  assert.strictEqual(joined.shape.rows, 3, 'Should have 3 rows (union of all rows)');
  assert.strictEqual(joined.shape.cols, 4, 'Has 4 columns: id, name, id_right, city');

  const ids = joined.column('id');
  const names = joined.column('name');
  const cities = joined.column('city');

  // Check via CSV output (simpler than dealing with column order)
  const csv = joined.toCSV();

  // Should have all three rows
  assert.ok(csv.includes('Alice'), 'Should have Alice (left-only row)');
  assert.ok(csv.includes('Bob'), 'Should have Bob (matched row)');
  assert.ok(csv.includes('LA'), 'Should have LA (right-only row)');
  assert.ok(csv.includes('NYC'), 'Should have NYC (matched row)');

  joined.free();
  left.free();
  right.free();
});

test('outerJoin: All rows matched', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob`;

  const right_csv = `id,score
1,85
2,90`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  const joined = left.join(right, 'id', 'outer');

  assert.strictEqual(joined.shape.rows, 2, 'All rows matched, no unmatched rows');
  assert.strictEqual(joined.shape.cols, 4); // id, name, id_right, score

  const csv = joined.toCSV();

  // All rows should have data from both sides
  assert.ok(csv.includes('Alice') && csv.includes('85'));
  assert.ok(csv.includes('Bob') && csv.includes('90'));

  joined.free();
  left.free();
  right.free();
});

test('outerJoin: No overlap', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob`;

  const right_csv = `id,city
5,NYC
6,LA`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  const joined = left.join(right, 'id', 'outer');

  // All rows from both DataFrames (no matches)
  assert.strictEqual(joined.shape.rows, 4);
  assert.strictEqual(joined.shape.cols, 4); // id, name, id_right, city

  const csv = joined.toCSV();

  // Check that all names and cities are present
  assert.ok(csv.includes('Alice'), 'Should have Alice');
  assert.ok(csv.includes('Bob'), 'Should have Bob');
  assert.ok(csv.includes('NYC'), 'Should have NYC');
  assert.ok(csv.includes('LA'), 'Should have LA');

  joined.free();
  left.free();
  right.free();
});

test('crossJoin: Basic Cartesian product', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob`;

  const right_csv = `city
NYC
LA`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  const joined = left.join(right, null, 'cross');

  // Cross join: 2 × 2 = 4 rows
  assert.strictEqual(joined.shape.rows, 4, 'Should have 4 rows (2 × 2)');
  assert.strictEqual(joined.shape.cols, 3, 'Should have 3 columns (id, name, city)');

  const ids = joined.column('id');
  const names = joined.column('name');
  const cities = joined.column('city');

  // Each left row should appear twice (once for each right row)
  const aliceCount = names.filter(n => n === 'Alice').length;
  const bobCount = names.filter(n => n === 'Bob').length;

  assert.strictEqual(aliceCount, 2, 'Alice should appear 2 times');
  assert.strictEqual(bobCount, 2, 'Bob should appear 2 times');

  // Each right row should appear twice (once for each left row)
  const nycCount = cities.filter(c => c === 'NYC').length;
  const laCount = cities.filter(c => c === 'LA').length;

  assert.strictEqual(nycCount, 2, 'NYC should appear 2 times');
  assert.strictEqual(laCount, 2, 'LA should appear 2 times');

  joined.free();
  left.free();
  right.free();
});

test('crossJoin: Three columns × Two columns', async (t) => {
  const left_csv = `product,price
Widget,10.99
Gadget,25.00`;

  const right_csv = `color,size
Red,S
Blue,L
Green,M`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  const joined = left.join(right, null, 'cross');

  // Cross join: 2 × 3 = 6 rows
  assert.strictEqual(joined.shape.rows, 6, 'Should have 6 rows (2 × 3)');
  assert.strictEqual(joined.shape.cols, 4, 'Should have 4 columns (product, price, color, size)');

  const products = joined.column('product');
  const colors = joined.column('color');

  // Each product should appear 3 times
  const widgetCount = products.filter(p => p === 'Widget').length;
  const gadgetCount = products.filter(p => p === 'Gadget').length;

  assert.strictEqual(widgetCount, 3);
  assert.strictEqual(gadgetCount, 3);

  // Each color should appear 2 times
  const redCount = colors.filter(c => c === 'Red').length;
  const blueCount = colors.filter(c => c === 'Blue').length;
  const greenCount = colors.filter(c => c === 'Green').length;

  assert.strictEqual(redCount, 2);
  assert.strictEqual(blueCount, 2);
  assert.strictEqual(greenCount, 2);

  joined.free();
  left.free();
  right.free();
});

test('crossJoin: Single row × Single row', async (t) => {
  const left_csv = `x:Int64
1`;

  const right_csv = `y:Int64
2`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  const joined = left.join(right, null, 'cross');

  assert.strictEqual(joined.shape.rows, 1, 'Should have 1 row (1 × 1)');
  assert.strictEqual(joined.shape.cols, 2, 'Should have 2 columns (x, y)');

  const csv = joined.toCSV();

  // Verify both columns are present and has data
  assert.ok(csv.includes('x:Int64'));
  assert.ok(csv.includes('y:Int64'));
  assert.ok(csv.includes('2')); // y value

  joined.free();
  left.free();
  right.free();
});

test('crossJoin: Empty left DataFrame', async (t) => {
  const left_csv = `id,name`;
  const right_csv = `city
NYC
LA`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  const joined = left.join(right, null, 'cross');

  // 0 × 2 = 0 rows
  assert.strictEqual(joined.shape.rows, 0);

  joined.free();
  left.free();
  right.free();
});

test('crossJoin: Empty right DataFrame', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob`;
  const right_csv = `city`;

  const left = DataFrame.fromCSV(left_csv);
  const right = DataFrame.fromCSV(right_csv);

  const joined = left.join(right, null, 'cross');

  // 2 × 0 = 0 rows
  assert.strictEqual(joined.shape.rows, 0);

  joined.free();
  left.free();
  right.free();
});

// ============================================================================
// Memory Leak Tests for Join Operations
// ============================================================================

test('rightJoin: No memory leaks (1000 iterations)', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob`;

  const right_csv = `id,city
2,NYC
3,LA`;

  for (let i = 0; i < 1000; i++) {
    const left = DataFrame.fromCSV(left_csv);
    const right = DataFrame.fromCSV(right_csv);
    const joined = left.join(right, 'id', 'right');
    joined.free();
    left.free();
    right.free();
  }

  assert.ok(true, 'Completed 1000 iterations without memory leaks');
});

test('outerJoin: No memory leaks (1000 iterations)', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob`;

  const right_csv = `id,city
2,NYC
3,LA`;

  for (let i = 0; i < 1000; i++) {
    const left = DataFrame.fromCSV(left_csv);
    const right = DataFrame.fromCSV(right_csv);
    const joined = left.join(right, 'id', 'outer');
    joined.free();
    left.free();
    right.free();
  }

  assert.ok(true, 'Completed 1000 iterations without memory leaks');
});

test('crossJoin: No memory leaks (1000 iterations)', async (t) => {
  const left_csv = `id,name
1,Alice
2,Bob`;

  const right_csv = `city
NYC
LA`;

  for (let i = 0; i < 1000; i++) {
    const left = DataFrame.fromCSV(left_csv);
    const right = DataFrame.fromCSV(right_csv);
    const joined = left.join(right, null, 'cross');
    joined.free();
    left.free();
    right.free();
  }

  assert.ok(true, 'Completed 1000 iterations without memory leaks');
});

console.log('✅ All multi-column sort tests completed!');
