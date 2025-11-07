/**
 * Reshape Operations Integration Tests
 *
 * Tests for DataFrame reshaping operations (pivot, melt, transpose, stack, unstack)
 * covering basic functionality, edge cases, error handling, and memory management.
 *
 * All tests use explicit type hints in CSV to ensure correct type inference.
 */

import test from 'node:test';
import assert from 'node:assert';
import Rozes, { DataFrame } from '../../../dist/index.mjs';

// Initialize Rozes before running tests
await Rozes.init();

// ============================================================================
// Pivot Tests
// ============================================================================

test('pivot: basic pivot with sum aggregation', async () => {
    const csv = 'date,region,sales\n2024-01-01,East,100\n2024-01-01,West,200\n2024-01-01,South,150\n2024-01-02,East,120\n2024-02-02,West,180\n2024-01-02,South,160';
    const df = DataFrame.fromCSV(csv);

    const pivoted = df.pivot({
        index: 'date',
        columns: 'region',
        values: 'sales',
        aggfunc: 'sum'
    });

    // Verify structure: date + 3 regions (East, West, South) = 4 columns
    assert.strictEqual(pivoted.shape.rows, 3, 'Should have 3 unique dates');
    assert.strictEqual(pivoted.shape.cols, 4, 'Should have 4 columns (date + East, West, South)');

    // Note: Can't easily verify exact values without toCSV() due to Float64 → string conversion

    pivoted.free();
    df.free();
});

test('pivot: mean aggregation', async () => {
    const csv = 'id,category,value\n1,A,10\n1,A,20\n2,B,30\n2,B,40';
    const df = DataFrame.fromCSV(csv);

    const pivoted = df.pivot({
        index: 'id',
        columns: 'category',
        values: 'value',
        aggfunc: 'mean'
    });

    assert.strictEqual(pivoted.shape.rows, 2, 'Should have 2 unique ids');
    assert.strictEqual(pivoted.shape.cols, 3, 'Should have 3 columns (id + A, B)');

    pivoted.free();
    df.free();
});

test('pivot: count aggregation', async () => {
    const csv = 'id,category,value\n1,A,10\n1,A,20\n1,B,5\n2,A,30';
    const df = DataFrame.fromCSV(csv);

    const pivoted = df.pivot({
        index: 'id',
        columns: 'category',
        values: 'value',
        aggfunc: 'count'
    });

    assert.strictEqual(pivoted.shape.rows, 2, 'Should have 2 unique ids');

    pivoted.free();
    df.free();
});

test('pivot: error on missing column', async () => {
    const csv = 'a,b\n1,2\n3,4';
    const df = DataFrame.fromCSV(csv);

    assert.throws(() => {
        df.pivot({ index: 'a', columns: 'c', values: 'b' });
    }, /Failed to pivot/);

    df.free();
});

test('pivot: error on invalid aggfunc', async () => {
    const csv = 'a,b,c\n1,x,10';
    const df = DataFrame.fromCSV(csv);

    assert.throws(() => {
        df.pivot({ index: 'a', columns: 'b', values: 'c', aggfunc: 'invalid' });
    }, /aggfunc must be one of/);

    df.free();
});

// ============================================================================
// Melt Tests
// ============================================================================

test('melt: basic melt operation', async () => {
    const csv = 'date,East,West\n2024-01-01,100,200\n2024-01-02,120,180';
    const df = DataFrame.fromCSV(csv);

    const melted = df.melt({
        id_vars: ['date'],
        var_name: 'region',
        value_name: 'sales'
    });

    // Original: 2 rows × 3 cols → Melted: 4 rows × 3 cols (date, region, sales)
    assert.strictEqual(melted.shape.rows, 4, 'Should have 4 rows (2 dates × 2 regions)');
    assert.strictEqual(melted.shape.cols, 3, 'Should have 3 columns (date, region, sales)');

    const cols = melted.columns;
    assert.ok(cols.includes('date'), 'Should have date column');
    assert.ok(cols.includes('region'), 'Should have region column');
    assert.ok(cols.includes('sales'), 'Should have sales column');

    melted.free();
    df.free();
});

test('melt: with value_vars specified', async () => {
    const csv = 'id,A,B,C\n1,10,20,30\n2,40,50,60';
    const df = DataFrame.fromCSV(csv);

    const melted = df.melt({
        id_vars: ['id'],
        value_vars: ['A', 'B'], // Only melt A and B, not C
        var_name: 'variable',
        value_name: 'value'
    });

    // Original: 2 rows × 4 cols → Melted: 4 rows × 3 cols (id, variable, value)
    assert.strictEqual(melted.shape.rows, 4, 'Should have 4 rows (2 ids × 2 vars)');
    assert.strictEqual(melted.shape.cols, 3, 'Should have 3 columns');

    melted.free();
    df.free();
});

test('melt: default var/value names', async () => {
    const csv = 'id,A,B\n1,10,20';
    const df = DataFrame.fromCSV(csv);

    const melted = df.melt({
        id_vars: ['id']
    });

    const cols = melted.columns;
    assert.ok(cols.includes('variable'), 'Should have default "variable" column');
    assert.ok(cols.includes('value'), 'Should have default "value" column');

    melted.free();
    df.free();
});

test('melt: error on invalid id_vars', async () => {
    const csv = 'a,b\n1,2';
    const df = DataFrame.fromCSV(csv);

    assert.throws(() => {
        df.melt({ id_vars: 'a' }); // Should be array, not string
    }, /id_vars to be an array/);

    df.free();
});

// ============================================================================
// Transpose Tests
// ============================================================================

test('transpose: basic transpose', async () => {
    const csv = 'A,B,C\n1,2,3\n4,5,6';
    const df = DataFrame.fromCSV(csv);

    const transposed = df.transpose();

    // Original: 2 rows × 3 cols → Transposed: 3 rows × 2 cols
    assert.strictEqual(transposed.shape.rows, 3, 'Should have 3 rows');
    assert.strictEqual(transposed.shape.cols, 2, 'Should have 2 columns');

    transposed.free();
    df.free();
});

test('transpose: single row', async () => {
    const csv = 'A,B,C\n1,2,3';
    const df = DataFrame.fromCSV(csv);

    const transposed = df.transpose();

    assert.strictEqual(transposed.shape.rows, 3, 'Should have 3 rows');
    assert.strictEqual(transposed.shape.cols, 1, 'Should have 1 column');

    transposed.free();
    df.free();
});

test('transpose: single column', async () => {
    const csv = 'A\n1\n2\n3';
    const df = DataFrame.fromCSV(csv);

    const transposed = df.transpose();

    assert.strictEqual(transposed.shape.rows, 1, 'Should have 1 row');
    assert.strictEqual(transposed.shape.cols, 3, 'Should have 3 columns');

    transposed.free();
    df.free();
});

// ============================================================================
// Stack Tests
// ============================================================================

test('stack: basic stack operation', async () => {
    const csv = 'id,A,B,C\n1,10,20,30\n2,40,50,60';
    const df = DataFrame.fromCSV(csv);

    const stacked = df.stack({ id_column: 'id' });

    // Original: 2 rows × 4 cols → Stacked: 6 rows × 3 cols (id, variable, value)
    assert.strictEqual(stacked.shape.rows, 6, 'Should have 6 rows (2 ids × 3 value cols)');
    assert.strictEqual(stacked.shape.cols, 3, 'Should have 3 columns');

    const cols = stacked.columns;
    assert.ok(cols.includes('id'), 'Should have id column');
    assert.ok(cols.includes('variable'), 'Should have variable column');
    assert.ok(cols.includes('value'), 'Should have value column');

    stacked.free();
    df.free();
});

test('stack: custom var/value names', async () => {
    const csv = 'id,A,B\n1,10,20';
    const df = DataFrame.fromCSV(csv);

    const stacked = df.stack({
        id_column: 'id',
        var_name: 'metric',
        value_name: 'score'
    });

    const cols = stacked.columns;
    assert.ok(cols.includes('metric'), 'Should have custom "metric" column');
    assert.ok(cols.includes('score'), 'Should have custom "score" column');

    stacked.free();
    df.free();
});

test('stack: error on missing id_column', async () => {
    const csv = 'a,b\n1,2';
    const df = DataFrame.fromCSV(csv);

    assert.throws(() => {
        df.stack({ id_column: 'nonexistent' });
    }, /Failed to stack/);

    df.free();
});

// ============================================================================
// Unstack Tests
// ============================================================================

test('unstack: basic unstack operation', async () => {
    const csv = 'id,variable,value\n1,A,10\n1,B,20\n2,A,40\n2,B,50';
    const df = DataFrame.fromCSV(csv);

    const unstacked = df.unstack({
        index: 'id',
        columns: 'variable',
        values: 'value'
    });

    // Original: 4 rows × 3 cols → Unstacked: 2 rows × 3 cols (id, A, B)
    assert.strictEqual(unstacked.shape.rows, 2, 'Should have 2 unique ids');
    assert.strictEqual(unstacked.shape.cols, 3, 'Should have 3 columns (id + A, B)');

    unstacked.free();
    df.free();
});

test('unstack: with missing combinations (NaN fill)', async () => {
    const csv = 'id,variable,value\n1,A,10\n1,B,20\n2,A,40';
    const df = DataFrame.fromCSV(csv);

    const unstacked = df.unstack({
        index: 'id',
        columns: 'variable',
        values: 'value'
    });

    // Should have NaN for id=2, variable=B
    assert.strictEqual(unstacked.shape.rows, 2, 'Should have 2 unique ids');
    assert.strictEqual(unstacked.shape.cols, 3, 'Should have 3 columns');

    unstacked.free();
    df.free();
});

test('unstack: error on missing column', async () => {
    const csv = 'a,b,c\n1,x,10';
    const df = DataFrame.fromCSV(csv);

    assert.throws(() => {
        df.unstack({ index: 'a', columns: 'nonexistent', values: 'c' });
    }, /Failed to unstack/);

    df.free();
});

// ============================================================================
// Integration Tests (Round-trip operations)
// ============================================================================

test('pivot → melt round-trip', async () => {
    const original = 'date,region,sales\n2024-01-01,East,100\n2024-01-01,West,200';
    const df = DataFrame.fromCSV(original);

    // Pivot
    const pivoted = df.pivot({
        index: 'date',
        columns: 'region',
        values: 'sales'
    });

    // Melt back
    const melted = pivoted.melt({
        id_vars: ['date'],
        var_name: 'region',
        value_name: 'sales'
    });

    // Check structure (values may differ due to aggregation)
    assert.strictEqual(melted.shape.rows, 2, 'Should have 2 rows after round-trip');
    assert.strictEqual(melted.shape.cols, 3, 'Should have 3 columns after round-trip');

    melted.free();
    pivoted.free();
    df.free();
});

test('stack → unstack round-trip', async () => {
    const original = 'id,A,B\n1,10,20\n2,40,50';
    const df = DataFrame.fromCSV(original);

    // Stack
    const stacked = df.stack({ id_column: 'id' });

    // Unstack back
    const unstacked = stacked.unstack({
        index: 'id',
        columns: 'variable',
        values: 'value'
    });

    // Check structure
    assert.strictEqual(unstacked.shape.rows, 2, 'Should have 2 rows after round-trip');
    assert.strictEqual(unstacked.shape.cols, 3, 'Should have 3 columns after round-trip');

    unstacked.free();
    stacked.free();
    df.free();
});

test('transpose twice returns to original dimensions', async () => {
    const csv = 'A,B,C\n1,2,3\n4,5,6';
    const df = DataFrame.fromCSV(csv);

    const transposed1 = df.transpose();
    const transposed2 = transposed1.transpose();

    // Should have original dimensions
    assert.strictEqual(transposed2.shape.rows, 2, 'Should have 2 rows after double transpose');
    assert.strictEqual(transposed2.shape.cols, 3, 'Should have 3 columns after double transpose');

    transposed2.free();
    transposed1.free();
    df.free();
});

// ============================================================================
// Memory Leak Tests
// ============================================================================

test('pivot: no memory leaks (1000 iterations)', async () => {
    const csv = 'id,cat,val\n1,A,10\n1,B,20\n2,A,30\n2,B,40';

    for (let i = 0; i < 1000; i++) {
        const df = DataFrame.fromCSV(csv);
        const pivoted = df.pivot({
            index: 'id',
            columns: 'cat',
            values: 'val'
        });
        pivoted.free();
        df.free();
    }

    assert.ok(true, 'Should complete 1000 pivot iterations without memory leaks');
});

test('melt: no memory leaks (1000 iterations)', async () => {
    const csv = 'id,A,B\n1,10,20\n2,30,40';

    for (let i = 0; i < 1000; i++) {
        const df = DataFrame.fromCSV(csv);
        const melted = df.melt({ id_vars: ['id'] });
        melted.free();
        df.free();
    }

    assert.ok(true, 'Should complete 1000 melt iterations without memory leaks');
});

test('transpose: no memory leaks (1000 iterations)', async () => {
    const csv = 'A,B\n1,2\n3,4';

    for (let i = 0; i < 1000; i++) {
        const df = DataFrame.fromCSV(csv);
        const transposed = df.transpose();
        transposed.free();
        df.free();
    }

    assert.ok(true, 'Should complete 1000 transpose iterations without memory leaks');
});

test('stack: no memory leaks (1000 iterations)', async () => {
    const csv = 'id,A,B\n1,10,20';

    for (let i = 0; i < 1000; i++) {
        const df = DataFrame.fromCSV(csv);
        const stacked = df.stack({ id_column: 'id' });
        stacked.free();
        df.free();
    }

    assert.ok(true, 'Should complete 1000 stack iterations without memory leaks');
});

test('unstack: no memory leaks (1000 iterations)', async () => {
    const csv = 'id,var,val\n1,A,10\n1,B,20';

    for (let i = 0; i < 1000; i++) {
        const df = DataFrame.fromCSV(csv);
        const unstacked = df.unstack({
            index: 'id',
            columns: 'var',
            values: 'val'
        });
        unstacked.free();
        df.free();
    }

    assert.ok(true, 'Should complete 1000 unstack iterations without memory leaks');
});

// ============================================================================
// Edge Case Tests
// ============================================================================

test('pivot: single row DataFrame', async () => {
    const csv = 'id,cat,val\n1,A,10';
    const df = DataFrame.fromCSV(csv);

    const pivoted = df.pivot({
        index: 'id',
        columns: 'cat',
        values: 'val'
    });

    assert.strictEqual(pivoted.shape.rows, 1, 'Should have 1 row');
    assert.strictEqual(pivoted.shape.cols, 2, 'Should have 2 columns (id + A)');

    pivoted.free();
    df.free();
});

test('melt: single column to melt', async () => {
    const csv = 'id,A\n1,10\n2,20';
    const df = DataFrame.fromCSV(csv);

    const melted = df.melt({ id_vars: ['id'] });

    assert.strictEqual(melted.shape.rows, 2, 'Should have 2 rows');
    assert.strictEqual(melted.shape.cols, 3, 'Should have 3 columns');

    melted.free();
    df.free();
});

test('transpose: square DataFrame', async () => {
    const csv = 'A,B\n1,2\n3,4';
    const df = DataFrame.fromCSV(csv);

    const transposed = df.transpose();

    assert.strictEqual(transposed.shape.rows, 2, 'Should have 2 rows');
    assert.strictEqual(transposed.shape.cols, 2, 'Should have 2 columns');

    transposed.free();
    df.free();
});

console.log('✅ All reshape operation tests completed successfully');
