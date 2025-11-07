/**
 * Integration tests for window operations
 * Tests rolling and expanding window functions
 */

import test from 'node:test';
import assert from 'node:assert';
import { Rozes, DataFrame } from '../../../dist/index.mjs';

// Initialize Rozes before running tests
await Rozes.init();

// ============================================================================
// Rolling Window Tests
// ============================================================================

test('Rolling sum with window=3', () => {
    const csv = 'value:Int64\n10\n20\n30\n40\n50\n';
    const df = DataFrame.fromCSV(csv);
    const rolling = df.rollingSum('value', 3);
    const result = rolling.column('value');
    
    // First 2 values should be NaN (not enough data), then 60, 90, 120
    assert.ok(isNaN(result[0]));
    assert.ok(isNaN(result[1]));
    assert.strictEqual(result[2], 60n);
    assert.strictEqual(result[3], 90n);
    assert.strictEqual(result[4], 120n);
    
    rolling.free();
    df.free();
});

test('Rolling mean with window=3', () => {
    const csv = 'value:Float64\n10\n20\n30\n40\n50\n';
    const df = DataFrame.fromCSV(csv);
    const rolling = df.rollingMean('value', 3);
    const result = rolling.column('value');
    
    // First 2 values should be NaN, then 20, 30, 40
    assert.ok(isNaN(result[0]));
    assert.ok(isNaN(result[1]));
    assert.strictEqual(result[2], 20);
    assert.strictEqual(result[3], 30);
    assert.strictEqual(result[4], 40);
    
    rolling.free();
    df.free();
});

test('Expanding sum', () => {
    const csv = 'value:Int64\n10\n20\n30\n40\n50\n';
    const df = DataFrame.fromCSV(csv);
    const expanding = df.expandingSum('value');
    const result = expanding.column('value');
    
    // Cumulative sum: 10, 30, 60, 100, 150
    assert.strictEqual(result[0], 10n);
    assert.strictEqual(result[1], 30n);
    assert.strictEqual(result[2], 60n);
    assert.strictEqual(result[3], 100n);
    assert.strictEqual(result[4], 150n);
    
    expanding.free();
    df.free();
});

test('Expanding mean', () => {
    const csv = 'value:Float64\n10\n20\n30\n40\n50\n';
    const df = DataFrame.fromCSV(csv);
    const expanding = df.expandingMean('value');
    const result = expanding.column('value');
    
    // Cumulative mean: 10, 15, 20, 25, 30
    assert.strictEqual(result[0], 10);
    assert.strictEqual(result[1], 15);
    assert.strictEqual(result[2], 20);
    assert.strictEqual(result[3], 25);
    assert.strictEqual(result[4], 30);
    
    expanding.free();
    df.free();
});

console.log('✅ Window operations tests complete');
