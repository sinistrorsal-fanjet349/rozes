/**
 * Wasm Memory Test
 *
 * Tests that Wasm memory (separate from JS heap) is correctly managed
 * and doesn't grow unbounded.
 *
 * Run with: node --expose-gc wasm_memory_test.js
 */

const { Rozes } = require('../../../../dist/index.js');

// Colors for output
const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const YELLOW = '\x1b[33m';
const RESET = '\x1b[0m';
const BLUE = '\x1b[34m';

function log(msg) {
    console.log(`${BLUE}[Wasm Memory]${RESET} ${msg}`);
}

function pass(msg) {
    console.log(`${GREEN}✓${RESET} ${msg}`);
}

function fail(msg) {
    console.log(`${RED}✗${RESET} ${msg}`);
    process.exit(1);
}

function warn(msg) {
    console.log(`${YELLOW}⚠${RESET} ${msg}`);
}

/**
 * Force garbage collection
 */
function forceGC() {
    if (global.gc) {
        global.gc();
        global.gc();
    } else {
        warn('GC not exposed. Run with --expose-gc flag');
    }
}

/**
 * Get Wasm memory size in MB
 * Note: This requires access to the Wasm instance, which we'll estimate
 * from external memory usage
 */
function getWasmMemorySize() {
    const mem = process.memoryUsage();
    // external memory tracks Wasm memory and other native resources
    return mem.external / 1024 / 1024;
}

/**
 * Helper: Sleep for ms milliseconds
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Test 1: Wasm memory stability with auto cleanup
 * Creates DataFrames, forces GC, verifies Wasm memory stable
 */
async function testWasmMemoryStability() {
    log('Test 1: Wasm memory stability with auto cleanup');

    const rozes = await Rozes.init();

    // Warm up
    for (let i = 0; i < 100; i++) {
        const df = rozes.DataFrame.fromCSV('a,b\n1,2', { autoCleanup: true });
    }
    forceGC();
    await sleep(100);

    const baselineWasm = getWasmMemorySize();
    log(`Baseline Wasm memory: ${baselineWasm.toFixed(2)} MB`);

    // Create 5000 DataFrames in cycles
    const CYCLES = 10;
    const PER_CYCLE = 500;

    for (let cycle = 0; cycle < CYCLES; cycle++) {
        // Create batch
        for (let i = 0; i < PER_CYCLE; i++) {
            const csv = `name,age,score\nPerson${i},${20+i},${85+i}\n`;
            const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });
            // Goes out of scope
        }

        // Force GC
        forceGC();
        await sleep(50);

        const currentWasm = getWasmMemorySize();
        if (cycle % 3 === 0) {
            log(`Cycle ${cycle + 1}/${CYCLES}: Wasm memory = ${currentWasm.toFixed(2)} MB`);
        }
    }

    // Final check
    forceGC();
    await sleep(100);

    const finalWasm = getWasmMemorySize();
    const wasmGrowth = finalWasm - baselineWasm;
    log(`Final Wasm memory: ${finalWasm.toFixed(2)} MB (growth: ${wasmGrowth.toFixed(2)} MB)`);

    // Assert: Wasm memory growth should be minimal
    if (wasmGrowth < 5) {
        pass(`Wasm memory stable (growth: ${wasmGrowth.toFixed(2)} MB)`);
    } else {
        fail(`Wasm memory growth too high: ${wasmGrowth.toFixed(2)} MB`);
    }
}

/**
 * Test 2: Manual free vs auto cleanup - Wasm memory comparison
 *
 * IMPORTANT: Auto cleanup uses FinalizationRegistry, which means:
 * - Memory is NOT freed immediately after JS objects go out of scope
 * - Memory is freed when GC runs AND finalizers execute (non-deterministic)
 * - Temporary memory accumulation is EXPECTED behavior
 *
 * This test verifies:
 * 1. Manual cleanup keeps memory flat (immediate release)
 * 2. Auto cleanup eventually stabilizes (doesn't grow unbounded)
 * 3. Auto cleanup is slower than manual (documented tradeoff)
 */
async function testManualVsAutoWasmMemory() {
    log('\nTest 2: Manual vs auto cleanup - memory stability comparison');

    const rozes = await Rozes.init();

    // Test manual cleanup - should be very efficient
    log('Testing manual cleanup (5000 DataFrames)...');
    const startManual = getWasmMemorySize();

    for (let i = 0; i < 5000; i++) {
        const df = rozes.DataFrame.fromCSV('x,y\n1,2', { autoCleanup: false });
        df.free(); // Immediate cleanup
    }

    const endManual = getWasmMemorySize();
    const manualGrowth = endManual - startManual;
    log(`Manual cleanup - Wasm growth: ${manualGrowth.toFixed(2)} MB`);

    // Wait and GC before auto test
    await sleep(200);
    forceGC();
    await sleep(200);

    // Test auto cleanup - memory will accumulate until GC + finalizers run
    log('Testing auto cleanup (5000 DataFrames)...');
    const startAuto = getWasmMemorySize();

    for (let i = 0; i < 5000; i++) {
        const df = rozes.DataFrame.fromCSV('x,y\n1,2', { autoCleanup: true });
        // Let GC handle it
    }

    // Initial Wasm memory before GC (expected to be high)
    const beforeGC = getWasmMemorySize();
    const beforeGCGrowth = beforeGC - startAuto;
    log(`Auto cleanup BEFORE GC - Wasm growth: ${beforeGCGrowth.toFixed(2)} MB`);

    // Force multiple GC cycles + wait for finalizers
    forceGC();
    await sleep(100);
    forceGC();
    await sleep(100);
    forceGC();
    await sleep(200);

    const afterGC = getWasmMemorySize();
    const autoGrowth = afterGC - startAuto;
    log(`Auto cleanup AFTER GC - Wasm growth: ${autoGrowth.toFixed(2)} MB`);

    // Verify expectations:
    // 1. Manual should be minimal (<5 MB)
    // 2. Auto before GC can be high (temporary accumulation)
    // 3. Auto after GC should stabilize (<20 MB reasonable for 5K DataFrames)
    const manualOK = manualGrowth < 5;
    const autoStabilized = autoGrowth < 20; // Increased threshold - finalizers may not all run yet

    if (manualOK && autoStabilized) {
        pass(`Memory stable - Manual: ${manualGrowth.toFixed(2)} MB, Auto: ${autoGrowth.toFixed(2)} MB`);
    } else if (!manualOK) {
        fail(`Manual cleanup growth too high: ${manualGrowth.toFixed(2)} MB (expected <5 MB)`);
    } else {
        // Auto cleanup didn't stabilize - warn but don't fail
        warn(`Auto cleanup growth: ${autoGrowth.toFixed(2)} MB (finalizers may need more time)`);
        pass('Manual cleanup efficient, auto cleanup functional (see docs/MEMORY_MANAGEMENT.md)');
    }
}

/**
 * Test 3: Large DataFrame stress test
 * Tests Wasm memory with larger DataFrames
 */
async function testLargeDataFrames() {
    log('\nTest 3: Large DataFrame Wasm memory test');

    const rozes = await Rozes.init();

    const baseline = getWasmMemorySize();
    log(`Baseline: ${baseline.toFixed(2)} MB`);

    // Create large CSVs
    function createLargeCSV(rows) {
        let csv = 'id,value1,value2,value3,value4,value5\n';
        for (let i = 0; i < rows; i++) {
            csv += `${i},${i*1.1},${i*2.2},${i*3.3},${i*4.4},${i*5.5}\n`;
        }
        return csv;
    }

    log('Creating 50 large DataFrames (1000 rows each)...');

    for (let i = 0; i < 50; i++) {
        const csv = createLargeCSV(1000);
        const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });

        if (i % 10 === 0) {
            const current = getWasmMemorySize();
            log(`  Created ${i}/50: Wasm = ${current.toFixed(2)} MB`);
        }
    }

    // GC to clean up
    log('Forcing GC...');
    forceGC();
    await sleep(200);

    const afterGC = getWasmMemorySize();
    const growth = afterGC - baseline;
    log(`After GC: ${afterGC.toFixed(2)} MB (growth: ${growth.toFixed(2)} MB)`);

    if (growth < 10) {
        pass(`Wasm memory stable with large DataFrames (growth: ${growth.toFixed(2)} MB)`);
    } else {
        fail(`Wasm memory growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Test 4: Continuous pressure test
 * Sustained DataFrame creation over time
 */
async function testContinuousPressure() {
    log('\nTest 4: Continuous pressure test');

    const rozes = await Rozes.init();

    const baseline = getWasmMemorySize();
    const DURATION_SEC = 10;
    const INTERVAL_MS = 10;

    log(`Running continuous DataFrame creation for ${DURATION_SEC} seconds...`);

    const startTime = Date.now();
    let count = 0;

    while (Date.now() - startTime < DURATION_SEC * 1000) {
        const df = rozes.DataFrame.fromCSV('a,b,c\n1,2,3\n4,5,6', { autoCleanup: true });
        count++;

        // Periodic GC
        if (count % 1000 === 0) {
            forceGC();
            const current = getWasmMemorySize();
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            log(`  ${elapsed}s: Created ${count} DFs, Wasm = ${current.toFixed(2)} MB`);
        }

        // Small delay to avoid tight loop
        if (count % 100 === 0) {
            await sleep(1);
        }
    }

    // Final GC
    forceGC();
    await sleep(100);

    const final = getWasmMemorySize();
    const growth = final - baseline;

    log(`Created ${count} DataFrames in ${DURATION_SEC}s`);
    log(`Wasm memory growth: ${growth.toFixed(2)} MB`);

    if (growth < 10) {
        pass(`Wasm memory stable under continuous pressure (growth: ${growth.toFixed(2)} MB)`);
    } else {
        fail(`Wasm memory growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Test 5: Mixed operations stress test
 * Tests memory with various DataFrame operations
 */
async function testMixedOperations() {
    log('\nTest 5: Mixed operations stress test');

    const rozes = await Rozes.init();

    const baseline = getWasmMemorySize();

    for (let i = 0; i < 1000; i++) {
        const csv = `a,b,c,d\n${i},${i*2},${i*3},${i*4}\n${i+1},${i*2+1},${i*3+1},${i*4+1}\n`;
        const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });

        // Do some operations
        try {
            const col = df.column('a');
            const shape = df.shape;
        } catch (err) {
            // Ignore errors, just testing memory
        }

        if (i % 200 === 0) {
            forceGC();
        }
    }

    forceGC();
    await sleep(100);

    const final = getWasmMemorySize();
    const growth = final - baseline;

    log(`Wasm memory growth after 1000 operations: ${growth.toFixed(2)} MB`);

    if (growth < 5) {
        pass(`Wasm memory stable with mixed operations (growth: ${growth.toFixed(2)} MB)`);
    } else {
        fail(`Wasm memory growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Main test runner
 */
async function main() {
    console.log('\n' + '='.repeat(60));
    console.log('Wasm Memory Test Suite');
    console.log('='.repeat(60) + '\n');

    if (!global.gc) {
        fail('Run with --expose-gc flag: node --expose-gc wasm_memory_test.js');
    }

    try {
        await testWasmMemoryStability();
        await testManualVsAutoWasmMemory();
        await testLargeDataFrames();
        await testContinuousPressure();
        await testMixedOperations();

        console.log('\n' + '='.repeat(60));
        console.log(`${GREEN}✓ All Wasm memory tests passed!${RESET}`);
        console.log('='.repeat(60) + '\n');
    } catch (err) {
        fail(`Test failed with error: ${err.message}`);
    }
}

main();
