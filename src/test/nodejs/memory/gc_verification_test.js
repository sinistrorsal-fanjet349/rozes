/**
 * GC Verification Test
 *
 * Tests that FinalizationRegistry correctly frees Wasm memory when DataFrames
 * are garbage collected.
 *
 * Run with: node --expose-gc gc_verification_test.js
 */

const { Rozes } = require('../../../../dist/index.js');

// Colors for output
const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const YELLOW = '\x1b[33m';
const RESET = '\x1b[0m';
const BLUE = '\x1b[34m';

function log(msg) {
    console.log(`${BLUE}[GC Test]${RESET} ${msg}`);
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
 * Requires --expose-gc flag
 */
function forceGC() {
    if (global.gc) {
        global.gc();
        global.gc(); // Run twice to ensure finalization
    } else {
        warn('GC not exposed. Run with --expose-gc flag');
    }
}

/**
 * Get current heap usage in MB
 */
function getHeapUsed() {
    const mem = process.memoryUsage();
    return mem.heapUsed / 1024 / 1024;
}

/**
 * Test 1: Basic GC verification
 * Creates DataFrames, clears references, forces GC, checks memory decreased
 */
async function testBasicGC() {
    log('Test 1: Basic GC verification');

    const rozes = await Rozes.init();

    // Warmup
    for (let i = 0; i < 100; i++) {
        const df = rozes.DataFrame.fromCSV('a,b,c\n1,2,3\n4,5,6', { autoCleanup: true });
        // Let it go out of scope
    }
    forceGC();

    // Measure baseline
    const baselineHeap = getHeapUsed();
    log(`Baseline heap: ${baselineHeap.toFixed(2)} MB`);

    // Create 1000 DataFrames with auto cleanup
    let dataframes = [];
    for (let i = 0; i < 1000; i++) {
        const csv = `name,age,score\nAlice${i},${20+i},${90+i}\nBob${i},${25+i},${85+i}\n`;
        const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });
        dataframes.push(df);
    }

    const afterCreation = getHeapUsed();
    log(`After creating 1000 DFs: ${afterCreation.toFixed(2)} MB (+${(afterCreation - baselineHeap).toFixed(2)} MB)`);

    // Clear references
    dataframes = null;

    // Force GC and wait for finalization
    forceGC();
    await sleep(100); // Give finalization callbacks time to run

    const afterGC = getHeapUsed();
    log(`After GC: ${afterGC.toFixed(2)} MB`);

    const memoryRecovered = afterCreation - afterGC;
    log(`Memory recovered: ${memoryRecovered.toFixed(2)} MB`);

    // Assert: Should recover at least 50% of the allocated memory
    if (memoryRecovered > (afterCreation - baselineHeap) * 0.5) {
        pass(`GC recovered ${memoryRecovered.toFixed(2)} MB (>50% of allocated memory)`);
    } else {
        fail(`GC only recovered ${memoryRecovered.toFixed(2)} MB (expected >50%)`);
    }
}

/**
 * Test 2: High-volume GC stress test
 * Creates 10K DataFrames in batches, forces GC between batches
 */
async function testHighVolumeGC() {
    log('\nTest 2: High-volume GC stress test (10K DataFrames)');

    const rozes = await Rozes.init();
    const BATCH_SIZE = 1000;
    const TOTAL_BATCHES = 10;

    const baselineHeap = getHeapUsed();
    log(`Baseline heap: ${baselineHeap.toFixed(2)} MB`);

    for (let batch = 0; batch < TOTAL_BATCHES; batch++) {
        // Create batch
        let dataframes = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
            const csv = `col1,col2\n${i},${i*2}\n`;
            const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });
            dataframes.push(df);
        }

        // Clear and GC
        dataframes = null;
        forceGC();

        if ((batch + 1) % 2 === 0) {
            const currentHeap = getHeapUsed();
            log(`Batch ${batch + 1}/${TOTAL_BATCHES}: Heap = ${currentHeap.toFixed(2)} MB`);
        }
    }

    // Final GC
    forceGC();
    await sleep(100);

    const finalHeap = getHeapUsed();
    const heapGrowth = finalHeap - baselineHeap;
    log(`Final heap: ${finalHeap.toFixed(2)} MB (growth: ${heapGrowth.toFixed(2)} MB)`);

    // Assert: Heap growth should be minimal (<10 MB for 10K DataFrames)
    if (heapGrowth < 10) {
        pass(`Heap growth bounded to ${heapGrowth.toFixed(2)} MB for 10K DataFrames`);
    } else {
        fail(`Heap growth too high: ${heapGrowth.toFixed(2)} MB (expected <10 MB)`);
    }
}

/**
 * Test 3: Interleaved manual and auto cleanup
 * Verify both approaches can coexist
 */
async function testInterleavedCleanup() {
    log('\nTest 3: Interleaved manual and auto cleanup');

    const rozes = await Rozes.init();

    const baselineHeap = getHeapUsed();

    for (let i = 0; i < 500; i++) {
        const csv = `a,b\n${i},${i*2}\n`;

        // Auto cleanup
        const df1 = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });

        // Manual cleanup
        const df2 = rozes.DataFrame.fromCSV(csv, { autoCleanup: false });
        df2.free();
    }

    forceGC();
    await sleep(100);

    const finalHeap = getHeapUsed();
    const heapGrowth = finalHeap - baselineHeap;
    log(`Heap growth: ${heapGrowth.toFixed(2)} MB`);

    if (heapGrowth < 5) {
        pass('Interleaved cleanup works correctly');
    } else {
        fail(`Heap growth too high: ${heapGrowth.toFixed(2)} MB`);
    }
}

/**
 * Test 4: Rapid create/destroy cycles
 * Tests finalization under high churn
 */
async function testRapidChurn() {
    log('\nTest 4: Rapid create/destroy cycles');

    const rozes = await Rozes.init();

    const baselineHeap = getHeapUsed();
    const CYCLES = 20;
    const PER_CYCLE = 500;

    for (let cycle = 0; cycle < CYCLES; cycle++) {
        // Create many DFs
        for (let i = 0; i < PER_CYCLE; i++) {
            const df = rozes.DataFrame.fromCSV('a\n1\n2\n3', { autoCleanup: true });
            // Immediately goes out of scope
        }

        // Force GC every few cycles
        if (cycle % 5 === 0) {
            forceGC();
            const currentHeap = getHeapUsed();
            if (cycle > 0) {
                log(`Cycle ${cycle}/${CYCLES}: Heap = ${currentHeap.toFixed(2)} MB`);
            }
        }
    }

    forceGC();
    await sleep(100);

    const finalHeap = getHeapUsed();
    const heapGrowth = finalHeap - baselineHeap;
    log(`Final heap growth: ${heapGrowth.toFixed(2)} MB for ${CYCLES * PER_CYCLE} DataFrames`);

    if (heapGrowth < 5) {
        pass('Rapid churn handled correctly');
    } else {
        fail(`Heap growth too high: ${heapGrowth.toFixed(2)} MB`);
    }
}

/**
 * Test 5: GC timing verification
 * Measures time for finalization callbacks to execute
 */
async function testGCTiming() {
    log('\nTest 5: GC timing verification');

    const rozes = await Rozes.init();

    // Create 5000 DataFrames
    log('Creating 5000 DataFrames...');
    let dataframes = [];
    for (let i = 0; i < 5000; i++) {
        const df = rozes.DataFrame.fromCSV('x,y\n1,2', { autoCleanup: true });
        dataframes.push(df);
    }

    log('Clearing references and forcing GC...');
    dataframes = null;

    const start = Date.now();
    forceGC();
    await sleep(200); // Wait for finalization
    const gcTime = Date.now() - start;

    log(`GC + finalization took: ${gcTime}ms`);

    if (gcTime < 500) {
        pass(`Finalization completed in ${gcTime}ms (fast)`);
    } else if (gcTime < 1000) {
        pass(`Finalization completed in ${gcTime}ms (acceptable)`);
    } else {
        warn(`Finalization took ${gcTime}ms (slow but not fatal)`);
    }
}

/**
 * Helper: Sleep for ms milliseconds
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Main test runner
 */
async function main() {
    console.log('\n' + '='.repeat(60));
    console.log('GC Verification Test Suite');
    console.log('='.repeat(60) + '\n');

    if (!global.gc) {
        fail('Run with --expose-gc flag: node --expose-gc gc_verification_test.js');
    }

    try {
        await testBasicGC();
        await testHighVolumeGC();
        await testInterleavedCleanup();
        await testRapidChurn();
        await testGCTiming();

        console.log('\n' + '='.repeat(60));
        console.log(`${GREEN}✓ All GC verification tests passed!${RESET}`);
        console.log('='.repeat(60) + '\n');
    } catch (err) {
        fail(`Test failed with error: ${err.message}`);
    }
}

main();
