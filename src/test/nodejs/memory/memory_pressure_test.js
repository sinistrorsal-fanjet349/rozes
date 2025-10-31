/**
 * Memory Pressure Test
 *
 * Tests DataFrame memory management under sustained memory pressure.
 * Adapted from auto_cleanup_test.js for fast automated testing.
 *
 * Run with: node --expose-gc memory_pressure_test.js
 */

const { Rozes } = require('../../../../dist/index.js');

// Colors for output
const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const YELLOW = '\x1b[33m';
const RESET = '\x1b[0m';
const BLUE = '\x1b[34m';

function log(msg) {
    console.log(`${BLUE}[Memory Pressure]${RESET} ${msg}`);
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
 * Get current heap usage in MB
 */
function getHeapUsed() {
    const mem = process.memoryUsage();
    return mem.heapUsed / 1024 / 1024;
}

/**
 * Get RSS (resident set size) in MB
 */
function getRSS() {
    const mem = process.memoryUsage();
    return mem.rss / 1024 / 1024;
}

/**
 * Helper: Sleep for ms milliseconds
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Generate large CSV data
 */
function createLargeCSV(rows, cols) {
    let csv = Array.from({ length: cols }, (_, i) => `col${i}`).join(',') + '\n';
    for (let i = 0; i < rows; i++) {
        const values = Array.from({ length: cols }, (_, j) => i * cols + j);
        csv += values.join(',') + '\n';
    }
    return csv;
}

/**
 * Test 1: Large DataFrames with auto cleanup
 * Creates 100 large DataFrames (1000 rows × 6 cols each)
 */
async function testLargeDataFrames() {
    log('Test 1: Large DataFrames with auto cleanup (100 DFs, 1000 rows each)');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();
    const baselineRSS = getRSS();
    log(`Baseline - Heap: ${baseline.toFixed(2)} MB, RSS: ${baselineRSS.toFixed(2)} MB`);

    // Pre-generate CSV to avoid string allocation during test
    const largeCSV = createLargeCSV(1000, 6);

    for (let i = 0; i < 100; i++) {
        const df = rozes.DataFrame.fromCSV(largeCSV, { autoCleanup: true });

        if (i % 10 === 0 && i > 0) {
            const heap = getHeapUsed();
            const rss = getRSS();
            log(`  ${i}/100 - Heap: ${heap.toFixed(2)} MB, RSS: ${rss.toFixed(2)} MB`);
        }

        // Periodic GC to prevent OOM
        if (i % 20 === 0 && i > 0) {
            forceGC();
        }
    }

    // Final GC
    forceGC();
    await sleep(100);

    const finalHeap = getHeapUsed();
    const finalRSS = getRSS();
    const heapGrowth = finalHeap - baseline;
    const rssGrowth = finalRSS - baselineRSS;

    log(`Final - Heap: ${finalHeap.toFixed(2)} MB, RSS: ${finalRSS.toFixed(2)} MB`);
    log(`Growth - Heap: ${heapGrowth.toFixed(2)} MB, RSS: ${rssGrowth.toFixed(2)} MB`);

    if (heapGrowth < 20) {
        pass(`Heap growth bounded to ${heapGrowth.toFixed(2)} MB`);
    } else {
        fail(`Heap growth too high: ${heapGrowth.toFixed(2)} MB`);
    }
}

/**
 * Test 2: Sustained pressure test
 * Creates DataFrames continuously for 30 seconds
 */
async function testSustainedPressure() {
    log('\nTest 2: Sustained pressure test (30 seconds)');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();
    const DURATION_SEC = 30;

    log(`Running for ${DURATION_SEC} seconds...`);

    const startTime = Date.now();
    let count = 0;
    let maxHeap = baseline;

    while (Date.now() - startTime < DURATION_SEC * 1000) {
        const csv = `a,b,c,d,e\n${count},${count*2},${count*3},${count*4},${count*5}\n`;
        const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });
        count++;

        // Periodic GC and reporting
        if (count % 1000 === 0) {
            forceGC();
            const currentHeap = getHeapUsed();
            maxHeap = Math.max(maxHeap, currentHeap);

            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            log(`  ${elapsed}s: Created ${count} DFs, Heap: ${currentHeap.toFixed(2)} MB (peak: ${maxHeap.toFixed(2)} MB)`);
        }

        // Small delay to avoid tight loop
        if (count % 100 === 0) {
            await sleep(1);
        }
    }

    // Final GC
    forceGC();
    await sleep(100);

    const finalHeap = getHeapUsed();
    const heapGrowth = finalHeap - baseline;
    const peakGrowth = maxHeap - baseline;

    log(`Created ${count} DataFrames in ${DURATION_SEC}s`);
    log(`Heap growth: ${heapGrowth.toFixed(2)} MB (peak: ${peakGrowth.toFixed(2)} MB)`);

    if (heapGrowth < 20 && peakGrowth < 50) {
        pass(`Memory stable under sustained pressure (final: ${heapGrowth.toFixed(2)} MB, peak: ${peakGrowth.toFixed(2)} MB)`);
    } else {
        fail(`Memory growth too high - final: ${heapGrowth.toFixed(2)} MB, peak: ${peakGrowth.toFixed(2)} MB`);
    }
}

/**
 * Test 3: Memory recovery test
 * Tests that memory is recovered after spike
 */
async function testMemoryRecovery() {
    log('\nTest 3: Memory recovery test');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();
    log(`Baseline: ${baseline.toFixed(2)} MB`);

    // Create spike
    log('Creating 500 large DataFrames...');
    const largeCSV = createLargeCSV(500, 6);

    for (let i = 0; i < 500; i++) {
        const df = rozes.DataFrame.fromCSV(largeCSV, { autoCleanup: true });

        if (i % 100 === 0 && i > 0) {
            const current = getHeapUsed();
            log(`  ${i}/500 - Heap: ${current.toFixed(2)} MB`);
        }
    }

    const peakHeap = getHeapUsed();
    const peakGrowth = peakHeap - baseline;
    log(`Peak heap: ${peakHeap.toFixed(2)} MB (growth: ${peakGrowth.toFixed(2)} MB)`);

    // Force recovery
    log('Forcing GC to recover memory...');
    forceGC();
    await sleep(200);

    const recoveredHeap = getHeapUsed();
    const finalGrowth = recoveredHeap - baseline;
    const recovered = peakGrowth - finalGrowth;

    log(`Recovered heap: ${recoveredHeap.toFixed(2)} MB (growth: ${finalGrowth.toFixed(2)} MB)`);
    log(`Memory recovered: ${recovered.toFixed(2)} MB (${((recovered / peakGrowth) * 100).toFixed(1)}%)`);

    if (finalGrowth < 20) {
        pass(`Memory recovered successfully (${recovered.toFixed(2)} MB recovered, ${finalGrowth.toFixed(2)} MB remaining)`);
    } else {
        fail(`Memory recovery insufficient (${finalGrowth.toFixed(2)} MB remaining)`);
    }
}

/**
 * Test 4: Wide DataFrames (many columns)
 * Tests memory with many columns
 */
async function testWideDataFrames() {
    log('\nTest 4: Wide DataFrames (100 columns)');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();

    // Create 100 DataFrames with 100 columns each
    log('Creating 100 DataFrames with 100 columns each...');

    for (let i = 0; i < 100; i++) {
        const csv = createLargeCSV(10, 100); // 10 rows, 100 columns
        const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });

        if (i % 20 === 0 && i > 0) {
            const current = getHeapUsed();
            log(`  ${i}/100 - Heap: ${current.toFixed(2)} MB`);
        }
    }

    forceGC();
    await sleep(100);

    const finalHeap = getHeapUsed();
    const growth = finalHeap - baseline;
    log(`Heap growth: ${growth.toFixed(2)} MB`);

    if (growth < 15) {
        pass(`Wide DataFrames handled efficiently (growth: ${growth.toFixed(2)} MB)`);
    } else {
        fail(`Wide DataFrames memory growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Test 5: Memory stability over iterations
 * Tests that memory doesn't gradually increase
 */
async function testMemoryStability() {
    log('\nTest 5: Memory stability over iterations (10 cycles)');

    const rozes = await Rozes.init();

    const csv = createLargeCSV(100, 10);
    const measurements = [];

    for (let cycle = 0; cycle < 10; cycle++) {
        // Create 100 DataFrames
        for (let i = 0; i < 100; i++) {
            const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });
        }

        // Force GC and measure
        forceGC();
        await sleep(50);

        const heap = getHeapUsed();
        measurements.push(heap);
        log(`  Cycle ${cycle + 1}/10: Heap = ${heap.toFixed(2)} MB`);
    }

    // Calculate trend (should be flat or decreasing)
    const firstThree = measurements.slice(0, 3).reduce((a, b) => a + b) / 3;
    const lastThree = measurements.slice(-3).reduce((a, b) => a + b) / 3;
    const trend = lastThree - firstThree;

    log(`Memory trend: ${trend > 0 ? '+' : ''}${trend.toFixed(2)} MB over 10 cycles`);

    if (trend < 5) {
        pass(`Memory stable over iterations (trend: ${trend > 0 ? '+' : ''}${trend.toFixed(2)} MB)`);
    } else {
        fail(`Memory increasing over iterations (trend: +${trend.toFixed(2)} MB)`);
    }
}

/**
 * Main test runner
 */
async function main() {
    console.log('\n' + '='.repeat(60));
    console.log('Memory Pressure Test Suite');
    console.log('='.repeat(60) + '\n');

    if (!global.gc) {
        fail('Run with --expose-gc flag: node --expose-gc memory_pressure_test.js');
    }

    try {
        await testLargeDataFrames();
        await testSustainedPressure();
        await testMemoryRecovery();
        await testWideDataFrames();
        await testMemoryStability();

        console.log('\n' + '='.repeat(60));
        console.log(`${GREEN}✓ All memory pressure tests passed!${RESET}`);
        console.log('='.repeat(60) + '\n');
    } catch (err) {
        fail(`Test failed with error: ${err.message}`);
    }
}

main();
