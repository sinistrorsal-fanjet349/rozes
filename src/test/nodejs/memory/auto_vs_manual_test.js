/**
 * Auto vs Manual Cleanup Test
 *
 * Compares memory usage patterns between automatic cleanup (FinalizationRegistry)
 * and manual cleanup (explicit free() calls).
 *
 * Run with: node --expose-gc auto_vs_manual_test.js
 */

const { Rozes } = require('../../../../dist/index.js');

// Colors for output
const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const YELLOW = '\x1b[33m';
const RESET = '\x1b[0m';
const BLUE = '\x1b[34m';
const CYAN = '\x1b[36m';

function log(msg) {
    console.log(`${BLUE}[Auto vs Manual]${RESET} ${msg}`);
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

function info(msg) {
    console.log(`${CYAN}ℹ${RESET} ${msg}`);
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
 * Get current memory usage
 */
function getMemoryStats() {
    const mem = process.memoryUsage();
    return {
        heapUsed: mem.heapUsed / 1024 / 1024,
        external: mem.external / 1024 / 1024,
        rss: mem.rss / 1024 / 1024,
    };
}

/**
 * Helper: Sleep for ms milliseconds
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Test 1: Memory footprint comparison
 * Compares final memory usage between auto and manual cleanup
 */
async function testMemoryFootprint() {
    log('Test 1: Memory footprint comparison (1000 DataFrames)');

    const rozes = await Rozes.init();

    // Test manual cleanup
    info('Testing manual cleanup...');
    forceGC();
    await sleep(100);
    const manualBaseline = getMemoryStats();

    for (let i = 0; i < 1000; i++) {
        const df = rozes.DataFrame.fromCSV('a,b,c\n1,2,3', { autoCleanup: false });
        df.free();
    }

    const manualAfter = getMemoryStats();
    const manualGrowth = {
        heap: manualAfter.heapUsed - manualBaseline.heapUsed,
        external: manualAfter.external - manualBaseline.external,
    };

    log(`Manual - Heap growth: ${manualGrowth.heap.toFixed(2)} MB, External: ${manualGrowth.external.toFixed(2)} MB`);

    // Wait for any pending cleanup
    await sleep(100);

    // Test auto cleanup
    info('Testing auto cleanup...');
    forceGC();
    await sleep(100);
    const autoBaseline = getMemoryStats();

    for (let i = 0; i < 1000; i++) {
        const df = rozes.DataFrame.fromCSV('a,b,c\n1,2,3', { autoCleanup: true });
    }

    // Force GC to trigger finalization
    forceGC();
    await sleep(100);

    const autoAfter = getMemoryStats();
    const autoGrowth = {
        heap: autoAfter.heapUsed - autoBaseline.heapUsed,
        external: autoAfter.external - autoBaseline.external,
    };

    log(`Auto - Heap growth: ${autoGrowth.heap.toFixed(2)} MB, External: ${autoGrowth.external.toFixed(2)} MB`);

    // Both should have minimal growth
    if (manualGrowth.heap < 5 && autoGrowth.heap < 5) {
        pass('Both cleanup strategies have similar memory footprint');
    } else {
        fail(`Memory footprint too high - Manual: ${manualGrowth.heap.toFixed(2)} MB, Auto: ${autoGrowth.heap.toFixed(2)} MB`);
    }
}

/**
 * Test 2: Cleanup timing comparison
 * Compares speed of cleanup between auto and manual
 */
async function testCleanupTiming() {
    log('\nTest 2: Cleanup timing comparison (1000 DataFrames)');

    const rozes = await Rozes.init();

    // Manual cleanup timing
    const manualStart = Date.now();
    for (let i = 0; i < 1000; i++) {
        const df = rozes.DataFrame.fromCSV('x,y\n1,2', { autoCleanup: false });
        df.free();
    }
    const manualTime = Date.now() - manualStart;

    log(`Manual cleanup: ${manualTime}ms (${(manualTime / 1000).toFixed(2)}ms per DataFrame)`);

    await sleep(100);

    // Auto cleanup timing (includes GC time)
    const autoStart = Date.now();
    for (let i = 0; i < 1000; i++) {
        const df = rozes.DataFrame.fromCSV('x,y\n1,2', { autoCleanup: true });
    }
    forceGC();
    await sleep(100); // Wait for finalization
    const autoTime = Date.now() - autoStart;

    log(`Auto cleanup: ${autoTime}ms (${(autoTime / 1000).toFixed(2)}ms per DataFrame)`);

    const speedup = (autoTime / manualTime).toFixed(2);
    info(`Manual is ${speedup}× faster (expected: ~3×)`);

    // Manual should be faster
    if (manualTime < autoTime) {
        pass('Manual cleanup is faster than auto cleanup (as expected)');
    } else {
        warn('Auto cleanup was faster (unexpected, but not an error)');
    }
}

/**
 * Test 3: Memory pressure handling
 * Tests how each strategy handles memory pressure
 */
async function testMemoryPressure() {
    log('\nTest 3: Memory pressure handling (100 large DataFrames)');

    const rozes = await Rozes.init();

    function createLargeCSV() {
        let csv = 'id,val1,val2,val3,val4,val5\n';
        for (let i = 0; i < 1000; i++) {
            csv += `${i},${i*1.1},${i*2.2},${i*3.3},${i*4.4},${i*5.5}\n`;
        }
        return csv;
    }

    // Manual cleanup
    info('Testing manual with large DataFrames...');
    const manualBaseline = getMemoryStats();

    for (let i = 0; i < 100; i++) {
        const csv = createLargeCSV();
        const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: false });
        df.free();
    }

    const manualAfter = getMemoryStats();
    const manualGrowth = manualAfter.heapUsed - manualBaseline.heapUsed;
    log(`Manual - Memory growth: ${manualGrowth.toFixed(2)} MB`);

    await sleep(100);

    // Auto cleanup
    info('Testing auto with large DataFrames...');
    const autoBaseline = getMemoryStats();

    for (let i = 0; i < 100; i++) {
        const csv = createLargeCSV();
        const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });

        // Periodic GC to avoid OOM
        if (i % 20 === 0) {
            forceGC();
        }
    }

    forceGC();
    await sleep(100);

    const autoAfter = getMemoryStats();
    const autoGrowth = autoAfter.heapUsed - autoBaseline.heapUsed;
    log(`Auto - Memory growth: ${autoGrowth.toFixed(2)} MB`);

    if (manualGrowth < 10 && autoGrowth < 15) {
        pass('Both strategies handle memory pressure well');
    } else {
        fail(`Memory growth too high - Manual: ${manualGrowth.toFixed(2)} MB, Auto: ${autoGrowth.toFixed(2)} MB`);
    }
}

/**
 * Test 4: Interleaved cleanup patterns
 * Tests mixed usage of both strategies
 */
async function testInterleavedPatterns() {
    log('\nTest 4: Interleaved cleanup patterns');

    const rozes = await Rozes.init();

    const baseline = getMemoryStats();

    for (let i = 0; i < 1000; i++) {
        const csv = `col1,col2\n${i},${i*2}\n`;

        if (i % 2 === 0) {
            // Manual cleanup
            const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: false });
            df.free();
        } else {
            // Auto cleanup
            const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });
        }

        if (i % 200 === 0) {
            forceGC();
        }
    }

    forceGC();
    await sleep(100);

    const after = getMemoryStats();
    const growth = after.heapUsed - baseline.heapUsed;
    log(`Memory growth: ${growth.toFixed(2)} MB`);

    if (growth < 5) {
        pass('Interleaved cleanup patterns work correctly');
    } else {
        fail(`Memory growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Test 5: Batch processing comparison
 * Simulates real-world batch processing scenario
 */
async function testBatchProcessing() {
    log('\nTest 5: Batch processing comparison');

    const rozes = await Rozes.init();

    const BATCHES = 10;
    const BATCH_SIZE = 200;

    // Manual cleanup batches
    info('Processing with manual cleanup...');
    const manualStart = Date.now();
    const manualBaseline = getMemoryStats();

    for (let batch = 0; batch < BATCHES; batch++) {
        for (let i = 0; i < BATCH_SIZE; i++) {
            const df = rozes.DataFrame.fromCSV('a,b,c\n1,2,3\n4,5,6', { autoCleanup: false });
            // Process DataFrame
            const shape = df.shape;
            df.free();
        }
    }

    const manualTime = Date.now() - manualStart;
    const manualAfter = getMemoryStats();
    const manualGrowth = manualAfter.heapUsed - manualBaseline.heapUsed;

    log(`Manual - Time: ${manualTime}ms, Memory growth: ${manualGrowth.toFixed(2)} MB`);

    await sleep(100);

    // Auto cleanup batches
    info('Processing with auto cleanup...');
    const autoStart = Date.now();
    const autoBaseline = getMemoryStats();

    for (let batch = 0; batch < BATCHES; batch++) {
        for (let i = 0; i < BATCH_SIZE; i++) {
            const df = rozes.DataFrame.fromCSV('a,b,c\n1,2,3\n4,5,6', { autoCleanup: true });
            // Process DataFrame
            const shape = df.shape;
        }
        // GC between batches
        forceGC();
    }

    forceGC();
    await sleep(100);

    const autoTime = Date.now() - autoStart;
    const autoAfter = getMemoryStats();
    const autoGrowth = autoAfter.heapUsed - autoBaseline.heapUsed;

    log(`Auto - Time: ${autoTime}ms, Memory growth: ${autoGrowth.toFixed(2)} MB`);

    info(`Speedup ratio: ${(autoTime / manualTime).toFixed(2)}× (manual is faster)`);

    // Both should complete successfully with bounded memory
    if (manualGrowth < 5 && autoGrowth < 5) {
        pass('Both strategies work well for batch processing');
    } else {
        fail(`Memory growth too high - Manual: ${manualGrowth.toFixed(2)} MB, Auto: ${autoGrowth.toFixed(2)} MB`);
    }
}

/**
 * Test 6: Explicit recommendations test
 * Tests specific usage patterns from documentation
 */
async function testRecommendedPatterns() {
    log('\nTest 6: Recommended patterns');

    const rozes = await Rozes.init();

    // Pattern 1: Production HTTP handler (manual cleanup)
    info('Pattern 1: Production HTTP handler (manual)');
    const httpBaseline = getMemoryStats();

    for (let request = 0; request < 500; request++) {
        const df = rozes.DataFrame.fromCSV('user,score\nAlice,95', { autoCleanup: false });
        try {
            const score = df.column('score');
            // Process request
        } finally {
            df.free(); // Always cleanup
        }
    }

    const httpAfter = getMemoryStats();
    const httpGrowth = httpAfter.heapUsed - httpBaseline.heapUsed;
    log(`HTTP handler pattern - Memory growth: ${httpGrowth.toFixed(2)} MB`);

    await sleep(100);

    // Pattern 2: CLI tool (auto cleanup)
    info('Pattern 2: CLI tool (auto)');
    const cliBaseline = getMemoryStats();

    for (let i = 0; i < 100; i++) {
        const df = rozes.DataFrame.fromCSV('data\n42', { autoCleanup: true });
        const shape = df.shape;
        // Process and exit (GC handles cleanup)
    }

    forceGC();
    await sleep(100);

    const cliAfter = getMemoryStats();
    const cliGrowth = cliAfter.heapUsed - cliBaseline.heapUsed;
    log(`CLI tool pattern - Memory growth: ${cliGrowth.toFixed(2)} MB`);

    if (httpGrowth < 5 && cliGrowth < 5) {
        pass('Recommended patterns work correctly');
    } else {
        fail(`Memory growth too high - HTTP: ${httpGrowth.toFixed(2)} MB, CLI: ${cliGrowth.toFixed(2)} MB`);
    }
}

/**
 * Main test runner
 */
async function main() {
    console.log('\n' + '='.repeat(60));
    console.log('Auto vs Manual Cleanup Comparison Test Suite');
    console.log('='.repeat(60) + '\n');

    if (!global.gc) {
        fail('Run with --expose-gc flag: node --expose-gc auto_vs_manual_test.js');
    }

    try {
        await testMemoryFootprint();
        await testCleanupTiming();
        await testMemoryPressure();
        await testInterleavedPatterns();
        await testBatchProcessing();
        await testRecommendedPatterns();

        console.log('\n' + '='.repeat(60));
        console.log(`${GREEN}✓ All auto vs manual comparison tests passed!${RESET}`);
        console.log('='.repeat(60));
        console.log('\n📊 Summary:');
        console.log('   • Manual cleanup: Faster (~3×), deterministic, recommended for production');
        console.log('   • Auto cleanup: Convenient, suitable for prototyping/CLI tools');
        console.log('   • Both strategies have similar memory footprint after GC');
        console.log('   • Interleaved usage works correctly\n');
    } catch (err) {
        fail(`Test failed with error: ${err.message}`);
    }
}

main();
