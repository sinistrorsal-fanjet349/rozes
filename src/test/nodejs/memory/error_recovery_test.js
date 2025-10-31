/**
 * Error Recovery Test
 *
 * Tests that DataFrames are still properly cleaned up (via FinalizationRegistry)
 * even when errors are thrown during processing.
 *
 * Run with: node --expose-gc error_recovery_test.js
 */

const { Rozes } = require('../../../../dist/index.js');

// Colors for output
const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const YELLOW = '\x1b[33m';
const RESET = '\x1b[0m';
const BLUE = '\x1b[34m';

function log(msg) {
    console.log(`${BLUE}[Error Recovery]${RESET} ${msg}`);
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
 * Helper: Sleep for ms milliseconds
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Test 1: Error thrown after DataFrame creation
 * DataFrame should still be cleaned up by GC
 */
async function testErrorAfterCreation() {
    log('Test 1: Error thrown after DataFrame creation');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();

    let errorCount = 0;
    for (let i = 0; i < 1000; i++) {
        try {
            const df = rozes.DataFrame.fromCSV('a,b\n1,2', { autoCleanup: true });

            // Throw error randomly
            if (i % 3 === 0) {
                throw new Error('Simulated error');
            }
        } catch (err) {
            errorCount++;
            // DataFrame goes out of scope, should still be cleaned up
        }
    }

    log(`Caught ${errorCount} errors`);

    // GC should clean up all DataFrames
    forceGC();
    await sleep(100);

    const final = getHeapUsed();
    const growth = final - baseline;
    log(`Heap growth: ${growth.toFixed(2)} MB`);

    if (growth < 5) {
        pass('DataFrames cleaned up despite errors');
    } else {
        fail(`Heap growth too high: ${growth.toFixed(2)} MB (possible leak)`);
    }
}

/**
 * Test 2: Error during DataFrame operation
 */
async function testErrorDuringOperation() {
    log('\nTest 2: Error during DataFrame operation');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();

    for (let i = 0; i < 1000; i++) {
        try {
            const df = rozes.DataFrame.fromCSV('x,y,z\n1,2,3', { autoCleanup: true });

            // Try invalid operation
            const badColumn = df.column('nonexistent');

            // More operations
            if (i % 2 === 0) {
                throw new Error('Operation failed');
            }
        } catch (err) {
            // DataFrame should still be cleaned up
        }
    }

    forceGC();
    await sleep(100);

    const final = getHeapUsed();
    const growth = final - baseline;
    log(`Heap growth: ${growth.toFixed(2)} MB`);

    if (growth < 5) {
        pass('Memory stable despite operation errors');
    } else {
        fail(`Heap growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Test 3: Exception in try-catch-finally
 * Tests that DataFrames are cleaned up even with try-catch-finally
 */
async function testTryCatchFinally() {
    log('\nTest 3: Exception in try-catch-finally');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();

    for (let i = 0; i < 1000; i++) {
        let df = null;
        try {
            df = rozes.DataFrame.fromCSV('col1,col2\n10,20', { autoCleanup: true });

            if (i % 4 === 0) {
                throw new Error('Simulated error');
            }
        } catch (err) {
            // Error caught
        } finally {
            // df goes out of scope here
        }
    }

    forceGC();
    await sleep(100);

    const final = getHeapUsed();
    const growth = final - baseline;
    log(`Heap growth: ${growth.toFixed(2)} MB`);

    if (growth < 5) {
        pass('Try-catch-finally cleanup works correctly');
    } else {
        fail(`Heap growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Test 4: Mixed manual and auto cleanup with errors
 */
async function testMixedCleanupWithErrors() {
    log('\nTest 4: Mixed manual and auto cleanup with errors');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();

    for (let i = 0; i < 500; i++) {
        try {
            // Auto cleanup
            const df1 = rozes.DataFrame.fromCSV('a\n1', { autoCleanup: true });

            // Manual cleanup
            const df2 = rozes.DataFrame.fromCSV('b\n2', { autoCleanup: false });

            if (i % 3 === 0) {
                throw new Error('Error before manual free');
            }

            df2.free(); // Only called if no error

        } catch (err) {
            // df1 cleaned up by GC
            // df2 may or may not be freed depending on when error occurred
        }
    }

    forceGC();
    await sleep(100);

    const final = getHeapUsed();
    const growth = final - baseline;
    log(`Heap growth: ${growth.toFixed(2)} MB`);

    // Some manual cleanup DataFrames might leak (expected), but growth should still be bounded
    if (growth < 10) {
        pass(`Memory growth bounded despite mixed cleanup errors (${growth.toFixed(2)} MB)`);
    } else {
        fail(`Heap growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Test 5: Invalid CSV with auto cleanup
 * Tests error handling in CSV parsing
 */
async function testInvalidCSVWithAutoCleanup() {
    log('\nTest 5: Invalid CSV with auto cleanup');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();

    let parseErrors = 0;

    for (let i = 0; i < 1000; i++) {
        try {
            // Mix of valid and invalid CSVs
            const csvs = [
                'a,b\n1,2',                    // Valid
                '',                            // Empty (might fail)
                'a,b\n1,2,3',                 // Mismatched columns
                'x\n42',                       // Valid
                'y,z\n"unclosed',             // Invalid quote
            ];

            const csv = csvs[i % csvs.length];
            const df = rozes.DataFrame.fromCSV(csv, { autoCleanup: true });
        } catch (err) {
            parseErrors++;
            // If parsing failed, no DataFrame was created, so nothing to clean up
        }
    }

    log(`Parse errors: ${parseErrors}/1000`);

    forceGC();
    await sleep(100);

    const final = getHeapUsed();
    const growth = final - baseline;
    log(`Heap growth: ${growth.toFixed(2)} MB`);

    if (growth < 5) {
        pass('Invalid CSV handling does not leak memory');
    } else {
        fail(`Heap growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Test 6: Async errors
 * Tests cleanup when errors occur in async context
 */
async function testAsyncErrors() {
    log('\nTest 6: Async errors');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();

    async function processDataFrame(i) {
        const df = rozes.DataFrame.fromCSV('x,y\n1,2', { autoCleanup: true });

        // Simulate async operation
        await sleep(1);

        if (i % 5 === 0) {
            throw new Error('Async error');
        }

        return df.shape.rows;
    }

    // Process many DataFrames with some failing
    let successCount = 0;
    for (let i = 0; i < 500; i++) {
        try {
            await processDataFrame(i);
            successCount++;
        } catch (err) {
            // DataFrame should still be cleaned up
        }
    }

    log(`Successful: ${successCount}/500`);

    forceGC();
    await sleep(100);

    const final = getHeapUsed();
    const growth = final - baseline;
    log(`Heap growth: ${growth.toFixed(2)} MB`);

    if (growth < 5) {
        pass('Async error handling does not leak memory');
    } else {
        fail(`Heap growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Test 7: Nested errors
 * Tests cleanup with nested error scenarios
 */
async function testNestedErrors() {
    log('\nTest 7: Nested errors');

    const rozes = await Rozes.init();

    const baseline = getHeapUsed();

    function innerFunction(rozes, i) {
        const df = rozes.DataFrame.fromCSV('a,b,c\n1,2,3', { autoCleanup: true });

        if (i % 3 === 0) {
            throw new Error('Inner error');
        }

        return df;
    }

    function outerFunction(rozes, i) {
        try {
            const df = innerFunction(rozes, i);

            if (i % 7 === 0) {
                throw new Error('Outer error');
            }

            return df.shape.rows;
        } catch (err) {
            // Both inner and outer DataFrames should be cleaned up
            throw err;
        }
    }

    let errorCount = 0;
    for (let i = 0; i < 1000; i++) {
        try {
            outerFunction(rozes, i);
        } catch (err) {
            errorCount++;
        }
    }

    log(`Caught ${errorCount} errors`);

    forceGC();
    await sleep(100);

    const final = getHeapUsed();
    const growth = final - baseline;
    log(`Heap growth: ${growth.toFixed(2)} MB`);

    if (growth < 5) {
        pass('Nested error cleanup works correctly');
    } else {
        fail(`Heap growth too high: ${growth.toFixed(2)} MB`);
    }
}

/**
 * Main test runner
 */
async function main() {
    console.log('\n' + '='.repeat(60));
    console.log('Error Recovery Test Suite');
    console.log('='.repeat(60) + '\n');

    if (!global.gc) {
        fail('Run with --expose-gc flag: node --expose-gc error_recovery_test.js');
    }

    try {
        await testErrorAfterCreation();
        await testErrorDuringOperation();
        await testTryCatchFinally();
        await testMixedCleanupWithErrors();
        await testInvalidCSVWithAutoCleanup();
        await testAsyncErrors();
        await testNestedErrors();

        console.log('\n' + '='.repeat(60));
        console.log(`${GREEN}✓ All error recovery tests passed!${RESET}`);
        console.log('='.repeat(60) + '\n');
    } catch (err) {
        fail(`Test failed with error: ${err.message}`);
    }
}

main();
