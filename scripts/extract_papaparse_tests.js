#!/usr/bin/env node
/**
 * Extract Papa Parse test cases into standalone CSV files
 *
 * Reads test-cases.js and creates individual CSV files from the test cases.
 */

const fs = require('fs');
const path = require('path');

const TEST_CASES_PATH = path.join(__dirname, '../testdata/external/PapaParse/tests/test-cases.js');
const OUTPUT_DIR = path.join(__dirname, '../testdata/external/PapaParse/extracted');

// Read test-cases.js
const content = fs.readFileSync(TEST_CASES_PATH, 'utf-8');

// Extract CORE_PARSER_TESTS array
const coreParserMatch = content.match(/var CORE_PARSER_TESTS = \[([\s\S]*?)\];/);
if (!coreParserMatch) {
    console.error('Could not find CORE_PARSER_TESTS in test-cases.js');
    process.exit(1);
}

// Parse test cases (simplified parsing)
const testCasesText = coreParserMatch[1];
const testCases = [];
let currentTest = {};
let braceDepth = 0;

// Simple state machine to parse test cases
const lines = testCasesText.split('\n');
for (const line of lines) {
    const trimmed = line.trim();

    // Track brace depth
    braceDepth += (line.match(/{/g) || []).length;
    braceDepth -= (line.match(/}/g) || []).length;

    // Start of new test case
    if (trimmed.startsWith('description:')) {
        if (currentTest.description) {
            testCases.push(currentTest);
        }
        currentTest = {};
        const descMatch = trimmed.match(/description:\s*"([^"]+)"/);
        if (descMatch) {
            currentTest.description = descMatch[1];
        }
    }

    // Extract input (CSV data)
    if (trimmed.startsWith('input:')) {
        const inputMatch = trimmed.match(/input:\s*'([^']*)'/);
        if (inputMatch) {
            currentTest.input = inputMatch[1]
                .replace(/\\n/g, '\n')
                .replace(/\\r/g, '\r')
                .replace(/\\t/g, '\t');
        }
    }

    // End of test case
    if (braceDepth === 1 && trimmed === '},') {
        if (currentTest.description && currentTest.input !== undefined) {
            testCases.push(currentTest);
            currentTest = {};
        }
    }
}

// Create output directory
if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

// Write CSV files
let extractedCount = 0;
testCases.forEach((test, index) => {
    if (!test.input) return;

    // Create safe filename from description
    const filename = test.description
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');

    const filepath = path.join(OUTPUT_DIR, `${String(index + 1).padStart(3, '0')}_${filename}.csv`);

    fs.writeFileSync(filepath, test.input, 'utf-8');
    extractedCount++;
});

console.log(`✅ Extracted ${extractedCount} Papa Parse test cases to ${OUTPUT_DIR}`);
console.log(`Total test cases found: ${testCases.length}`);
