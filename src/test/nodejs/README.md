# Node.js API Integration Tests

Basic integration tests for the Rozes Node.js API. These tests verify that the fundamental DataFrame operations work correctly when called from JavaScript/Node.js.

## Purpose

These tests catch basic API issues that would break the examples and real-world usage. If these tests fail, the examples won't work.

## Running Tests

```bash
# Run from project root
npm run test:api

# Or directly
node src/test/nodejs/basic_api.test.js
```

## Test Coverage

### ✅ Core Functionality (13 test groups, 53 assertions)

1. **CSV Parsing** - Parse simple CSV into DataFrame
2. **Column Names** - Access column metadata
3. **Numeric Columns** - Access Float64/Int64 typed arrays
4. **String Columns** - Access string arrays
5. **Mixed Type Columns** - Numeric + String columns together
6. **Non-existent Columns** - Returns null for missing columns
7. **SIMD Aggregations** - sum, mean, min, max
8. **Select Columns** - Column projection
9. **Filter** - Numeric filtering
10. **Sort** - Ascending/descending sort
11. **GroupBy** - Single aggregation grouping
12. **Join** - Inner join on key column
13. **CSV Export** - Export DataFrame back to CSV

### Current Results

```
Tests run: 53
Passed: 53
Failed: 0
```

**✅ 100% pass rate**

## What Gets Tested

### Data Types
- Float64 columns (numeric decimal data)
- Int64 columns (integer data)
- String columns (text data)
- Boolean columns
- Mixed type DataFrames

### Operations
- **Parsing**: CSV → DataFrame
- **Access**: Column retrieval by name
- **Projection**: Select specific columns
- **Filtering**: Numeric predicates (>, <, >=, <=, ==, !=)
- **Sorting**: Ascending/descending by column
- **Grouping**: GroupBy with aggregations (sum, mean, count, min, max)
- **Joining**: Inner/left joins on keys
- **Aggregation**: SIMD-accelerated sum, mean, min, max, variance, stddev
- **Export**: DataFrame → CSV

### Memory Management
- DataFrame.free() (manual cleanup)
- Memory leak prevention
- Proper cleanup after errors

## Test Structure

Each test follows this pattern:

```javascript
// Test N: Feature description
console.log('Test N: Feature description');
const csv = 'name,age\nAlice,30\n';
const df = DataFrame.fromCSV(csv);

// Assertions
assert(condition, 'Descriptive message');
assertEquals(actual, expected, 'What should match');

df.free(); // Cleanup
console.log('');
```

## Adding New Tests

To add a new test:

1. Add a new test block in `basic_api.test.js`
2. Follow the existing pattern
3. Run `npm run test:api` to verify
4. Update this README if adding a new test category

## Known Limitations

These tests only cover the **currently implemented API** (Milestone 1.2.0). Advanced features planned for Milestone 1.4.0 are not tested here:

### Not Yet Tested (Future API)
- `withColumn()` - Adding computed columns
- `filter()` with JavaScript callbacks
- `groupBy()` with complex aggregations (`.agg()` method)
- Multi-column groupBy
- Column methods: `.toArray()`, `.unique()`
- Helper methods: `.rowCount()`, `.columnNames()`

These features are tested separately once implemented.

## Debugging Failed Tests

If a test fails:

1. **Check the error message** - Shows expected vs actual
2. **Check WASM compilation** - Run `zig build -Doptimize=ReleaseSmall`
3. **Check memory** - Look for memory-related errors
4. **Verify CSV format** - Ensure test CSV is valid
5. **Check type mismatches** - Numeric vs string columns

### Common Issues

**Issue**: `TypeError: DataFrame.fromCSV is not a function`
- **Fix**: Ensure WASM is built: `zig build -Doptimize=ReleaseSmall`

**Issue**: `RozesError: Type mismatch`
- **Fix**: Check if column type matches what you're requesting (Float64 vs Int64 vs String)

**Issue**: Test passes but crashes later
- **Fix**: Missing `df.free()` call - add cleanup

## Performance

These tests run in **<500ms** total on a typical machine.

## Integration with CI/CD

Add to your CI pipeline:

```yaml
# .github/workflows/test.yml
- name: Run Node.js API Tests
  run: npm run test:api
```

## Related

- **Examples**: `examples/nodejs/` - Real-world usage examples
- **Benchmarks**: `src/test/benchmark/` - Performance tests
- **Unit Tests**: `src/test/unit/` - Zig-level unit tests
