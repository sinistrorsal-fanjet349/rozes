# Complex CSV Test Suite

This directory contains comprehensive test cases for complex CSV scenarios, including:

## Complex Quoted Strings (01-03)

### 01_long_quoted_descriptions.csv
- **Purpose**: Test quoted fields with many commas
- **Characteristics**:
  - 10-300 character descriptions with high comma density
  - Real-world product descriptions
  - Tests parser's handling of quoted fields with embedded delimiters

### 02_nested_quotes_and_commas.csv
- **Purpose**: Test escaped quotes within quoted fields
- **Characteristics**:
  - Double-quote escaping (`""` → `"`)
  - Complex addresses with multiple comma-separated components
  - Nested quoted sections within fields

### 03_very_long_fields.csv
- **Purpose**: Test parser with very long fields (>500 characters)
- **Characteristics**:
  - Technical specifications with extensive comma-separated lists
  - Tests field length limits (MAX_FIELD_LENGTH = 1MB)
  - Real-world long-form content

## Alternative Delimiters (04-06)

### 04_pipe_delimited.csv
- **Purpose**: Test pipe (|) as delimiter
- **Characteristics**:
  - Common in Unix/Linux data exports
  - 5 rows x 4 columns
  - Clean data, no quotes needed

### 05_semicolon_delimited.csv
- **Purpose**: Test semicolon (;) as delimiter
- **Characteristics**:
  - Common in European CSV exports (Excel)
  - Numeric data with decimals (uses period as decimal separator)
  - Product inventory data

### 06_tab_delimited.tsv
- **Purpose**: Test tab character as delimiter
- **Characteristics**:
  - TSV format (Tab-Separated Values)
  - Log file format with timestamps
  - No quotes required for any fields

## Variable Length (11)

### 11_variable_length_descriptions.csv
- **Purpose**: Test parser with fields of varying lengths in same column
- **Characteristics**:
  - Descriptions range from 10 to 300+ characters
  - Tests parser's buffer growth strategy
  - Mix of simple and complex quoted content

## Extreme Cases (12)

### 12_extreme_comma_density.csv
- **Purpose**: Stress test with maximum comma density
- **Characteristics**:
  - Fields containing 20-40 commas each
  - Lists of items (colors, fruits, Greek letters)
  - Tests parser performance with high delimiter count

## Malformed/Corrupted CSVs (07-10)

### 07_malformed_unclosed_quotes.csv
- **Purpose**: Test error handling for unclosed quoted fields
- **Expected**: Should fail with `error.InvalidQuoting` in Strict mode
- **Lenient mode**: May recover by treating as unquoted field

### 08_malformed_mismatched_columns.csv
- **Purpose**: Test rows with inconsistent column counts
- **Expected**:
  - Strict mode: Fail with `error.ColumnCountMismatch`
  - Lenient mode: Pad short rows with empty fields, truncate long rows

### 09_malformed_invalid_escapes.csv
- **Purpose**: Test invalid quote escaping
- **Characteristics**:
  - Single `"` within quoted field (should be `""`)
  - Triple quotes `"""`
- **Expected**: Fail with `error.InvalidQuoting` in Strict mode

### 10_malformed_mixed_line_endings.csv
- **Purpose**: Test file with missing line breaks
- **Characteristics**:
  - Some rows run together without line ending
- **Expected**: Parse incorrectly (fewer rows than intended)

## Usage

### With Conformance Runner

```bash
# Run all complex tests
zig build conformance

# The conformance runner automatically discovers all CSV/TSV files in:
# - testdata/csv/complex/
```

### Manual Testing

```zig
// Test specific file
const csv = @embedFile("testdata/csv/complex/01_long_quoted_descriptions.csv");

var parser = try CSVParser.init(allocator, csv, .{
    .delimiter = ',',  // Use '|', ';', or '\t' for alternate delimiters
    .parse_mode = .Strict,  // or .Lenient for malformed files
});
defer parser.deinit();

var df = try parser.toDataFrame();
defer df.deinit();
```

### Expected Results

| File | Rows | Cols | Should Pass | Notes |
|------|------|------|-------------|-------|
| 01_long_quoted_descriptions | 5 | 4 | ✅ Yes | String support required (0.2.0+) |
| 02_nested_quotes_and_commas | 3 | 4 | ✅ Yes | Tests quote escaping |
| 03_very_long_fields | 2 | 3 | ✅ Yes | Tests MAX_FIELD_LENGTH |
| 04_pipe_delimited | 5 | 4 | ⚠️ Needs config | Set `delimiter = '|'` |
| 05_semicolon_delimited | 5 | 5 | ⚠️ Needs config | Set `delimiter = ';'` |
| 06_tab_delimited.tsv | 5 | 5 | ⚠️ Needs config | Set `delimiter = '\t'` |
| 07_malformed_unclosed_quotes | - | - | ❌ Should fail | `error.InvalidQuoting` |
| 08_malformed_mismatched_columns | - | - | ❌ Should fail | `error.ColumnCountMismatch` |
| 09_malformed_invalid_escapes | - | - | ❌ Should fail | `error.InvalidQuoting` |
| 10_malformed_mixed_line_endings | 3 | 3 | ⚠️ Partial | Parses incorrectly |
| 11_variable_length_descriptions | 7 | 4 | ✅ Yes | String support required |
| 12_extreme_comma_density | 5 | 3 | ✅ Yes | Stress test |

## Performance Benchmarks

Use these files for performance testing:

- **01_long_quoted_descriptions.csv**: Quote handling overhead
- **03_very_long_fields.csv**: Large field allocation
- **12_extreme_comma_density.csv**: Delimiter parsing speed

Target: <1ms per file for all valid CSVs

## Test Coverage

These 12 files test:

- ✅ Quoted fields with embedded delimiters (01, 02, 03, 11, 12)
- ✅ Escaped quotes (`""`) (02)
- ✅ Very long fields (03)
- ✅ Alternative delimiters (04, 05, 06)
- ✅ Variable field lengths (11)
- ✅ Extreme comma density (12)
- ✅ Error cases (07, 08, 09, 10)

**Total**: 12 complex test cases covering edge cases beyond RFC 4180

---

**Created**: 2025-10-28
**Status**: Comprehensive test suite for 100% conformance target
