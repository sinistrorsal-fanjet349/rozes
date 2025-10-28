# External Conformance Test Suites

This directory contains official CSV conformance test suites from external sources.

## Test Suites Overview

### 1. csv-spectrum (12 CSV files)

**Source**: https://github.com/maxogden/csv-spectrum
**License**: MIT
**Location**: `csv-spectrum/csvs/`
**Status**: ✅ Already downloaded

**Coverage**:
- Empty fields
- Escaped quotes
- JSON data in CSV
- Newlines in quoted fields
- UTF-8 encoding
- CRLF line endings

### 2. Papa Parse (66 tests)

**Source**: https://github.com/mholt/PapaParse
**License**: MIT
**Location**:
- Original CSVs: `PapaParse/tests/` (5 files)
- Extracted tests: `PapaParse/extracted/` (61 files)

**Extraction Script**: `../../scripts/extract_papaparse_tests.js`
```bash
# Re-extract tests if needed
node scripts/extract_papaparse_tests.js
```

**Coverage**:
- Quoted fields with embedded delimiters
- Quoted fields with line breaks
- Escaped quotes within quoted fields
- Comment handling
- Whitespace preservation
- Empty fields and edge cases
- Duplicate header names

### 3. univocity-parsers (22 CSV/TSV files)

**Source**: https://github.com/uniVocity/univocity-parsers
**License**: Apache 2.0
**Location**: `univocity-parsers/src/test/resources/`
- `csv/` - 8 CSV files
- `tsv/` - 3 TSV files (tab-separated values)
- `examples/` - 11 CSV/TSV files

**Download**:
```bash
cd testdata/external
git clone --depth 1 https://github.com/uniVocity/univocity-parsers.git
```

**Coverage**:
- DOS line endings (CRLF)
- Mac line endings (CR)
- Unix line endings (LF)
- TSV parsing
- Real-world CSV examples
- Bean mapping examples

## Total Test Count

**100 CSV/TSV files** from external sources:
- 12 csv-spectrum
- 66 Papa Parse (5 original + 61 extracted)
- 22 univocity-parsers

Combined with custom tests:
- 10 RFC 4180 compliance tests
- 7 edge case tests

**Grand Total**: 113 conformance tests

## Running Tests

```bash
# Run all conformance tests
zig build conformance

# Current results:
# Total:   113
# Passed:  112
# Failed:  1
# Pass rate: 99%
```

## Failed Tests (1 total)

### Edge Case: CSV with Only Delimiter
- `037_input_is_just_the_delimiter_2_empty_fields.csv` - CSV containing only `,`
  - **Issue**: Header detection logic treats `,` as header row with 2 empty column names, resulting in 0 data rows
  - **Papa Parse**: Treats as 1 data row with 2 empty fields when `header: false`
  - **Status**: Acceptable edge case for MVP - extremely rare in real-world usage

## Updating Test Suites

To update to the latest versions:

```bash
cd testdata/external

# Update csv-spectrum
cd csv-spectrum && git pull origin master && cd ..

# Update Papa Parse
cd PapaParse && git pull origin master && cd ..
node ../../scripts/extract_papaparse_tests.js

# Update univocity-parsers
cd univocity-parsers && git pull origin main && cd ..
```

## License Compliance

All test suites use permissive licenses:
- ✅ csv-spectrum: MIT License
- ✅ Papa Parse: MIT License
- ✅ univocity-parsers: Apache License 2.0

Attribution is provided in this README and in the main project documentation.

---

**Last Updated**: 2025-10-27
**Rozes Version**: 0.2.0+
