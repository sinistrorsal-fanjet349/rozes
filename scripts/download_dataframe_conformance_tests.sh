#!/bin/bash
# Download official test suites from Polars, pandas, and DuckDB
# for DataFrame conformance testing
#
# Usage: ./scripts/download_dataframe_conformance_tests.sh
#
# Test suites:
# - Polars (MIT): 8 CSV test files
# - pandas (BSD-3): 6 CSV test files
# - DuckDB (MIT): 6 CSV test files (scattered in subdirectories)
#
# Note: Arrow and NumPy were excluded (binary test formats only, no usable CSVs)
#
# Total size: ~400MB (includes binary files and SQL tests)
# Usable CSV files: 20
# Location: testdata/external/

set -e

TESTDATA_DIR="testdata/external"
mkdir -p "$TESTDATA_DIR"

echo "=================================================="
echo "Downloading DataFrame Library Conformance Tests"
echo "=================================================="
echo ""

# Polars - Rust DataFrame library
echo "[1/5] Downloading Polars test data..."
echo "  Repository: https://github.com/pola-rs/polars"
echo "  License: MIT"
echo "  Size: ~150MB"
echo ""

if [ -d "$TESTDATA_DIR/polars" ]; then
    echo "  ⚠️  Polars directory exists, skipping..."
else
    git clone --depth 1 --filter=blob:none --sparse https://github.com/pola-rs/polars.git "$TESTDATA_DIR/polars"
    cd "$TESTDATA_DIR/polars"
    git sparse-checkout set py-polars/tests/unit/io/files crates/polars-io/src/csv/read/tests
    cd ../../..
    echo "  ✅ Polars test data downloaded"
fi
echo ""

# pandas - Python DataFrame library
echo "[2/5] Downloading pandas test data..."
echo "  Repository: https://github.com/pandas-dev/pandas"
echo "  License: BSD 3-Clause"
echo "  Size: ~200MB"
echo ""

if [ -d "$TESTDATA_DIR/pandas" ]; then
    echo "  ⚠️  pandas directory exists, skipping..."
else
    git clone --depth 1 --filter=blob:none --sparse https://github.com/pandas-dev/pandas.git "$TESTDATA_DIR/pandas"
    cd "$TESTDATA_DIR/pandas"
    git sparse-checkout set pandas/tests/io/parser/data pandas/tests/io/parser
    cd ../../..
    echo "  ✅ pandas test data downloaded"
fi
echo ""

# Apache Arrow - Columnar format library
# DuckDB - In-process SQL database
echo "[3/3] Downloading DuckDB test data..."
echo "  Repository: https://github.com/duckdb/duckdb"
echo "  License: MIT"
echo "  Size: ~150MB"
echo ""

if [ -d "$TESTDATA_DIR/duckdb" ]; then
    echo "  ⚠️  DuckDB directory exists, skipping..."
else
    git clone --depth 1 --filter=blob:none --sparse https://github.com/duckdb/duckdb.git "$TESTDATA_DIR/duckdb"
    cd "$TESTDATA_DIR/duckdb"
    git sparse-checkout set test/sql/copy/csv/data test/sql/copy/csv test/sql/aggregate test/sql/join
    cd ../../..
    echo "  ✅ DuckDB test data downloaded"
fi
echo ""

# Summary
echo "=================================================="
echo "Download Complete!"
echo "=================================================="
echo ""
echo "Test data location: $TESTDATA_DIR"
echo ""
echo "Statistics:"
echo "  - Polars:   8 CSV files (27 rows each, foods dataset)"
echo "  - pandas:   6 CSV files (salaries, unicode, encoding tests)"
echo "  - DuckDB:   6 CSV files (joins, aggregates)"
echo ""
echo "Total: 20 usable CSV test files"
echo ""
echo "Next steps:"
echo "  1. Run conformance tests: zig build conformance"
echo "  2. Check test results: cat test-results/conformance-report.txt"
echo "  3. See testdata/external/README.md for details"
echo ""
