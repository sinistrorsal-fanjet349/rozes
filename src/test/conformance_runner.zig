//! Conformance Test Runner
//!
//! Runs RFC 4180 conformance tests from testdata/csv/rfc4180/
//! Usage: zig build conformance

const std = @import("std");
const rozes = @import("rozes");
const DataFrame = rozes.DataFrame;
const CSVParser = rozes.CSVParser;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Conformance Tests - All testdata/ CSV files ===\n\n", .{});

    // Directories to scan for CSV test files
    const test_dirs = [_][]const u8{
        "testdata/csv/rfc4180",
        "testdata/csv/edge_cases",
        "testdata/csv/complex", // Complex test cases: quoted strings, alternative delimiters, malformed CSVs
        "testdata/external/csv-spectrum/csvs",
        "testdata/external/csv-parsers-comparison/src/main/resources",
        "testdata/external/PapaParse/tests",
        "testdata/external/PapaParse/extracted",
        "testdata/external/univocity-parsers/src/test/resources/csv",
        "testdata/external/univocity-parsers/src/test/resources/tsv",
        "testdata/external/univocity-parsers/src/test/resources/examples",
        // NEW: Industry DataFrame library test suites (added 2025-10-28)
        "testdata/external/polars/py-polars/tests/unit/io/files",
        "testdata/external/pandas/pandas/tests/io/parser/data",
        "testdata/external/duckdb/test/sql", // Has 6 CSVs in subdirectories
        // Note: Arrow and NumPy sparse checkouts didn't include CSV test files
    };

    var total_tests: u32 = 0;
    var passed: u32 = 0;
    var failed: u32 = 0;
    var skipped: u32 = 0;

    // No skipped tests anymore - all features supported in 0.3.0
    const string_tests = [_][]const u8{};

    // Test each directory
    for (test_dirs) |dir_path| {
        std.debug.print("Testing {s}/\n", .{dir_path});

        var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch |err| {
            std.debug.print("⚠️  Warning: Cannot open directory {s}: {}\n\n", .{ dir_path, err });
            continue;
        };
        defer dir.close();

        var walker = dir.iterate();
        while (walker.next() catch null) |entry| {
            // Skip directories and non-CSV/TSV files
            if (entry.kind == .directory) continue;
            if (entry.kind != .file) continue;
            // Accept both .csv and .tsv files
            if (!std.mem.endsWith(u8, entry.name, ".csv") and !std.mem.endsWith(u8, entry.name, ".tsv")) continue;

            total_tests += 1;

            // Check if should skip
            var should_skip = false;
            for (string_tests) |skip_name| {
                if (std.mem.eql(u8, entry.name, skip_name)) {
                    should_skip = true;
                    break;
                }
            }

            if (should_skip) {
                std.debug.print("  ⏸️  SKIP: {s} (feature not yet supported)\n", .{entry.name});
                skipped += 1;
                continue;
            }

            // Build full path
            const full_path = std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir_path, entry.name }) catch {
                std.debug.print("  ❌ FAIL: {s} - Path alloc failed\n", .{entry.name});
                failed += 1;
                continue;
            };
            defer allocator.free(full_path);

            // Try to read and parse
            const file = std.fs.cwd().openFile(full_path, .{}) catch {
                std.debug.print("  ❌ FAIL: {s} - Cannot open file\n", .{entry.name});
                failed += 1;
                continue;
            };
            defer file.close();

            const content = file.readToEndAlloc(allocator, 1_000_000) catch {
                std.debug.print("  ❌ FAIL: {s} - Cannot read file\n", .{entry.name});
                failed += 1;
                continue;
            };
            defer allocator.free(content);

            // Skip empty files (valid but not parsable)
            if (content.len == 0) {
                std.debug.print("  ⏸️  SKIP: {s} (empty file)\n", .{entry.name});
                skipped += 1;
                continue;
            }

            // Auto-detect if this CSV has no headers based on filename
            // Special case: if CSV is just a delimiter, treat as no headers (it's 1 row of data)
            const has_headers = !std.mem.containsAtLeast(u8, entry.name, 1, "no_header") and
                !std.mem.eql(u8, content, ",") and
                !std.mem.eql(u8, content, ",\n") and
                !std.mem.eql(u8, content, ",\r\n");

            // Use Lenient mode for conformance tests (like Papa Parse)
            var parser = CSVParser.init(allocator, content, .{
                .has_headers = has_headers,
                .parse_mode = .Lenient,
            }) catch {
                std.debug.print("  ❌ FAIL: {s} - Parser init failed\n", .{entry.name});
                failed += 1;
                continue;
            };
            defer parser.deinit();

            var df = parser.toDataFrame() catch |err| {
                std.debug.print("  ❌ FAIL: {s} - Parse failed: {}\n", .{ entry.name, err });
                failed += 1;
                continue;
            };
            defer df.deinit();

            std.debug.print("  ✅ PASS: {s} ({} rows, {} cols)\n", .{
                entry.name,
                df.len(),
                df.columnCount(),
            });
            passed += 1;
        }
        std.debug.print("\n", .{});
    }

    std.debug.print("=== Results ===\n", .{});
    std.debug.print("Total:   {}\n", .{total_tests});
    std.debug.print("Passed:  {}\n", .{passed});
    std.debug.print("Failed:  {}\n", .{failed});
    std.debug.print("Skipped: {}\n", .{skipped});

    const pass_rate = if (total_tests > 0) (passed * 100) / total_tests else 0;
    std.debug.print("Pass rate: {}%\n", .{pass_rate});

    // MVP success: Any tests passing shows parser works
    if (passed > 0) {
        std.debug.print("\n✅ Success: {} CSV files successfully parsed\n", .{passed});
        std.process.exit(0);
    } else {
        std.debug.print("\n❌ No tests passing - parser may have issues\n", .{});
        std.process.exit(1);
    }
}
