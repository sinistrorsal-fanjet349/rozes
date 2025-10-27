/**
 * Rozes DataFrame Library - JavaScript Wrapper
 *
 * High-performance DataFrame library powered by WebAssembly.
 * Provides zero-copy access to columnar data via TypedArrays.
 *
 * @example
 * ```javascript
 * const rozes = await Rozes.init();
 * const csv = "name,age,score\nAlice,30,95.5\nBob,25,87.3\n";
 * const df = rozes.DataFrame.fromCSV(csv);
 *
 * console.log(df.shape); // { rows: 2, cols: 3 }
 * const ages = df.column('age'); // Float64Array [30, 25]
 * console.log(df.columns); // ['name', 'age', 'score']
 *
 * df.free(); // Release memory
 * ```
 */

/**
 * Error codes from Wasm module
 */
const ErrorCode = {
    Success: 0,
    OutOfMemory: -1,
    InvalidFormat: -2,
    InvalidHandle: -3,
    ColumnNotFound: -4,
    TypeMismatch: -5,
    IndexOutOfBounds: -6,
    TooManyDataFrames: -7,
    InvalidOptions: -8,
};

/**
 * Error messages for each error code
 */
const ErrorMessages = {
    [ErrorCode.OutOfMemory]: 'Out of memory',
    [ErrorCode.InvalidFormat]: 'Invalid CSV format',
    [ErrorCode.InvalidHandle]: 'Invalid DataFrame handle',
    [ErrorCode.ColumnNotFound]: 'Column not found',
    [ErrorCode.TypeMismatch]: 'Type mismatch - column is not the requested type',
    [ErrorCode.IndexOutOfBounds]: 'Index out of bounds',
    [ErrorCode.TooManyDataFrames]: 'Too many DataFrames (max 1000)',
    [ErrorCode.InvalidOptions]: 'Invalid CSV options',
};

/**
 * Rozes Error class
 */
class RozesError extends Error {
    constructor(code, message) {
        const errorMsg = ErrorMessages[code] || `Unknown error (code: ${code})`;
        super(message ? `${errorMsg}: ${message}` : errorMsg);
        this.code = code;
        this.name = 'RozesError';
    }
}

/**
 * Check Wasm function result and throw error if failed
 */
function checkResult(code, context = '') {
    if (code < 0) {
        throw new RozesError(code, context);
    }
    return code;
}

/**
 * DataFrame class - represents a 2D columnar data structure
 */
class DataFrame {
    /**
     * @private
     * @param {number} handle - Wasm handle to the DataFrame
     * @param {Object} wasm - Wasm instance
     */
    constructor(handle, wasm) {
        this._handle = handle;
        this._wasm = wasm;
        this._freed = false;

        // Cache dimensions
        const rowsPtr = new Uint32Array(wasm.memory.buffer, 0, 1);
        const colsPtr = new Uint32Array(wasm.memory.buffer, 4, 1);
        checkResult(
            wasm.instance.exports.rozes_getDimensions(handle, rowsPtr.byteOffset, colsPtr.byteOffset),
            'Failed to get DataFrame dimensions'
        );

        this._rows = rowsPtr[0];
        this._cols = colsPtr[0];

        // Cache column names
        this._columnNames = this._getColumnNames();
    }

    /**
     * Parse CSV string into DataFrame
     *
     * @param {string} csvText - CSV data as string
     * @param {Object} options - Parsing options
     * @param {string} [options.delimiter=','] - Field delimiter
     * @param {boolean} [options.has_headers=true] - Whether first row contains headers
     * @param {boolean} [options.skip_blank_lines=true] - Skip blank lines
     * @param {boolean} [options.trim_whitespace=false] - Trim whitespace from fields
     * @returns {DataFrame} - New DataFrame instance
     *
     * @example
     * const df = DataFrame.fromCSV("age,score\n30,95.5\n25,87.3");
     */
    static fromCSV(csvText, options = {}) {
        const wasm = DataFrame._wasm;
        if (!wasm) {
            throw new Error('Rozes not initialized. Call Rozes.init() first.');
        }

        // Encode CSV to UTF-8
        const csvBytes = new TextEncoder().encode(csvText);

        // Allocate memory for CSV buffer using Wasm allocator
        const csvPtr = wasm.instance.exports.rozes_alloc(csvBytes.length);
        if (csvPtr === 0) {
            throw new RozesError(ErrorCode.OutOfMemory, 'Failed to allocate CSV buffer');
        }

        let optsPtr = 0;
        let optsLen = 0;

        try {
            // Copy CSV data to Wasm memory
            const csvArray = new Uint8Array(wasm.memory.buffer, csvPtr, csvBytes.length);
            csvArray.set(csvBytes);

            // Encode options to JSON if provided
            if (Object.keys(options).length > 0) {
                const optsJSON = JSON.stringify(options);
                const optsBytes = new TextEncoder().encode(optsJSON);
                optsPtr = wasm.instance.exports.rozes_alloc(optsBytes.length);

                if (optsPtr === 0) {
                    throw new RozesError(ErrorCode.OutOfMemory, 'Failed to allocate options buffer');
                }

                const optsArray = new Uint8Array(wasm.memory.buffer, optsPtr, optsBytes.length);
                optsArray.set(optsBytes);
                optsLen = optsBytes.length;
            }

            // Call Wasm function
            const handle = wasm.instance.exports.rozes_parseCSV(
                csvPtr,
                csvBytes.length,
                optsPtr,
                optsLen
            );

            checkResult(handle, 'Failed to parse CSV');

            return new DataFrame(handle, wasm);
        } finally {
            // Always free allocated buffers
            wasm.instance.exports.rozes_free_buffer(csvPtr, csvBytes.length);
            if (optsPtr !== 0) {
                wasm.instance.exports.rozes_free_buffer(optsPtr, optsLen);
            }
        }
    }

    /**
     * Get DataFrame dimensions
     * @returns {{rows: number, cols: number}}
     */
    get shape() {
        this._checkNotFreed();
        return { rows: this._rows, cols: this._cols };
    }

    /**
     * Get column names
     * @returns {string[]}
     */
    get columns() {
        this._checkNotFreed();
        return this._columnNames;
    }

    /**
     * Get column data as TypedArray (zero-copy)
     *
     * @param {string} name - Column name
     * @returns {Float64Array|BigInt64Array|null} - Column data or null if not found
     *
     * @example
     * const ages = df.column('age'); // Float64Array
     */
    column(name) {
        this._checkNotFreed();

        const wasm = this._wasm;
        const nameBytes = new TextEncoder().encode(name);

        // Allocate scratch space in Wasm memory for outputs
        // Layout: [ptr: u64 at offset 0][len: u32 at offset 8][name bytes at offset 12]
        const scratchOffset = 0;
        const ptrOffset = scratchOffset;      // 8 bytes for pointer (usize/u64)
        const lenOffset = scratchOffset + 8;  // 4 bytes for length (u32)
        const nameOffset = scratchOffset + 12;

        // Write column name to memory
        new Uint8Array(wasm.memory.buffer).set(nameBytes, nameOffset);

        // Try Float64 first - pass ADDRESSES where Wasm should write results
        const f64Result = wasm.instance.exports.rozes_getColumnF64(
            this._handle,
            nameOffset,
            nameBytes.length,
            ptrOffset,  // Address where pointer should be written
            lenOffset   // Address where length should be written
        );

        if (f64Result === ErrorCode.Success) {
            // Read the pointer and length that Wasm wrote
            // For wasm32, pointers are 32-bit, but we use 64-bit to be safe
            const view = new DataView(wasm.memory.buffer);
            const ptr = view.getUint32(ptrOffset, true);  // true = little-endian
            const len = view.getUint32(lenOffset, true);

            // Validate pointer alignment (Float64Array needs 8-byte alignment)
            if (ptr % 8 !== 0) {
                throw new Error(`Invalid pointer alignment: ${ptr} (must be 8-byte aligned for Float64Array)`);
            }

            // Validate length and bounds
            if (len === 0) {
                return new Float64Array(0);
            }

            const byteLength = len * 8; // 8 bytes per Float64
            if (ptr + byteLength > wasm.memory.buffer.byteLength) {
                throw new Error(`Pointer out of bounds: ${ptr} + ${byteLength} > ${wasm.memory.buffer.byteLength}`);
            }

            return new Float64Array(wasm.memory.buffer, ptr, len);
        }

        if (f64Result !== ErrorCode.TypeMismatch) {
            checkResult(f64Result, `Failed to get column '${name}'`);
        }

        // Try Int64
        const i64Result = wasm.instance.exports.rozes_getColumnI64(
            this._handle,
            nameOffset,
            nameBytes.length,
            ptrOffset,  // Address where pointer should be written
            lenOffset   // Address where length should be written
        );

        if (i64Result === ErrorCode.Success) {
            // Read the pointer and length that Wasm wrote
            const view = new DataView(wasm.memory.buffer);
            const ptr = view.getUint32(ptrOffset, true);  // true = little-endian
            const len = view.getUint32(lenOffset, true);

            // Validate pointer alignment (BigInt64Array needs 8-byte alignment)
            if (ptr % 8 !== 0) {
                throw new Error(`Invalid pointer alignment: ${ptr} (must be 8-byte aligned for BigInt64Array)`);
            }

            // Validate length and bounds
            if (len === 0) {
                return new BigInt64Array(0);
            }

            const byteLength = len * 8; // 8 bytes per Int64
            if (ptr + byteLength > wasm.memory.buffer.byteLength) {
                throw new Error(`Pointer out of bounds: ${ptr} + ${byteLength} > ${wasm.memory.buffer.byteLength}`);
            }

            return new BigInt64Array(wasm.memory.buffer, ptr, len);
        }

        checkResult(i64Result, `Failed to get column '${name}'`);
        return null;
    }

    /**
     * Free DataFrame memory
     * Must be called when done with DataFrame to prevent memory leaks
     */
    free() {
        if (!this._freed) {
            this._wasm.instance.exports.rozes_free(this._handle);
            this._freed = true;
            this._handle = -1;
        }
    }

    /**
     * Pretty-print DataFrame info
     * @returns {string}
     */
    toString() {
        if (this._freed) {
            return 'DataFrame(freed)';
        }
        return `DataFrame(${this._rows} rows × ${this._cols} cols)\nColumns: ${this._columnNames.join(', ')}`;
    }

    /**
     * @private
     */
    _getColumnNames() {
        const wasm = this._wasm;
        const bufferSize = 1024; // 1KB for column names JSON
        const buffer = new Uint8Array(wasm.memory.buffer, 1024, bufferSize);
        const writtenPtr = new Uint32Array(wasm.memory.buffer, 2048, 1);

        checkResult(
            wasm.instance.exports.rozes_getColumnNames(
                this._handle,
                buffer.byteOffset,
                bufferSize,
                writtenPtr.byteOffset
            ),
            'Failed to get column names'
        );

        const written = writtenPtr[0];
        const jsonStr = new TextDecoder().decode(buffer.slice(0, written));
        return JSON.parse(jsonStr);
    }

    /**
     * @private
     */
    _checkNotFreed() {
        if (this._freed) {
            throw new Error('DataFrame has been freed');
        }
    }

    /**
     * @private
     * Set by Rozes.init()
     */
    static _wasm = null;
}

/**
 * Rozes namespace - main entry point
 */
class Rozes {
    /**
     * Initialize Rozes library by loading Wasm module
     *
     * @param {string|URL} [wasmPath='./rozes.wasm'] - Path to Wasm file
     * @returns {Promise<Rozes>} - Initialized Rozes instance
     *
     * @example
     * const rozes = await Rozes.init('./zig-out/bin/rozes.wasm');
     */
    static async init(wasmPath = './rozes.wasm') {
        // Load Wasm module
        const response = await fetch(wasmPath);
        const buffer = await response.arrayBuffer();

        // Instantiate with WASI imports (complete snapshot_preview1)
        const wasi = {
            // Args
            args_get: () => 0,
            args_sizes_get: () => 0,

            // Environment
            environ_get: () => 0,
            environ_sizes_get: () => 0,

            // Clock
            clock_res_get: () => 0,
            clock_time_get: () => 0,

            // File descriptors
            fd_advise: () => 0,
            fd_allocate: () => 0,
            fd_close: () => 0,
            fd_datasync: () => 0,
            fd_fdstat_get: () => 0,
            fd_fdstat_set_flags: () => 0,
            fd_fdstat_set_rights: () => 0,
            fd_filestat_get: () => 0,
            fd_filestat_set_size: () => 0,
            fd_filestat_set_times: () => 0,
            fd_pread: () => 0,
            fd_prestat_get: () => 0,
            fd_prestat_dir_name: () => 0,
            fd_pwrite: () => 0,
            fd_read: () => 0,
            fd_readdir: () => 0,
            fd_renumber: () => 0,
            fd_seek: () => 0,
            fd_sync: () => 0,
            fd_tell: () => 0,
            fd_write: () => 0,

            // Path
            path_create_directory: () => 0,
            path_filestat_get: () => 0,
            path_filestat_set_times: () => 0,
            path_link: () => 0,
            path_open: () => 0,
            path_readlink: () => 0,
            path_remove_directory: () => 0,
            path_rename: () => 0,
            path_symlink: () => 0,
            path_unlink_file: () => 0,

            // Poll
            poll_oneoff: () => 0,

            // Process
            proc_exit: () => {},
            proc_raise: () => 0,

            // Random
            random_get: () => 0,

            // Sched
            sched_yield: () => 0,

            // Socket
            sock_recv: () => 0,
            sock_send: () => 0,
            sock_shutdown: () => 0,
        };

        const result = await WebAssembly.instantiate(buffer, {
            wasi_snapshot_preview1: wasi,
        });

        // Store Wasm instance
        const wasm = {
            instance: result.instance,
            memory: result.instance.exports.memory,
        };

        DataFrame._wasm = wasm;

        return new Rozes(wasm);
    }

    /**
     * @private
     */
    constructor(wasm) {
        this._wasm = wasm;
        this.DataFrame = DataFrame;
    }

    /**
     * Get library version
     * @returns {string}
     */
    get version() {
        return '0.1.0-dev';
    }
}

// Export for ES modules
export { Rozes, DataFrame, RozesError };

// Also support CommonJS and browser globals
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { Rozes, DataFrame, RozesError };
}

if (typeof window !== 'undefined') {
    window.Rozes = Rozes;
    window.DataFrame = DataFrame;
    window.RozesError = RozesError;
}
