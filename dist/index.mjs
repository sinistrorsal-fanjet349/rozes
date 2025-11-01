/**
 * Rozes DataFrame Library - Node.js ESM Entry Point
 *
 * High-performance DataFrame library powered by WebAssembly.
 * 3-10× faster than Papa Parse and csv-parse.
 */

import fs from 'fs';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

// Get __dirname equivalent in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Import full browser API from js/rozes.js
import { Rozes as RozesBrowser, DataFrame as DataFrameBrowser, RozesError } from '../js/rozes.js';

/**
 * Node.js-specific Rozes wrapper
 * Extends browser Rozes with Node.js file I/O capabilities
 */
export class Rozes extends RozesBrowser {
    static async init(wasmPath) {
        if (!wasmPath) {
            wasmPath = join(__dirname, '../zig-out/bin/rozes.wasm');
        }

        const wasmBuffer = fs.readFileSync(wasmPath);

        // WASI imports
        const wasiImports = {
            wasi_snapshot_preview1: {
                fd_close: () => 0,
                fd_write: () => 0,
                fd_read: () => 0,
                fd_seek: () => 0,
                fd_fdstat_get: () => 0,
                fd_fdstat_set_flags: () => 0,
                fd_fdstat_set_rights: () => 0,
                fd_filestat_get: () => 0,
                fd_filestat_set_size: () => 0,
                fd_filestat_set_times: () => 0,
                fd_prestat_get: () => 0,
                fd_prestat_dir_name: () => 0,
                fd_pread: () => 0,
                fd_pwrite: () => 0,
                fd_readdir: () => 0,
                fd_renumber: () => 0,
                fd_sync: () => 0,
                fd_tell: () => 0,
                fd_advise: () => 0,
                fd_allocate: () => 0,
                fd_datasync: () => 0,
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
                environ_sizes_get: () => 0,
                environ_get: () => 0,
                args_sizes_get: () => 0,
                args_get: () => 0,
                proc_exit: (code) => process.exit(code),
                proc_raise: () => 0,
                sched_yield: () => 0,
                random_get: (buf, len) => {
                    const bytes = new Uint8Array(len);
                    crypto.randomFillSync(bytes);
                    return 0;
                },
                clock_res_get: () => 0,
                clock_time_get: () => 0,
                poll_oneoff: () => 0,
                sock_recv: () => 0,
                sock_send: () => 0,
                sock_shutdown: () => 0
            }
        };

        const wasmModule = await WebAssembly.instantiate(wasmBuffer, wasiImports);

        const wasm = {
            instance: wasmModule.instance,
            memory: wasmModule.instance.exports.memory
        };

        DataFrameBrowser._wasm = wasm;

        const instance = new Rozes(wasm);
        instance.DataFrame = DataFrame;
        return instance;
    }
}

/**
 * Node.js-specific DataFrame class
 * Extends browser DataFrame with file I/O methods
 */
export class DataFrame extends DataFrameBrowser {
    /**
     * Load CSV from file (Node.js only)
     */
    static fromCSVFile(filePath, options = {}) {
        const csvText = fs.readFileSync(filePath, 'utf-8');
        return this.fromCSV(csvText, options);
    }

    /**
     * Write DataFrame to CSV file (Node.js only)
     */
    toCSVFile(filePath, options = {}) {
        const csvText = this.toCSV(options);
        fs.writeFileSync(filePath, csvText, 'utf-8');
    }
}

// Export RozesError
export { RozesError };

// Default export
export default Rozes;
