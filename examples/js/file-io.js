/**
 * Rozes File I/O Example - Node.js
 *
 * Demonstrates reading and writing CSV files
 */

import { Rozes } from '../../dist/index.mjs';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function main() {
    console.log('🌹 Rozes DataFrame Library - File I/O Example\n');

    // Initialize Rozes
    const wasmPath = path.join(__dirname, '../../zig-out/bin/rozes.wasm');
    const rozes = await Rozes.init(wasmPath);
    console.log(`✅ Rozes ${rozes.version.trim()} initialized\n`);

    // Create sample CSV file
    const csvPath = path.join(__dirname, 'sample.csv');
    const csvContent = `product,price,quantity
Apple,1.20,100
Banana,0.50,150
Orange,1.80,75
Grape,2.50,60`;

    fs.writeFileSync(csvPath, csvContent);
    console.log(`✅ Created sample CSV: ${csvPath}\n`);

    // Load from file
    console.log('Loading CSV from file...');
    const df = rozes.DataFrame.fromCSVFile(csvPath);
    console.log(`✅ Loaded: ${df.shape.rows} rows × ${df.shape.cols} columns`);
    console.log(`   Columns: ${df.columns.join(', ')}\n`);

    // Access data
    const prices = df.column('price');
    const quantities = df.column('quantity');

    console.log('Product data:');
    for (let i = 0; i < prices.length; i++) {
        console.log(`   Product ${i + 1}: $${prices[i].toFixed(2)}, Qty: ${quantities[i]}`);
    }

    // Calculate total value
    let totalValue = 0;
    for (let i = 0; i < prices.length; i++) {
        // Convert BigInt to Number for calculation
        totalValue += prices[i] * Number(quantities[i]);
    }
    console.log(`\n   Total inventory value: $${totalValue.toFixed(2)}\n`);

    // Export to CSV file
    console.log('Exporting DataFrame to CSV...');
    const outputPath = path.join(__dirname, 'output.csv');
    df.toCSVFile(outputPath);
    console.log(`✅ Exported to: ${outputPath}\n`);

    // Verify the exported file
    const exported = fs.readFileSync(outputPath, 'utf8');
    console.log('Exported CSV content:');
    console.log(exported);

    // Export as TSV (tab-separated)
    const tsvPath = path.join(__dirname, 'output.tsv');
    df.toCSVFile(tsvPath, { delimiter: '\t' });
    console.log(`\n✅ Also exported as TSV: ${tsvPath}\n`);

    // Clean up
    df.free();
    console.log('✅ DataFrame freed');

    // Clean up example files
    fs.unlinkSync(csvPath);
    fs.unlinkSync(outputPath);
    fs.unlinkSync(tsvPath);
    console.log('✅ Cleaned up example files');

    console.log('\n🎉 File I/O example completed successfully!');
}

main().catch(err => {
    console.error('❌ Error:', err);
    process.exit(1);
});
