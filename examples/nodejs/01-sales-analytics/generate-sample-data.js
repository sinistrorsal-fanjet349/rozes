import fs from 'fs';

/**
 * Generate sample sales data for the analytics example
 */

const products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Webcam', 'Headset', 'USB Drive', 'HDMI Cable'];
const regions = ['North', 'South', 'East', 'West', 'Central'];
const salespeople = ['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson', 'Emma Brown', 'Frank Miller'];

function randomDate(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

function formatDate(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function generateSalesData(numRows) {
  const rows = [];
  const startDate = new Date('2024-01-01');
  const endDate = new Date('2024-12-31');

  for (let i = 0; i < numRows; i++) {
    const product = products[Math.floor(Math.random() * products.length)];
    const region = regions[Math.floor(Math.random() * regions.length)];
    const salesperson = salespeople[Math.floor(Math.random() * salespeople.length)];
    const date = formatDate(randomDate(startDate, endDate));
    const quantity = Math.floor(Math.random() * 50) + 1;
    const basePrice = 50 + Math.random() * 950; // $50-$1000
    const unitPrice = Math.round(basePrice * 100) / 100;
    const revenue = Math.round(unitPrice * quantity * 100) / 100;
    const cost = Math.round(revenue * (0.4 + Math.random() * 0.3) * 100) / 100;
    const profit = Math.round((revenue - cost) * 100) / 100;

    rows.push({
      order_id: `ORD-${String(i + 1).padStart(6, '0')}`,
      date,
      product,
      region,
      salesperson,
      quantity,
      unit_price: unitPrice,
      revenue,
      cost,
      profit
    });
  }

  return rows;
}

// Generate 10,000 sales records
const salesData = generateSalesData(10000);

// Convert to CSV
const headers = Object.keys(salesData[0]);
const csvLines = [headers.join(',')];

for (const row of salesData) {
  const values = headers.map(h => row[h]);
  csvLines.push(values.join(','));
}

const csv = csvLines.join('\n');

// Write to file
fs.writeFileSync('sales_data.csv', csv);
console.log('✓ Generated sales_data.csv with 10,000 records');
console.log(`  Columns: ${headers.join(', ')}`);
console.log(`  Date range: 2024-01-01 to 2024-12-31`);
console.log(`  Products: ${products.length}`);
console.log(`  Regions: ${regions.length}`);
console.log(`  Salespeople: ${salespeople.length}`);
