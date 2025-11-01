import fs from 'fs';

/**
 * Generate sample data for ETL pipeline demonstration
 */

// Generate customer data
function generateCustomers(count) {
  const firstNames = ['Alice', 'Bob', 'Carol', 'David', 'Emma', 'Frank', 'Grace', 'Henry', 'Iris', 'Jack'];
  const lastNames = ['Smith', 'Johnson', 'Williams', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor', 'Anderson'];
  const countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Japan', 'Australia', 'Brazil', 'India', 'China'];
  const segments = ['Premium', 'Standard', 'Basic'];

  const customers = [];
  for (let i = 1; i <= count; i++) {
    const firstName = firstNames[Math.floor(Math.random() * firstNames.length)];
    const lastName = lastNames[Math.floor(Math.random() * lastNames.length)];
    const country = countries[Math.floor(Math.random() * countries.length)];
    const segment = segments[Math.floor(Math.random() * segments.length)];
    const signupYear = 2020 + Math.floor(Math.random() * 5);

    customers.push({
      customer_id: i,
      first_name: firstName,
      last_name: lastName,
      email: `${firstName.toLowerCase()}.${lastName.toLowerCase()}${i}@example.com`,
      country,
      segment,
      signup_year: signupYear,
      is_active: Math.random() > 0.1 ? 1 : 0 // 90% active
    });
  }

  return customers;
}

// Generate product data
function generateProducts(count) {
  const categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books'];
  const brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE'];

  const products = [];
  for (let i = 1; i <= count; i++) {
    const category = categories[Math.floor(Math.random() * categories.length)];
    const brand = brands[Math.floor(Math.random() * brands.length)];
    const basePrice = 10 + Math.random() * 990;
    const cost = basePrice * (0.3 + Math.random() * 0.4);

    products.push({
      product_id: i,
      product_name: `Product-${i}`,
      category,
      brand,
      price: Math.round(basePrice * 100) / 100,
      cost: Math.round(cost * 100) / 100,
      stock_quantity: Math.floor(Math.random() * 1000),
      is_available: Math.random() > 0.05 ? 1 : 0 // 95% available
    });
  }

  return products;
}

// Generate transaction data
function generateTransactions(count, maxCustomerId, maxProductId) {
  const transactions = [];
  const startDate = new Date('2024-01-01');
  const endDate = new Date('2024-12-31');

  for (let i = 1; i <= count; i++) {
    const customerId = Math.floor(Math.random() * maxCustomerId) + 1;
    const productId = Math.floor(Math.random() * maxProductId) + 1;
    const quantity = Math.floor(Math.random() * 10) + 1;

    // Random date in 2024
    const timestamp = new Date(startDate.getTime() + Math.random() * (endDate.getTime() - startDate.getTime()));
    const date = timestamp.toISOString().split('T')[0];

    // Simulate payment status (95% completed)
    const status = Math.random() > 0.05 ? 'completed' : (Math.random() > 0.5 ? 'pending' : 'cancelled');

    // Some transactions have missing/invalid data (5%)
    const isValid = Math.random() > 0.05;

    transactions.push({
      transaction_id: i,
      customer_id: isValid ? customerId : (Math.random() > 0.5 ? customerId : ''), // Some missing
      product_id: productId,
      quantity: isValid ? quantity : (Math.random() > 0.5 ? quantity : -1), // Some invalid
      transaction_date: date,
      status,
      // Some transactions missing amount (to be calculated from product price)
      amount: Math.random() > 0.2 ? '' : Math.round(Math.random() * 1000 * 100) / 100
    });
  }

  return transactions;
}

// Convert to CSV
function toCSV(data, filename) {
  if (data.length === 0) return;

  const headers = Object.keys(data[0]);
  const csvLines = [headers.join(',')];

  for (const row of data) {
    const values = headers.map(h => {
      const val = row[h];
      // Quote strings containing commas
      if (typeof val === 'string' && val.includes(',')) {
        return `"${val}"`;
      }
      return val;
    });
    csvLines.push(values.join(','));
  }

  const csv = csvLines.join('\n');
  fs.writeFileSync(filename, csv);
}

// Generate all datasets
console.log('Generating ETL pipeline datasets...\n');

const customers = generateCustomers(1000);
const products = generateProducts(200);
const transactions = generateTransactions(5000, 1000, 200);

toCSV(customers, 'customers.csv');
console.log(`✓ Generated customers.csv with ${customers.length} records`);
console.log(`  Columns: customer_id, first_name, last_name, email, country, segment, signup_year, is_active`);

toCSV(products, 'products.csv');
console.log(`\n✓ Generated products.csv with ${products.length} records`);
console.log(`  Columns: product_id, product_name, category, brand, price, cost, stock_quantity, is_available`);

toCSV(transactions, 'transactions.csv');
console.log(`\n✓ Generated transactions.csv with ${transactions.length} records`);
console.log(`  Columns: transaction_id, customer_id, product_id, quantity, transaction_date, status, amount`);
console.log(`  Note: ~5% of transactions have data quality issues (missing/invalid values)`);

console.log('\n✓ All datasets generated successfully!');
