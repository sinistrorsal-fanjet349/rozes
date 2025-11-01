/**
 * ⚠️  NOTE: This example uses future API features planned for Milestone 1.4.0
 *
 * Features used that are not yet implemented:
 * - withColumn() - Adding computed columns
 * - filter() with JavaScript callbacks
 * - groupBy() with complex aggregations (agg() method)
 * - Column methods: .toArray(), .unique()
 * - Helper methods: .rowCount(), .columnNames()
 *
 * This is an aspirational example showing the planned API.
 * See docs/TODO.md for the implementation timeline.
 */

import { Rozes, DataFrame } from 'rozes';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Time Series Analysis - IoT Sensor Data
 *
 * Demonstrates:
 * - Time series data parsing and indexing
 * - Rolling window aggregations (moving averages)
 * - Resampling (minute → hour → day)
 * - Anomaly detection
 * - Multi-sensor correlation analysis
 * - Performance on high-frequency time series data
 */

/**
 * Calculate rolling average for a column
 */
function rollingAverage(df, column, windowSize) {
  const values = df.column(column); // Already returns TypedArray
  const result = [];

  for (let i = 0; i < values.length; i++) {
    const start = Math.max(0, i - windowSize + 1);
    const window = values.slice(start, i + 1);
    const avg = window.reduce((a, b) => a + b, 0) / window.length;
    result.push(Math.round(avg * 100) / 100);
  }

  return result;
}

/**
 * Resample time series to hourly averages
 */
function resampleToHourly(df) {
  // Extract hour from timestamp (ISO format)
  const withHour = df.withColumn('hour', row => {
    const timestamp = row.get('timestamp');
    return timestamp.substring(0, 13); // YYYY-MM-DDTHH
  });

  // Group by sensor and hour
  return withHour
    .groupBy(['sensor_id', 'hour'])
    .agg({
      temperature: 'mean',
      humidity: 'mean',
      pressure: 'mean',
      light: 'mean',
      co2: 'mean',
      readings: 'count',
      anomalies: { column: 'anomaly', fn: 'sum' }
    })
    .sortBy('hour', 'asc');
}

/**
 * Detect anomalies using simple statistical method (3-sigma rule)
 */
function detectAnomalies(df, column, threshold = 3) {
  const values = df.column(column); // Already returns TypedArray
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / values.length;
  const stddev = Math.sqrt(variance);

  const anomalies = Array.from(values).map(v => Math.abs(v - mean) > threshold * stddev ? 1 : 0);
  return anomalies;
}

async function main() {
  console.log('=== Rozes Time Series Analysis - IoT Sensors ===\n');

  // Check if data exists
  if (!fs.existsSync('sensor_data.csv')) {
    console.log('⚠️  sensor_data.csv not found. Run: npm run generate-data');
    process.exit(1);
  }

  // Initialize Rozes
  console.log('🚀 Initializing Rozes...');
  const wasmPath = path.join(__dirname, '../../../zig-out/bin/rozes.wasm');
  await Rozes.init(wasmPath);

  // Load sensor data
  console.log('📊 Loading sensor data...');
  const startLoad = performance.now();
  const csv = fs.readFileSync('sensor_data.csv', 'utf8');
  const df = DataFrame.fromCSV(csv);
  const loadTime = performance.now() - startLoad;

  console.log(`✓ Loaded ${df.shape.rows.toLocaleString()} records in ${loadTime.toFixed(2)}ms`);
  console.log(`  Columns: ${df.columns.join(', ')}`);

  // Count unique sensors (manual implementation since .unique() not available)
  const sensorIds = df.column('sensor_id');
  const uniqueSensors = new Set(sensorIds).size;
  console.log(`  Sensors: ${uniqueSensors}`);

  // Get time range
  const timestamps = df.column('timestamp');
  console.log(`  Time range: ${timestamps[0]} to ${timestamps[timestamps.length - 1]}\n`);

  // 1. Overall Statistics by Sensor
  console.log('1️⃣  Overall Statistics by Sensor');
  console.log('─'.repeat(80));
  const startStats = performance.now();
  const sensorStats = df
    .groupBy('sensor_id')
    .agg({
      location: 'first',
      avg_temp: { column: 'temperature', fn: 'mean' },
      avg_humidity: { column: 'humidity', fn: 'mean' },
      avg_co2: { column: 'co2', fn: 'mean' },
      readings: 'count',
      anomalies: { column: 'anomaly', fn: 'sum' }
    })
    .sortBy('sensor_id', 'asc');
  const statsTime = performance.now() - startStats;

  console.log(sensorStats.toString());
  console.log(`⚡ Computed in ${statsTime.toFixed(2)}ms\n`);

  // 2. Hourly Resampling
  console.log('2️⃣  Hourly Resampling (First 24 hours, Sensor 01)');
  console.log('─'.repeat(80));
  const startResample = performance.now();
  const hourlyData = resampleToHourly(df);
  const sensor01Hourly = hourlyData
    .filter(row => row.get('sensor_id') === 'sensor_01')
    .head(24);
  const resampleTime = performance.now() - startResample;

  console.log(sensor01Hourly.toString());
  console.log(`⚡ Resampled in ${resampleTime.toFixed(2)}ms`);
  console.log(`  Total hourly records: ${hourlyData.shape.rows}\n`);

  // 3. Rolling Average (Temperature)
  console.log('3️⃣  Rolling Average - Temperature (12-reading window, Sensor 01)');
  console.log('─'.repeat(80));
  const startRolling = performance.now();

  // Filter to sensor_01 for cleaner output
  const sensor01 = df.filter(row => row.get('sensor_id') === 'sensor_01');
  const rollingTemp = rollingAverage(sensor01, 'temperature', 12);

  // Add rolling average column
  const withRolling = sensor01.withColumn('temp_ma12', (row, idx) => rollingTemp[idx]);
  const rollingTime = performance.now() - startRolling;

  console.log(withRolling.select(['timestamp', 'temperature', 'temp_ma12']).head(20).toString());
  console.log(`⚡ Computed in ${rollingTime.toFixed(2)}ms`);
  console.log(`  Window size: 12 readings (3 hours)\n`);

  // 4. Anomaly Detection (Statistical)
  console.log('4️⃣  Anomaly Detection - CO2 Levels (3-sigma rule, Sensor 01)');
  console.log('─'.repeat(80));
  const startAnomaly = performance.now();

  const co2Anomalies = detectAnomalies(sensor01, 'co2', 3);
  const withAnomalyDetection = sensor01.withColumn('co2_anomaly', (row, idx) => co2Anomalies[idx]);
  const detectedAnomalies = withAnomalyDetection.filter(row => row.get('co2_anomaly') === 1);
  const anomalyTime = performance.now() - startAnomaly;

  console.log(detectedAnomalies.select(['timestamp', 'co2', 'co2_anomaly']).head(10).toString());
  console.log(`⚡ Detected in ${anomalyTime.toFixed(2)}ms`);
  console.log(`  Found ${detectedAnomalies.shape.rows} statistical anomalies (out of ${sensor01.shape.rows}`)
  console.log(`  Detection rate: ${(detectedAnomalies.shape.rows / sensor01.shape.rows * 100).toFixed(2)}%\n`);

  // 5. Correlation Analysis
  console.log('5️⃣  Temperature vs Humidity Correlation (Sensor 01)');
  console.log('─'.repeat(80));
  const startCorr = performance.now();

  const temps = sensor01.column('temperature'); // Already returns TypedArray
  const humidities = sensor01.column('humidity'); // Already returns TypedArray

  // Calculate Pearson correlation coefficient
  const n = temps.length;
  const meanTemp = temps.reduce((a, b) => a + b, 0) / n;
  const meanHumidity = humidities.reduce((a, b) => a + b, 0) / n;

  let numerator = 0;
  let denomTemp = 0;
  let denomHumidity = 0;

  for (let i = 0; i < n; i++) {
    const diffTemp = temps[i] - meanTemp;
    const diffHumidity = humidities[i] - meanHumidity;
    numerator += diffTemp * diffHumidity;
    denomTemp += diffTemp * diffTemp;
    denomHumidity += diffHumidity * diffHumidity;
  }

  const correlation = numerator / Math.sqrt(denomTemp * denomHumidity);
  const corrTime = performance.now() - startCorr;

  console.log(`Pearson Correlation (Temperature vs Humidity): ${correlation.toFixed(4)}`);
  console.log(`⚡ Computed in ${corrTime.toFixed(2)}ms`);
  console.log(`  Sample size: ${n.toLocaleString()} readings`);
  console.log(`  Mean Temperature: ${meanTemp.toFixed(2)}°C`);
  console.log(`  Mean Humidity: ${meanHumidity.toFixed(2)}%\n`);

  // 6. Daily Aggregations
  console.log('6️⃣  Daily Aggregations (All Sensors)');
  console.log('─'.repeat(80));
  const startDaily = performance.now();

  const withDate = df.withColumn('date', row => {
    const timestamp = row.get('timestamp');
    return timestamp.substring(0, 10); // YYYY-MM-DD
  });

  const dailyStats = withDate
    .groupBy('date')
    .agg({
      avg_temp: { column: 'temperature', fn: 'mean' },
      min_temp: { column: 'temperature', fn: 'min' },
      max_temp: { column: 'temperature', fn: 'max' },
      avg_co2: { column: 'co2', fn: 'mean' },
      max_co2: { column: 'co2', fn: 'max' },
      total_anomalies: { column: 'anomaly', fn: 'sum' },
      readings: 'count'
    })
    .sortBy('date', 'asc')
    .head(10);
  const dailyTime = performance.now() - startDaily;

  console.log(dailyStats.toString());
  console.log(`⚡ Computed in ${dailyTime.toFixed(2)}ms\n`);

  // 7. Performance Summary
  console.log('7️⃣  Performance Summary');
  console.log('─'.repeat(80));
  const totalAnalysisTime = statsTime + resampleTime + rollingTime + anomalyTime + corrTime + dailyTime;

  console.log(`CSV Parsing:           ${loadTime.toFixed(2)}ms`);
  console.log(`Sensor Statistics:     ${statsTime.toFixed(2)}ms`);
  console.log(`Hourly Resampling:     ${resampleTime.toFixed(2)}ms`);
  console.log(`Rolling Average:       ${rollingTime.toFixed(2)}ms`);
  console.log(`Anomaly Detection:     ${anomalyTime.toFixed(2)}ms`);
  console.log(`Correlation Analysis:  ${corrTime.toFixed(2)}ms`);
  console.log(`Daily Aggregations:    ${dailyTime.toFixed(2)}ms`);
  console.log('─'.repeat(80));
  console.log(`Total Analysis Time:   ${totalAnalysisTime.toFixed(2)}ms`);
  console.log(`Total Time (w/ load):  ${(loadTime + totalAnalysisTime).toFixed(2)}ms`);

  console.log('\n✓ Time series analysis complete!');
}

main().catch(console.error);
