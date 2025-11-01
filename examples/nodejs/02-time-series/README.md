# Time Series Analysis Example

Demonstrates Rozes DataFrame capabilities for IoT sensor time series analysis:

- **Rolling Windows**: Moving averages for smoothing
- **Resampling**: Minute → Hour → Day aggregations
- **Anomaly Detection**: Statistical outlier detection (3-sigma rule)
- **Correlation Analysis**: Multi-variate relationships
- **High-Frequency Data**: Handling 15-minute interval sensor readings
- **Performance**: Efficient processing of 14K+ time series records

## Dataset

30 days of IoT sensor data:
- **5 sensors** in different locations
- **4 readings per hour** (every 15 minutes)
- **14,400 total records** (5 sensors × 30 days × 24 hours × 4 readings)
- **Metrics**: temperature, humidity, pressure, light, CO2
- **Anomalies**: ~5% of readings flagged

## Installation

```bash
npm install
```

## Usage

### 1. Generate Sample Data

```bash
npm run generate-data
```

This creates `sensor_data.csv` with 14,400 sensor readings.

### 2. Run Analysis

```bash
npm start
```

### Expected Output

```
=== Rozes Time Series Analysis - IoT Sensors ===

📊 Loading sensor data...
✓ Loaded 14,400 records in 38.56ms
  Columns: timestamp, sensor_id, location, temperature, humidity, pressure, light, co2, anomaly
  Sensors: 5
  Time range: 2024-01-01T00:00:00.000Z to 2024-01-30T23:45:00.000Z

1️⃣  Overall Statistics by Sensor
────────────────────────────────────────────────────────────────────────────────
sensor_id  location       avg_temp  avg_humidity  avg_co2  readings  anomalies
sensor_01  Room-A         20.1      52.3          612      2,880     144
sensor_02  Room-B         19.9      53.1          598      2,880     142
...

⚡ Computed in 15.23ms
```

## Key Techniques

### Rolling Windows (Moving Averages)

```javascript
function rollingAverage(df, column, windowSize) {
  const values = df.column(column).toArray();
  const result = [];

  for (let i = 0; i < values.length; i++) {
    const start = Math.max(0, i - windowSize + 1);
    const window = values.slice(start, i + 1);
    const avg = window.reduce((a, b) => a + b, 0) / window.length;
    result.push(avg);
  }

  return result;
}

const rollingTemp = rollingAverage(df, 'temperature', 12);
const withRolling = df.withColumn('temp_ma12', (row, idx) => rollingTemp[idx]);
```

### Resampling (Hourly Aggregations)

```javascript
function resampleToHourly(df) {
  const withHour = df.withColumn('hour', row => {
    const timestamp = row.get('timestamp');
    return timestamp.substring(0, 13); // YYYY-MM-DDTHH
  });

  return withHour
    .groupBy(['sensor_id', 'hour'])
    .agg({
      temperature: 'mean',
      humidity: 'mean',
      co2: 'mean',
      readings: 'count'
    });
}
```

### Anomaly Detection (3-Sigma Rule)

```javascript
function detectAnomalies(df, column, threshold = 3) {
  const values = df.column(column).toArray();
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / values.length;
  const stddev = Math.sqrt(variance);

  return values.map(v => Math.abs(v - mean) > threshold * stddev ? 1 : 0);
}

const anomalies = detectAnomalies(df, 'co2', 3);
```

### Correlation Analysis

```javascript
const temps = df.column('temperature').toArray();
const humidities = df.column('humidity').toArray();

// Pearson correlation coefficient
const correlation = calculatePearsonCorrelation(temps, humidities);
console.log(`Correlation: ${correlation.toFixed(4)}`);
```

## Performance Notes

- **CSV Parsing**: ~40ms for 14K records (SIMD-optimized)
- **Hourly Resampling**: ~20-30ms (parallel groupBy)
- **Rolling Average**: ~15ms for 12-window on 2,880 points
- **Anomaly Detection**: ~10ms (SIMD-optimized statistics)
- **Correlation**: ~5ms for 2,880 point pairs
- **Total Analysis**: ~150-200ms for complete workflow

On larger datasets (100K+ readings), expect 2-10× speedup with SIMD/parallel optimizations.

## Use Cases

- **Environmental Monitoring**: HVAC optimization, energy management
- **Industrial IoT**: Equipment health monitoring, predictive maintenance
- **Smart Buildings**: Occupancy detection, air quality monitoring
- **Research**: Scientific sensor networks, climate studies

## License

MIT
