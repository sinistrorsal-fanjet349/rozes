import fs from 'fs';

/**
 * Generate sample IoT sensor data for time series analysis
 */

const SENSORS = ['sensor_01', 'sensor_02', 'sensor_03', 'sensor_04', 'sensor_05'];
const LOCATIONS = ['Room-A', 'Room-B', 'Room-C', 'Warehouse', 'Loading-Dock'];

function generateTimestamp(baseDate, minutesOffset) {
  const date = new Date(baseDate.getTime() + minutesOffset * 60000);
  return date.toISOString();
}

function generateSensorReading(sensorId, timestamp, location, index) {
  // Simulate realistic sensor patterns
  const hourOfDay = new Date(timestamp).getHours();

  // Base temperature: higher during day (8am-6pm)
  const isDayTime = hourOfDay >= 8 && hourOfDay <= 18;
  const baseTemp = isDayTime ? 22 : 18;
  const tempVariation = Math.sin(index / 100) * 3 + (Math.random() - 0.5) * 2;
  let temperature = Math.round((baseTemp + tempVariation) * 10) / 10;

  // Humidity inversely correlated with temperature
  const baseHumidity = isDayTime ? 45 : 60;
  const humidityVariation = -tempVariation + (Math.random() - 0.5) * 5;
  let humidity = Math.round((baseHumidity + humidityVariation) * 10) / 10;

  // Pressure: slowly varying
  const basePressure = 1013.25;
  const pressureVariation = Math.sin(index / 500) * 5 + (Math.random() - 0.5);
  let pressure = Math.round((basePressure + pressureVariation) * 100) / 100;

  // Light level: high during day
  const baseLight = isDayTime ? 800 : 50;
  const lightVariation = (Math.random() - 0.5) * 100;
  let light = Math.max(0, Math.round(baseLight + lightVariation));

  // CO2: higher when occupied (day time)
  const baseCO2 = isDayTime ? 800 : 400;
  const co2Variation = (Math.random() - 0.5) * 100;
  let co2 = Math.max(350, Math.round(baseCO2 + co2Variation));

  // Random anomalies (5% chance)
  let isAnomaly = Math.random() < 0.05;
  if (isAnomaly) {
    // Spike one metric
    const spikeMetric = Math.floor(Math.random() * 3);
    if (spikeMetric === 0) temperature += 10;
    else if (spikeMetric === 1) humidity += 20;
    else co2 += 500;
  }

  return {
    timestamp,
    sensor_id: sensorId,
    location,
    temperature,
    humidity,
    pressure,
    light,
    co2,
    anomaly: isAnomaly ? 1 : 0
  };
}

function generateSensorData(numDays, readingsPerHour) {
  const rows = [];
  const startDate = new Date('2024-01-01T00:00:00Z');
  const totalReadings = numDays * 24 * readingsPerHour * SENSORS.length;
  const minutesPerReading = 60 / readingsPerHour;

  let index = 0;
  for (let day = 0; day < numDays; day++) {
    for (let hour = 0; hour < 24; hour++) {
      for (let reading = 0; reading < readingsPerHour; reading++) {
        for (let sensorIdx = 0; sensorIdx < SENSORS.length; sensorIdx++) {
          const minutesOffset = (day * 24 * 60) + (hour * 60) + (reading * minutesPerReading);
          const timestamp = generateTimestamp(startDate, minutesOffset);
          const sensorId = SENSORS[sensorIdx];
          const location = LOCATIONS[sensorIdx];

          rows.push(generateSensorReading(sensorId, timestamp, location, index));
          index++;
        }
      }
    }
  }

  return rows;
}

// Generate 30 days of data, 4 readings per hour (every 15 minutes)
console.log('Generating sensor data...');
const numDays = 30;
const readingsPerHour = 4;
const sensorData = generateSensorData(numDays, readingsPerHour);

// Convert to CSV
const headers = Object.keys(sensorData[0]);
const csvLines = [headers.join(',')];

for (const row of sensorData) {
  const values = headers.map(h => row[h]);
  csvLines.push(values.join(','));
}

const csv = csvLines.join('\n');

// Write to file
fs.writeFileSync('sensor_data.csv', csv);
console.log(`✓ Generated sensor_data.csv with ${sensorData.length.toLocaleString()} records`);
console.log(`  Sensors: ${SENSORS.length} (${SENSORS.join(', ')})`);
console.log(`  Locations: ${LOCATIONS.join(', ')}`);
console.log(`  Duration: ${numDays} days`);
console.log(`  Frequency: ${readingsPerHour} readings/hour (every ${60/readingsPerHour} minutes)`);
console.log(`  Metrics: temperature, humidity, pressure, light, co2`);
console.log(`  Anomalies: ~5% of readings flagged`);
