const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./transform-weather.db');
const consumer = require('../services/kafkaConsumer');

const init = async () => {
  await createTableIfNotExists();
};

const tableName = 'transformed-weather-data';

const createTableIfNotExists = async () => {
  await db.run(`CREATE TABLE IF NOT EXISTS ${tableName} (
    time DATETIME,
    measurement VARCHAR(20),
    location VARCHAR(100),
    state VARCHAR(50),
    county VARCHAR(50),
    latitude REAL,
    longitude REAL,
    remarks TEXT
  )`);
};

const storeMessage = async (message) => {
  await init();
  const statement = `SELECT time, location, state, county
        FROM ${tableName}
        WHERE time = ? AND location = ? AND state = ? AND county = ?`;
  const params = [message];
  const result = await db.get(statement, params);
  if (!result) {
    const insertStatement = `INSERT INTO ${tableName} (
        time,
        measurement,
        location,
        state,
        county,
        latitude,
        longitude,
        remarks
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
    await db.run(insertStatement, [message]);
  } else {
    console.warn(`Message already exists: ${message}`);
  }
};

const getStats = async () => {
    return consumer.consumerStats()
};

module.exports = { storeMessage, getStats };