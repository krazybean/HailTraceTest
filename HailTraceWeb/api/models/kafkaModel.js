const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./transformweather.db');
const consumer = require('../services/kafkaConsumer');

const init = async () => {
  await createTableIfNotExists();
};

const tableName = 'transformedweatherdata';

const createTableIfNotExists = async () => {
  await db.run(`CREATE TABLE IF NOT EXISTS ${tableName} (
    time VARCHAR(50),
    measurement VARCHAR(20),
    location VARCHAR(100),
    state VARCHAR(50),
    county VARCHAR(50),
    latitude VARCHAR(20),
    longitude VARCHAR(20),
    remarks TEXT
  );`);
};

const storeMessage = async (message) => {
  console.debug('Storing message: ', JSON.stringify(message));
  await init();
  const statement = `SELECT time, location, state, county
        FROM ${tableName}
        WHERE time = ? AND location = ? AND state = ? AND county = ?;`;
  const params = [message['time'], message['location'], message['state'], message['county']];
  const result = db.get(statement, params);
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);`;
    const insertParams = [
      message['time'],
      `'${message['measurement'].toString()}'`,
      message['location'],
      message['state'],
      message['county'],
      `'${message['latitude'].toString()}'`,
      `'${message['longitude'].toString()}'`,
      message['remarks']
    ];
    console.log("InsertParams: ", insertParams);
    db.run(insertStatement, insertParams);
    db.close()
  } else {
    console.warn(`Message already exists: ${JSON.stringify(message)}`);
  }
};

const getStats = async () => {
    return consumer.consumerStats()
};

module.exports = { storeMessage, getStats };