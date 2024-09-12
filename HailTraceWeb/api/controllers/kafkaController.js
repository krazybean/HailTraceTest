const KafkaConsumer = require('../services/kafkaConsumer');
const KafkaModel = require('../models/kafkaModel');

const translator = (JSONobj1) => {
  var JSONobj2 = {}
  JSONobj2['time'] = JSONobj1['time'];
  JSONobj2['measurement'] = JSONobj1['measurement'].toString();
  JSONobj2['location'] = JSONobj1['location'];
  JSONobj2['state'] = JSONobj1['state'];
  JSONobj2['county'] = JSONobj1['county'];
  JSONobj2['latitude'] = JSONobj1['latitude'].toString();
  JSONobj2['longitude'] = JSONobj1['longitude'].toString();
  JSONobj2['remarks'] = JSONobj1['remarks'];
  return JSONobj2;
}

const parseMessage = (message) => {
  console.debug('Parsing message');
  try {
    const jsonObject = translator(JSON.parse(message));
    const requiredKeys = [
        'time',
        'measurement',
        'location',
        'state',
        'county',
        'latitude',
        'longitude',
        'remarks'];
    if (requiredKeys.every((key) => key in jsonObject)) {
      return jsonObject;
    } else {
      console.warn(`Skipping message: ${message} (missing required keys)`);
      return null;
    }
  } catch (error) {
    console.error(`Error parsing message: ${message} (invalid JSON)`);
    return null;
  }
};

const consumeMessages = (message) => {
  const parsedMessage = parseMessage(message);
  if (parsedMessage) {
    console.debug('Storing message');
    KafkaModel.storeMessage(parsedMessage);
  }
};

const getStatistics = async () => {
  console.debug('Getting statistics');
  const statistics = await KafkaModel.getStats();
  return statistics;
};

module.exports = { consumeMessages, parseMessage, getStatistics };