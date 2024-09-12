const KafkaConsumer = require('../services/kafkaConsumer');
const KafkaModel = require('../models/kafkaModel');

const parseMessage = (message) => {
  try {
    const jsonObject = JSON.parse(message);
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

const consumeMessages = async () => {
  KafkaConsumer.consumeMessages((message) => {
    const parsedMessage = parseMessage(message);
    if (parsedMessage) {
      KafkaModel.storeMessage(parsedMessage);
    }
  });
};

const getStatistics = async () => {
  console.debug('Getting statistics');
  const statistics = await KafkaModel.getStats();
  return statistics;
};

module.exports = { consumeMessages, parseMessage, getStatistics };