const { Kafka } = require('kafkajs');
const kafkaConfig = require('../kafkaConfig');
const kafkaController = require('../controllers/kafkaController');

const kafka = new Kafka(kafkaConfig.kafka);
const consumer = kafka.consumer({ groupId: 'weather-ui-consumer-group' });

async function startConsumer() {
    console.log('Connecting to Kafka...');
    await consumer.connect();
    console.log('Connected to Kafka!');

    console.log('Subscribing to topic...');
    await consumer.subscribe({ topic: kafkaConfig.kafka.topic, fromBeginning: true });
    console.log('Subscribed to topic!');

    const interval = 30 * 1000; // 30 seconds
  
    setInterval(async () => {
      await consumer.run({
        eachMessage: {
          topic: kafkaConfig.kafka.topic,
          partition: 0,
          callback: async ({ topic, partition, message }) => {
            console.log('Received message: ', message.value);
            kafkaController.consumeMessages(message.value)
          },
        },
      });
    }, interval);
  }

module.exports = { startConsumer };