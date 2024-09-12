const { Kafka } = require('kafkajs');
const kafkaConfig = require('../kafkaConfig');

const kafka = new Kafka(kafkaConfig.kafka);
const consumer = kafka.consumer({ groupId: 'weather-ui-consumer-group' });

async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: kafkaConfig.kafka.topic, fromBeginning: true });

  const interval = 30 * 1000; // 30 seconds

  setInterval(async () => {
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log('Received message: ', message.value);
        // Process the message
      },
    });
  }, interval);
}

module.exports = { startConsumer };