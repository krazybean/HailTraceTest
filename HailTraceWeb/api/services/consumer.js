// consumer.js

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

    await consumer.run({
        eachBatch: async ({ batch }) => {
            for (const message of batch.messages) {
                console.log('Received message: ', message.value.toString('utf8'));
                kafkaController.consumeMessages(message.value.toString('utf8')); 
            }
        },
    });
}

module.exports = { startConsumer };