const { parentPort } = require('worker_threads');
const { Kafka } = require('kafkajs');
const kafkaConfig = require('../kafkaConfig');
const { getStats } = require('../models/kafkaModel');

const kafka = new Kafka(kafkaConfig.kafka);
console.error("GroupID: ", kafkaConfig.kafka.groupId);

let isRunning = false;
const consumer = kafka.consumer({ groupId: kafkaConfig.kafka.groupId });

// To store stats data
let stats = {
  messagesConsumed: 0,
  batchesFetched: 0,
  errorCount: 0,
  batchDetails: [],
};

function setupConsumerEventListeners() {
  consumer.on(consumer.events.FETCH, (event) => {
    stats.batchesFetched++;
    stats.batchDetails.push(event.payload);
    console.info("FETCH event captured");
  });

  consumer.on(consumer.events.END_BATCH_PROCESS, (event) => {
    stats.messagesConsumed += event.payload.messages.length;
    console.info("END_BATCH_PROCESS event captured");
  });

  consumer.on(consumer.events.CRASH, () => {
    stats.errorCount++;
    console.error("CRASH event captured");
  });
}

// Start Kafka consumer
// Seperated these out due to collisions
async function startConsumer() {
  if (!isRunning) {
    await consumer.connect();
    await consumer.subscribe({ topic: kafkaConfig.kafka.topic, fromBeginning: true });
    setupConsumerEventListeners();

    // Run the Kafka consumer
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log('Received message: ', message.value.toString());
      },
    });

    isRunning = true;
  }
}

// Stop Kafka consumer
async function stopConsumer() {
  if (isRunning) {
    await consumer.disconnect();
    isRunning = false;
  }
}

// Listen for messages from the main thread
parentPort.on('message', (message) => {
  if (message.type === 'requestStats') {
    // Send the current stats to the main thread
    parentPort.postMessage({ type: 'stats', data: { ...stats } });
  } else if (message.type === 'stopConsumer') {
    stopConsumer().catch(console.error);
  }
});

// Start the consumer when the worker thread starts
startConsumer().catch(console.error);