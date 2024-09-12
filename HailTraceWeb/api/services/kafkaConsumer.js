const { Kafka } = require('kafkajs');
const kafkaConfig = require('../kafkaConfig');

const kafka = new Kafka(kafkaConfig.kafka);
console.error("GroupID: ", kafkaConfig.kafka.groupId);

let isRunning = false;
const consumer = kafka.consumer({ groupId: kafkaConfig.kafka.groupId });

// Build base stats
let stats = {
  messagesConsumed: 0,
  batchesFetched: 0,
  errorCount: 0,
  batchDetails: [],
};

// Populate stats
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

// Start consumer
async function start() {
  if (!isRunning) {
    await consumer.connect();
    await consumer.subscribe({ topic: kafkaConfig.kafka.topic, fromBeginning: true });
    setupConsumerEventListeners();

    setImmediate(async () => {
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          console.log('Received message: ', message.value.toString());
          handleCallback(message.value.toString());
        },
      });
    });

    isRunning = true;
  }
}

let messageCallback = null;

function consumeMessages(callback) {
  messageCallback = callback;
}

function handleCallback(message) {
  if (messageCallback) {
    messageCallback(message);
  }
}

// TODO: Still causing locking contention
function consumerStats() {
    console.info("Returning stats:", stats);
    return { ...stats };
  }
  

async function stop() {
  if (isRunning) {
    await consumer.disconnect();
    isRunning = false;
  }
}

module.exports = { start, consumeMessages, consumerStats, stop };