// Import required modules
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const { Worker } = require('worker_threads');

const userRoutes = require('./routes/userRoutes');
const kafkaRoute = require('./routes/kafkaRoute'); 

const app = express();

// Middlewares
app.use(bodyParser.json());
app.use(cors());

// API routes
app.use('/api/users', userRoutes); // boilerplate stuff
app.use('/kafka', kafkaRoute); // TODO: Find base response for this url before delegation

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).send('Internal Server Error');
});

// Start Kafka consumer thread
const workerPath = path.resolve(__dirname, './workers/kafkaConsumerWorker.js');
const worker = new Worker(workerPath);

// To store Kafka stats data received from the worker
let kafkaStats = {};

// Worker listens
worker.on('message', (message) => {
  if (message.type === 'stats') {
    kafkaStats = message.data; // Update stats from the worker thread
  }
});

// Expose Kafka stats via API endpoint
app.get('/kafka/stats', (req, res) => {
  res.json(kafkaStats); // Return the latest Kafka stats
});

// Graceful Death
process.on('exit', async () => {
  worker.postMessage({ type: 'stopConsumer' });  // Instruct worker to stop the consumer
});

module.exports = app;