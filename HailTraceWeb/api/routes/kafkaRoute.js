const express = require('express');
const router = express.Router();
const { consumerStats } = require('../services/kafkaConsumer');

// Using this for testing purposes mostly
router.get('/stats', (req, res) => {
  const stats = consumerStats();
  res.json(stats);
});

module.exports = router;