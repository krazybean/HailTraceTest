// Import the Express Router
const express = require('express');
const router = express.Router();

// Define a simple route
router.get('/', (req, res) => {
  res.send('User routes work!');
});

// Export the router
module.exports = router;