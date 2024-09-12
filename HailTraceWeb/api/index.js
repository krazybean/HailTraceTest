// Import the Express app instance
require('app-module-path').addPath(__dirname); // Testing this out, cant' seem to make it work
const app = require('./app');

// Export the app instance
module.exports = app;

// Start the server
const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`API listening on port ${port}`);
});