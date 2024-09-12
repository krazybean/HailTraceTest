// Import React and ReactDOM
import React from 'react';
import ReactDOM from 'react-dom';

// Import the top-level React component
import App from './App';

// Render the App component to the DOM
ReactDOM.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
  document.getElementById('root')
);