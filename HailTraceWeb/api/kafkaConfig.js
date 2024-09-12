module.exports = {
    kafka: {
      clientId: 'weather-ui-consumer',
      brokers: ['localhost:9092'],
      topic: 'transformed-weather-data',
      groupId: 'weather-ui-consumer-group',
      fromBeginning: false
    }
  };