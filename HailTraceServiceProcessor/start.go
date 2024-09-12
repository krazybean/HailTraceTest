package main

import (
	"os"
	"os/signal"
	"syscall"

	"utils"
)

func main() {
	bootstrapServers := "localhost:9092" // TODO: move this to config
	groupID := "weather-data-consumer-group" // TODO: Move to config

	// Initialize Kafka Producer
	producer, err := utils.InitProducer(bootstrapServers)
	if err != nil {
		utils.Logger.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	utils.Producer = producer
	defer utils.CloseProducer()

	// Initialize Kafka Admin Client
	adminClient, err := utils.NewKafkaAdminClient(bootstrapServers)
	if err != nil {
		utils.Logger.Fatalf("Failed to create Kafka admin client: %v", err)
	}
	defer adminClient.Close()

	// Ensure required topics exist
	topics := []string{utils.RAW_WEATHER_REPORTS_TOPIC, utils.TRANSFORMED_WEATHER_DATA}
	err = utils.CheckAndCreateTopicsIfNotExists(bootstrapServers, topics, 1, 1)
	if err != nil {
		utils.Logger.Fatalf("Failed to check or create topics: %v", err)
	}

	// Initialize Kafka Consumer
	kafkaConsumer, err := utils.NewKafkaConsumer(bootstrapServers, groupID)
	if err != nil {
		utils.Logger.Fatalf("Failed to initialize Kafka consumer: %v", err)
	}
	defer kafkaConsumer.CloseConsumer()

	// Signal handling
	stopChan := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

    // Graceful shutdown
	go func() {
		sig := <-sigChan
		utils.Logger.Infof("Received signal: %s. Initiating shutdown...", sig)
		close(stopChan)
	}()

	// Start Consumer to read in the raw-weather-reports topic and transform the data
	utils.Logger.Info("Starting to consume messages from topic...")
	err = kafkaConsumer.ConsumeFromRawWeatherReports(utils.TransformData, stopChan)
	if err != nil {
		utils.Logger.Fatalf("Error during message consumption: %v", err)
	}

	utils.Logger.Info("Application has shut down gracefully.")
}