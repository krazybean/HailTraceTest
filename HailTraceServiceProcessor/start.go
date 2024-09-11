package main

import (
	"os"
	"os/signal"
	"syscall"

	"utils"
)

func main() {
	bootstrapServers := "localhost:9092" // Update this to your Kafka bootstrap server address
	groupID := "weather-data-consumer-group" // Your consumer group ID

	// Initialize Kafka Producer
	err := utils.InitProducer(bootstrapServers)
	if err != nil {
		utils.Logger.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer utils.CloseProducer()

	// Initialize Kafka Admin Client
	adminClient, err := utils.NewKafkaAdminClient(bootstrapServers)
	if err != nil {
		utils.Logger.Fatalf("Failed to create Kafka admin client: %v", err)
	}
	defer adminClient.Close()

	// Ensure required topics exist
	err = utils.CheckAndCreateTopicsIfNotExists(bootstrapServers, adminClient)
	if err != nil {
		utils.Logger.Fatalf("Failed to check or create topics: %v", err)
	}

	// Initialize Kafka Consumer
	kafkaConsumer, err := utils.NewKafkaConsumer(bootstrapServers, groupID)
	if err != nil {
		utils.Logger.Fatalf("Failed to initialize Kafka consumer: %v", err)
	}
	defer kafkaConsumer.CloseConsumer()

	// Set up a channel to listen for OS signals for graceful shutdown
	stopChan := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		utils.Logger.Infof("Received signal: %s. Initiating shutdown...", sig)
		close(stopChan)
	}()

	// Start consuming from the topic and processing messages
	utils.Logger.Info("Starting to consume messages from topic...")
	err = kafkaConsumer.ConsumeFromRawWeatherReports(utils.TransformData, stopChan)
	if err != nil {
		utils.Logger.Fatalf("Error during message consumption: %v", err)
	}

	utils.Logger.Info("Application has shut down gracefully.")
}