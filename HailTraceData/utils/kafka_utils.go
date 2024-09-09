package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
)

var Logger *logrus.Logger
var producer *kafka.Producer

const (
	RAW_WEATHER_REPORTS_TOPIC string = "raw-weather-reports"
)

var topicName string = RAW_WEATHER_REPORTS_TOPIC

func init() {
	Logger = logrus.New()
	Logger.SetLevel(logrus.DebugLevel)
	Logger.SetFormatter(&logrus.TextFormatter{})
}

func InitKafkaProducer(bootstrapServers string) error {
	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		// Log the error using the logger instead of printing it
		Logger.Errorf("Failed to create Kafka producer: %v", err)
		return err
	}

	// Test the connection by checking for producer availability
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create an admin client to test the connection
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		Logger.Errorf("Failed to create Kafka admin client: %v", err)
		return err
	}
	defer adminClient.Close()

	// Extract the deadline and calculate the remaining timeout in milliseconds
	deadline, ok := ctx.Deadline()
	if !ok {
		Logger.Errorf("Failed to retrieve context deadline")
		return fmt.Errorf("failed to retrieve context deadline")
	}
	remainingTime := int(time.Until(deadline).Milliseconds())

	// Test if we can get metadata for the brokers
	_, err = adminClient.GetMetadata(nil, true, remainingTime)
	if err != nil {
		Logger.Errorf("Failed to connect to Kafka %s: %v", bootstrapServers, err)
		return err
	}

	Logger.Infof("Kafka producer initialized successfully with bootstrap servers: %s", bootstrapServers)
	return nil
}


func SendStormToTopic(jsonBody []byte) error {
	timeout := time.Duration(10) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Create new message
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topicName,
			Partition: kafka.PartitionAny,
		},
		Value: jsonBody,
	}

	// Check if the producer is nil
	if producer == nil {
		return fmt.Errorf("Kafka producer is not initialized")
	}

	// Produce the message asynchronously
	producer.ProduceChannel() <- msg

	select {
	case <-ctx.Done():
		return fmt.Errorf("message delivery timed out")
	case delivery := <-producer.Events():
		switch ev := delivery.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				return fmt.Errorf("error delivering message: %w", ev.TopicPartition.Error)
			}
			// Handle successful delivery
			Logger.Infof("Message delivered: Topic=%s, Partition=%d, Offset=%d",
				*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
		}
	}

	return nil
}

func CreateTopicIfNotExists(bootstrapServers, topic string) error {
	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer client.Close()

	topics, err := client.GetMetadata(nil, true, 10000)
	if err != nil {
		fmt.Println(err)
		return err
	}

	Logger.Info("Topics:")
	for _, topic := range topics.Topics {
		Logger.Info(topic.Topic)
	}

	var topicExists bool
	for _, topic := range topics.Topics {
		if topic.Topic == "raw-weather-reports" {
			topicExists = true
			break
		}
	}

	if !topicExists {
		Logger.Info("Topic 'raw-weather-reports' doesn't exist, creating...")
		_, err = client.CreateTopics(context.Background(), []kafka.TopicSpecification{
			{
				Topic:             "raw-weather-reports",
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		})

		if err != nil {
			fmt.Println(err)
			return err
		}
		Logger.Info("Topic 'raw-weather-reports' created successfully.")
	} else {
		Logger.Info("Topic 'raw-weather-reports' already exists.")
	}
	return err
}
