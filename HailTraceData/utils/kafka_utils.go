package utils

import (
	"context"
	"fmt"

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

func initProducer() {
	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		Logger.Fatal(err)
	}
}

func SendStormToTopic(jsonBody []byte) {
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topicName,
			Partition: kafka.PartitionAny,
		},
		Value: jsonBody,
	}, nil)
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
