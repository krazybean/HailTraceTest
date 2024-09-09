package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
)

var Logger *logrus.Logger
var producer ProducerInterface

const (
	RAW_WEATHER_REPORTS_TOPIC string = "raw-weather-reports"
)

var topicName string = RAW_WEATHER_REPORTS_TOPIC

func init() {
	Logger = logrus.New()
	Logger.SetLevel(logrus.DebugLevel)
	Logger.SetFormatter(&logrus.TextFormatter{})
}

type ProducerInterface interface {
	ProduceChannel() chan *kafka.Message
	Events() chan kafka.Event
	Close()
}

type AdminClientInterface interface {
	GetMetadata(*string, bool, int) (*kafka.Metadata, error)
	CreateTopics(context.Context, []kafka.TopicSpecification) ([]kafka.TopicResult, error)
	Close()
}

func NewKafkaAdminClient(bootstrapServers string) (AdminClientInterface, error) {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka admin client: %w", err)
	}
	return adminClientWrapper{adminClient}, nil
}

type adminClientWrapper struct {
	*kafka.AdminClient
}

func (w adminClientWrapper) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	return w.AdminClient.GetMetadata(topic, allTopics, timeoutMs)
}

func (w adminClientWrapper) CreateTopics(ctx context.Context, specs []kafka.TopicSpecification) ([]kafka.TopicResult, error) {
	return w.AdminClient.CreateTopics(ctx, specs)
}

func (w adminClientWrapper) Close() {
	w.AdminClient.Close()
}


func InitKafkaProducer(bootstrapServers string, adminClient AdminClientInterface) error {

	
	// Stayed with Confluent Kafka although the setup seems to be overkill
	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		Logger.Errorf("Failed to create Kafka producer: %v", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	defer adminClient.Close()

	deadline, ok := ctx.Deadline()
	if !ok {
		Logger.Errorf("Failed to retrieve context deadline")
		return fmt.Errorf("failed to retrieve context deadline")
	}
	remainingTime := int(time.Until(deadline).Milliseconds())

	// We set no timeout for the context but we do set it for the adminClient connections
	_, err = adminClient.GetMetadata(nil, true, remainingTime)
	if err != nil {
		Logger.Errorf("Failed to connect to Kafka %s: %v", bootstrapServers, err)
		return err
	}

	Logger.Infof("Kafka producer initialized successfully with bootstrap servers: %s", bootstrapServers)
	return nil
}


func SendStormToTopic(jsonBody []byte) error {
	// This is broken out into its own function incase we plan on sending other topics to other locations
	timeout := time.Duration(10) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Create message with our JSoN data
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topicName,
			Partition: kafka.PartitionAny,
		},
		Value: jsonBody,
	}

	// Check if the producer is empty, then fail out
	if producer == nil {
		return fmt.Errorf("Kafka producer is not initialized")
	}

	// Put the message on the channel
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
			// We made it to the topic send!
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