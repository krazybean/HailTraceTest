package utils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var producer ProducerInterface

const (
	RAW_WEATHER_REPORTS_TOPIC string = "raw-weather-reports"
	TRANSFORMED_WEATHER_DATA  string = "transformed-weather-data"
)

var rawTopicName string = RAW_WEATHER_REPORTS_TOPIC
var transformedTopicName string = TRANSFORMED_WEATHER_DATA

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

type ConsumerInterface interface {
	SubscribeTopics([]string, kafka.RebalanceCb) error
	Events() chan kafka.Event
	Close() error
	CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error)
}

type kafkaConsumerWrapper struct {
	*kafka.Consumer
}

func (kc kafkaConsumerWrapper) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error {
	return kc.Consumer.SubscribeTopics(topics, rebalanceCb)
}

func (kc kafkaConsumerWrapper) Events() chan kafka.Event {
	Logger.Debugf("Events details: %v", kc.Consumer.Events())
	return kc.Consumer.Events()
}

func (kc kafkaConsumerWrapper) Close() error {
	return kc.Consumer.Close()
}

func (kc kafkaConsumerWrapper) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	Logger.Infof("Committing message: Topic=%s, Partition=%d, Offset=%d",
        *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
	return kc.Consumer.CommitMessage(msg)
}

type KafkaConsumer struct {
	consumer ConsumerInterface
}

// CheckAndCreateTopicsIfNotExists checks if the required topics exist and creates them if they don't.
func CheckAndCreateTopicsIfNotExists(bootstrapServers string, adminClient AdminClientInterface) error {
	topicsToCheck := []string{RAW_WEATHER_REPORTS_TOPIC, TRANSFORMED_WEATHER_DATA}

	// Get metadata for all topics
	metadata, err := adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}

	// Check if topics exist
	existingTopics := make(map[string]bool)
	for _, topic := range metadata.Topics {
		existingTopics[topic.Topic] = true
	}

	var topicsToCreate []kafka.TopicSpecification
	for _, topicName := range topicsToCheck {
		if !existingTopics[topicName] {
			Logger.Infof("Topic '%s' does not exist, creating...", topicName)
			topicsToCreate = append(topicsToCreate, kafka.TopicSpecification{
				Topic:             topicName,
				NumPartitions:     1,
				ReplicationFactor: 1,
			})
		} else {
			Logger.Infof("Topic '%s' already exists.", topicName)
		}
	}

	// Create missing topics if necessary
	if len(topicsToCreate) > 0 {
		_, err = adminClient.CreateTopics(context.Background(), topicsToCreate)
		if err != nil {
			return fmt.Errorf("failed to create topics: %w", err)
		}
		Logger.Info("Topics created successfully.")
	}

	return nil
}

func NewKafkaConsumer(bootstrapServers string, groupID string) (*KafkaConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       bootstrapServers,
		"group.id":                groupID,
		"auto.offset.reset":       "latest",  // Set to 'earliest' or 'latest' to avoid INVALID offset.
		"enable.auto.commit":      false,       // Use manual commit to have more control.
		"session.timeout.ms":      6000,
		"max.poll.interval.ms":    300000,
		"auto.commit.interval.ms": 1000,
		"partition.assignment.strategy": "range",  // Use a valid strategy.
		"debug":                  "all",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}
	return &KafkaConsumer{consumer: kafkaConsumerWrapper{consumer}}, nil
}

func rebalanceCallback(consumer *kafka.Consumer, event kafka.Event) error {
    switch ev := event.(type) {
    case kafka.AssignedPartitions:
        Logger.Infof("Assigned partitions: %v", ev.Partitions)
        err := consumer.Assign(ev.Partitions)  // Make sure partitions are assigned correctly
        if err != nil {
            Logger.Errorf("Error assigning partition: %v", err)
        }
    case kafka.RevokedPartitions:
        Logger.Infof("Revoked partitions: %v", ev.Partitions)
        err := consumer.Unassign()  // Make sure partitions are unassigned properly
        if err != nil {
            Logger.Errorf("Error unassigning partitions: %v", err)
        }
    default:
        Logger.Infof("Rebalance event: %v", ev)
    }
    return nil
}

// ConsumeFromRawWeatherReports reads messages from the raw-weather-reports topic and processes them.
func (kc *KafkaConsumer) ConsumeFromRawWeatherReports(transformFunc func([]byte) ([]byte, error), stopChan <-chan struct{}) error {
	// Subscribe to the raw-weather-reports topic with a rebalance callback
	Logger.Infof("Inside ConsumeFromRawWeatherReports")
	err := kc.consumer.SubscribeTopics([]string{RAW_WEATHER_REPORTS_TOPIC}, rebalanceCallback) // Use the correct callback function signature
	if err != nil {
		Logger.Errorf("failed to subscribe to raw-weather-reports topic: %v", err)
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	// Event loop to consume messages
	Logger.Info("Made it to the event loop")
	for {
		Logger.Infof("Waiting for messages")
		select {
		case <-stopChan:
			Logger.Info("Stop signal received. Exiting consume loop.")
			return nil
		case msgEvent := <-kc.consumer.Events():
			switch ev := msgEvent.(type) {
			case *kafka.Message:
				Logger.Infof("Received message event: %v", ev)
				Logger.Infof("Consumed message from topic %s: %s", *ev.TopicPartition.Topic, string(ev.Value))

				// Transform the message
				transformedData, err := transformFunc(ev.Value)
				if err != nil {
					Logger.Errorf("Error transforming message: %v", err)
					continue
				}

				// Produce the transformed message to the new topic
				err = SendTransformedDataToTopic(transformedData)
				if err != nil {
					Logger.Errorf("Error sending transformed message: %v", err)
				}

				// Manually commit the message
				_, err = kc.consumer.CommitMessage(ev)
				if err != nil {
					Logger.Errorf("Failed to commit message: %v", err)
				}

			case kafka.Error:
				Logger.Errorf("Kafka consumer error: %v", ev)
			}
		}
	}
}

// CloseConsumer closes the Kafka consumer.
func (kc *KafkaConsumer) CloseConsumer() {
	if kc.consumer != nil {
		kc.consumer.Close()
	}
}

// TransformData is a sample function to transform raw data.
func TransformData(rawData []byte) ([]byte, error) {
	// Example: Convert the raw data to uppercase (simple transformation)
	transformedData := []byte(strings.ToUpper(string(rawData)))
	return transformedData, nil
}

// SendTransformedDataToTopic sends transformed data to the transformed-weather-data topic.
func SendTransformedDataToTopic(jsonBody []byte) error {
	// Define a timeout for the context
	timeout := time.Duration(10) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Create message with transformed data
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &transformedTopicName,
			Partition: 0,
		},
		Value: jsonBody,
	}

	// Fail if producer is not initialized
	if producer == nil {
		return fmt.Errorf("Kafka producer is not initialized")
	}

	// Put the message on the producer channel
	producer.ProduceChannel() <- msg

	// Wait for the delivery report or timeout
	select {
	case <-ctx.Done():
		return fmt.Errorf("message delivery timed out")
	case delivery := <-producer.Events():
		switch ev := delivery.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				return fmt.Errorf("error delivering message: %w", ev.TopicPartition.Error)
			}
			Logger.Infof("Transformed message delivered: Topic=%s, Partition=%d, Offset=%d",
				*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
		default:
			Logger.Infof("Received unexpected event type: %v", ev)
		}
	}

	return nil
}

// InitProducer initializes the Kafka producer.
func InitProducer(bootstrapServers string) error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}
	producer = p
	return nil
}

// CloseProducer closes the Kafka producer.
func CloseProducer() {
	if producer != nil {
		producer.Close()
	}
}