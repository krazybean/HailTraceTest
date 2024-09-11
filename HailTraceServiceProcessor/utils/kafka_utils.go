package utils

import (
	"context"
	"fmt"
	"time"
	"errors"

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
	CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error)
	Close()
}

func InitProducer(bootstrapServers string) error {
    producer, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": bootstrapServers,
    })
    if err != nil {
        return err
    }
    producer.ProduceChannel()
    producer.Events()
    producer.Close()
    return nil
}

func CloseProducer() {
	if producer != nil {
		producer.Close()
	}
}

func NewKafkaAdminClient(bootstrapServers string) (AdminClientInterface, error) {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka admin client: %w", err)
	}
	return adminClient, nil
}

func (kc *KafkaConsumer) CloseConsumer() {
	if kc.consumer != nil {
		kc.consumer.Close()
	}
}

type KafkaConsumer struct {
	consumer *kafka.Consumer
}

// Kafka params
func NewKafkaConsumer(bootstrapServers string, groupID string) (*KafkaConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       bootstrapServers,
		"group.id":                groupID,
		"auto.offset.reset":       "latest",
		"enable.auto.commit":      false,
		"session.timeout.ms":      6000,
		"max.poll.interval.ms":    300000,
		"auto.commit.interval.ms": 1000,
		"partition.assignment.strategy": "range",
	})
	if consumer != nil {
		return &KafkaConsumer{consumer: consumer}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}
	consumer.Close()
	return &KafkaConsumer{consumer: consumer}, nil
}

func rebalanceCallback(consumer *kafka.Consumer, event kafka.Event) error {
	if consumer == nil {
        return errors.New("consumer is nil")
    }
    switch ev := event.(type) {
    case kafka.AssignedPartitions:
        Logger.Infof("Assigned partitions: %v", ev.Partitions)
        err := consumer.Assign(ev.Partitions)
        if err != nil {
            Logger.Errorf("Error assigning partitions: %v", err)
        }
    case kafka.RevokedPartitions:
        Logger.Infof("Revoked partitions: %v", ev.Partitions)
        err := consumer.Unassign()
        if err != nil {
            Logger.Errorf("Error unassigning partitions: %v", err)
        }
    default:
        Logger.Infof("Rebalance event: %v", ev)
    }
    return nil
}

func (kc *KafkaConsumer) ConsumeFromRawWeatherReports(transformFunc func([]byte) ([]byte, error), stopChan <-chan struct{}) error {
    Logger.Infof("Inside ConsumeFromRawWeatherReports")

    err := kc.consumer.SubscribeTopics([]string{RAW_WEATHER_REPORTS_TOPIC}, rebalanceCallback)
    if err != nil {
        Logger.Errorf("failed to subscribe to raw-weather-reports topic: %v", err)
        return fmt.Errorf("failed to subscribe to topics: %w", err)
    }

    run := true
    for run {
        select {
        case <-stopChan:
            Logger.Info("Stop signal received. Exiting consume loop.")
            run = false

        default:
            // using .Poll() becuase .Events() is rubbish
            ev := kc.consumer.Poll(100)
            if ev == nil {
                continue
            }

            switch e := ev.(type) {
            case *kafka.Message:
                Logger.Infof("Received message event: %v", e)
                Logger.Infof("Consumed message from topic %s: %s", *e.TopicPartition.Topic, string(e.Value))

                // Transform the message, we pass in this func, not sure why it just isn't referenced directly.
                transformedData, err := transformFunc(e.Value)
                if err != nil {
                    Logger.Errorf("Error transforming message: %v", err)
                    continue
                }

                // Here is were we hand off data to the new transformed-weather-data topic.
                err = SendTransformedDataToTopic(transformedData)
                if err != nil {
                    Logger.Errorf("Error sending transformed message: %v", err)
                }

                // Manually commit the message
                _, err = kc.consumer.CommitOffsets([]kafka.TopicPartition{
                    {
                        Topic:     e.TopicPartition.Topic,
                        Partition: e.TopicPartition.Partition,
                        Offset:    e.TopicPartition.Offset + 1,
                    },
                })
                if err != nil {
                    Logger.Errorf("Failed to commit offsets: %v", err)
                }

            case kafka.Error:
                Logger.Errorf("Kafka consumer error: %v", e)
                if e.Code() == kafka.ErrAllBrokersDown {
                    run = false
                }

            default:
                Logger.Infof("Ignored event: %v", e)
            }
        }
    }

    return nil
}

func SendTransformedDataToTopic(jsonBody []byte) error {
	timeout := time.Duration(10) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Create message with transformed data
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &transformedTopicName,
			Partition: 0, // Due to issues i statically assigned this partition
		},
		Value: jsonBody,
	}

	if producer == nil {
		return fmt.Errorf("Kafka producer is not initialized")
	}

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
			Logger.Infof("Transformed message delivered: Topic=%s, Partition=%d, Offset=%d",
				*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
		default:
			Logger.Infof("Received unexpected event type: %v", ev)
		}
	}

	return nil
}

func CheckAndCreateTopicsIfNotExists(bootstrapServers string, topics []string, numPartitions int, replicationFactor int) error {
	// CheckAndCreateTopicsIfNotExists checks if required topics exist in Kafka, and creates them if not.
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return fmt.Errorf("failed to create Kafka admin client: %w", err)
	}
	defer adminClient.Close()

	// Get current metadata
	metadata, err := adminClient.GetMetadata(nil, false, 5000)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}

	existingTopics := make(map[string]struct{})
	for _, topic := range metadata.Topics {
		existingTopics[topic.Topic] = struct{}{}
	}

	// Create topics that do not exist
	var topicsToCreate []kafka.TopicSpecification
	for _, topic := range topics {
		if _, exists := existingTopics[topic]; !exists {
			topicsToCreate = append(topicsToCreate, kafka.TopicSpecification{
				Topic:             topic,
				NumPartitions:     numPartitions,
				ReplicationFactor: replicationFactor,
			})
		}
	}

	if len(topicsToCreate) > 0 {
		_, err = adminClient.CreateTopics(context.Background(), topicsToCreate)
		if err != nil {
			return fmt.Errorf("failed to create topics: %w", err)
		}
		Logger.Infof("Created topics: %v", topicsToCreate)
	} else {
		Logger.Infof("All topics already exist")
	}

	return nil
}

func TransformData(data []byte) ([]byte, error) {
	// TransformData is a placeholder function for transforming the weather report data.
	// For now, we just return the data as is
	return data, nil
}