package utils

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func CreateTopicIfNotExists(bootstrapServers, topic string) error {
	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	topics, err := client.GetMetadata(nil, true, 10000)
	if err != nil {
		return err
	}

	var topicExists bool
	for _, t := range topics.Topics {
		if t.Topic == topic {
			topicExists = true
			break
		}
	}

	if !topicExists {
		_, err = client.CreateTopics(context.Background(), []kafka.TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}
