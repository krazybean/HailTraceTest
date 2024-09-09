package utils

import (
	"context"
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockProducer
type MockProducer struct {
	mock.Mock
}

// ProduceChannel mock
func (m *MockProducer) ProduceChannel() chan *kafka.Message {
	args := m.Called()
	return args.Get(0).(chan *kafka.Message)
}

// Events mock
func (m *MockProducer) Events() chan kafka.Event {
	args := m.Called()
	return args.Get(0).(chan kafka.Event)
}

// Nuke the MockProducer
func (m *MockProducer) Close() {
	m.Called()
}

// MockAdminClient struct is required sadly
type MockAdminClient struct {
	mock.Mock
}

// GetMetadata mocks
func (m *MockAdminClient) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	args := m.Called(topic, allTopics, timeoutMs)
	return args.Get(0).(*kafka.Metadata), args.Error(1)
}

// CreateTopics mocks
func (m *MockAdminClient) CreateTopics(ctx context.Context, specs []kafka.TopicSpecification) ([]kafka.TopicResult, error) {
	args := m.Called(ctx, specs)
	return args.Get(0).([]kafka.TopicResult), args.Error(1)
}

// Nuke MockAdminClient
func (m *MockAdminClient) Close() {
	m.Called()
}

func TestInitKafkaProducer_Success(t *testing.T) {
	// Tests the producer connection did make a successful connection.
	mockAdminClient := new(MockAdminClient)

	mockAdminClient.On("GetMetadata", (*string)(nil), true, mock.AnythingOfType("int")).Return(&kafka.Metadata{}, nil)
	mockAdminClient.On("Close").Return() 

	oldProducer := producer
	defer func() { producer = oldProducer }()

	mockProducer := new(MockProducer)
	producer = mockProducer

	err := InitKafkaProducer("localhost:9092", mockAdminClient)

	// Assertions
	assert.NoError(t, err, "Expected no error when initializing Kafka producer")
	mockAdminClient.AssertExpectations(t)
}

func TestInitKafkaProducer_Failure(t *testing.T) {
	// Tests the producer with a fake connection to ensure a failure is returned
	mockAdminClient := new(MockAdminClient)

	mockAdminClient.On("GetMetadata", (*string)(nil), true, mock.AnythingOfType("int")).Return(&kafka.Metadata{}, errors.New("failed to connect"))
	mockAdminClient.On("Close").Return()

	oldProducer := producer
	defer func() { producer = oldProducer }()

	mockProducer := new(MockProducer)
	producer = mockProducer

	err := InitKafkaProducer("localhost:9092", mockAdminClient)

	// Assertions
	assert.Error(t, err, "Expected error when initializing Kafka producer with invalid settings")
	mockAdminClient.AssertExpectations(t)
}

func TestSendStormToTopic_Success(t *testing.T) {
	// Test positive case on sending message to topic
	mockProducer := new(MockProducer)
	producer = mockProducer

	messageChan := make(chan *kafka.Message, 1)
	eventChan := make(chan kafka.Event, 1)
	mockProducer.On("ProduceChannel").Return(messageChan)
	mockProducer.On("Events").Return(eventChan)

	eventChan <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topicName,
			Partition: 0,
			Offset:    0,
			Error:     nil,
		},
	}

	err := SendStormToTopic([]byte(`{"data":"storm"}`))
	assert.NoError(t, err, "Expected no error for successful message delivery")

	// Assertions
	mockProducer.AssertExpectations(t)
}

func TestSendStormToTopic_Timeout(t *testing.T) {
	// Tests a timeout in the event a topic cannot be reached
	mockProducer := new(MockProducer)
	producer = mockProducer

	messageChan := make(chan *kafka.Message, 1)
	eventChan := make(chan kafka.Event)
	mockProducer.On("ProduceChannel").Return(messageChan)
	mockProducer.On("Events").Return(eventChan)

	err := SendStormToTopic([]byte(`{"data":"storm"}`))
	assert.Error(t, err, "Expected error due to message delivery timeout")

	// Assertions
	mockProducer.AssertExpectations(t)
}
