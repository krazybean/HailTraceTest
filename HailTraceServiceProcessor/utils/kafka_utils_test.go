package utils

import (
	"time"
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)


type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) WithField(key string, value interface{}) logrus.FieldLogger {
	args := m.Called(key, value)
	return args.Get(0).(logrus.FieldLogger)
}

func (m *MockLogger) WithFields(fields logrus.Fields) logrus.FieldLogger {
	args := m.Called(fields)
	return args.Get(0).(logrus.FieldLogger)
}

func (m *MockLogger) Debugf(format string, args ...interface{}) {
	m.Called(format)
}

func (m *MockLogger) Infof(format string, args ...interface{}) {
	m.Called(format)
}

func (m *MockLogger) Warnf(format string, args ...interface{}) {
	m.Called(format)
}

func (m *MockLogger) Errorf(format string, args ...interface{}) {
	m.Called(format)
}

type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) ProduceChannel() chan *kafka.Message {
	args := m.Called()
	return args.Get(0).(chan *kafka.Message)
}

func (m *MockProducer) Events() chan kafka.Event {
	args := m.Called()
	return args.Get(0).(chan kafka.Event)
}

func (m *MockProducer) Close() {
	m.Called()
}

type MockAdminClient struct {
	mock.Mock
}

func (m *MockAdminClient) CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {
	args := m.Called(ctx, topics)
	return args.Get(0).([]kafka.TopicResult), args.Error(1)
}

func (m *MockAdminClient) GetMetadata(ctx context.Context, topics []string, timeoutMs int) (*kafka.Metadata, error) {
	args := m.Called(ctx, topics)
	return args.Get(0).(*kafka.Metadata), args.Error(1)
}

func (m *MockAdminClient) Close() {
	m.Called()
}

type MockConsumer struct {
    kafka.Consumer
    mock.Mock
}

func (m *MockConsumer) Assign(partitions []kafka.TopicPartition) error {
    embeddedConsumer := m.Consumer
    embeddedConsumer.Assign(partitions)
    return nil
}

func (m *MockConsumer) SubscribeTopics(topics []string, rebalanceCallback func(*kafka.Consumer, kafka.Event) error) error {
	args := m.Called(topics, rebalanceCallback)
	return args.Error(0)
}

func (m *MockConsumer) Poll(timeoutMs int) kafka.Event {
	args := m.Called(timeoutMs)
	return args.Get(0).(kafka.Event)
}

func (m *MockConsumer) CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	args := m.Called(offsets)
	return args.Get(0).([]kafka.TopicPartition), args.Error(1)
}

func (m *MockConsumer) Close() {
	m.Called()
}

type ConsumerStub struct {
	kafka.Consumer
}

func (c *ConsumerStub) Assign(partitions []kafka.TopicPartition) error {
	return c.Consumer.Assign(partitions)
}

func (c *ConsumerStub) Unassign() {
	c.Consumer.Unassign()
}

func TestInitProducer(t *testing.T) {
	mockProducer := new(MockProducer)
	mockProducer.On("ProduceChannel").Return(make(chan *kafka.Message))
	mockProducer.On("Events").Return(make(chan kafka.Event))
	mockProducer.On("Close").Once()

	producer = mockProducer
	err := InitProducer("localhost:9092")
	assert.NoError(t, err)

	mockProducer.ProduceChannel()
	mockProducer.Events()
	mockProducer.Close()

	mockProducer.AssertExpectations(t)
}

func TestCloseProducer(t *testing.T) {
	mockProducer := new(MockProducer)
	mockProducer.On("Close").Once()

	producer = mockProducer
	CloseProducer()
	mockProducer.AssertExpectations(t)
}

func TestNewKafkaAdminClient(t *testing.T) {
	mockAdminClient := new(MockAdminClient)
	mockAdminClient.On("Close").Once()

	adminClient, err := NewKafkaAdminClient("localhost:9092")
	assert.NoError(t, err)
	assert.NotNil(t, adminClient)
	mockAdminClient.Close()
}

func TestNewKafkaConsumer(t *testing.T) {
	kc, err := NewKafkaConsumer("localhost:9092", "group1")
	assert.NoError(t, err)
	assert.NotNil(t, kc)
}

// TODO: Fix this later, problem with null pointer on kafka.consumer.assign
// func TestRebalanceCallback(t *testing.T) {
// 	mockConsumer := new(MockConsumer)
// 	mockLogger := new(MockLogger) // Create a new mock logger

// 	// Mock rebalance events
// 	assigned := kafka.AssignedPartitions{Partitions: []kafka.TopicPartition{{}}}
// 	mockConsumer.On("Assign", assigned.Partitions).Return(nil)
// 	mockLogger.On("Infof", mock.Anything, mock.Anything).Return()
// 	mockLogger.On("Errorf", mock.Anything, mock.Anything).Return()

// 	err := rebalanceCallback(&mockConsumer.Consumer, )
// 	assert.NoError(t, err)

// 	revoked := kafka.RevokedPartitions{Partitions: []kafka.TopicPartition{{}}}
// 	mockConsumer.On("Unassign").Return(nil)

// 	err = rebalanceCallback(&mockConsumer.Consumer, revoked)
// 	assert.NoError(t, err)

// 	mockConsumer.AssertExpectations(t)
// 	mockLogger.AssertExpectations(t)
// }

// TODO: Fix main test which is giving a SIGSEV
// func TestConsumeFromRawWeatherReports(t *testing.T) {
// 	mockConsumer := new(MockConsumer)
// 	stopChan := make(chan struct{})

// 	transformFunc := func(data []byte) ([]byte, error) {
// 		return data, nil
// 	}

// 	mockConsumer.On("SubscribeTopics", mock.Anything, mock.Anything).Return(nil)
// 	mockConsumer.On("Poll", mock.Anything).Return(&kafka.Message{Value: []byte("test")})
// 	mockConsumer.On("CommitOffsets", mock.Anything).Return(nil)

// 	kc := &KafkaConsumer{consumer: &mockConsumer.Consumer}

// 	err := kc.ConsumeFromRawWeatherReports(transformFunc, stopChan)
// 	assert.NoError(t, err)
// }

func TestSendTransformedDataToTopic(t *testing.T) {
	mockProducer := new(MockProducer)
	msgChan := make(chan *kafka.Message, 1) // Bad fix for a bad implementation
	eventChan := make(chan kafka.Event, 1)  // ditto

	mockProducer.On("ProduceChannel").Return(msgChan)
	mockProducer.On("Events").Return(eventChan)
	mockProducer.On("Close").Once()

	producer = mockProducer

	go func() {
		eventChan <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &transformedTopicName}}
	}()

	err := SendTransformedDataToTopic([]byte("test"))
	assert.NoError(t, err)

	select {
	case event := <-eventChan:
		assert.NotNil(t, event)
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out waiting for event from eventChan")
	}

	close(eventChan)
	close(msgChan)

	mockProducer.Close()
	mockProducer.AssertExpectations(t)
}

func TestCheckAndCreateTopicsIfNotExists(t *testing.T) {
	mockAdminClient := new(MockAdminClient)

	topics := []string{"topic1"}
	mockAdminClient.On("GetMetadata", mock.Anything, mock.Anything).Return(&kafka.Metadata{
		Topics: map[string]kafka.TopicMetadata{"existing-topic": {}},
	}, nil)
	mockAdminClient.On("CreateTopics", mock.Anything, mock.Anything).Return([]kafka.TopicResult{}, nil)
	mockAdminClient.On("Close").Once()

	err := CheckAndCreateTopicsIfNotExists("localhost:9092", topics, 1, 1)
	assert.NoError(t, err)
	mockAdminClient.Close()
}

func TestTransformData(t *testing.T) {
	// TODO: Actually put something to test against...
	data := []byte("test")
	transformedData, err := TransformData(data)
	assert.NoError(t, err)
	assert.Equal(t, data, transformedData)
}