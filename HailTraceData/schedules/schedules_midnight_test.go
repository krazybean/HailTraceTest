package schedules

import (
	"testing"
	"time"
	"strings"
	"io/ioutil"
	"sync"
	"github.com/sirupsen/logrus"
)

type MockLogger struct {
	*logrus.Logger
	logs []string
}

func NewMockLogger() *MockLogger {
	logger := logrus.New()
	logger.SetOutput(ioutil.Discard)
	mockLogger := &MockLogger{
		Logger: logger,
		logs:   []string{},
	}

	// Replace default hooks to capture logs
	mockLogger.SetOutput(ioutil.Discard)
	mockLogger.Hooks.Add(mockLogger)

	return mockLogger
}

func (m *MockLogger) Fire(entry *logrus.Entry) error {
	m.logs = append(m.logs, entry.Message)
	return nil
}

func (m *MockLogger) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (m *MockLogger) ContainsLog(log string) bool {
	for _, l := range m.logs {
		if strings.Contains(l, log) {
			return true
		}
	}
	return false
}

func TestRunMidnightTask_ImmediateRunAndSchedule(t *testing.T) {
	// These tests got really nasty, we have to mock time then set the task
	// then fake sleep and override the new timeNow and sleepFunc
	mockLogger := NewMockLogger()

	// Create a WaitGroup to synchronize task execution
	var wg sync.WaitGroup
	wg.Add(1)

	// Fake task to be executed
	fakeTask := func() {
		wg.Done()
	}

	// Override timeNow to return the current time
	originalTimeNow := timeNow
	timeNow = func() time.Time { return time.Now() }
	defer func() { timeNow = originalTimeNow }()

	// Override sleepFunc to avoid actually sleeping
	originalSleepFunc := sleepFunc
	sleepFunc = func(d time.Duration) {}
	defer func() { sleepFunc = originalSleepFunc }()

	go RunMidnightTask(fakeTask, mockLogger.Logger)

	wg.Wait()

	// Assert expected log message
	if !mockLogger.ContainsLog("Running daily task...") {
		t.Errorf("Expected log message 'Running daily task...' not found")
	}
}

func TestRunMidnightTask_MidnightRun(t *testing.T) {
	mockLogger := NewMockLogger()

	var wg sync.WaitGroup
	wg.Add(1)

	fakeTask := func() {
		wg.Done()
	}

	// Set the current time to be just before midnight
	specificTime := time.Date(2024, 9, 9, 23, 59, 59, 0, time.UTC)

	// Override timeNow
	originalTimeNow := timeNow
	timeNow = func() time.Time { return specificTime }
	defer func() { timeNow = originalTimeNow }()

	// Override sleepFunc
	originalSleepFunc := sleepFunc
	sleepFunc = func(d time.Duration) {}
	defer func() { sleepFunc = originalSleepFunc }()

	// Run the function
	go RunMidnightTask(fakeTask, mockLogger.Logger)

	wg.Wait()

	// Assert expected log message
	if !mockLogger.ContainsLog("Running daily task...") {
		t.Errorf("Expected log message 'Running daily task...' not found")
	}
}

func TestRunMidnightTask_PastMidnightRun(t *testing.T) {
	mockLogger := NewMockLogger()

	var wg sync.WaitGroup
	wg.Add(1)

	// Fake task
	fakeTask := func() {
		wg.Done()
	}

	// Set the current time to be just after midnight
	specificTime := time.Date(2024, 9, 10, 0, 0, 1, 0, time.UTC)

	// Override timeNow
	originalTimeNow := timeNow
	timeNow = func() time.Time { return specificTime }
	defer func() { timeNow = originalTimeNow }()

	// Override sleepFunc
	originalSleepFunc := sleepFunc
	sleepFunc = func(d time.Duration) {}
	defer func() { sleepFunc = originalSleepFunc }()

	// Run the function
	go RunMidnightTask(fakeTask, mockLogger.Logger)

	wg.Wait()

	// Assertions
	if !mockLogger.ContainsLog("Running daily task...") {
		t.Errorf("Expected log message 'Running daily task...' not found")
	}
}
