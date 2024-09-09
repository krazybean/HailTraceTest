package parsers

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

// MockLogger to capture log output
type MockLogger struct {
	*logrus.Logger  // Embedding *logrus.Logger to inherit its methods
	logs []string
}

func NewMockLogger() *MockLogger {
	logger := logrus.New()
	logger.SetOutput(ioutil.Discard) // Disable default output to stdout
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

func TestParseStormCSV(t *testing.T) {
	mockLogger := NewMockLogger()

	tests := []struct {
		name         string
		csvContent   string
		expectDelete bool
		expectedLogs []string
	}{
		{
			name: "Valid CSV with three lines",
			csvContent: `# This is a comment
time,measurement,location,country,state,lat,lon,remarks
2023-09-01T12:00:00Z,Heavy Rain,City A,Country X,State Y,12.34,56.78,Severe flooding`,
			expectDelete: true,
			expectedLogs: []string{"Temp file", "deleted successfully"},
		},
		{
			name: "CSV with missing data on third line",
			csvContent: `# This is a comment
time,measurement,location,country,state,lat,lon,remarks
,,,,,,,`,
			expectDelete: true,
			expectedLogs: []string{"The third line of the CSV is empty"},
		},
		{
			name: "Malformed CSV with incorrect number of fields",
			csvContent: `# This is a comment
time,measurement,location,country,state,lat,lon,remarks
2023-09-01T12:00:00Z,Heavy Rain,City A,Country X`,
			expectDelete: true,
			expectedLogs: []string{"Skipping record with insufficient fields"},
		},
		{
			name: "CSV with insufficient lines",
			csvContent: `# This is a comment
time,measurement,location,country,state,lat,lon,remarks`,
			expectDelete: true,
			expectedLogs: []string{"The CSV file does not contain enough lines to process"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create a temporary CSV file
			tempFile, err := ioutil.TempFile("", "test_storm_*.csv")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tempFile.Name()) // Clean up in case of failure

			// Write test content to the temp file
			if _, err := tempFile.WriteString(test.csvContent); err != nil {
				t.Fatalf("Failed to write to temp file: %v", err)
			}
			tempFile.Close()

			// Call the function with the temp file path and the embedded *logrus.Logger
			ParseStormCSV(tempFile.Name(), mockLogger.Logger)

			// Check if the file has been deleted
			if _, err := os.Stat(tempFile.Name()); !os.IsNotExist(err) && test.expectDelete {
				t.Errorf("Expected file %s to be deleted, but it still exists", tempFile.Name())
			}

			// Check expected log messages
			for _, expectedLog := range test.expectedLogs {
				if !mockLogger.ContainsLog(expectedLog) {
					t.Errorf("Expected log message containing '%s', but it was not found", expectedLog)
				}
			}
		})
	}
}
