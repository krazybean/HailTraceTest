package webrequests

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Mock Logger
type MockLoggerHook struct {
	logs []string
}

func (hook *MockLoggerHook) Fire(entry *logrus.Entry) error {
	hook.logs = append(hook.logs, entry.Message)
	return nil
}

func (hook *MockLoggerHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func NewMockLogger() (*logrus.Logger, *MockLoggerHook) {
	logger := logrus.New()
	hook := &MockLoggerHook{}
	logger.AddHook(hook)
	logger.SetOutput(io.Discard)
	return logger, hook
}

func TestDownloadStormCSV(t *testing.T) {
	// Mock Logger setup
	logger, hook := NewMockLogger()

	// Mock HTTP server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("test,csv,content\nrow1,data1,data2\n"))
	}))
	defer mockServer.Close()

	// Test downloading the CSV file from the mock server
	filePath, err := DownloadStormCSV(mockServer.URL+"/testfile.csv", logger)
	assert.NoError(t, err, "Expected no error when downloading CSV")

	// Verify file exists
	_, err = os.Stat(filePath)
	assert.NoError(t, err, "Expected file to exist after download")

	// Verify Contents
	fileContent, err := os.ReadFile(filePath)
	assert.NoError(t, err, "Expected no error reading the downloaded file")
	assert.Equal(t, "test,csv,content\nrow1,data1,data2\n", string(fileContent))

	// File Cleanup
	err = os.RemoveAll(filepath.Dir(filePath))
	assert.NoError(t, err, "Expected no error when deleting the temporary directory")

	// Ensure no errors in output
	for _, log := range hook.logs {
		assert.NotContains(t, log, "Get \"http://invalid-url\":", "Unexpected log entry found")
	}
}

func TestDownloadStormCSVInvalidURL(t *testing.T) {
    // Mock the logger
    logger, hook := NewMockLogger()

    // Test downloading from an invalid URL
    _, err := DownloadStormCSV("http://invalid-url", logger)
    assert.Error(t, err, "Expected an error when downloading from an invalid URL")

    // Check and match error response
    found := false
    for _, log := range hook.logs {
        if strings.Contains(log, "Error: Get \"http://invalid-url\"") {
            found = true
            break
        }
    }
    assert.True(t, found, "Expected log for invalid URL not found")
}