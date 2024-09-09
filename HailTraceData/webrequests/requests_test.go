package webrequests_te

import (
        "net/http"
        "net/http/httptest"
        "os"
        "testing"
        "path/filepath"

	"requests"
        "github.com/sirupsen/logrus"
        "github.com/stretchr/testify/assert"
        "github.com/stretchr/testify/mock"
)

// MockLogger defines a mock for the Logger interface
type MockLogger struct {
        mock.Mock
}

func (m *MockLogger) Fire(entry *logrus.Entry) error {
        args := m.Called(entry)
        return args.Error(0)
}

func (m *MockLogger) Levels() []logrus.Level {
        args := m.Called()
        return args.Get(0).([]logrus.Level)
}

func (m *MockLogger) ContainsLog(log string) bool {
        args := m.Called(log)
        return args.Bool(0)
}

func TestDownloadStormCSV(t *testing.T) {
        // Create a mock logger
        mockLogger := &MockLogger{}

        // Create a mock HTTP server
        mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                w.WriteHeader(http.StatusOK)
                _, _ = w.Write([]byte("test,csv,content\nrow1,data1,data2\n"))
        }))
        defer mockServer.Close()

        // Set expectations on the mock logger
        mock.On(mockLogger, "ContainsLog", "Get \"http://invalid-url\":").Return(false) // No log for valid URL

        // Test downloading the CSV file from the mock server
        filePath, err := DownloadStormCSV(mockServer.URL+"/testfile.csv", mockLogger)
        assert.NoError(t, err, "Expected no error when downloading CSV")

        // Check that the file is created
        _, err = os.Stat(filePath)
        assert.NoError(t, err, "Expected file to exist after download")

        // Check the file content
        fileContent, err := os.ReadFile(filePath)
        assert.NoError(t, err, "Expected no error reading the downloaded file")
        assert.Equal(t, "test,csv,content\nrow1,data1,data2\n", string(fileContent))

        // Cleanup the downloaded file
        err = os.RemoveAll(filepath.Dir(filePath))
        assert.NoError(t, err, "Expected no error when deleting the temporary directory")

        // Verify the mock's expectations
        mock.VerifyAll(mockLogger)
}