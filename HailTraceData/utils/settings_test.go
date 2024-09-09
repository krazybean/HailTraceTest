package utils

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadConfig(t *testing.T) {
	// Creates a fake config and verifies it has correct JSON (positive test)
	tempFile, err := ioutil.TempFile("", "config_test.json")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	configData := []byte(`{"noaa_urls": ["url1", "url2"]}`)
	_, err = tempFile.Write(configData)
	require.NoError(t, err)
	tempFile.Close()

	config, err := ReadConfig(tempFile.Name())
	assert.NoError(t, err)

	expectedConfig := Config{
		Urls: []string{"url1", "url2"},
	}
	assert.Equal(t, expectedConfig, config)
}

func TestReadConfigInvalidFile(t *testing.T) {
	// This test just makes sure that the file exists and logs if not (Negative Test)
	config, err := ReadConfig("nonexistent.json")
	assert.Error(t, err)

	assert.Equal(t, Config{}, config)
	assert.Contains(t, err.Error(), "config file nonexistent.json does not exist")
}

func TestReadConfigInvalidJSON(t *testing.T) {
	// This test verifys that if the JSON is bad, it will log the error correctly. (Negative Test)
	tempFile, err := ioutil.TempFile("", "config_test_invalid.json")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	invalidConfigData := []byte(`invalid json`)
	_, err = tempFile.Write(invalidConfigData)
	require.NoError(t, err)
	tempFile.Close()

	config, err := ReadConfig(tempFile.Name())
	assert.Error(t, err)
	assert.Equal(t, Config{}, config)

	assert.Contains(t, err.Error(), "invalid character 'i' looking for beginning of value")
}