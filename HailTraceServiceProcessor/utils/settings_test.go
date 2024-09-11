package utils

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
)

func TestReadConfig_FileNotFound(t *testing.T) {
	configFile := "nonexistent_config.json"

	_, err := ReadConfig(configFile)
	if err == nil {
		t.Errorf("Expected error when reading nonexistent config file, got nil")
	}
}

func TestReadConfig_InvalidJSON(t *testing.T) {
	configFile := "invalid_config.json"

	err := ioutil.WriteFile(configFile, []byte("{invalid-json}"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}
	defer os.Remove(configFile)

	_, err = ReadConfig(configFile)
	if err == nil {
		t.Errorf("Expected error when reading invalid JSON, got nil")
	}
}

func TestReadConfig_Success(t *testing.T) {
	configFile := "valid_config.json"

	validConfig := Config{
		// Havent added anything just yet
	}

	configData, err := json.Marshal(validConfig)
	if err != nil {
		t.Fatalf("Failed to marshal config to JSON: %v", err)
	}

	err = ioutil.WriteFile(configFile, configData, 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}
	defer os.Remove(configFile)

	config, err := ReadConfig(configFile)
	if err != nil {
		t.Errorf("Error reading valid config: %v", err)
	}

	if config != validConfig {
		t.Errorf("Expected config %+v, got %+v", validConfig, config)
	}
}

func TestReadConfig_FileReadError(t *testing.T) {
	configFile := "readonly_config.json"

	err := ioutil.WriteFile(configFile, []byte("{}"), 0444)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}
	defer os.Remove(configFile)

	_, err = ReadConfig(configFile)
	if err != nil {
		t.Errorf("Error reading config file that should be readable: %v", err)
	}
}