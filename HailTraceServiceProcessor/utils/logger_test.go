package utils

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func TestLoggerInitialization(t *testing.T) {
	if Logger == nil {
		t.Errorf("Expected logger to be initialized, but got nil")
	}
}

func TestLoggerLevel(t *testing.T) {
	expectedLevel := logrus.DebugLevel
	if Logger.Level != expectedLevel {
		t.Errorf("Expected logger level %v, but got %v", expectedLevel, Logger.Level)
	}
}

func TestLoggerFormatter(t *testing.T) {
	_, ok := Logger.Formatter.(*logrus.TextFormatter)
	if !ok {
		t.Errorf("Expected logger formatter to be of type *logrus.TextFormatter, but got %T", Logger.Formatter)
	}
}