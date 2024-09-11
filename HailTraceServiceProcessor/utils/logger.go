package utils

import (
	"github.com/sirupsen/logrus"
)

var Logger *logrus.Logger

func init() {
	// Simple logger with default settings
	Logger = logrus.New()
	Logger.SetLevel(logrus.DebugLevel)
	Logger.SetFormatter(&logrus.TextFormatter{})
}
