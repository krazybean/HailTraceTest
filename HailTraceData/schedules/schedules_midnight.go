package schedules

import (
	"time"
	"github.com/sirupsen/logrus"
)

// timeNow is a variable holding a function that returns the current time.
// It defaults to time.Now, but can be overridden in tests.
var timeNow = time.Now

// sleepFunc is a variable holding a function that pauses execution for a given duration.
// It defaults to time.Sleep, but can be overridden in tests.
var sleepFunc = time.Sleep

func RunMidnightTask(task func(), Logger *logrus.Logger) {
	daily := time.NewTicker(24 * time.Hour)
	defer daily.Stop()

	// Run the task for the first time at midnight
	go func() {
		sleepFunc(untilMidnight())  // Use sleepFunc instead of time.Sleep
		Logger.Info("Running daily task...")
		task()
	}()

	// Run the task every 24 hours
	for range daily.C {
		Logger.Info("Running daily task...")
		task()
	}
}

func untilMidnight() time.Duration {
	now := timeNow()
	midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	return midnight.Sub(now)
}
