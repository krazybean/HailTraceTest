package schedules

import (
	"time"

	"utils"
)

func RunMidnightTask(task func()) {
	daily := time.NewTicker(24 * time.Hour)
	defer daily.Stop()

	// Run the task for the first time at midnight
	go func() {
		time.Sleep(untilMidnight())
		utils.Logger.Info("Running daily task...")
		task()
	}()

	// Run the task every 24 hours
	for range daily.C {
		utils.Logger.Info("Running daily task...")
		task()
	}
}

func untilMidnight() time.Duration {
	now := time.Now()
	midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	return midnight.Sub(now)
}
