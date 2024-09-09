package main

import (
	"bufio"
	"fmt"
	"os"

	"parsers"
	"schedules"
	"utils"
	"webrequests"
)

// Task to run at midnight
func runTask() {
	bootstrapServers := "localhost:9092"
	topic := "raw-weather-reports"

	// Ensure Kafka producer is initialized
	err := utils.InitKafkaProducer(bootstrapServers)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Check if the topic exists and create it if needed
	err = utils.CreateTopicIfNotExists(bootstrapServers, topic)
	if err != nil {
		fmt.Println(err)
		return
	}

	config := utils.ReadConfig("config.json")
	utils.Logger.Debugf("Configuration: %+v", config)

	// Process each URL in the configuration
	for i, url := range config.Urls {
		utils.Logger.Debugf("Processing URL %d: %s", i, url)
		filePath, err := webrequests.DownloadStormCSV(url, utils.Logger)
		if err != nil {
			utils.Logger.Errorf("Error downloading %s: %v", url, err)
			continue
		} else {
			// Parse the downloaded CSV file
			parsers.ParseStormCSV(filePath, utils.Logger)
		}
	}

	// Close the Kafka producer if needed (depending on your implementation)
	// utils.CloseKafkaProducer()
}

func main() {
	// Schedule the task to run at midnight
	go schedules.RunMidnightTask(runTask)

	// Allow on-demand execution by reading user input
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("Enter 'run' to execute the task immediately or 'exit' to quit:")
		input, _ := reader.ReadString('\n')

		switch input = input[:len(input)-1]; input { // Trim the newline character
		case "run":
			utils.Logger.Info("Running task on demand...")
			runTask()
		case "exit":
			utils.Logger.Info("Exiting program...")
			return
		default:
			fmt.Println("Invalid input. Please enter 'run' or 'exit'.")
		}
	}
}
