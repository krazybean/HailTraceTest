package main

import (
	"fmt"

	"github.com/krazybean/HailTraceTest/utils"
)

func main() {
	bootstrapServers := "localhost:9092"
	topic := "raw-weather-reports"

	// Check kafka topic to ensure it exists, if not create it
	err := utils.CreateTopicIfNotExists(bootstrapServers, topic)
	if err != nil {
		fmt.Println(err)
		return
	}

	config := utils.ReadConfig("config.json")
	for _, url := range config.Urls {
		requests.downloadStormCSV(url, utils.Logger)
	}

}
