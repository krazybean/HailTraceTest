package main

import (
	"fmt"

	"github.com/krazybean/HailTraceTest/utils"
)

func main() {
	bootstrapServers := "localhost:9092"
	topic := "raw-weather-reports"

	err := utils.CreateTopicIfNotExists(bootstrapServers, topic)
	if err != nil {
		fmt.Println(err)
		return
	}
}
