package main

import (
	"fmt"
)

func main() {
	bootstrapServers := "localhost:9092"
	topic := "raw-weather-reports"

	err := CreateTopicIfNotExists(bootstrapServers, topic)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Topic created successfully.")
}
