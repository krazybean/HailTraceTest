package utils

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

type Config struct {
	Urls []string `json:"urls"`
}

func ReadConfig(configFile string) Config {
	var config Config
	file, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(file, &config)
	if err != nil {
		log.Fatal(err)
	}
	return config
}
