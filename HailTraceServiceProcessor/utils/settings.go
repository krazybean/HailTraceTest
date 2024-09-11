package utils

import (
	"os"
	"fmt"
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	Urls []string `json:"noaa_urls"`
}

func ReadConfig(configFile string) (Config, error) {
    // This sets up scoped config variables for unified focal point of changes.
    var config Config

    // Check if the file exists
    _, err := os.Stat(configFile)
    if err != nil {
        if os.IsNotExist(err) {
            return config, fmt.Errorf("config file %s does not exist", configFile)
        }
        return config, err
    }

    file, err := ioutil.ReadFile(configFile)
    if err != nil {
        return config, err
    }

    err = json.Unmarshal(file, &config)
    if err != nil {
        return config, err
    }

    return config, nil
}
