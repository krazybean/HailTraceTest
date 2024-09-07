package parsers

import (
	"encoding/csv"
	"encoding/json"
	"os"

	"github.com/araddon/dateparse"
	"github.com/krazybean/HailTraceTest/utils"
)

func parseStormCSV(storm_csv string) {
	file, err := os.Open(storm_csv)
	if err != nil {
		utils.Logger.Error(err)
		return
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read the header row
	header, err := reader.Read()
	if err != nil {
		utils.Logger.Error(err)
		return
	}

	// Print the header
	utils.Logger.Info(header)

	// Read each row of the CSV file
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		// Extract the desired fields
		time := record[0]
		measurement := record[1]
		location := record[2]
		country := record[3]
		state := record[4]
		lat := record[5]
		lon := record[6]
		remarks := record[7]

		// Create a JSON-friendly message body
		body := struct {
			Time        string `json:"time"`
			Measurement string `json:"measurement"`
			Location    string `json:"location"`
			Country     string `json:"country"`
			State       string `json:"state"`
			LAT         string `json:"lat"`
			LON         string `json:"lon"`
			Remarks     string `json:"remarks"`
		}{
			Time:        time,
			Measurement: measurement,
			Location:    location,
			Country:     country,
			State:       state,
			LAT:         lat,
			LON:         lon,
			Remarks:     remarks,
		}

		jsonBody, err := json.Marshal(body)
		if err != nil {
			utils.Logger.Error(err)
		}

		// Parse the time field into a Go time.Time object
		parsedTime, err := dateparse.ParseAny(time)

		if err != nil {
			utils.Logger.Error(err)
			continue
		}

		// Log the extracted data
		utils.Logger.Infof("Time: %v, Location: %v, Remarks: %v", parsedTime, location, remarks)
		utils.Logger.Infof("JSON Body: %s", jsonBody)

		// Send the JSON message to the Kafka topic
		utils.SendStormToTopic(jsonBody)
	}
}
