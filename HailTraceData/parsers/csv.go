package parsers

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"utils"
	"github.com/araddon/dateparse"
	"github.com/sirupsen/logrus"
)

func ParseStormCSV(storm_csv string, Logger *logrus.Logger) {
	// Open the CSV file
	file, err := os.Open(storm_csv)
	if err != nil {
		utils.Logger.Error(err)
		return
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Set the reader to be more lenient by not enforcing the number of fields per record
	reader.FieldsPerRecord = -1 // This allows variable number of fields in rows

	// Skip the first line (typically a comment or metadata)
	if _, err := reader.Read(); err != nil {
		Logger.Error("Failed to read the first line: ", err)
		return
	}

	// Read the header row (the second line)
	header, err := reader.Read()
	if err != nil {
		Logger.Error("Failed to read the header line: ", err)
		return
	}

	// Print the header
	Logger.Infof("Header: %v", header)

	// Read the third line to check if it is empty
	thirdLine, err := reader.Read()
	if err != nil {
		if errors.Is(err, csv.ErrFieldCount) || errors.Is(err, csv.ErrQuote) {
			Logger.Error("Malformed CSV data or unexpected number of fields.")
		} else if err.Error() == "EOF" {
			Logger.Error("The CSV file does not contain enough lines to process.")
		} else {
			Logger.Error("Error reading third line: ", err)
		}
		deleteFile(storm_csv, Logger)
		return
	}

	// Check if the third line is empty
	if isEmptyRecord(thirdLine) {
		Logger.Error("The third line of the CSV is empty. Exiting processing.")
		// Delete the empty file before returning
		deleteFile(storm_csv, Logger)
		return
	}

	// Process the third line first, then continue reading from the fourth line onward
	processCSVRecord(thirdLine, Logger)

	// Continue processing the rest of the CSV file starting from the fourth line
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break // EOF keeps being a thing
			}
			Logger.Error("Error reading record: ", err)
			continue // skipping past anything that would cause breakage
		}

		// Process each CSV record
		processCSVRecord(record, Logger)
	}

	// Delete the temporary file after processing
	Logger.Infof("CSV file %s processed successfully.", storm_csv)
	deleteFile(storm_csv, Logger)
}

// Helper function to check if a CSV record is empty
func isEmptyRecord(record []string) bool {
	for _, field := range record {
		if strings.TrimSpace(field) != "" {
			return false
		}
	}
	return true
}

func processCSVRecord(record []string, Logger *logrus.Logger) {
	// Ensure there are enough fields in the record to avoid index out of range errors
	if len(record) < 8 {
		Logger.Warnf("Skipping record with insufficient fields: %v", record)
		return
	}

	// Extract the desired fields
	time := record[0]
	measurement := record[1]
	location := record[2]
	county := record[3]
	state := record[4]
	lat := record[5]
	lon := record[6]
	remarks := record[7]

	// Create a JSON-friendly message body
	body := struct {
		Time        string `json:"time"`
		Measurement string `json:"measurement"`
		Location    string `json:"location"`
		County     	string `json:"county"`
		State       string `json:"state"`
		LAT         string `json:"lat"`
		LON         string `json:"lon"`
		Remarks     string `json:"remarks"`
	}{
		Time:        time,
		Measurement: measurement,
		Location:    location,
		County:      county,
		State:       state,
		LAT:         lat,
		LON:         lon,
		Remarks:     remarks,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		Logger.Error(err)
		return
	}

	// Parse the time field into a Go time.Time object
	parsedTime, err := dateparse.ParseAny(time)
	if err != nil {
		Logger.Error(err)
		return
	}

	// Log output when done
	Logger.Infof("Time: %v, Location: %v, Remarks: %v", parsedTime, location, remarks)
	Logger.Infof("JSON Body: %s", jsonBody)

	// Send the JSONBody to the Kafka
	utils.SendStormToTopic(jsonBody)
}

// Helper function to delete a file and log the result
func deleteFile(filePath string, Logger *logrus.Logger) {
	err := os.Remove(filePath)
	if err != nil {
		Logger.Errorf("Failed to delete temp file %s: %v", filePath, err)
	} else {
		Logger.Infof("Temp file %s deleted successfully.", filePath)
	}
}
