package requests

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/krazybean/HailTraceTest/utils"
)

type URL string

const (
	TORNADO_REPORTS_URL URL = "https://www.spc.noaa.gov/climo/reports/today_raw_torn.csv"
	HAIL_REPORTS_URL    URL = "https://www.spc.noaa.gov/climo/reports/today_raw_hail.csv"
	WIND_REPORTS_URL    URL = "https://www.spc.noaa.gov/climo/reports/today_raw_wind.csv"
)

func main() {
	urls := []URL{TORNADO_REPORTS_URL, HAIL_REPORTS_URL, WIND_REPORTS_URL}

	for _, url := range urls {
		downloadAndParseCSV(string(url))
	}
}

func downloadAndParseCSV(url string) {
	// Download the file
	resp, err := http.Get(url)
	if err != nil {
		utils.Logger.Fatal(err)
	}
	defer resp.Body.Close()

	// Create a temp directory
	tempDir, err := os.MkdirTemp("", "reports")
	if err != nil {
		utils.Logger.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Save the file to the temp directory
	filePath := filepath.Join(tempDir, getFileNameFromURL(url))
	file, err := os.Create(filePath)
	if err != nil {
		utils.Logger.Fatal(err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		utils.Logger.Fatal(err)
	}

	parsers.parseStormCSV(file)

}

func getFileNameFromURL(url string) string {
	// This is a simple implementation, you may need to adjust it based on your needs
	return url[strings.LastIndex(url, "/")+1:]
}
