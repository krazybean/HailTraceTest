package webrequests

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"utils"
	"github.com/sirupsen/logrus"

)

func init() {
}

func DownloadStormCSV(url string, Logger *logrus.Logger) (string, error) {
	utils.Logger.Infof("Inside DownloadStormCSV")
	// Download the file
	resp, err := http.Get(url)
	if err != nil {
		Logger.Fatal(err)
		return "", err // Return an empty string and the error
	}
	defer resp.Body.Close()

	// Create a temp directory
	tempDir, err := os.MkdirTemp("", "reports")
	if err != nil {
		Logger.Fatal(err)
		return "", err // Return an empty string and the error
	}
	// defer os.RemoveAll(tempDir)

	// Save the file to the temp directory
	filePath := filepath.Join(tempDir, getFileNameFromURL(url))
	Logger.Infof("Temp file saved to {%v}", filePath)
	file, err := os.Create(filePath)
	if err != nil {
		Logger.Fatal(err)
		return "", err // Return an empty string and the error
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		Logger.Fatal(err)
		return "", err // Return an empty string and the error
	}
	return filePath, nil // Return the file path and no error
}

func getFileNameFromURL(url string) string {
	// This is a simple implementation, you may need to adjust it based on your needs
	return url[strings.LastIndex(url, "/")+1:]
}
