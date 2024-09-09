package webrequests

import (
	"io"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"

)

func init() {
}

func DownloadStormCSV(url string, Logger *logrus.Logger) (string, error) {
	Logger.Infof("Inside DownloadStormCSV")
	// Download the file
	resp, err := http.Get(url)
	if resp == nil {
		Logger.Errorf("Failed to download file from %s", url)
		Logger.Errorf("Error: %v", err)
		return "", err
	}	
	if resp.StatusCode != http.StatusOK {
		// Log the error message and status code
		Logger.Errorf("Download failed with status code: %d", resp.StatusCode)
		return "", fmt.Errorf("download failed: status code %d", resp.StatusCode) 
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
		return "", err 
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		Logger.Fatal(err)
		return "", err
	}
	return filePath, nil 
}

func getFileNameFromURL(url string) string {
	// Pulling filename from url, since its directly referenced
	return url[strings.LastIndex(url, "/")+1:]
}
