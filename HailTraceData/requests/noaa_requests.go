package requests

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
)

func init() {
	fmt.Println("Simple interest package initialized")
}

func downloadStormCSV(url string, Logger *logrus.Logger) {
	// Download the file
	resp, err := http.Get(url)
	if err != nil {
		Logger.Fatal(err)
	}
	defer resp.Body.Close()

	// Create a temp directory
	tempDir, err := os.MkdirTemp("", "reports")
	if err != nil {
		Logger.Fatal(err)
	}
	// defer os.RemoveAll(tempDir)

	// Save the file to the temp directory
	filePath := filepath.Join(tempDir, getFileNameFromURL(url))
	file, err := os.Create(filePath)
	if err != nil {
		Logger.Fatal(err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		Logger.Fatal(err)
	}

}

func getFileNameFromURL(url string) string {
	// This is a simple implementation, you may need to adjust it based on your needs
	return url[strings.LastIndex(url, "/")+1:]
}
