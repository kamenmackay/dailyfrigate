package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
)

func downloadClip(url string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Send HTTP request
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error fetching URL %s: %v\n", url, err)
		return
	}
	defer resp.Body.Close()

	// Read response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response body for URL %s: %v\n", url, err)
		return
	}

	// Run ffmpeg command to save MP4 clip to file
	cmd := exec.Command("ffmpeg", "-movflags", "frag_keyframe+empty_moov", "-i", "pipe:0", "-c", "copy", "-y", "clip.mp4")
	cmd.Stdin = strings.NewReader(string(body))
	err = cmd.Run()
	if err != nil {
		fmt.Printf("Error running ffmpeg command for URL %s: %v\n", url, err)
		return
	}

	fmt.Printf("Clip downloaded from URL %s\n", url)
}

func main() {
	// Get URLs from command-line arguments
	urls := os.Args[1:]

	// Download each clip concurrently
	var wg sync.WaitGroup
	for _, url := range urls {
		wg.Add(1)
		go downloadClip(url, &wg)
	}
	wg.Wait()
}
