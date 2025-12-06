package e2e

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// LogCapture captures log lines from stdout/stderr
type LogCapture struct {
	mu    sync.RWMutex
	lines []string
}

// NewLogCapture creates a new log capture
func NewLogCapture() *LogCapture {
	return &LogCapture{
		lines: make([]string, 0),
	}
}

// captureStream reads from stream, captures lines, and optionally writes to output
func (lc *LogCapture) captureStream(stream io.Reader, output io.Writer) {
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		line := scanner.Text()
		lc.mu.Lock()
		lc.lines = append(lc.lines, line)
		lc.mu.Unlock()

		if output != nil {
			_, _ = fmt.Fprintln(output, line)
		}
	}
}

// GetLines returns all captured log lines
func (lc *LogCapture) GetLines() []string {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	linesCopy := make([]string, len(lc.lines))
	copy(linesCopy, lc.lines)
	return linesCopy
}

// HasMessage checks if any captured line contains the given message
func (lc *LogCapture) HasMessage(message string) bool {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	for _, line := range lc.lines {
		if strings.Contains(line, message) {
			return true
		}
	}
	return false
}

// waitFor waits for a specific message to appear in logs within timeout
func (lc *LogCapture) waitFor(message string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if lc.HasMessage(message) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// buildBinary builds the main binary to a temp location
func buildBinary(t *testing.T) string {
	tmpBinary, err := os.CreateTemp("", "d8a-test-*")
	if err != nil {
		t.Fatalf("failed to create temp binary file: %v", err)
	}
	binaryPath := tmpBinary.Name()
	if err := tmpBinary.Close(); err != nil {
		t.Fatalf("failed to close temp binary file: %v", err)
	}

	cmd := exec.Command("go", "build", "-o", binaryPath, "../..")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build binary: %v\nOutput: %s", err, output)
	}

	return binaryPath
}

// createTempConfig creates a temporary config file for testing
func createTempConfig(t *testing.T, port int) string {
	tmpDir, err := os.MkdirTemp("", "d8a-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	configContent := fmt.Sprintf(`warehouse: noop

receiver:
  batch_size: 100
  batch_timeout: 100ms

sessions:
  duration: 2s

monitoring:
  enabled: false

storage:
  bolt_directory: %s/
  queue_directory: %s/queue

server:
  port: %d

property:
  id: test-property
  name: Test Property
`, tmpDir, tmpDir, port)

	configPath := tmpDir + "/config.yaml"
	if err := os.WriteFile(configPath, []byte(configContent), 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	return configPath
}

// GA4RequestGenerator sends GA4 collect requests for testing purposes
type GA4RequestGenerator struct {
	host   string
	port   int
	client *http.Client
}

// NewGA4RequestGenerator creates a new GA4 request generator instance
func NewGA4RequestGenerator(host string, port int) *GA4RequestGenerator {
	return &GA4RequestGenerator{
		host:   host,
		port:   port,
		client: &http.Client{},
	}
}

// Hit sends a GA4 hit with the specified parameters
func (g *GA4RequestGenerator) Hit(clientID, eventType, sessionStamp string) error {
	baseURL := fmt.Sprintf("http://%s:%d/g/collect", g.host, g.port)

	params := g.buildParams(clientID, eventType, sessionStamp)

	req, err := http.NewRequest("POST", baseURL+"?"+params.Encode(), http.NoBody)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	g.addHeaders(req)

	resp, err := g.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logrus.Error(err)
		}
	}()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("request failed with status: %d", resp.StatusCode)
	}

	return nil
}

func (g *GA4RequestGenerator) buildParams( // nolint:funlen // it's a test helper
	clientID, eventType, sessionStamp string,
) url.Values {
	params := url.Values{}

	// Base GA4 parameters
	params.Set("v", "2")
	params.Set("tid", "G-5T0Z13HKP4")
	params.Set("gtm", "45je5580h2v9219555710za200")
	params.Set("_p", "1766755958000")
	params.Set("gcd", "13l3l3l2l1l1")
	params.Set("npa", "1")
	params.Set("dma_cps", "syphamo")
	params.Set("dma", "1")
	params.Set("tag_exp", "101509157~103101750~103101752~103116026~103130495~103130497~"+
		"103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116")
	params.Set("cid", clientID)
	params.Set("ul", "en-us")
	params.Set("sr", "1745x982")
	params.Set("uaa", "x86")
	params.Set("uab", "64")
	params.Set("uafvl", "Not(A%3ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171")
	params.Set("uamb", "0")
	params.Set("uam", "")
	params.Set("uap", "Linux")
	params.Set("uapv", "6.14.4")
	params.Set("uaw", "0")
	params.Set("frm", "0")
	params.Set("pscdl", "noapi")
	params.Set("sid", "1746817858")
	params.Set("sct", "1")
	params.Set("seg", "1")
	params.Set("dl", "https://d8a-tech.github.io/analytics-playground/index.html")
	params.Set("dr", "https://d8a-tech.github.io/analytics-playground/checkout.html")
	params.Set("dt", "Food Shop")
	params.Set("sessionStamp", sessionStamp)

	// Set event-specific parameters
	switch eventType {
	case "page_view":
		params.Set("_eu", "AAAAAAQ")
		params.Set("_s", "1")
		params.Set("en", "page_view")
		params.Set("_ee", "1")
		params.Set("tfd", "565")
	case "scroll":
		params.Set("_eu", "AEAAAAQ")
		params.Set("_s", "2")
		params.Set("en", "scroll")
		params.Set("epn.percent_scrolled", "90")
		params.Set("_et", "10")
		params.Set("tfd", "5567")
	case "user_engagement":
		params.Set("_eu", "AAAAAAQ")
		params.Set("_s", "3")
		params.Set("en", "user_engagement")
		params.Set("_et", "16002")
		params.Set("tfd", "16582")
	}

	return params
}

func (g *GA4RequestGenerator) addHeaders(req *http.Request) {
	req.Header.Set("authority", "region1.google-analytics.com")
	req.Header.Set("accept", "*/*")
	req.Header.Set("accept-language", "en-US,en;q=0.8")
	req.Header.Set("content-length", "0")
	req.Header.Set("origin", "https://d8a-tech.github.io")
	req.Header.Set("priority", "u=1, i")
	req.Header.Set("referer", "https://d8a-tech.github.io/")
	req.Header.Set("sec-ch-ua", `"Not(A:Brand";v="24", "Chromium";v="122"`)
	req.Header.Set("sec-ch-ua-mobile", "?0")
	req.Header.Set("sec-ch-ua-platform", `"Linux"`)
	req.Header.Set("sec-fetch-dest", "empty")
	req.Header.Set("sec-fetch-mode", "no-cors")
	req.Header.Set("sec-fetch-site", "cross-site")
	req.Header.Set("user-agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "+
		"(KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36")
}

// HitSequenceItem represents a single hit in a sequence from the shell script
type HitSequenceItem struct {
	ClientID     string
	EventType    string
	SessionStamp string
	Description  string
	SleepBefore  time.Duration
}

// Replay sends a sequence of hits with optional delays
func (g *GA4RequestGenerator) Replay(sequence []HitSequenceItem) error {
	for _, hit := range sequence {
		time.Sleep(hit.SleepBefore)

		if err := g.Hit(hit.ClientID, hit.EventType, hit.SessionStamp); err != nil {
			return fmt.Errorf("failed to send hit %s: %w", hit.Description, err)
		}
	}

	return nil
}

type runningServer struct {
	port int
	logs *LogCapture
}

func withFullRunningServer(t *testing.T, f func(runningServer)) {
	const port = 17031

	// Build binary
	binaryPath := buildBinary(t)
	t.Cleanup(func() { _ = os.Remove(binaryPath) })

	// Create config
	configPath := createTempConfig(t, port)
	t.Cleanup(func() {
		// Clean up config and associated temp directories
		dir := strings.TrimSuffix(configPath, "/config.yaml")
		_ = os.RemoveAll(dir)
	})

	// Start server subprocess
	cmd := exec.Command(binaryPath, "server", "--config", configPath)

	// Set up log capture
	logCapture := NewLogCapture()

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("failed to create stdout pipe: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("failed to create stderr pipe: %v", err)
	}

	// Start capturing logs in background and pass through to stdout/stderr
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		logCapture.captureStream(stdoutPipe, os.Stdout)
	}()
	go func() {
		defer wg.Done()
		logCapture.captureStream(stderrPipe, os.Stderr)
	}()

	// Start the server
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	// Clean up: kill process and wait for log capture to finish
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		wg.Wait()
	})

	// Wait for server to be ready
	if !waitForServerReady(port, 10*time.Second) {
		t.Fatalf("server did not become ready in time")
	}

	// Run test
	f(runningServer{
		port: port,
		logs: logCapture,
	})
}

// waitForServerReady polls the server until it responds or timeout
func waitForServerReady(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 100 * time.Millisecond}

	for time.Now().Before(deadline) {
		resp, err := client.Get(fmt.Sprintf("http://localhost:%d/healthz", port))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
