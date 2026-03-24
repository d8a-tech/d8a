package e2e

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

const dockerSharedStoragePath = "/storage"

var (
	dockerImageBuildOnce sync.Once
	dockerImageName      string
	dockerImageBuildErr  error
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

type dockerMount struct {
	sourcePath string
	targetPath string
	readOnly   bool
}

type dockerRunOptions struct {
	name          string
	args          []string
	mounts        []dockerMount
	env           map[string]string
	networkMode   string
	storageVolume string
}

type dockerRunOption func(*dockerRunOptions)

func mountSamePath(path string, readOnly bool) dockerMount {
	return dockerMount{
		sourcePath: path,
		targetPath: path,
		readOnly:   readOnly,
	}
}

func ensureDockerAvailable(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is not installed: %v", err)
	}

	cmd := exec.Command("docker", "version", "--format", "{{.Server.Version}}")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("docker daemon is not available: %v (%s)", err, strings.TrimSpace(string(output)))
	}
}

func dockerImage(t *testing.T) string {
	t.Helper()

	ensureDockerAvailable(t)

	dockerImageBuildOnce.Do(func() {
		dockerImageName = fmt.Sprintf("d8a-e2e:%d", time.Now().UnixNano())

		cmd := exec.Command("docker", "build", "--build-arg", "GO_BUILD_TAGS=e2e", "-t", dockerImageName, ".")
		cmd.Dir = "../.."

		output, err := cmd.CombinedOutput()
		if err != nil {
			dockerImageBuildErr = fmt.Errorf("failed to build docker image: %w\nOutput: %s", err, output)
		}
	})

	if dockerImageBuildErr != nil {
		t.Fatal(dockerImageBuildErr)
	}

	return dockerImageName
}

func uniqueDockerContainerName(t *testing.T) string {
	t.Helper()

	replacer := strings.NewReplacer("/", "-", "_", "-", " ", "-")
	name := strings.ToLower(replacer.Replace(t.Name()))

	return fmt.Sprintf("d8a-e2e-%s-%d", name, time.Now().UnixNano())
}

func createDockerVolume(t *testing.T) string {
	t.Helper()

	volumeName := uniqueDockerContainerName(t) + "-storage"
	cmd := exec.Command("docker", "volume", "create", volumeName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to create docker volume: %v\nOutput: %s", err, output)
	}

	t.Cleanup(func() {
		cleanupCmd := exec.Command("docker", "volume", "rm", "-f", volumeName)
		_, _ = cleanupCmd.CombinedOutput()
	})

	return volumeName
}

func newDockerStorageVolume(t *testing.T) string {
	t.Helper()

	return createDockerVolume(t)
}

func withDockerNetworkMode(networkMode string) dockerRunOption {
	return func(options *dockerRunOptions) {
		options.networkMode = networkMode
	}
}

func withDockerStorageVolume(storageVolume string) dockerRunOption {
	return func(options *dockerRunOptions) {
		options.storageVolume = storageVolume
	}
}

func applyDockerRunOptions(base *dockerRunOptions, extraOptions ...dockerRunOption) dockerRunOptions {
	for _, option := range extraOptions {
		option(base)
	}

	return *base
}

func defaultDockerRunOptions(args []string, configPath string, extraOptions ...dockerRunOption) *dockerRunOptions {
	base := &dockerRunOptions{
		args: args,
		mounts: []dockerMount{
			mountSamePath(configPath, true),
		},
		networkMode: "host",
	}

	configured := applyDockerRunOptions(base, extraOptions...)

	return &configured
}

func defaultDockerServerRunOptions(configPath string, port int, extraOptions ...dockerRunOption) *dockerRunOptions {
	_ = port

	return defaultDockerRunOptions([]string{"server", "--config", configPath}, configPath, extraOptions...)
}

func startDockerProcessInBackground(t *testing.T, options *dockerRunOptions) (*processHandle, error) {
	t.Helper()

	imageName := dockerImage(t)
	containerName := options.name
	if containerName == "" {
		containerName = uniqueDockerContainerName(t)
	}

	networkMode := options.networkMode
	if networkMode == "" {
		networkMode = "host"
	}

	args := []string{"run", "--rm", "--name", containerName, "--network", networkMode}

	storageVolume := options.storageVolume
	if storageVolume == "" {
		storageVolume = newDockerStorageVolume(t)
	}

	args = append(args, "--mount", fmt.Sprintf("type=volume,src=%s,dst=%s", storageVolume, dockerSharedStoragePath))

	mounts := dedupeDockerMounts(options.mounts)
	for _, mount := range mounts {
		mountArg := fmt.Sprintf("type=bind,src=%s,dst=%s", mount.sourcePath, mount.targetPath)
		if mount.readOnly {
			mountArg += ",readonly"
		}

		args = append(args, "--mount", mountArg)
	}

	if len(options.env) > 0 {
		keys := make([]string, 0, len(options.env))
		for key := range options.env {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			args = append(args, "-e", fmt.Sprintf("%s=%s", key, options.env[key]))
		}
	}

	args = append(args, imageName)
	args = append(args, options.args...)

	cmd := exec.Command("docker", args...)

	handle, err := startCommandInBackground(t, cmd, func() {
		cleanupCmd := exec.Command("docker", "rm", "-f", containerName)
		_, _ = cleanupCmd.CombinedOutput()
	})
	if err != nil {
		return nil, err
	}

	handle.name = containerName

	return handle, nil
}

func dedupeDockerMounts(mounts []dockerMount) []dockerMount {
	unique := make([]dockerMount, 0, len(mounts))
	seen := make(map[string]struct{}, len(mounts))

	for _, mount := range mounts {
		key := mount.sourcePath + "|" + mount.targetPath + "|" + fmt.Sprintf("%t", mount.readOnly)
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		unique = append(unique, mount)
	}

	return unique
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
  timeout: 2s

monitoring:
  enabled: false

telemetry:
  url: ""

storage:
  spool_enabled: false

server:
  port: %d

property:
  id: test-property
  name: Test Property
`, port)

	configPath := tmpDir + "/config.yaml"
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
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

func (g *GA4RequestGenerator) QueryString(clientID, eventType, sessionStamp string) string {
	return g.buildParams(clientID, eventType, sessionStamp).Encode()
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
	params.Set("fss", sessionStamp)

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

	// Create config
	configPath := createTempConfig(t, port)
	handle, err := startDockerProcessInBackground(t, defaultDockerServerRunOptions(configPath, port))
	if err != nil {
		t.Fatalf("failed to start server container: %v", err)
	}

	// Wait for server to be ready
	if !waitForServerReady(port, 10*time.Second) {
		t.Fatalf("server did not become ready in time")
	}

	// Run test
	f(runningServer{
		port: port,
		logs: handle.logs,
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

// processHandle encapsulates the state of a running background process
type processHandle struct {
	cmd     *exec.Cmd
	name    string
	logs    *LogCapture
	cleanup func()
}

func (h *processHandle) dockerExec(args ...string) ([]byte, error) {
	if h.name == "" {
		return nil, fmt.Errorf("docker container name is not set")
	}

	commandArgs := append([]string{"exec", h.name}, args...)
	cmd := exec.Command("docker", commandArgs...)

	return cmd.CombinedOutput()
}

func startCommandInBackground(t *testing.T, cmd *exec.Cmd, stop func()) (*processHandle, error) {
	t.Helper()

	// Set up log capture
	logCapture := NewLogCapture()

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
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

	// Start the process
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start process: %w", err)
	}

	// Create cleanup function
	cleanup := func() {
		if stop != nil {
			stop()
		} else if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}

		_ = cmd.Wait()
		wg.Wait()
	}

	// Register cleanup with test
	t.Cleanup(cleanup)

	handle := &processHandle{
		cmd:     cmd,
		logs:    logCapture,
		cleanup: cleanup,
	}

	return handle, nil
}

// testConfigBuilder builds YAML configuration files for multi-process tests
type testConfigBuilder struct {
	port                                   *int
	queueDirectory                         string
	storageDirectory                       string
	warehouse                              string
	sessionTimeout                         time.Duration
	queueBackend                           string
	objectStorageType                      string
	objectStorageS3Host                    string
	objectStorageS3Port                    *int
	objectStorageS3AccessKey               string
	objectStorageS3SecretKey               string
	objectStorageS3Bucket                  string
	objectStorageS3CreateBucket            *bool
	queueObjectPrefix                      string
	queueObjectStorageMinInterval          time.Duration
	queueObjectStorageMaxInterval          time.Duration
	queueObjectStorageIntervalExpFactor    float64
	queueObjectStorageMaxItemsToReadAtOnce int
}

// newTestConfigBuilder creates a new config builder with default values
func newTestConfigBuilder() *testConfigBuilder {
	return &testConfigBuilder{
		warehouse:      "noop",
		sessionTimeout: 2 * time.Second,
		queueBackend:   "filesystem",
	}
}

// WithPort sets the server port (receiver only)
func (b *testConfigBuilder) WithPort(port int) *testConfigBuilder {
	b.port = &port
	return b
}

// WithQueueDirectory sets the queue directory path
func (b *testConfigBuilder) WithQueueDirectory(dir string) *testConfigBuilder {
	b.queueDirectory = dir
	return b
}

// WithStorageDirectory sets the storage directory path
func (b *testConfigBuilder) WithStorageDirectory(dir string) *testConfigBuilder {
	b.storageDirectory = dir
	return b
}

// WithWarehouse sets the warehouse driver
func (b *testConfigBuilder) WithWarehouse(warehouse string) *testConfigBuilder {
	b.warehouse = warehouse
	return b
}

// WithSessionTimeout sets the session timeout
func (b *testConfigBuilder) WithSessionTimeout(timeout time.Duration) *testConfigBuilder {
	b.sessionTimeout = timeout
	return b
}

// WithQueueBackend sets the queue backend (filesystem or objectstorage)
func (b *testConfigBuilder) WithQueueBackend(backend string) *testConfigBuilder {
	b.queueBackend = backend
	return b
}

// WithObjectStorageType sets the object storage type (s3 or gcs)
func (b *testConfigBuilder) WithObjectStorageType(osType string) *testConfigBuilder {
	b.objectStorageType = osType
	return b
}

// WithObjectStorageS3Host sets the S3 host
func (b *testConfigBuilder) WithObjectStorageS3Host(host string) *testConfigBuilder {
	b.objectStorageS3Host = host
	return b
}

// WithObjectStorageS3Port sets the S3 port
func (b *testConfigBuilder) WithObjectStorageS3Port(port int) *testConfigBuilder {
	b.objectStorageS3Port = &port
	return b
}

// WithObjectStorageS3AccessKey sets the S3 access key
func (b *testConfigBuilder) WithObjectStorageS3AccessKey(key string) *testConfigBuilder {
	b.objectStorageS3AccessKey = key
	return b
}

// WithObjectStorageS3SecretKey sets the S3 secret key
func (b *testConfigBuilder) WithObjectStorageS3SecretKey(key string) *testConfigBuilder {
	b.objectStorageS3SecretKey = key
	return b
}

// WithObjectStorageS3Bucket sets the S3 bucket name
func (b *testConfigBuilder) WithObjectStorageS3Bucket(bucket string) *testConfigBuilder {
	b.objectStorageS3Bucket = bucket
	return b
}

// WithObjectStorageS3CreateBucket sets whether to create the bucket on startup
func (b *testConfigBuilder) WithObjectStorageS3CreateBucket(create bool) *testConfigBuilder {
	b.objectStorageS3CreateBucket = &create
	return b
}

// WithQueueObjectPrefix sets the object storage prefix for queue
func (b *testConfigBuilder) WithQueueObjectPrefix(prefix string) *testConfigBuilder {
	b.queueObjectPrefix = prefix
	return b
}

// WithQueueObjectStorageMinInterval sets the minimum polling interval
func (b *testConfigBuilder) WithQueueObjectStorageMinInterval(interval time.Duration) *testConfigBuilder {
	b.queueObjectStorageMinInterval = interval
	return b
}

// WithQueueObjectStorageMaxInterval sets the maximum polling interval
func (b *testConfigBuilder) WithQueueObjectStorageMaxInterval(interval time.Duration) *testConfigBuilder {
	b.queueObjectStorageMaxInterval = interval
	return b
}

// WithQueueObjectStorageIntervalExpFactor sets the exponential backoff factor
func (b *testConfigBuilder) WithQueueObjectStorageIntervalExpFactor(factor float64) *testConfigBuilder {
	b.queueObjectStorageIntervalExpFactor = factor
	return b
}

// WithQueueObjectStorageMaxItemsToReadAtOnce sets the maximum items to read in one batch
func (b *testConfigBuilder) WithQueueObjectStorageMaxItemsToReadAtOnce(maxItems int) *testConfigBuilder {
	b.queueObjectStorageMaxItemsToReadAtOnce = maxItems
	return b
}

// Build writes the config to a temporary file and returns its path
func (b *testConfigBuilder) Build(t *testing.T) string {
	tmpDir, err := os.MkdirTemp("", "d8a-config-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	configContent := b.buildYAML()

	configPath := tmpDir + "/config.yaml"
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})

	return configPath
}

// buildYAML constructs the YAML config content
func (b *testConfigBuilder) buildYAML() string {
	var content strings.Builder

	fmt.Fprintf(&content, "warehouse: %s\n\n", b.warehouse)

	content.WriteString("receiver:\n")
	content.WriteString("  batch_size: 100\n")
	content.WriteString("  batch_timeout: 100ms\n\n")

	fmt.Fprintf(&content, "sessions:\n  timeout: %s\n\n", b.sessionTimeout)

	content.WriteString("monitoring:\n  enabled: false\n\n")
	content.WriteString("telemetry:\n  url: \"\"\n\n")

	content.WriteString("storage:\n")
	if b.storageDirectory != "" {
		fmt.Fprintf(&content, "  bolt_directory: %s/\n", b.storageDirectory)
	}
	if b.queueDirectory != "" && b.queueBackend == "filesystem" {
		fmt.Fprintf(&content, "  queue_directory: %s\n", b.queueDirectory)
	}
	content.WriteString("  spool_enabled: false\n\n")

	// Queue configuration
	if b.queueBackend == "objectstorage" {
		content.WriteString("queue:\n")
		fmt.Fprintf(&content, "  backend: %s\n", b.queueBackend)
		content.WriteString("\n")

		// Object storage configuration (nested under queue)
		content.WriteString("  object_storage:\n")
		if b.queueObjectPrefix != "" {
			fmt.Fprintf(&content, "    prefix: %s\n", b.queueObjectPrefix)
		}
		if b.queueObjectStorageMinInterval > 0 {
			fmt.Fprintf(&content, "    min_interval: %s\n", b.queueObjectStorageMinInterval)
		}
		if b.queueObjectStorageMaxInterval > 0 {
			fmt.Fprintf(&content, "    max_interval: %s\n", b.queueObjectStorageMaxInterval)
		}
		if b.queueObjectStorageIntervalExpFactor > 0 {
			fmt.Fprintf(&content, "    interval_exp_factor: %g\n", b.queueObjectStorageIntervalExpFactor)
		}
		if b.queueObjectStorageMaxItemsToReadAtOnce > 0 {
			fmt.Fprintf(&content, "    max_items_to_read_at_once: %d\n", b.queueObjectStorageMaxItemsToReadAtOnce)
		}
		fmt.Fprintf(&content, "    type: %s\n", b.objectStorageType)
		content.WriteString("    s3:\n")
		if b.objectStorageS3Host != "" {
			fmt.Fprintf(&content, "      host: %s\n", b.objectStorageS3Host)
		}
		if b.objectStorageS3Port != nil {
			fmt.Fprintf(&content, "      port: %d\n", *b.objectStorageS3Port)
		}
		if b.objectStorageS3AccessKey != "" {
			fmt.Fprintf(&content, "      access_key: %s\n", b.objectStorageS3AccessKey)
		}
		if b.objectStorageS3SecretKey != "" {
			fmt.Fprintf(&content, "      secret_key: %s\n", b.objectStorageS3SecretKey)
		}
		if b.objectStorageS3Bucket != "" {
			fmt.Fprintf(&content, "      bucket: %s\n", b.objectStorageS3Bucket)
		}
		if b.objectStorageS3CreateBucket != nil {
			fmt.Fprintf(&content, "      create_bucket: %v\n", *b.objectStorageS3CreateBucket)
		}
		content.WriteString("      region: us-east-1\n")
		content.WriteString("      protocol: http\n\n")
	}

	if b.port != nil {
		fmt.Fprintf(&content, "server:\n  port: %d\n\n", *b.port)
	}

	content.WriteString("property:\n")
	content.WriteString("  id: test-property\n")
	content.WriteString("  name: Test Property\n")

	return content.String()
}

// TestConfigBuilder verifies the test config builder generates valid configs
func TestConfigBuilder(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(*testConfigBuilder) *testConfigBuilder
		expectPort    bool
		expectQueue   bool
		expectStorage bool
	}{
		{
			name: "receiver config with port",
			setup: func(b *testConfigBuilder) *testConfigBuilder {
				return b.WithPort(17000).
					WithQueueDirectory("/tmp/queue").
					WithStorageDirectory("/tmp/storage")
			},
			expectPort:    true,
			expectQueue:   true,
			expectStorage: true,
		},
		{
			name: "worker config without port",
			setup: func(b *testConfigBuilder) *testConfigBuilder {
				return b.WithQueueDirectory("/tmp/queue").
					WithStorageDirectory("/tmp/storage")
			},
			expectPort:    false,
			expectQueue:   true,
			expectStorage: true,
		},
		{
			name: "minimal config",
			setup: func(b *testConfigBuilder) *testConfigBuilder {
				return b
			},
			expectPort:    false,
			expectQueue:   false,
			expectStorage: false,
		},
		{
			name: "objectstorage queue config",
			setup: func(b *testConfigBuilder) *testConfigBuilder {
				return b.WithPort(17000).
					WithStorageDirectory("/tmp/storage").
					WithQueueBackend("objectstorage").
					WithObjectStorageType("s3").
					WithObjectStorageS3Host("localhost").
					WithObjectStorageS3Port(9000).
					WithObjectStorageS3AccessKey("minioadmin").
					WithObjectStorageS3SecretKey("minioadmin").
					WithObjectStorageS3Bucket("test-queue").
					WithObjectStorageS3CreateBucket(true).
					WithQueueObjectPrefix("d8a/queue").
					WithQueueObjectStorageMinInterval(10 * time.Millisecond).
					WithQueueObjectStorageMaxInterval(50 * time.Millisecond).
					WithQueueObjectStorageIntervalExpFactor(1.1).
					WithQueueObjectStorageMaxItemsToReadAtOnce(500)
			},
			expectPort:    true,
			expectQueue:   false,
			expectStorage: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			builder := newTestConfigBuilder()
			builder = tt.setup(builder)

			// when
			configPath := builder.Build(t)

			// then
			content, err := os.ReadFile(configPath)
			if err != nil {
				t.Fatalf("failed to read config file: %v", err)
			}

			configStr := string(content)

			// Verify port field presence
			hasPort := strings.Contains(configStr, "server:\n  port:")
			if hasPort != tt.expectPort {
				t.Errorf("port field: got %v, want %v", hasPort, tt.expectPort)
			}

			// Verify queue directory presence
			hasQueue := strings.Contains(configStr, "queue_directory:")
			if hasQueue != tt.expectQueue {
				t.Errorf("queue_directory field: got %v, want %v", hasQueue, tt.expectQueue)
			}

			// Verify storage directory presence
			hasStorage := strings.Contains(configStr, "bolt_directory:")
			if hasStorage != tt.expectStorage {
				t.Errorf("bolt_directory field: got %v, want %v", hasStorage, tt.expectStorage)
			}

			// Verify required fields are present
			if !strings.Contains(configStr, "warehouse:") {
				t.Error("config should contain warehouse field")
			}
			if !strings.Contains(configStr, "sessions:") {
				t.Error("config should contain sessions field")
			}
			if !strings.Contains(configStr, "property:") {
				t.Error("config should contain property field")
			}

			// For objectstorage tests, verify queue and nested object_storage sections
			if tt.name == "objectstorage queue config" {
				if !strings.Contains(configStr, "queue:") {
					t.Error("objectstorage config should contain queue section")
				}
				if !strings.Contains(configStr, "backend: objectstorage") {
					t.Error("queue section should specify objectstorage backend")
				}
				if !strings.Contains(configStr, "  object_storage:") {
					t.Error("objectstorage config should contain nested object_storage section under queue")
				}
				if !strings.Contains(configStr, "    prefix: d8a/queue") {
					t.Error("object_storage section should contain prefix field")
				}
				if !strings.Contains(configStr, "      host: localhost") {
					t.Error("object_storage.s3 section should contain host")
				}
				if !strings.Contains(configStr, "      port: 9000") {
					t.Error("object_storage.s3 section should contain port")
				}
				if !strings.Contains(configStr, "    min_interval: 10ms") {
					t.Error("object_storage section should contain min_interval")
				}
			}

			// Verify file permissions
			info, err := os.Stat(configPath)
			if err != nil {
				t.Fatalf("failed to stat config file: %v", err)
			}
			if info.Mode().Perm() != 0o600 {
				t.Errorf("config file permissions: got %o, want 0600", info.Mode().Perm())
			}
		})
	}
}

// TestMultiProcessUtilities verifies that the multi-process utilities work correctly
func TestMultiProcessUtilities(t *testing.T) {
	// given
	storageVolume := newDockerStorageVolume(t)

	// Create minimal configs for receiver and worker using the builder
	receiverConfigPath := newTestConfigBuilder().
		WithPort(17999).
		WithQueueDirectory(dockerSharedStoragePath + "/queue").
		WithStorageDirectory(dockerSharedStoragePath).
		Build(t)

	workerConfigPath := newTestConfigBuilder().
		WithQueueDirectory(dockerSharedStoragePath + "/queue").
		WithStorageDirectory(dockerSharedStoragePath).
		Build(t)

	// when - start two processes in background
	process1, err := startDockerProcessInBackground(
		t,
		defaultDockerRunOptions(
			[]string{"receiver", "--config", receiverConfigPath},
			receiverConfigPath,
			withDockerStorageVolume(storageVolume),
		),
	)
	if err != nil {
		t.Fatalf("failed to start process 1: %v", err)
	}

	process2, err := startDockerProcessInBackground(
		t,
		defaultDockerRunOptions(
			[]string{"worker", "--config", workerConfigPath},
			workerConfigPath,
			withDockerStorageVolume(storageVolume),
		),
	)
	if err != nil {
		t.Fatalf("failed to start process 2: %v", err)
	}

	// Give processes time to start and log something
	time.Sleep(2 * time.Second)

	// then - verify processes are running and log capture works
	if process1.cmd.Process == nil {
		t.Fatal("process1 should have a process handle")
	}
	if process2.cmd.Process == nil {
		t.Fatal("process2 should have a process handle")
	}

	// Verify log capture is independent
	if process1.logs == process2.logs {
		t.Fatal("log captures should be independent")
	}

	// Verify logs were captured (each process should log something on startup)
	// Wait for logs to appear with a short timeout
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		p1Lines := process1.logs.GetLines()
		p2Lines := process2.logs.GetLines()
		if len(p1Lines) > 0 && len(p2Lines) > 0 {
			// Success - both processes logged something
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	p1Lines := process1.logs.GetLines()
	p2Lines := process2.logs.GetLines()

	if len(p1Lines) == 0 {
		t.Error("process1 logs should not be empty")
	}
	if len(p2Lines) == 0 {
		t.Error("process2 logs should not be empty")
	}

	// Cleanup will be handled automatically by t.Cleanup()
}
