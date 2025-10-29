// Package cmd provides command line interface for tracker-api
package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/d8a-tech/d8a/pkg/debugger"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

const debuggerPort = 8090

// openBrowser attempts to open the URL in the default web browser
func openBrowser(url string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start"}
	case "darwin":
		cmd = "open"
	default: // "linux", "freebsd", "openbsd", "netbsd"
		cmd = "xdg-open"
	}
	args = append(args, url)
	return exec.Command(cmd, args...).Start() //nolint:gosec // Controlled browser opening command
}

// validateAndNormalizeURL validates and normalizes the provided URL format
func validateAndNormalizeURL(rawURL string) (string, error) {
	if rawURL == "" {
		return "", fmt.Errorf("URL cannot be empty")
	}

	// Add scheme if not present
	if len(rawURL) < 4 || rawURL[:4] != "http" {
		rawURL = "https://" + rawURL
	}

	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL format: %w", err)
	}

	if parsedURL.Host == "" {
		return "", fmt.Errorf("URL must have a valid host")
	}

	return rawURL, nil
}

// createRawlogDebuggerCommand creates the rawlog-debugger CLI command
func createRawlogDebuggerCommand() *cli.Command {
	return &cli.Command{
		Name:  "rawlog-debugger",
		Usage: "Start rawlog debugger web interface",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "url",
				Usage:    "Base URL to the rawlog storage (e.g., horhezio.d8astage.xyz)",
				Required: true,
				Sources:  cli.EnvVars("RAWLOG_STORAGE_URL"),
			},
		},
		Action: func(cmdCtx context.Context, cmd *cli.Command) error {
			ctx, cancel := context.WithCancel(cmdCtx)
			defer cancel()

			// Validate and normalize URL parameter
			rawStorageURL := cmd.String("url")
			storageURL, err := validateAndNormalizeURL(rawStorageURL)
			if err != nil {
				return fmt.Errorf("invalid storage URL: %w", err)
			}

			logrus.Infof("Starting rawlog debugger with storage URL: %s", storageURL)
			logrus.Infof("Web interface will be available at: http://localhost:%d/debugger", debuggerPort)

			// Set up signal handling for graceful shutdown
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigChan
				logrus.Info("Received shutdown signal, initiating graceful shutdown...")
				cancel()
			}()

			// Start the debugger server
			server := debugger.NewServer(debuggerPort, storageURL)
			serverErrChan := make(chan error, 1)
			go func() {
				if err := server.Start(ctx); err != nil {
					serverErrChan <- fmt.Errorf("server error: %w", err)
				} else {
					serverErrChan <- nil
				}
			}()

			// Give server a moment to start before opening browser
			time.Sleep(100 * time.Millisecond)

			// Open browser to debugger interface
			debuggerURL := fmt.Sprintf("http://localhost:%d/debugger", debuggerPort)
			if err := openBrowser(debuggerURL); err != nil {
				logrus.Warnf("Failed to open browser automatically: %v", err)
				logrus.Infof("Please open your browser manually and navigate to: %s", debuggerURL)
			} else {
				logrus.Infof("Opening browser to: %s", debuggerURL)
			}

			// Wait for server to finish
			return <-serverErrChan
		},
	}
}
