//go:build e2e

package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
)

func performLocalfetch(ctx context.Context, targetURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, http.NoBody)
	if err != nil {
		return fmt.Errorf("failed to build local request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute local request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
		return fmt.Errorf("failed to copy response body: %w", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("local request failed with status %d", resp.StatusCode)
	}

	return nil
}
