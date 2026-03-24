package cmd

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/urfave/cli/v3"
)

var localfetchPortFlag *cli.IntFlag = &cli.IntFlag{
	Name:     "port",
	Usage:    "Port of the local D8A HTTP server to call",
	Required: true,
}

var localfetchPathFlag *cli.StringFlag = &cli.StringFlag{
	Name:     "path",
	Usage:    "HTTP path to call on the local D8A server",
	Required: true,
}

var localfetchQueryStringFlag *cli.StringFlag = &cli.StringFlag{
	Name:  "query-string",
	Usage: "Raw query string to append to the request URL",
}

func localfetchCommands() []*cli.Command {
	return []*cli.Command{newLocalfetchCommand()}
}

func newLocalfetchCommand() *cli.Command {
	return &cli.Command{
		Name:   "localfetch",
		Usage:  "Send a local HTTP request from inside the container",
		Hidden: true,
		Flags: []cli.Flag{
			localfetchPortFlag,
			localfetchPathFlag,
			localfetchQueryStringFlag,
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			targetURL, err := buildLocalfetchURL(
				cmd.Int(localfetchPortFlag.Name),
				cmd.String(localfetchPathFlag.Name),
				cmd.String(localfetchQueryStringFlag.Name),
			)
			if err != nil {
				return err
			}

			return performLocalfetch(ctx, targetURL)
		},
	}
}

func buildLocalfetchURL(port int, path, queryString string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("path is required")
	}

	if _, err := url.ParseQuery(queryString); err != nil {
		return "", fmt.Errorf("invalid query string: %w", err)
	}

	normalizedPath := "/" + strings.TrimLeft(path, "/")

	return (&url.URL{
		Scheme:   "http",
		Host:     fmt.Sprintf("localhost:%d", port),
		Path:     normalizedPath,
		RawQuery: queryString,
	}).String(), nil
}
