package cmd

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/urfave/cli/v3"
)

type flagInfo struct {
	Name        string
	ConfigKey   string
	EnvVar      string
	Description string
	Type        string
	Default     string
}

var (
	envVarRegex   = regexp.MustCompile(`envVarValueSource\{Key:"([^"]+)"\}`)
	yamlPathRegex = regexp.MustCompile(`yamlValueSource\{[^}]*keyPath:"([^"]+)"[^}]*\}`)
)

func extractFlagInfo(flag cli.Flag) (*flagInfo, error) {
	var name, usage, configKey, envVar, flagType, defaultValue string

	// Type assertion to get flag-specific fields
	switch f := flag.(type) {
	case *cli.BoolFlag:
		name = f.Name
		usage = f.Usage
		flagType = "boolean"
		defaultValue = fmt.Sprintf("%v", f.Value)
		if len(f.Sources.Chain) > 0 {
			configKey, envVar = parseSources(f.Sources)
		}
	case *cli.StringFlag:
		name = f.Name
		usage = f.Usage
		flagType = "string"
		defaultValue = f.Value
		if len(f.Sources.Chain) > 0 {
			configKey, envVar = parseSources(f.Sources)
		}
	case *cli.IntFlag:
		name = f.Name
		usage = f.Usage
		flagType = "integer"
		defaultValue = fmt.Sprintf("%d", f.Value)
		if len(f.Sources.Chain) > 0 {
			configKey, envVar = parseSources(f.Sources)
		}
	case *cli.DurationFlag:
		name = f.Name
		usage = f.Usage
		flagType = "duration"
		defaultValue = f.Value.String()
		if len(f.Sources.Chain) > 0 {
			configKey, envVar = parseSources(f.Sources)
		}
	case *cli.Float64Flag:
		name = f.Name
		usage = f.Usage
		flagType = "float64"
		defaultValue = fmt.Sprintf("%g", f.Value)
		if len(f.Sources.Chain) > 0 {
			configKey, envVar = parseSources(f.Sources)
		}
	default:
		return nil, fmt.Errorf("unsupported flag type: %T", flag)
	}

	return &flagInfo{
		Name:        name,
		ConfigKey:   configKey,
		EnvVar:      envVar,
		Description: usage,
		Type:        flagType,
		Default:     defaultValue,
	}, nil
}

func parseSources(sources cli.ValueSourceChain) (configKey, envVar string) {
	if sources.Chain == nil {
		return "", ""
	}

	for _, source := range sources.Chain {
		if source == nil {
			continue
		}

		goStr := source.GoString()

		// Extract environment variable
		if matches := envVarRegex.FindStringSubmatch(goStr); len(matches) > 1 {
			envVar = matches[1]
		}

		// Extract YAML config key
		if matches := yamlPathRegex.FindStringSubmatch(goStr); len(matches) > 1 {
			configKey = matches[1]
		}
	}

	return configKey, envVar
}

func generateConfigDocs(flags []cli.Flag) (string, error) {
	flagInfos := make([]*flagInfo, 0, len(flags))

	for _, flag := range flags {
		info, err := extractFlagInfo(flag)
		if err != nil {
			return "", fmt.Errorf("failed to extract info for flag: %w", err)
		}
		flagInfos = append(flagInfos, info)
	}

	// Sort by flag name
	sort.Slice(flagInfos, func(i, j int) bool {
		return flagInfos[i].Name < flagInfos[j].Name
	})

	var buf strings.Builder
	buf.WriteString("---\n\n")

	for _, info := range flagInfos {
		fmt.Fprintf(&buf, "### `--%s`\n\n", info.Name)

		if info.Description != "" {
			fmt.Fprintf(&buf, "%s\n\n", info.Description)
		}

		var details []string
		if info.ConfigKey != "" {
			details = append(details, fmt.Sprintf("**Configuration key:** `%s`", info.ConfigKey))
		}
		if info.EnvVar != "" {
			details = append(details, fmt.Sprintf("**Environment variable:** `%s`", info.EnvVar))
		}
		if len(details) > 0 {
			buf.WriteString(strings.Join(details, "  \n"))
			buf.WriteString("\n\n")
		}

		if info.Default != "" && info.Default != "false" && info.Default != "0" {
			fmt.Fprintf(&buf, "**Default:** `%s`\n\n", info.Default)
		}

		buf.WriteString("---\n\n")
	}

	return buf.String(), nil
}
