package cmd

import (
	altsrc "github.com/urfave/cli-altsrc/v3"
	"github.com/urfave/cli-altsrc/v3/yaml"
	"github.com/urfave/cli/v3"
)

var configFile string

var configFlag = &cli.StringFlag{
	Name:        "config",
	Aliases:     []string{"c"},
	Value:       "config.yaml",
	Usage:       "Load configuration from `FILE`",
	Destination: &configFile,
}

func defaultSourceChain(envVar, yamlPath string) cli.ValueSourceChain {
	return cli.NewValueSourceChain(
		func() cli.ValueSource {
			f := cli.EnvVars(envVar)
			return &f
		}(),
		yaml.YAML(yamlPath, altsrc.NewStringPtrSourcer(&configFile)),
	)
}
