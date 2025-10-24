package cmd

import (
	altsrc "github.com/urfave/cli-altsrc/v3"
	"github.com/urfave/cli-altsrc/v3/yaml"
	"github.com/urfave/cli/v3"
)

var configFile string // Change from *string to string, and remove the func()

// Add a config flag that sets the configFile variable
var configFlag = &cli.StringFlag{
	Name:        "config",
	Aliases:     []string{"c"},
	Value:       "config.yaml",
	Usage:       "Load configuration from `FILE`",
	Destination: &configFile, // This is the key part!
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
