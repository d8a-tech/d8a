package cmd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v3"
)

func TestValidateWarehouseFilesFlags(t *testing.T) {
	tests := []struct {
		name        string
		template    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid default template",
			template:    "table={{.Table}}/schema={{.Schema}}/dt={{.Year}}/{{.MonthPadded}}/{{.DayPadded}}/{{.SegmentID}}.{{.Extension}}", //nolint:lll // test data
			expectError: false,
		},
		{
			name:        "valid hive-style template",
			template:    "table={{.Table}}/year={{.Year}}/month={{.MonthPadded}}/day={{.DayPadded}}/{{.SegmentID}}.{{.Extension}}", //nolint:lll // test data
			expectError: false,
		},
		{
			name:        "valid flat template",
			template:    "{{.Table}}_{{.Year}}{{.MonthPadded}}{{.DayPadded}}_{{.SegmentID}}.{{.Extension}}",
			expectError: false,
		},
		{
			name:        "empty template",
			template:    "",
			expectError: true,
			errorMsg:    "warehouse-files-path-template cannot be empty",
		},
		{
			name:        "whitespace only template",
			template:    "   ",
			expectError: true,
			errorMsg:    "warehouse-files-path-template cannot be empty",
		},
		{
			name:        "invalid template syntax",
			template:    "{{.Table}}/{{.InvalidField",
			expectError: true,
			errorMsg:    "invalid warehouse-files-path-template",
		},
		{
			name:        "template with hardcoded path traversal",
			template:    "{{.Table}}/../{{.SegmentID}}.{{.Extension}}",
			expectError: true,
			errorMsg:    "path traversal (..) which is not allowed",
		},
		{
			name:        "template execution error - undefined field",
			template:    "{{.Table}}/{{.NonExistentField}}/{{.SegmentID}}.{{.Extension}}",
			expectError: true,
			errorMsg:    "failed to execute warehouse-files-path-template",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			app := &cli.Command{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  warehouseFilesPathTemplateFlag.Name,
						Value: tt.template,
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					// when
					err := validateWarehouseFilesFlags(cmd)

					// then
					if tt.expectError {
						assert.Error(t, err)
						if tt.errorMsg != "" {
							assert.Contains(t, err.Error(), tt.errorMsg)
						}
					} else {
						assert.NoError(t, err)
					}
					return nil
				},
			}

			// Execute the command to trigger validation
			_ = app.Run(context.Background(), []string{"test"})
		})
	}
}
