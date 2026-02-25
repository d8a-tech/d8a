package cmd

import (
	"time"

	"github.com/urfave/cli/v3"
)

type FlagType string

const (
	FlagTypeString   FlagType = "string"
	FlagTypeInt      FlagType = "int"
	FlagTypeBool     FlagType = "bool"
	FlagTypeDuration FlagType = "duration"
)

type FlagSpec struct {
	Name         string
	Usage        string
	EnvVar       string
	ConfigPath   string
	FlagType     FlagType
	DefaultValue any
}

type objectStorageFlagSet struct {
	Type           FlagSpec
	Prefix         FlagSpec
	S3Host         FlagSpec
	S3Port         FlagSpec
	S3Bucket       FlagSpec
	S3AccessKey    FlagSpec
	S3SecretKey    FlagSpec
	S3Region       FlagSpec
	S3Protocol     FlagSpec
	S3CreateBucket FlagSpec
	GCSBucket      FlagSpec
	GCSProject     FlagSpec
	GCSCredsJSON   FlagSpec
}

type objectStorageFlags struct {
	Queue     objectStorageFlagSet
	Warehouse objectStorageFlagSet
}

// nolint:funlen // struct initialization for 13 flags
func createObjectStorageFlagSet(envPrefix, flagPrefix, configPrefix, defaultPrefix string) objectStorageFlagSet {
	return objectStorageFlagSet{
		Type: FlagSpec{
			Name:       flagPrefix + "-type",
			Usage:      envPrefix + " object storage type (s3 or gcs)",
			EnvVar:     envPrefix + "_TYPE",
			ConfigPath: configPrefix + ".type",
			FlagType:   FlagTypeString,
		},
		Prefix: FlagSpec{
			Name:         flagPrefix + "-prefix",
			Usage:        "Object storage prefix/namespace for " + envPrefix + " objects",
			EnvVar:       envPrefix + "_PREFIX",
			ConfigPath:   configPrefix + ".prefix",
			FlagType:     FlagTypeString,
			DefaultValue: defaultPrefix,
		},
		S3Host: FlagSpec{
			Name:       flagPrefix + "-s3-host",
			Usage:      envPrefix + " S3/MinIO host (only used when " + flagPrefix + "-type=s3)",
			EnvVar:     envPrefix + "_S3_HOST",
			ConfigPath: configPrefix + ".s3.host",
			FlagType:   FlagTypeString,
		},
		S3Port: FlagSpec{
			Name:         flagPrefix + "-s3-port",
			Usage:        envPrefix + " S3/MinIO port (only used when " + flagPrefix + "-type=s3)",
			EnvVar:       envPrefix + "_S3_PORT",
			ConfigPath:   configPrefix + ".s3.port",
			FlagType:     FlagTypeInt,
			DefaultValue: 9000,
		},
		S3Bucket: FlagSpec{
			Name:       flagPrefix + "-s3-bucket",
			Usage:      envPrefix + " S3/MinIO bucket name (only used when " + flagPrefix + "-type=s3)",
			EnvVar:     envPrefix + "_S3_BUCKET",
			ConfigPath: configPrefix + ".s3.bucket",
			FlagType:   FlagTypeString,
		},
		S3AccessKey: FlagSpec{
			Name:       flagPrefix + "-s3-access-key",
			Usage:      envPrefix + " S3/MinIO access key (only used when " + flagPrefix + "-type=s3)",
			EnvVar:     envPrefix + "_S3_ACCESS_KEY",
			ConfigPath: configPrefix + ".s3.access_key",
			FlagType:   FlagTypeString,
		},
		S3SecretKey: FlagSpec{
			Name:       flagPrefix + "-s3-secret-key",
			Usage:      envPrefix + " S3/MinIO secret key (only used when " + flagPrefix + "-type=s3)",
			EnvVar:     envPrefix + "_S3_SECRET_KEY",
			ConfigPath: configPrefix + ".s3.secret_key",
			FlagType:   FlagTypeString,
		},
		S3Region: FlagSpec{
			Name:         flagPrefix + "-s3-region",
			Usage:        envPrefix + " S3 region (only used when " + flagPrefix + "-type=s3)",
			EnvVar:       envPrefix + "_S3_REGION",
			ConfigPath:   configPrefix + ".s3.region",
			FlagType:     FlagTypeString,
			DefaultValue: "us-east-1",
		},
		S3Protocol: FlagSpec{
			Name:         flagPrefix + "-s3-protocol",
			Usage:        envPrefix + " S3 endpoint protocol (http or https; only used when " + flagPrefix + "-type=s3)", //nolint:lll // it's a description
			EnvVar:       envPrefix + "_S3_PROTOCOL",
			ConfigPath:   configPrefix + ".s3.protocol",
			FlagType:     FlagTypeString,
			DefaultValue: "http",
		},
		S3CreateBucket: FlagSpec{
			Name:         flagPrefix + "-s3-create-bucket",
			Usage:        envPrefix + ": create bucket on startup if missing (only used when " + flagPrefix + "-type=s3)", //nolint:lll // it's a description
			EnvVar:       envPrefix + "_S3_CREATE_BUCKET",
			ConfigPath:   configPrefix + ".s3.create_bucket",
			FlagType:     FlagTypeBool,
			DefaultValue: false,
		},
		GCSBucket: FlagSpec{
			Name:       flagPrefix + "-gcs-bucket",
			Usage:      envPrefix + " GCS bucket name (only used when " + flagPrefix + "-type=gcs)",
			EnvVar:     envPrefix + "_GCS_BUCKET",
			ConfigPath: configPrefix + ".gcs.bucket",
			FlagType:   FlagTypeString,
		},
		GCSProject: FlagSpec{
			Name:       flagPrefix + "-gcs-project",
			Usage:      envPrefix + " GCS project ID (optional; only used when " + flagPrefix + "-type=gcs)",
			EnvVar:     envPrefix + "_GCS_PROJECT",
			ConfigPath: configPrefix + ".gcs.project",
			FlagType:   FlagTypeString,
		},
		GCSCredsJSON: FlagSpec{
			Name:       flagPrefix + "-gcs-creds-json",
			Usage:      envPrefix + " GCS credentials JSON (raw or base64); empty uses ADC (only used when " + flagPrefix + "-type=gcs)", //nolint:lll // it's a description
			EnvVar:     envPrefix + "_GCS_CREDS_JSON",
			ConfigPath: configPrefix + ".gcs.creds_json",
			FlagType:   FlagTypeString,
		},
	}
}

func ToCliFlags(specs *objectStorageFlagSet) []cli.Flag {
	allSpecs := []FlagSpec{
		specs.Type,
		specs.Prefix,
		specs.S3Host,
		specs.S3Port,
		specs.S3Bucket,
		specs.S3AccessKey,
		specs.S3SecretKey,
		specs.S3Region,
		specs.S3Protocol,
		specs.S3CreateBucket,
		specs.GCSBucket,
		specs.GCSProject,
		specs.GCSCredsJSON,
	}

	flags := make([]cli.Flag, 0, len(allSpecs))
	for _, spec := range allSpecs {
		flags = append(flags, specToCliFlag(&spec))
	}
	return flags
}

func specToCliFlag(spec *FlagSpec) cli.Flag {
	sources := defaultSourceChain(spec.EnvVar, spec.ConfigPath)

	switch spec.FlagType {
	case FlagTypeString:
		flag := &cli.StringFlag{
			Name:    spec.Name,
			Usage:   spec.Usage,
			Sources: sources,
		}
		if spec.DefaultValue != nil {
			if val, ok := spec.DefaultValue.(string); ok {
				flag.Value = val
			}
		}
		return flag

	case FlagTypeInt:
		flag := &cli.IntFlag{
			Name:    spec.Name,
			Usage:   spec.Usage,
			Sources: sources,
		}
		if spec.DefaultValue != nil {
			if val, ok := spec.DefaultValue.(int); ok {
				flag.Value = val
			}
		}
		return flag

	case FlagTypeBool:
		flag := &cli.BoolFlag{
			Name:    spec.Name,
			Usage:   spec.Usage,
			Sources: sources,
		}
		if spec.DefaultValue != nil {
			if val, ok := spec.DefaultValue.(bool); ok {
				flag.Value = val
			}
		}
		return flag

	case FlagTypeDuration:
		flag := &cli.DurationFlag{
			Name:    spec.Name,
			Usage:   spec.Usage,
			Sources: sources,
		}
		if spec.DefaultValue != nil {
			if val, ok := spec.DefaultValue.(time.Duration); ok {
				flag.Value = val
			}
		}
		return flag

	default:
		// Fallback to string flag if unknown type
		return &cli.StringFlag{
			Name:    spec.Name,
			Usage:   spec.Usage,
			Sources: sources,
		}
	}
}

// Different default prefixes: queue uses "d8a/queue", warehouse uses "" (no default)
var objectStorageFlagsSpec = objectStorageFlags{
	Queue: createObjectStorageFlagSet(
		"QUEUE_OBJECT_STORAGE",
		"queue-object-storage",
		"queue.object_storage",
		"d8a/queue",
	),
	Warehouse: createObjectStorageFlagSet(
		"WAREHOUSE_FILES",
		"warehouse-files",
		"warehouse.files",
		"",
	),
}
