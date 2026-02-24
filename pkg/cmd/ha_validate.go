package cmd

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

func validateHAFlags(mode string, cmd *cli.Command) error {
	backend := strings.ToLower(cmd.String(queueBackendFlag.Name))
	if backend == "" {
		backend = queueBackendFilesystem
	}

	// Warn if using filesystem queue in receiver or worker mode (multi-machine deployments)
	if backend == queueBackendFilesystem && (mode == "receiver" || mode == "worker") {
		logrus.Warn("Filesystem queue backend is not suitable for multi-machine deployments. " +
			"Receiver and worker modes are designed to run on separate machines, " +
			"but filesystem queues cannot be shared across machines. " +
			"Consider using --queue-backend=objectstorage for production deployments.")
	}

	switch backend {
	case queueBackendFilesystem:
		if strings.TrimSpace(cmd.String(storageQueueDirectoryFlag.Name)) == "" {
			return fmt.Errorf("%s is required when queue-backend=filesystem", storageQueueDirectoryFlag.Name)
		}
	case "objectstorage":
		if strings.TrimSpace(cmd.String(ObjectStorageFlagsSpec.Queue.Prefix.Name)) == "" {
			return fmt.Errorf("%s is required when queue-backend=objectstorage", ObjectStorageFlagsSpec.Queue.Prefix.Name)
		}
		if strings.TrimSpace(cmd.String(ObjectStorageFlagsSpec.Queue.Type.Name)) == "" {
			return fmt.Errorf("%s is required when queue-backend=objectstorage", ObjectStorageFlagsSpec.Queue.Type.Name)
		}
		storageType := strings.ToLower(strings.TrimSpace(cmd.String(ObjectStorageFlagsSpec.Queue.Type.Name)))
		switch storageType {
		case "s3":
			if strings.TrimSpace(cmd.String(ObjectStorageFlagsSpec.Queue.S3Host.Name)) == "" {
				return fmt.Errorf("%s is required when queue-object-storage-type=s3", ObjectStorageFlagsSpec.Queue.S3Host.Name)
			}
			if strings.TrimSpace(cmd.String(ObjectStorageFlagsSpec.Queue.S3Bucket.Name)) == "" {
				return fmt.Errorf("%s is required when queue-object-storage-type=s3", ObjectStorageFlagsSpec.Queue.S3Bucket.Name)
			}
			if strings.TrimSpace(cmd.String(ObjectStorageFlagsSpec.Queue.S3AccessKey.Name)) == "" {
				return fmt.Errorf("%s is required when queue-object-storage-type=s3", ObjectStorageFlagsSpec.Queue.S3AccessKey.Name)
			}
			if strings.TrimSpace(cmd.String(ObjectStorageFlagsSpec.Queue.S3SecretKey.Name)) == "" {
				return fmt.Errorf("%s is required when queue-object-storage-type=s3", ObjectStorageFlagsSpec.Queue.S3SecretKey.Name)
			}
			proto := strings.ToLower(strings.TrimSpace(cmd.String(ObjectStorageFlagsSpec.Queue.S3Protocol.Name)))
			if proto != "http" && proto != "https" {
				return fmt.Errorf("%s must be http or https", ObjectStorageFlagsSpec.Queue.S3Protocol.Name)
			}
		case "gcs":
			if strings.TrimSpace(cmd.String(ObjectStorageFlagsSpec.Queue.GCSBucket.Name)) == "" {
				return fmt.Errorf("%s is required when queue-object-storage-type=gcs", ObjectStorageFlagsSpec.Queue.GCSBucket.Name)
			}
		default:
			return fmt.Errorf("unsupported queue-object-storage-type: %s", storageType)
		}
	default:
		return fmt.Errorf("unsupported queue-backend: %s", backend)
	}

	if mode == "worker" {
		if strings.TrimSpace(cmd.String(storageBoltDirectoryFlag.Name)) == "" {
			return fmt.Errorf("%s is required for worker mode", storageBoltDirectoryFlag.Name)
		}
	}

	return nil
}
