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
		if strings.TrimSpace(cmd.String(queueObjectPrefixFlag.Name)) == "" {
			return fmt.Errorf("%s is required when queue-backend=objectstorage", queueObjectPrefixFlag.Name)
		}
		if strings.TrimSpace(cmd.String(objectStorageTypeFlag.Name)) == "" {
			return fmt.Errorf("%s is required when queue-backend=objectstorage", objectStorageTypeFlag.Name)
		}
		storageType := strings.ToLower(strings.TrimSpace(cmd.String(objectStorageTypeFlag.Name)))
		switch storageType {
		case "s3":
			if strings.TrimSpace(cmd.String(objectStorageS3HostFlag.Name)) == "" {
				return fmt.Errorf("%s is required when object-storage-type=s3", objectStorageS3HostFlag.Name)
			}
			if strings.TrimSpace(cmd.String(objectStorageS3BucketFlag.Name)) == "" {
				return fmt.Errorf("%s is required when object-storage-type=s3", objectStorageS3BucketFlag.Name)
			}
			if strings.TrimSpace(cmd.String(objectStorageS3AccessKeyFlag.Name)) == "" {
				return fmt.Errorf("%s is required when object-storage-type=s3", objectStorageS3AccessKeyFlag.Name)
			}
			if strings.TrimSpace(cmd.String(objectStorageS3SecretKeyFlag.Name)) == "" {
				return fmt.Errorf("%s is required when object-storage-type=s3", objectStorageS3SecretKeyFlag.Name)
			}
			proto := strings.ToLower(strings.TrimSpace(cmd.String(objectStorageS3ProtocolFlag.Name)))
			if proto != "http" && proto != "https" {
				return fmt.Errorf("%s must be http or https", objectStorageS3ProtocolFlag.Name)
			}
		case "gcs":
			if strings.TrimSpace(cmd.String(objectStorageGCSBucketFlag.Name)) == "" {
				return fmt.Errorf("%s is required when object-storage-type=gcs", objectStorageGCSBucketFlag.Name)
			}
		default:
			return fmt.Errorf("unsupported object-storage-type: %s", storageType)
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
