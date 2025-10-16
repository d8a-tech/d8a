// Package dbip provides utilities to download DB-IP datasets from an OCI registry.
package dbip

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/memory"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
)

// MMDBCityDatabaseDownloader downloads an MMDB city database and returns the saved file path.
type MMDBCityDatabaseDownloader interface {
	// Download obtains the MMDB city database and returns the path to the downloaded file
	Download(ctx context.Context, artifactName, tag, destinationDir string) (string, error)
}

// OCIRegistryCreds contains credentials and settings for accessing an OCI registry.
type OCIRegistryCreds struct {
	User     string
	Password string
	Repo     string

	IgnoreCert bool
}

type onlyOnceDownloader struct {
	downloader MMDBCityDatabaseDownloader
	path       string
	err        error
	once       sync.Once
}

// NewOnlyOnceDownloader creates a new OnlyOnceDownloader
func NewOnlyOnceDownloader(downloader MMDBCityDatabaseDownloader) MMDBCityDatabaseDownloader {
	return &onlyOnceDownloader{downloader: downloader}
}

// Download implements MMDBCityDatabaseDownloader
func (d *onlyOnceDownloader) Download(ctx context.Context, artifactName, tag, destinationDir string) (string, error) {
	d.once.Do(func() {
		path, err := d.downloader.Download(ctx, artifactName, tag, destinationDir)
		if err != nil {
			logrus.WithError(err).Error("failed to download artifact")
			d.err = err
			return
		}
		d.path = path
	})
	return d.path, d.err
}

// Download implements MMDBCityDatabaseDownloader

// ociMMDBDownloader implements MMDBCityDatabaseDownloader using OCI registry
type ociMMDBDownloader struct {
	creds OCIRegistryCreds
}

// NewOCIMMDBDownloader creates a new MMDBCityDatabaseDownloader backed by an OCI registry
func NewOCIMMDBDownloader(creds OCIRegistryCreds) MMDBCityDatabaseDownloader {
	return &ociMMDBDownloader{creds: creds}
}

// Download implements MMDBCityDatabaseDownloader
func (d *ociMMDBDownloader) Download(
	ctx context.Context,
	artifactName,
	tag,
	destinationDir string,
) (string, error) {
	repository, err := d.createRepository(d.creds, artifactName)
	if err != nil {
		return "", fmt.Errorf("failed to create repository: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"repository":  repository.Reference.Registry,
		"tag":         tag,
		"destination": destinationDir,
	}).Info("OCI: validating remote artifact")

	// Get remote descriptor to check the digest
	remoteDesc, err := repository.Resolve(ctx, tag)
	if err != nil {
		return "", fmt.Errorf("failed to resolve remote artifact: %w", err)
	}

	// Prepare destination directory and check stored manifest digest
	destDir := destinationDir
	if err := os.MkdirAll(destDir, 0o750); err != nil { // restrict perms to satisfy gosec
		return "", fmt.Errorf("failed to create destination directory: %w", err)
	}

	digestFilePath := filepath.Join(destDir, ".manifest.digest")
	var mmdbPath string

	// #nosec G304 - digest file path is constructed, not user-controlled traversal
	if b, err := os.ReadFile(digestFilePath); err == nil {
		localDigest := string(b)
		if localDigest == remoteDesc.Digest.String() {
			logrus.WithFields(logrus.Fields{
				"repository":  repository.Reference.Registry,
				"destination": destinationDir,
				"tag":         tag,
				"digest":      remoteDesc.Digest,
			}).Info("OCI: local files are up to date, skipping download")

			// Find existing MMDB file
			entries, err := os.ReadDir(destDir)
			if err != nil {
				return "", fmt.Errorf("failed to read destination directory: %w", err)
			}
			for _, entry := range entries {
				if !entry.IsDir() && filepath.Ext(entry.Name()) == ".mmdb" {
					mmdbPath = filepath.Join(destDir, entry.Name())
					break
				}
			}
			if mmdbPath == "" {
				return "", fmt.Errorf("no .mmdb file found in destination directory")
			}
			return mmdbPath, nil
		}
		logrus.WithFields(logrus.Fields{
			"local_digest":  localDigest,
			"remote_digest": remoteDesc.Digest.String(),
		}).Info("OCI: local manifest digest differs from remote, downloading update")
	}

	// Pull into memory store to avoid writing OCI layout to filesystem
	mem := memory.New()
	desc, err := oras.Copy(ctx, repository, tag, mem, tag, oras.DefaultCopyOptions)
	if err != nil {
		return "", fmt.Errorf("failed to copy artifact: %w", err)
	}

	// Fetch and parse manifest
	rc, err := mem.Fetch(ctx, desc)
	if err != nil {
		return "", fmt.Errorf("failed to fetch manifest: %w", err)
	}
	manifestBytes, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("failed to read manifest bytes: %w", err)
	}
	if err := rc.Close(); err != nil {
		logrus.WithError(err).Warn("failed to close manifest reader")
	}

	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return "", fmt.Errorf("failed to unmarshal manifest: %w", err)
	}

	// Write layers to destination using safe file names
	for _, layer := range manifest.Layers {
		fileName := layer.Annotations[ocispec.AnnotationTitle]
		if fileName == "" {
			// Fallback to digest-based name
			fileName = layer.Digest.Encoded()
		}
		// Ensure we do not allow path traversal from annotations
		safeName := filepath.Base(filepath.Clean(fileName))
		outPath := filepath.Join(destDir, safeName)

		// Track the MMDB file path
		if filepath.Ext(safeName) == ".mmdb" {
			mmdbPath = outPath
		}

		// Fetch blob content
		blobReader, err := mem.Fetch(ctx, layer)
		if err != nil {
			return "", fmt.Errorf("failed to fetch blob %s: %w", layer.Digest, err)
		}
		// Write atomically
		tmpPath := outPath + ".tmp"
		// #nosec G304 - tmpPath is built from a sanitized base file name under destDir
		outFile, err := os.Create(tmpPath)
		if err != nil {
			_ = blobReader.Close()
			return "", fmt.Errorf("failed to create file %s: %w", tmpPath, err)
		}
		if _, err := io.Copy(outFile, blobReader); err != nil {
			_ = blobReader.Close()
			_ = outFile.Close()
			_ = os.Remove(tmpPath)
			return "", fmt.Errorf("failed to write file %s: %w", tmpPath, err)
		}
		if err := blobReader.Close(); err != nil {
			logrus.WithError(err).Warn("failed to close blob reader")
		}
		if err := outFile.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return "", fmt.Errorf("failed to close file %s: %w", tmpPath, err)
		}
		if err := os.Rename(tmpPath, outPath); err != nil {
			_ = os.Remove(tmpPath)
			return "", fmt.Errorf("failed to move temp file into place for %s: %w", outPath, err)
		}
	}

	// Persist manifest digest for future checks
	if err := os.WriteFile(digestFilePath, []byte(desc.Digest.String()), 0o600); err != nil {
		return "", fmt.Errorf("failed to write digest file: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"repository":  repository.Reference.Registry,
		"destination": destinationDir,
		"tag":         tag,
		"digest":      desc.Digest,
	}).Info("OCI: download completed and files saved")

	if mmdbPath == "" {
		return "", fmt.Errorf("no .mmdb file found in downloaded layers")
	}

	return mmdbPath, nil
}

func (d *ociMMDBDownloader) createRepository(creds OCIRegistryCreds, artifactName string) (*remote.Repository, error) {
	repo := fmt.Sprintf("%s/%s", creds.Repo, artifactName)
	repository, err := remote.NewRepository(repo)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize repository: %w", err)
	}
	if creds.User != "" && creds.Password != "" {
		repository.Client = &auth.Client{
			Credential: auth.StaticCredential(repo, auth.Credential{
				Username: creds.User,
				Password: creds.Password,
			}),
		}
	} else {
		// No authentication - use default client
		repository.Client = &auth.Client{}
	}
	return repository, nil
}
