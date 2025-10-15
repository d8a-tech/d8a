// Package dbip provides utilities to download DB-IP datasets from an OCI registry.
package dbip

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/memory"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
)

// OCIRegistryCreds contains credentials and settings for accessing an OCI registry.
type OCIRegistryCreds struct {
	User     string
	Password string
	Repo     string

	IgnoreCert bool
}

// DownloadDBIPCityLite downloads the DB-IP GeoLite2 City database from GitHub OCI registry
func DownloadDBIPCityLite(ctx context.Context, creds OCIRegistryCreds, artifactName, tag, destination string) error {

	// Construct the repository URL
	repo := fmt.Sprintf("%s/%s", creds.Repo, artifactName)

	// Initialize the remote repository
	repository, err := remote.NewRepository(repo)
	if err != nil {
		return fmt.Errorf("failed to initialize repository: %w", err)
	}

	// Set up authentication if credentials are provided
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

	logrus.WithFields(logrus.Fields{
		"repository":  repo,
		"tag":         tag,
		"destination": destination,
	}).Info("starting download")

	// Get remote descriptor to check the digest
	remoteDesc, err := repository.Resolve(ctx, tag)
	if err != nil {
		return fmt.Errorf("failed to resolve remote artifact: %w", err)
	}

	// Prepare destination directory and check stored manifest digest
	destDir := destination
	if err := os.MkdirAll(destDir, 0o750); err != nil { // restrict perms to satisfy gosec
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	digestFilePath := filepath.Join(destDir, ".manifest.digest")
	// #nosec G304 - digest file path is constructed, not user-controlled traversal
	if b, err := os.ReadFile(digestFilePath); err == nil {
		localDigest := string(b)
		if localDigest == remoteDesc.Digest.String() {
			logrus.WithFields(logrus.Fields{
				"repository":  repo,
				"destination": destination,
				"tag":         tag,
				"digest":      remoteDesc.Digest,
			}).Info("local files are up to date, skipping download")
			return nil
		}
		logrus.WithFields(logrus.Fields{
			"local_digest":  localDigest,
			"remote_digest": remoteDesc.Digest.String(),
		}).Info("local manifest digest differs from remote, downloading update")
	}

	// Pull into memory store to avoid writing OCI layout to filesystem
	mem := memory.New()
	desc, err := oras.Copy(ctx, repository, tag, mem, tag, oras.DefaultCopyOptions)
	if err != nil {
		return fmt.Errorf("failed to copy artifact: %w", err)
	}

	// Fetch and parse manifest
	rc, err := mem.Fetch(ctx, desc)
	if err != nil {
		return fmt.Errorf("failed to fetch manifest: %w", err)
	}
	manifestBytes, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to read manifest bytes: %w", err)
	}
	if err := rc.Close(); err != nil {
		logrus.WithError(err).Warn("failed to close manifest reader")
	}

	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("failed to unmarshal manifest: %w", err)
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

		// Fetch blob content
		blobReader, err := mem.Fetch(ctx, layer)
		if err != nil {
			return fmt.Errorf("failed to fetch blob %s: %w", layer.Digest, err)
		}
		// Write atomically
		tmpPath := outPath + ".tmp"
		// #nosec G304 - tmpPath is built from a sanitized base file name under destDir
		outFile, err := os.Create(tmpPath)
		if err != nil {
			_ = blobReader.Close()
			return fmt.Errorf("failed to create file %s: %w", tmpPath, err)
		}
		if _, err := io.Copy(outFile, blobReader); err != nil {
			_ = blobReader.Close()
			_ = outFile.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("failed to write file %s: %w", tmpPath, err)
		}
		if err := blobReader.Close(); err != nil {
			logrus.WithError(err).Warn("failed to close blob reader")
		}
		if err := outFile.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("failed to close file %s: %w", tmpPath, err)
		}
		if err := os.Rename(tmpPath, outPath); err != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("failed to move temp file into place for %s: %w", outPath, err)
		}
	}

	// Persist manifest digest for future checks
	if err := os.WriteFile(digestFilePath, []byte(desc.Digest.String()), 0o600); err != nil {
		return fmt.Errorf("failed to write digest file: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"repository":  repo,
		"destination": destination,
		"tag":         tag,
		"digest":      desc.Digest,
	}).Info("download completed and files saved")

	return nil
}
