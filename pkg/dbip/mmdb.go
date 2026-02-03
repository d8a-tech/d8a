// Package dbip provides utilities to download DB-IP datasets from an OCI registry.
package dbip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/memory"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
)

// Downloader downloads an MMDB city database and returns the saved file path.
type Downloader interface {
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
	downloader Downloader
	path       string
	err        error
	once       sync.Once
}

// NewOnlyOnceDownloader creates a new OnlyOnceDownloader
func NewOnlyOnceDownloader(downloader Downloader) Downloader {
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

// extensionBasedOCIDownloader implements MMDBCityDatabaseDownloader using OCI registry, downloading first file with
// given extension from the registry.
type extensionBasedOCIDownloader struct {
	creds             OCIRegistryCreds
	searchedExtension string
}

// NewExtensionBasedOCIDownloader creates a new MMDBCityDatabaseDownloader backed by an OCI registry
func NewExtensionBasedOCIDownloader(creds OCIRegistryCreds, extension string) Downloader {
	return &extensionBasedOCIDownloader{creds: creds, searchedExtension: func() string {
		if strings.HasPrefix(extension, ".") {
			return extension
		}
		return "." + extension
	}()}
}

// Download implements MMDBCityDatabaseDownloader
func (d *extensionBasedOCIDownloader) Download(
	ctx context.Context,
	artifactName,
	tag,
	destinationDir string,
) (string, error) {
	repository, err := d.createRepository(d.creds, artifactName)
	if err != nil {
		return "", fmt.Errorf("failed to create repository: %w", err)
	}
	destDir := destinationDir
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return "", fmt.Errorf("failed to create destination directory: %w", err)
	}
	_, existingMMDBPath, err := d.validate(ctx, repository, tag, destDir)
	if err != nil {
		return "", err
	}
	if existingMMDBPath != "" {
		logrus.WithFields(logrus.Fields{
			"path": existingMMDBPath,
		}).Info("existing MMDB file found, skipping download")
		return existingMMDBPath, nil
	}
	logrus.WithFields(logrus.Fields{
		"destination": destDir,
		"repository":  d.creds.Repo,
		"artifact":    artifactName,
		"tag":         tag,
	}).Info("no existing MMDB file found, downloading")

	manifest, mem, desc, err := d.fetchManifest(ctx, repository, tag)
	if err != nil {
		return "", err
	}

	// Write layers to destination using safe file names
	var mmdbPath string
	for _, layer := range manifest.Layers {
		outPath, err := d.writeBlob(ctx, mem, &layer, destDir)
		if err != nil {
			return "", err
		}

		// Track the MMDB file path
		if filepath.Ext(outPath) == d.searchedExtension {
			mmdbPath = outPath
		}
	}

	// Persist manifest digest for future checks
	digestFilePath := d.manifestDigestFilePath(destDir)
	if err := os.WriteFile(digestFilePath, []byte(desc.Digest.String()), 0o600); err != nil {
		return "", fmt.Errorf("failed to write digest file: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"path": mmdbPath,
	}).Info("download completed and MMDB file saved")

	if mmdbPath == "" {
		return "", fmt.Errorf("no .mmdb file found in downloaded layers")
	}

	// Clean up old MMDB files after successful download
	if err := d.cleanupOldMMDBFiles(destDir, mmdbPath); err != nil {
		logrus.WithError(err).Warn("failed to cleanup old MMDB files")
		// Don't fail the download if cleanup fails
	}

	return mmdbPath, nil
}

func (d *extensionBasedOCIDownloader) fetchManifest(
	ctx context.Context,
	repository *remote.Repository,
	tag string,
) (*ocispec.Manifest, *memory.Store, ocispec.Descriptor, error) {
	// Pull into memory store to avoid writing OCI layout to filesystem
	mem := memory.New()
	desc, err := oras.Copy(ctx, repository, tag, mem, tag, oras.DefaultCopyOptions)
	if err != nil {
		return nil, nil, ocispec.Descriptor{}, fmt.Errorf("failed to copy artifact: %w", err)
	}

	// Fetch and parse manifest
	rc, err := mem.Fetch(ctx, desc)
	if err != nil {
		return nil, nil, ocispec.Descriptor{}, fmt.Errorf("failed to fetch manifest: %w", err)
	}
	manifestBytes, err := io.ReadAll(rc)
	if err != nil {
		return nil, nil, ocispec.Descriptor{}, fmt.Errorf("failed to read manifest bytes: %w", err)
	}
	if err := rc.Close(); err != nil {
		logrus.WithError(err).Warn("failed to close manifest reader")
	}

	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, nil, ocispec.Descriptor{}, fmt.Errorf("failed to unmarshal manifest: %w", err)
	}

	return &manifest, mem, desc, nil
}

func (d *extensionBasedOCIDownloader) writeBlob(
	ctx context.Context,
	fetcher content.Fetcher,
	layer *ocispec.Descriptor,
	destDir string,
) (string, error) {
	fileName := layer.Annotations[ocispec.AnnotationTitle]
	if fileName == "" {
		// Fallback to digest-based name
		fileName = layer.Digest.Encoded()
	}
	// Ensure we do not allow path traversal from annotations
	safeName := filepath.Base(filepath.Clean(fileName))
	outPath := filepath.Join(destDir, safeName)

	// Fetch blob content
	blobReader, err := fetcher.Fetch(ctx, *layer)
	if err != nil {
		return "", fmt.Errorf("failed to fetch blob %s: %w", layer.Digest, err)
	}
	defer func() {
		if err := blobReader.Close(); err != nil {
			logrus.WithError(err).Warn("failed to close blob reader")
		}
	}()

	// Write atomically
	tmpPath := outPath + ".tmp"
	// #nosec G304 - tmpPath is built from a sanitized base file name under destDir
	outFile, err := os.Create(tmpPath)
	if err != nil {
		return "", fmt.Errorf("failed to create file %s: %w", tmpPath, err)
	}

	if _, err := io.Copy(outFile, blobReader); err != nil {
		return "", errors.Join(
			fmt.Errorf("failed to write file %s: %w", tmpPath, err),
			os.Remove(tmpPath),
		)
	}

	if err := outFile.Close(); err != nil {
		return "", errors.Join(
			fmt.Errorf("failed to close file %s: %w", tmpPath, err),
			os.Remove(tmpPath),
		)
	}

	if err := os.Rename(tmpPath, outPath); err != nil {
		return "", errors.Join(
			fmt.Errorf("failed to move temp file into place for %s: %w", outPath, err),
			os.Remove(tmpPath),
		)
	}

	return outPath, nil
}

// selectBestMMDBFile deterministically selects the best MMDB file from the destination directory.
// It prefers files with YYYY-MM version pattern (e.g., dbip-city-lite-2026-01.mmdb), selecting the highest year-month.
// If no versioned files are found, it falls back to lexicographically greatest filename.
// Returns empty string if no .mmdb files are found.
func selectBestMMDBFile(destDir, extension string) (string, error) {
	entries, err := os.ReadDir(destDir)
	if err != nil {
		return "", fmt.Errorf("failed to read destination directory: %w", err)
	}

	var mmdbFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == extension {
			mmdbFiles = append(mmdbFiles, entry.Name())
		}
	}

	if len(mmdbFiles) == 0 {
		return "", nil
	}

	// Pattern to match YYYY-MM in filename (e.g., dbip-city-lite-2026-01.mmdb)
	versionPattern := regexp.MustCompile(`(\d{4})-(\d{2})`)

	type versionedFile struct {
		name       string
		year       int
		month      int
		hasVersion bool
	}

	var versioned []versionedFile
	var unversioned []string

	for _, f := range mmdbFiles {
		matches := versionPattern.FindStringSubmatch(f)
		if len(matches) == 3 {
			var year, month int
			_, err := fmt.Sscanf(matches[1], "%d", &year)
			if err != nil {
				return "", fmt.Errorf("failed to scan year: %w", err)
			}
			_, err = fmt.Sscanf(matches[2], "%d", &month)
			if err != nil {
				return "", fmt.Errorf("failed to scan month: %w", err)
			}
			versioned = append(versioned, versionedFile{
				name:       f,
				year:       year,
				month:      month,
				hasVersion: true,
			})
		} else {
			unversioned = append(unversioned, f)
		}
	}

	// If we have versioned files, prefer the one with highest year-month
	if len(versioned) > 0 {
		sort.Slice(versioned, func(i, j int) bool {
			if versioned[i].year != versioned[j].year {
				return versioned[i].year > versioned[j].year
			}
			return versioned[i].month > versioned[j].month
		})
		return filepath.Join(destDir, versioned[0].name), nil
	}

	// Fallback to lexicographically greatest filename
	if len(unversioned) > 0 {
		sort.Strings(unversioned)
		return filepath.Join(destDir, unversioned[len(unversioned)-1]), nil
	}

	return "", nil
}

func (d *extensionBasedOCIDownloader) validate(
	ctx context.Context,
	repository *remote.Repository,
	tag,
	destDir string,
) (ocispec.Descriptor, string, error) {
	// Get remote descriptor to check the digest
	remoteDesc, err := repository.Resolve(ctx, tag)
	if err != nil {
		return ocispec.Descriptor{}, "", fmt.Errorf("failed to resolve remote artifact: %w", err)
	}

	digestFilePath := d.manifestDigestFilePath(destDir)

	// #nosec G304 - digest file path is constructed, not user-controlled traversal
	b, err := os.ReadFile(digestFilePath)
	if err != nil {
		// No local digest file, need to download
		return remoteDesc, "", nil
	}

	localDigest := string(b)
	if localDigest != remoteDesc.Digest.String() {
		return remoteDesc, "", nil
	}

	// Find existing MMDB file using deterministic selection
	mmdbPath, err := selectBestMMDBFile(destDir, d.searchedExtension)
	if err != nil {
		return ocispec.Descriptor{}, "", err
	}

	if mmdbPath == "" {
		return ocispec.Descriptor{}, "", fmt.Errorf("no .mmdb file found in destination directory")
	}

	return remoteDesc, mmdbPath, nil
}

// cleanupOldMMDBFiles removes all .mmdb files in destDir except the one specified by keepPath.
func (d *extensionBasedOCIDownloader) cleanupOldMMDBFiles(destDir, keepPath string) error {
	entries, err := os.ReadDir(destDir)
	if err != nil {
		return fmt.Errorf("failed to read destination directory: %w", err)
	}

	keepBase := filepath.Base(keepPath)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == d.searchedExtension && entry.Name() != keepBase {
			oldPath := filepath.Join(destDir, entry.Name())
			if err := os.Remove(oldPath); err != nil {
				logrus.WithFields(logrus.Fields{
					"path":  oldPath,
					"error": err,
				}).Warn("failed to remove old MMDB file")
				// Continue with other files even if one fails
			} else {
				logrus.WithFields(logrus.Fields{
					"path": oldPath,
				}).Info("removed old MMDB file")
			}
		}
	}
	return nil
}

func (d *extensionBasedOCIDownloader) manifestDigestFilePath(destDir string) string {
	return filepath.Join(destDir, fmt.Sprintf(".manifest.%s.digest", d.searchedExtension))
}

func (d *extensionBasedOCIDownloader) createRepository(
	creds OCIRegistryCreds,
	artifactName string,
) (*remote.Repository, error) {
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
