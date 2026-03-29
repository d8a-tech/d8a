package sessions

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

// SpoolFailureStrategy defines the action to take when a spool file exceeds
// the maximum number of consecutive child writer failures.
type SpoolFailureStrategy interface {
	OnExceededFailures(spoolPath string) error
}

// deleteSpoolStrategy removes the spool file on exceeded failures (best_effort behavior).
type deleteSpoolStrategy struct{}

// NewDeleteSpoolStrategy creates a SpoolFailureStrategy that deletes spool files
// when consecutive child writer failures exceed the threshold.
func NewDeleteSpoolStrategy() SpoolFailureStrategy {
	return &deleteSpoolStrategy{}
}

// OnExceededFailures implements SpoolFailureStrategy.
func (s *deleteSpoolStrategy) OnExceededFailures(spoolPath string) error {
	if err := os.Remove(spoolPath); err != nil {
		return fmt.Errorf("removing discarded spool file %q: %w", spoolPath, err)
	}
	return nil
}

// quarantineSpoolStrategy renames the spool file to .quarantine on exceeded failures (at_least_once behavior).
type quarantineSpoolStrategy struct{}

// NewQuarantineSpoolStrategy creates a SpoolFailureStrategy that quarantines spool files
// by renaming them with a .quarantine suffix when consecutive child writer failures
// exceed the threshold.
func NewQuarantineSpoolStrategy() SpoolFailureStrategy {
	return &quarantineSpoolStrategy{}
}

// OnExceededFailures implements SpoolFailureStrategy.
func (s *quarantineSpoolStrategy) OnExceededFailures(spoolPath string) error {
	quarantinePath := spoolPath + ".quarantine"
	if err := os.Rename(spoolPath, quarantinePath); err != nil {
		return fmt.Errorf("quarantining spool file %q: %w", spoolPath, err)
	}
	logrus.Warnf("quarantined spool file %q to %q after exceeding failure threshold", spoolPath, quarantinePath)
	return nil
}
