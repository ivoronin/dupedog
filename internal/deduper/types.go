package deduper

import (
	"fmt"
	"strings"
)

// ActionType describes the action taken during deduplication.
type ActionType int

const (
	ActionHardlink ActionType = iota
	ActionSymlink             // Fallback for cross-device
	ActionSkipped             // Skipped due to error
)

// DedupeResult describes the outcome of a single dedupe operation.
type DedupeResult struct {
	Source     string     // Path kept
	Target     string     // Path replaced
	Action     ActionType // Hardlink, Symlink, or Skipped
	BytesSaved int64      // Bytes reclaimed (0 if skipped)
	Err        error      // Non-nil if skipped
}

// String formats the dedupe result for display.
func (r *DedupeResult) String() string {
	switch r.Action {
	case ActionHardlink:
		return fmt.Sprintf("Replaced %s with hardlink to %s", escapePath(r.Target), escapePath(r.Source))
	case ActionSymlink:
		return fmt.Sprintf("Replaced %s with symlink to %s", escapePath(r.Target), escapePath(r.Source))
	case ActionSkipped:
		return fmt.Sprintf("skipped %s: %v", escapePath(r.Target), r.Err)
	default:
		return fmt.Sprintf("Unknown action for %s", escapePath(r.Target))
	}
}

// escapePath escapes special characters in paths for safe terminal output.
func escapePath(path string) string {
	r := strings.NewReplacer(
		"\t", "\\t",
		"\n", "\\n",
		"\r", "\\r",
	)
	return r.Replace(path)
}
