package main

import (
	"fmt"
	"path/filepath"

	"github.com/dustin/go-humanize"
)

// parseSize parses a human-readable size string into bytes.
// Supports formats: "100", "1K", "1MB", "1GiB", etc.
func parseSize(s string) (int64, error) {
	bytes, err := humanize.ParseBytes(s)
	if err != nil {
		return 0, err
	}
	return int64(bytes), nil
}

// validateGlobPatterns checks that all patterns are valid filepath.Match patterns.
func validateGlobPatterns(patterns []string) error {
	for _, pattern := range patterns {
		if _, err := filepath.Match(pattern, ""); err != nil {
			return fmt.Errorf("pattern %q: %w", pattern, err)
		}
	}
	return nil
}
