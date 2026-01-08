//go:build unix

package deduper

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const (
	// orphanedTmpMaxAge is the minimum age for a .dupedog.tmp file to be considered orphaned.
	// Files younger than this are assumed to be from an active operation.
	orphanedTmpMaxAge = 1 * time.Minute
)

// CreateHardlink creates a hardlink atomically by linking to a temp file then renaming.
// If the temp file exists and is orphaned (old + safe to delete), it will be cleaned up and retried.
func CreateHardlink(source, target string) error {
	tmp := target + ".dupedog.tmp"

	err := os.Link(source, tmp)
	if errors.Is(err, syscall.EEXIST) {
		if cleanupErr := tryCleanupOrphanedTmp(tmp, orphanedTmpMaxAge); cleanupErr != nil {
			return fmt.Errorf("tmp file exists and cannot be cleaned: %w", cleanupErr)
		}
		// Retry after cleanup
		err = os.Link(source, tmp)
	}
	if err != nil {
		return err
	}

	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp) // cleanup on failure
		return err
	}
	return nil
}

// CreateSymlink creates a symlink atomically by linking to a temp file then renaming.
// If the temp file exists and is orphaned (old + safe to delete), it will be cleaned up and retried.
func CreateSymlink(source, target string) error {
	// Verify source exists before creating a symlink to it.
	// This prevents creating dangling symlinks if source was deleted after verification.
	if _, err := os.Stat(source); err != nil {
		return fmt.Errorf("source missing before symlink creation: %w", err)
	}

	tmp := target + ".dupedog.tmp"

	// For symlinks, we need the relative path from target's perspective
	relPath, err := filepath.Rel(filepath.Dir(target), source)
	if err != nil {
		relPath = source // fallback to absolute if relative fails
	}

	err = os.Symlink(relPath, tmp)
	if errors.Is(err, syscall.EEXIST) {
		if cleanupErr := tryCleanupOrphanedTmp(tmp, orphanedTmpMaxAge); cleanupErr != nil {
			return fmt.Errorf("tmp file exists and cannot be cleaned: %w", cleanupErr)
		}
		// Retry after cleanup
		err = os.Symlink(relPath, tmp)
	}
	if err != nil {
		return err
	}

	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp) // cleanup on failure
		return err
	}
	return nil
}

// tryCleanupOrphanedTmp attempts to clean up an orphaned .dupedog.tmp file.
// Returns nil if successfully removed, or an error explaining why cleanup was skipped/failed.
//
// Safety criteria (ALL must be met):
// 1. File is older than maxAge (protects against race with active operations)
// 2. File is a symlink OR regular file with nlink > 1 (protects against data loss)
//
// If nlink == 1, the file is NOT deleted as it may be the only copy of data.
func tryCleanupOrphanedTmp(path string, maxAge time.Duration) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("lstat: %w", err)
	}

	// Safety check 1: Age
	cutoff := time.Now().Add(-maxAge)
	if info.ModTime().After(cutoff) {
		return fmt.Errorf("file too recent (mtime %v, cutoff %v)", info.ModTime(), cutoff)
	}

	// Safety check 2: Type and nlink
	mode := info.Mode()

	// Symlinks are always safe - they don't contain actual data
	if mode&os.ModeSymlink != 0 {
		return os.Remove(path)
	}

	// For regular files, check nlink
	if !mode.IsRegular() {
		return fmt.Errorf("not a regular file or symlink (mode %v)", mode)
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot get syscall.Stat_t")
	}

	// CRITICAL: Only delete if other hardlinks exist (nlink > 1)
	// If nlink == 1, this IS the only copy - DO NOT DELETE
	if stat.Nlink <= 1 {
		return fmt.Errorf("nlink=%d, may be only copy of data", stat.Nlink)
	}

	return os.Remove(path)
}
