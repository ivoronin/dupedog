// Package deduper replaces duplicate files with hardlinks to reclaim disk space.
//
// # Overview
//
// The deduper is the final stage in the duplicate detection pipeline.
// It takes confirmed duplicate groups (as sibling groups) and replaces all but one
// sibling group's files with hardlinks (or symlinks as fallback across device boundaries).
//
// # Processing Pipeline
//
//	Input: types.DuplicateGroups (confirmed duplicate sibling groups)
//	    │
//	    ├──► For each DuplicateGroup:
//	    │        │
//	    │        ├──► Select source file (searching ALL paths in ALL sibling groups)
//	    │        │
//	    │        ├──► Skip source's sibling group (already hardlinked)
//	    │        │
//	    │        └──► For each file in other sibling groups (targets):
//	    │                 │
//	    │                 ├──► Verify mtime unchanged (safety check)
//	    │                 │
//	    │                 ├──► Try hardlink (atomic replace)
//	    │                 │
//	    │                 └──► If EXDEV and --symlink-fallback: try symlink
//	    │
//	    └──► Output: stats (sets deduplicated, bytes saved)
//
// # Sibling Group Optimization
//
// Files in the same sibling group (same dev+ino) are already hardlinks to each other.
// The deduper skips the source's sibling group entirely - no redundant work.
// Path priority searches ALL paths in ALL sibling groups for correct selection.
//
// # Safety Mechanisms
//
//   - Mtime verification prevents replacing files modified during scan
//   - Atomic replacement via rename (write temp → rename over target)
//   - Path priority allows preserving preferred copies (e.g., backups)
//   - Dry-run mode for previewing changes
//
// # Why This Design?
//
//   - Sequential processing (I/O bound, not CPU bound)
//   - Hardlinks preferred (same device, no dangling refs)
//   - Symlinks as fallback (across device boundaries)
//   - Sibling groups preserve all paths for correct priority matching
//   - Verbose mode for auditing replacements
package deduper

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ivoronin/dupedog/internal/progress"
	"github.com/ivoronin/dupedog/internal/types"
)

// Deduper replaces duplicate files with hardlinks (or symlinks as fallback).
//
// The deduper is designed for single-use: create with New(), call Run() once.
type Deduper struct {
	// Config (immutable, set by New)
	groups       types.DuplicateGroups // Confirmed duplicate groups to process
	pathPriority []string              // Preferred source paths (first match wins)
	dryRun       bool                  // Preview mode (don't modify files)
	symlinkFallback bool               // Fall back to symlinks across device boundaries
	verbose      bool                  // Print each replacement to stdout
	showProgress bool                  // Whether to display progress bar
	errCh        chan error            // Non-fatal errors (permission denied, etc.)
}

// New creates a Deduper for replacing duplicates with links.
func New(groups types.DuplicateGroups, pathPriority []string, dryRun, symlinkFallback, verbose, showProgress bool, errCh chan error) *Deduper {
	return &Deduper{
		groups:          groups,
		pathPriority:    pathPriority,
		dryRun:          dryRun,
		symlinkFallback: symlinkFallback,
		verbose:      verbose,
		showProgress: showProgress,
		errCh:        errCh,
	}
}

// stats tracks deduplication progress.
type stats struct {
	totalFiles     int
	processedFiles int
	totalSets      int
	processedSets  int
	savedBytes     int64
	startTime      time.Time
}

func (s *stats) String() string {
	pct := 0.0
	if s.totalFiles > 0 {
		pct = float64(s.processedFiles) / float64(s.totalFiles) * 100
	}
	return fmt.Sprintf("Deduplicated %d/%d files in %d/%d sets (%.0f%%), saved %s in %.1fs",
		s.processedFiles, s.totalFiles,
		s.processedSets, s.totalSets,
		pct,
		humanize.IBytes(uint64(s.savedBytes)),
		time.Since(s.startTime).Seconds())
}

// countTargetFiles counts the total number of files to be deduplicated.
// This excludes source files (one sibling group per duplicate group).
func (d *Deduper) countTargetFiles() int {
	total := 0
	for _, dupeGroup := range d.groups.Items() {
		if dupeGroup.Len() < 2 {
			continue
		}
		for _, siblings := range dupeGroup.Items() {
			total += siblings.Len()
		}
		total -= dupeGroup.First().Len() // Subtract source sibling group (approximate)
	}
	return total
}

// Run executes deduplication on all duplicate groups.
//
// Processing sequence:
//  1. For each duplicate group, select source file (searching all sibling groups)
//  2. Skip source's sibling group (already hardlinked)
//  3. For each file in other sibling groups, verify unchanged and replace with link
//  4. Track bytes saved and report stats
func (d *Deduper) Run() {
	bar := progress.New(d.showProgress, -1)
	st := &stats{totalFiles: d.countTargetFiles(), totalSets: d.groups.Len(), startTime: time.Now()}
	bar.Describe(st) // Render progress bar immediately

	for _, dupeGroup := range d.groups.Items() {
		if dupeGroup.Len() < 2 {
			continue
		}

		source := selectSource(dupeGroup, d.pathPriority)

		for _, targetSiblings := range dupeGroup.Items() {
			// Skip source's sibling group - files are already hardlinked to each other
			if containsFile(targetSiblings, source) {
				continue
			}

			for _, target := range targetSiblings.Items() {
				result := d.dedupeFile(source, target)
				if result.Err != nil {
					d.sendError(fmt.Errorf("%s: %w", target.Path, result.Err))
					continue
				}
				st.savedBytes += result.BytesSaved
				st.processedFiles++
				if d.verbose {
					fmt.Fprintf(os.Stderr, "\r\033[K") // Clear progress line
					_, _ = fmt.Fprintln(os.Stdout, result)
				}
				bar.Describe(st)
			}
		}

		st.processedSets++
		bar.Describe(st)
	}

	bar.Finish(st)
}

// containsFile checks if a sibling group contains the given file (by inode).
func containsFile(siblings types.SiblingGroup, f *types.FileInfo) bool {
	for _, sib := range siblings.Items() {
		if sib.Dev == f.Dev && sib.Ino == f.Ino {
			return true
		}
	}
	return false
}

// dedupeFile replaces target with a link to source.
//
// Safety checks:
//   - Acquires exclusive advisory lock on target (skips if file in use)
//   - Verifies target mtime unchanged since scan
//   - Returns skip result if file was modified or locked
//
// Link strategy:
//   - Tries hardlink first (preferred)
//   - Falls back to symlink if EXDEV and symlinkFallback enabled
func (d *Deduper) dedupeFile(source, target *types.FileInfo) *DedupeResult {
	// Open target file to acquire advisory lock.
	// This prevents race conditions with other processes modifying the file.
	f, err := os.Open(target.Path)
	if err != nil {
		return &DedupeResult{
			Source: source.Path,
			Target: target.Path,
			Action: ActionSkipped,
			Err:    err,
		}
	}
	defer func() { _ = f.Close() }()

	// Try to acquire exclusive non-blocking lock.
	// If file is in use by another process, skip it rather than wait.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return &DedupeResult{
			Source: source.Path,
			Target: target.Path,
			Action: ActionSkipped,
			Err:    errors.New("file in use (locked by another process)"),
		}
	}
	// Lock released automatically when file is closed (deferred above)

	// Check if mtime changed since scan
	info, err := f.Stat()
	if err != nil {
		return &DedupeResult{
			Source: source.Path,
			Target: target.Path,
			Action: ActionSkipped,
			Err:    err,
		}
	}
	if !info.ModTime().Equal(target.ModTime) {
		return &DedupeResult{
			Source: source.Path,
			Target: target.Path,
			Action: ActionSkipped,
			Err:    errors.New("file modified since scan"),
		}
	}

	if d.dryRun {
		return &DedupeResult{
			Source:     source.Path,
			Target:     target.Path,
			Action:     ActionHardlink,
			BytesSaved: target.Size,
		}
	}

	// Try hardlink first
	err = CreateHardlink(source.Path, target.Path)
	if err == nil {
		return &DedupeResult{
			Source:     source.Path,
			Target:     target.Path,
			Action:     ActionHardlink,
			BytesSaved: target.Size,
		}
	}

	// Check for EXDEV error
	if errors.Is(err, syscall.EXDEV) {
		if !d.symlinkFallback {
			return &DedupeResult{
				Source: source.Path,
				Target: target.Path,
				Action: ActionSkipped,
				Err:    errors.New("cannot hardlink across device boundaries (use --symlink-fallback)"),
			}
		}

		// Try symlink as fallback
		err = CreateSymlink(source.Path, target.Path)
		if err == nil {
			return &DedupeResult{
				Source:     source.Path,
				Target:     target.Path,
				Action:     ActionSymlink,
				BytesSaved: target.Size,
			}
		}
		return &DedupeResult{
			Source: source.Path,
			Target: target.Path,
			Action: ActionSkipped,
			Err:    err,
		}
	}

	// Other errors (EMLINK, EACCES, etc.) - skip and continue
	return &DedupeResult{
		Source: source.Path,
		Target: target.Path,
		Action: ActionSkipped,
		Err:    err,
	}
}

// selectSource chooses which file to keep as the source for hardlinks.
//
// Selection priority:
//  1. First file matching any pathPriority prefix (searching ALL sibling groups)
//  2. Sibling group with highest nlink count (preserves existing hardlink groups)
//  3. Falls back to lexicographically first path if tie
//
// The nlink preference ensures that when a standalone duplicate is found
// alongside files that are already hardlinked, the existing hardlink group
// is preserved and the duplicate joins it.
//
// By searching across ALL sibling groups, path priority works correctly even
// when the preferred path is in a sibling group with other hardlinks.
//
// Note: No explicit sorting needed here - DuplicateGroup and SiblingGroup
// maintain sorted order by construction (via types.NewDuplicateGroup/NewSiblingGroup).
func selectSource(dupeGroup types.DuplicateGroup, pathPriority []string) *types.FileInfo {
	// Check path priority across ALL files in ALL sibling groups
	for _, pref := range pathPriority {
		for _, siblings := range dupeGroup.Items() {
			for _, f := range siblings.Items() {
				if strings.HasPrefix(f.Path, pref) {
					return f
				}
			}
		}
	}

	// Prefer sibling group with highest nlink (most existing hardlinks)
	// On tie, prefer lexicographically first path for determinism
	var best *types.FileInfo
	for _, siblings := range dupeGroup.Items() {
		rep := siblings.First() // All siblings share same nlink count
		if best == nil || rep.Nlink > best.Nlink || (rep.Nlink == best.Nlink && rep.Path < best.Path) {
			best = rep
		}
	}
	return best
}

// sendError sends an error to the errors channel if it's not nil.
func (d *Deduper) sendError(err error) {
	if d.errCh != nil {
		d.errCh <- err
	}
}
