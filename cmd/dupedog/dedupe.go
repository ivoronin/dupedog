package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/ivoronin/dupedog/internal/deduper"
	"github.com/ivoronin/dupedog/internal/cache"
	"github.com/ivoronin/dupedog/internal/scanner"
	"github.com/ivoronin/dupedog/internal/screener"
	"github.com/ivoronin/dupedog/internal/verifier"
	"github.com/spf13/cobra"
)

// dedupeOptions holds CLI flags for the dedupe command.
type dedupeOptions struct {
	minSizeStr            string
	excludes              []string
	workers               int
	noProgress            bool
	verbose               bool
	dryRun                bool
	symlinkFallback       bool
	trustDeviceBoundaries bool
	cacheFile             string
}


// newDedupeCmd creates the dedupe subcommand.
func newDedupeCmd() *cobra.Command {
	opts := &dedupeOptions{
		minSizeStr: "1",
		workers:    runtime.NumCPU(),
	}

	cmd := &cobra.Command{
		Use:   "dedupe [paths...]",
		Short: "Find and deduplicate files",
		Long: `Scans for duplicates and replaces them with hardlinks (or symlinks as fallback).

When using --symlink-fallback, path order determines which location keeps actual data
(symlink source) vs which become symlinks. For example:
  dupedog dedupe /primary /secondary --symlink-fallback
keeps files in /primary, with /secondary containing symlinks pointing to them.

Use --dry-run to preview without making changes.`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runDedupe(args, opts)
		},
	}

	// Bind flags to options
	cmd.Flags().StringVarP(&opts.minSizeStr, "min-size", "m", opts.minSizeStr, "Minimum file size (e.g., 100, 1K, 10M, 1G)")
	cmd.Flags().StringSliceVarP(&opts.excludes, "exclude", "e", nil, "Glob patterns to exclude")
	cmd.Flags().IntVarP(&opts.workers, "workers", "w", opts.workers, "Number of parallel workers")
	cmd.Flags().BoolVar(&opts.noProgress, "no-progress", false, "Disable progress output")
	cmd.Flags().BoolVarP(&opts.verbose, "verbose", "v", false, "Show individual file operations")
	cmd.Flags().BoolVarP(&opts.dryRun, "dry-run", "n", false, "Preview changes without executing")
	cmd.Flags().BoolVar(&opts.symlinkFallback, "symlink-fallback", false, "Fall back to symlinks when deduplicating files across device boundaries")
	cmd.Flags().BoolVar(&opts.trustDeviceBoundaries, "trust-device-boundaries", false,
		"Assume devices have independent inode spaces. WARNING: Unsafe if the same filesystem is mounted at multiple paths (e.g., NFS)")
	cmd.Flags().StringVar(&opts.cacheFile, "cache-file", "", "Path to hash cache file (enables caching)")

	return cmd
}

// drainErrors consumes errors from a channel and writes them to stderr.
// Clears progress bar line before printing to avoid visual collision.
func drainErrors(errs <-chan error) {
	for err := range errs {
		fmt.Fprintf(os.Stderr, "\r\033[Kerror: %v\n", err)
	}
}

// runDedupe executes the dedupe pipeline: scan → screen → verify → dedupe.
func runDedupe(paths []string, opts *dedupeOptions) error {
	minSize, err := parseSize(opts.minSizeStr)
	if err != nil {
		return fmt.Errorf("invalid --min-size: %w", err)
	}

	if err := validateGlobPatterns(opts.excludes); err != nil {
		return fmt.Errorf("invalid --exclude: %w", err)
	}

	showProgress := !opts.noProgress

	// Create shared error channel
	errors := make(chan error, 100)
	go drainErrors(errors)
	defer close(errors)

	// Phase 1: Scan filesystem
	files := scanner.New(paths, minSize, opts.excludes, opts.workers, showProgress, errors).Run()

	if len(files) == 0 {
		return nil
	}

	// Phase 2: Screen for duplicate candidates
	candidates := screener.New(files, showProgress, opts.trustDeviceBoundaries).Run()
	if candidates.Len() == 0 {
		return nil
	}

	// Phase 3: Open cache (if enabled) and verify duplicates
	hashCache, err := cache.Open(opts.cacheFile)
	if err != nil {
		return fmt.Errorf("open cache: %w", err)
	}
	defer func() { _ = hashCache.Close() }()

	duplicates := verifier.New(candidates, opts.workers, showProgress, errors, hashCache).Run()

	// Phase 4: Execute deduplication (paths define source priority)
	deduper.New(duplicates, paths, opts.dryRun, opts.symlinkFallback, opts.verbose, showProgress, errors).Run()

	return nil
}
