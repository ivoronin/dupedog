//go:build e2e

package testfs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/docker/docker/api/types/container"
)

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

const (
	// baseImage is the Docker image used for E2E tests.
	baseImage = "alpine:3.21"

	// Binary names and paths inside container.
	binaryName       = "dupedog"
	helperBinaryName = "testfs-helper"
	binaryPath       = "/tmp/" + binaryName
	helperBinaryPath = "/tmp/" + helperBinaryName
)

// -----------------------------------------------------------------------------
// Harness - Public API
// -----------------------------------------------------------------------------

// Harness provides E2E test infrastructure using Docker containers.
//
// Usage:
//
//	given := testfs.FileTree{
//	    Volumes: []Volume{
//	        {MountPoint: "/vol1", Files: []File{{Path: []string{"a.txt"}, Size: "1m", Tag: "same"}}},
//	        {MountPoint: "/vol2", Files: []File{{Path: []string{"b.txt"}, Size: "1m", Tag: "same"}}},
//	    },
//	}
//	then := testfs.FileTree{
//	    Volumes: []Volume{
//	        {MountPoint: "/vol1", Files: []File{{Path: []string{"a.txt"}}}},
//	        {MountPoint: "/vol2", Symlinks: []Symlink{{Path: "b.txt", Target: "../vol1/a.txt"}}},
//	    },
//	}
//	h := testfs.New(t, given)
//	h.RunDupedog("--symlink-fallback", "/vol1", "/vol2")
//	h.Assert(then)
type Harness struct {
	t          *testing.T
	ctx        context.Context
	given      FileTree
	container  *Container
	lastResult *RunResult
}

// New creates a new Harness with the given FileTree specification.
//
// The harness:
//  1. Starts a Docker container with tmpfs volumes for each Volume in the spec
//  2. Bind-mounts pre-built dupedog binaries into the container
//  3. Creates files, hardlinks, and symlinks according to the spec
//
// Requires DUPEDOG_E2E_BINDIR env var (set by 'make test-e2e').
// The container is automatically cleaned up when the test finishes via t.Cleanup().
func New(t *testing.T, given FileTree) *Harness {
	t.Helper()

	ctx := context.Background()
	h := &Harness{
		t:     t,
		ctx:   ctx,
		given: given,
	}

	// Build container config
	cfg, hostCfg, err := h.buildContainerConfig()
	if err != nil {
		t.Fatalf("failed to build container config: %v", err)
	}

	// Create container
	c, err := NewContainer(ctx, cfg, hostCfg)
	if err != nil {
		t.Fatalf("failed to create container: %v", err)
	}
	h.container = c

	// Register cleanup
	t.Cleanup(func() {
		h.Cleanup()
	})

	// Setup files according to spec
	if err := h.sowFileTree(); err != nil {
		t.Fatalf("failed to setup files: %v", err)
	}

	return h
}

// RunDupedog executes the dupedog binary inside the container with the given arguments.
//
// Example:
//
//	h.RunDupedog("--symlink-fallback", "/vol1", "/vol2")
//	h.RunDupedog("dedupe", "--dry-run", "/vol1")
//
// The result (exit code, stdout, stderr) is stored for later assertion.
func (h *Harness) RunDupedog(args ...string) *RunResult {
	h.t.Helper()

	cmd := append([]string{binaryPath}, args...)
	stdout, stderr, exitCode, err := h.container.Run(h.ctx, cmd, nil)
	if err != nil {
		h.t.Fatalf("failed to run dupedog: %v", err)
	}

	h.lastResult = &RunResult{
		ExitCode: exitCode,
		Stdout:   stdout,
		Stderr:   stderr,
	}
	return h.lastResult
}

// Assert verifies the filesystem state matches the expected FileTree.
//
// Checks:
//   - Files exist at all specified paths
//   - Files in the same File entry share the same inode (hardlinks)
//   - Files in different File entries have different inodes
//   - Symlinks point to the expected targets
//   - Exit code matches (if non-zero in expected)
func (h *Harness) Assert(expected FileTree) {
	h.t.Helper()

	// Check exit code
	if expected.ExitCode != 0 || h.lastResult != nil {
		if h.lastResult == nil {
			h.t.Fatal("Assert called before RunDupedog")
		}
		if h.lastResult.ExitCode != expected.ExitCode {
			h.t.Errorf("exit code: got %d, want %d\nstdout: %s\nstderr: %s",
				h.lastResult.ExitCode, expected.ExitCode,
				h.lastResult.Stdout, h.lastResult.Stderr)
		}
	}

	// Verify filesystem state for each volume
	for _, vol := range expected.Volumes {
		h.assertState(vol)
	}
}

// Cleanup terminates the container and releases resources.
func (h *Harness) Cleanup() {
	if h.container != nil {
		_ = h.container.Close(h.ctx)
		h.container = nil
	}
}

// -----------------------------------------------------------------------------
// Container Configuration
// -----------------------------------------------------------------------------

// buildContainerConfig creates Docker container and host configs for E2E tests.
func (h *Harness) buildContainerConfig() (*container.Config, *container.HostConfig, error) {
	// Get binary directory from environment
	binDir := os.Getenv("DUPEDOG_E2E_BINDIR")
	if binDir == "" {
		return nil, nil, fmt.Errorf("DUPEDOG_E2E_BINDIR not set - run via 'make test-e2e'")
	}

	// Extract mount paths from volumes
	mountPaths := make([]string, len(h.given.Volumes))
	for i, v := range h.given.Volumes {
		mountPaths[i] = v.MountPoint
	}

	// Sort mount paths so parents come before children
	sort.Strings(mountPaths)

	// Build tmpfs mounts
	tmpfs := make(map[string]string)
	for _, path := range mountPaths {
		tmpfs[path] = "size=100m"
	}

	// Build bind mounts for binaries (read-only)
	binds := []string{
		fmt.Sprintf("%s:%s:ro", filepath.Join(binDir, binaryName), binaryPath),
		fmt.Sprintf("%s:%s:ro", filepath.Join(binDir, helperBinaryName), helperBinaryPath),
	}

	cfg := &container.Config{
		Image: baseImage,
		Cmd:   []string{"sleep", "infinity"},
	}

	hostCfg := &container.HostConfig{
		Binds:      binds,
		Tmpfs:      tmpfs,
		AutoRemove: true,
	}

	return cfg, hostCfg, nil
}

// -----------------------------------------------------------------------------
// FileTree Operations
// -----------------------------------------------------------------------------

// sowFileTree creates filesystem from FileTree spec using testfs-helper.
func (h *Harness) sowFileTree() error {
	specJSON, err := json.Marshal(h.given)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}

	cmd := []string{helperBinaryPath, "sow"}
	stdout, stderr, exitCode, err := h.container.Run(h.ctx, cmd, specJSON)
	if err != nil {
		return fmt.Errorf("run sow: %w", err)
	}
	if exitCode != 0 {
		return fmt.Errorf("sow failed (exit %d): %s%s", exitCode, stdout, stderr)
	}
	return nil
}

// reapPaths captures filesystem state using testfs-helper.
func (h *Harness) reapPaths(paths []string) (*ReapResult, error) {
	cmd := append([]string{helperBinaryPath, "reap"}, paths...)
	stdout, stderr, exitCode, err := h.container.Run(h.ctx, cmd, nil)
	if err != nil {
		return nil, fmt.Errorf("run reap: %w", err)
	}
	if exitCode != 0 {
		return nil, fmt.Errorf("reap failed (exit %d): %s%s", exitCode, stdout, stderr)
	}

	var result ReapResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		return nil, fmt.Errorf("parse reap output: %w", err)
	}
	return &result, nil
}

// -----------------------------------------------------------------------------
// Assertion Helpers
// -----------------------------------------------------------------------------

// assertState verifies files and symlinks match expected state for a volume.
func (h *Harness) assertState(vol Volume) {
	h.t.Helper()

	actual, err := h.reapPaths([]string{vol.MountPoint})
	if err != nil {
		h.t.Fatalf("reap %s: %v", vol.MountPoint, err)
	}
	if len(actual.Volumes) == 0 {
		h.t.Fatalf("reap returned no volumes for %s", vol.MountPoint)
	}

	AssertVolume(h.t, vol, actual.Volumes[0])
}
