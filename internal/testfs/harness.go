//go:build unix && !e2e

package testfs

import (
	"testing"
)

// -----------------------------------------------------------------------------
// Harness - Integration Test API
// -----------------------------------------------------------------------------

// Harness provides integration test infrastructure using t.TempDir().
//
// Unlike the E2E Harness that uses Docker containers with tmpfs mounts,
// this Harness creates files in a temporary directory on the local filesystem.
//
// Limitations:
//   - Cannot test cross-device scenarios (EXDEV errors)
//   - All "volumes" are directories on the same filesystem
//   - Use E2E tests with Docker for cross-device testing
//
// Usage:
//
//	given := testfs.FileTree{
//	    Volumes: []Volume{
//	        {MountPoint: "/vol1", Files: []File{{Path: []string{"a.txt"}, Size: "1m", Tag: "same"}}},
//	    },
//	}
//	then := testfs.FileTree{
//	    Volumes: []Volume{
//	        {MountPoint: "/vol1", Files: []File{{Path: []string{"a.txt", "b.txt"}}}},
//	    },
//	}
//	h := testfs.New(t, given)
//	files := scanner.New([]string{h.Root()}, minSize, nil, 2, false, nil).Run()
//	// ... run pipeline
//	h.Assert(then)
type Harness struct {
	t     *testing.T
	root  string   // Temporary directory root
	given FileTree // Original spec
}

// New creates a new Harness with the given FileTree specification.
//
// The harness:
//  1. Creates a temporary directory via t.TempDir()
//  2. Creates subdirectories for each Volume's MountPoint
//  3. Creates files, hardlinks, and symlinks according to the spec
//
// The temporary directory is automatically cleaned up by t.TempDir() mechanics.
func New(t *testing.T, given FileTree) *Harness {
	t.Helper()

	root := t.TempDir()
	h := &Harness{
		t:     t,
		root:  root,
		given: given,
	}

	// Create filesystem according to spec
	if err := SowFileTree(root, given); err != nil {
		t.Fatalf("failed to setup files: %v", err)
	}

	return h
}

// Root returns the temporary directory root path.
func (h *Harness) Root() string {
	return h.root
}

// Assert verifies the filesystem state matches the expected FileTree.
//
// Checks:
//   - Files exist at all specified paths
//   - Files in the same File entry share the same inode (hardlinks)
//   - Files in different File entries have different inodes
//   - Symlinks point to the expected targets
//
// Fails the test with descriptive errors if any assertion fails.
func (h *Harness) Assert(expected FileTree) {
	h.t.Helper()

	for _, vol := range expected.Volumes {
		h.assertState(vol)
	}
}

// -----------------------------------------------------------------------------
// Assertion Helpers
// -----------------------------------------------------------------------------

// assertState verifies files and symlinks match expected state for a volume.
func (h *Harness) assertState(vol Volume) {
	h.t.Helper()

	actual, err := ReapPaths(h.root, []string{vol.MountPoint})
	if err != nil {
		h.t.Fatalf("reap %s: %v", vol.MountPoint, err)
	}
	if len(actual.Volumes) == 0 {
		h.t.Fatalf("reap returned no volumes for %s", vol.MountPoint)
	}

	AssertVolume(h.t, vol, actual.Volumes[0])
}
