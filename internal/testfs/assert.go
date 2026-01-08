package testfs

import "testing"

// -----------------------------------------------------------------------------
// Assertion Functions - Shared between TempDirHarness and E2E Harness
// -----------------------------------------------------------------------------

// AssertVolume verifies the actual filesystem state matches expected.
//
// Checks:
//   - Files exist at all specified paths
//   - Files in the same File entry share the same inode (hardlinks)
//   - Files in different File entries have different inodes
//   - Symlinks point to the expected targets
func AssertVolume(t *testing.T, expected Volume, actual ReapVolume) {
	t.Helper()
	AssertFiles(t, expected.Files, actual.Files)
	AssertSymlinks(t, expected.Symlinks, actual.Symlinks)
}

// AssertFiles verifies expected files exist and hardlinks are correct.
//
// For each File entry:
//   - All paths must exist
//   - All paths must share the same inode (hardlinks)
//   - Different File entries must have different inodes
func AssertFiles(t *testing.T, expected []File, actual []ReapFile) {
	t.Helper()

	pathToInode := buildPathToInodeMap(actual)
	entryInodes := verifyFileEntries(t, expected, pathToInode)
	verifyUniqueInodes(t, expected, entryInodes)
}

// AssertSymlinks verifies expected symlinks exist with correct targets.
func AssertSymlinks(t *testing.T, expected []Symlink, actual []ReapSymlink) {
	t.Helper()

	// Build path-to-target map from actual state
	pathToTarget := make(map[string]string)
	for _, rs := range actual {
		pathToTarget[rs.Path] = rs.Target
	}

	// Verify each expected symlink
	for _, expectedSym := range expected {
		target, ok := pathToTarget[expectedSym.Path]
		if !ok {
			t.Errorf("expected symlink not found: %s", expectedSym.Path)
			continue
		}
		if target != expectedSym.Target {
			t.Errorf("symlink %s: got target %q, want %q",
				expectedSym.Path, target, expectedSym.Target)
		}
	}
}

// -----------------------------------------------------------------------------
// Helper Functions (unexported)
// -----------------------------------------------------------------------------

// buildPathToInodeMap creates a map from file path to inode number.
func buildPathToInodeMap(files []ReapFile) map[string]uint64 {
	m := make(map[string]uint64)
	for _, rf := range files {
		for _, p := range rf.Path {
			m[p] = rf.Inode
		}
	}
	return m
}

// verifyFileEntries checks that all expected files exist and share inodes correctly.
// Returns a map of entry index to inode for cross-entry uniqueness checking.
func verifyFileEntries(t *testing.T, expected []File, pathToInode map[string]uint64) map[int]uint64 {
	t.Helper()
	entryInodes := make(map[int]uint64)

	for i, ef := range expected {
		if len(ef.Path) == 0 {
			continue
		}
		if inode, ok := verifyFileEntry(t, ef, pathToInode); ok {
			entryInodes[i] = inode
		}
	}
	return entryInodes
}

// verifyFileEntry checks a single file entry and returns its inode if valid.
func verifyFileEntry(t *testing.T, ef File, pathToInode map[string]uint64) (uint64, bool) {
	t.Helper()

	firstPath := ef.Path[0]
	firstInode, ok := pathToInode[firstPath]
	if !ok {
		t.Errorf("expected file not found: %s", firstPath)
		return 0, false
	}

	// Verify all paths share the same inode (hardlinks)
	for _, p := range ef.Path[1:] {
		ino, ok := pathToInode[p]
		if !ok {
			t.Errorf("expected file not found: %s", p)
			continue
		}
		if ino != firstInode {
			t.Errorf("hardlink mismatch: %s (inode %d) != %s (inode %d)",
				firstPath, firstInode, p, ino)
		}
	}
	return firstInode, true
}

// verifyUniqueInodes checks that different File entries have different inodes.
func verifyUniqueInodes(t *testing.T, expected []File, entryInodes map[int]uint64) {
	t.Helper()
	for i, ino1 := range entryInodes {
		for j, ino2 := range entryInodes {
			if i < j && ino1 == ino2 {
				t.Errorf("files from different entries share inode %d: %v and %v",
					ino1, expected[i].Path, expected[j].Path)
			}
		}
	}
}
