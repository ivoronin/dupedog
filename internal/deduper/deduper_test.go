//go:build unix

package deduper

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/ivoronin/dupedog/internal/types"
)

// =============================================================================
// Section 6.1: Core Deduper Tests
// =============================================================================

// TestSelectSourceWithPathPriority tests that path priority selects correct source.
func TestSelectSourceWithPathPriority(t *testing.T) {
	dupeGroup := types.NewDuplicateGroup([]types.SiblingGroup{
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/backup/file.txt", Size: 100, Nlink: 1},
		}),
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/archive/file.txt", Size: 100, Nlink: 1},
		}),
	})

	// Prefer /archive
	source := selectSource(dupeGroup, []string{"/archive"})
	if source.Path != "/archive/file.txt" {
		t.Errorf("expected /archive/file.txt, got %s", source.Path)
	}

	// Prefer /backup
	source = selectSource(dupeGroup, []string{"/backup"})
	if source.Path != "/backup/file.txt" {
		t.Errorf("expected /backup/file.txt, got %s", source.Path)
	}
}

// TestSelectSourceByNlink tests that higher nlink count is preferred.
func TestSelectSourceByNlink(t *testing.T) {
	dupeGroup := types.NewDuplicateGroup([]types.SiblingGroup{
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/a.txt", Size: 100, Nlink: 1},
		}),
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/b.txt", Size: 100, Nlink: 3}, // Higher nlink
		}),
	})

	source := selectSource(dupeGroup, nil)
	if source.Path != "/b.txt" {
		t.Errorf("expected /b.txt (higher nlink), got %s", source.Path)
	}
}

// TestSelectSourceFallbackToPath tests lexicographic fallback on tie.
func TestSelectSourceFallbackToPath(t *testing.T) {
	dupeGroup := types.NewDuplicateGroup([]types.SiblingGroup{
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/b.txt", Size: 100, Nlink: 1},
		}),
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/a.txt", Size: 100, Nlink: 1}, // Same nlink, earlier path
		}),
	})

	source := selectSource(dupeGroup, nil)
	if source.Path != "/a.txt" {
		t.Errorf("expected /a.txt (lexicographic first), got %s", source.Path)
	}
}

// TestSelectSourcePathPriorityOverridesNlink tests path priority beats nlink.
func TestSelectSourcePathPriorityOverridesNlink(t *testing.T) {
	dupeGroup := types.NewDuplicateGroup([]types.SiblingGroup{
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/archive/file.txt", Size: 100, Nlink: 1},
		}),
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/backup/file.txt", Size: 100, Nlink: 5}, // Higher nlink
		}),
	})

	// Path priority should override nlink preference
	source := selectSource(dupeGroup, []string{"/archive"})
	if source.Path != "/archive/file.txt" {
		t.Errorf("expected /archive/file.txt (path priority), got %s", source.Path)
	}
}

// TestCreateHardlink tests atomic hardlink creation.
func TestCreateHardlink(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")
	source := filepath.Join(root, "source.txt")
	target := filepath.Join(root, "target.txt")

	if err := os.WriteFile(source, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("old content"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := CreateHardlink(source, target); err != nil {
		t.Fatalf("CreateHardlink failed: %v", err)
	}

	// Verify target is now hardlinked to source
	sourceInfo, _ := os.Stat(source)
	targetInfo, _ := os.Stat(target)
	sourceStat := sourceInfo.Sys().(*syscall.Stat_t)
	targetStat := targetInfo.Sys().(*syscall.Stat_t)

	if sourceStat.Ino != targetStat.Ino {
		t.Error("target should be hardlinked to source (same inode)")
	}

	// Verify content
	data, _ := os.ReadFile(target)
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %s, want %s", data, content)
	}
}

// TestCreateSymlink tests atomic symlink creation.
func TestCreateSymlink(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")
	source := filepath.Join(root, "source.txt")
	target := filepath.Join(root, "target.txt")

	if err := os.WriteFile(source, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("old content"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := CreateSymlink(source, target); err != nil {
		t.Fatalf("CreateSymlink failed: %v", err)
	}

	// Verify target is a symlink
	linkTarget, err := os.Readlink(target)
	if err != nil {
		t.Fatalf("target should be a symlink: %v", err)
	}
	t.Logf("symlink points to: %s", linkTarget)

	// Verify content via symlink
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("failed to read through symlink: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %s, want %s", data, content)
	}
}

// TestDryRunMode tests that dry run doesn't modify files.
func TestDryRunMode(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")
	sourcePath := filepath.Join(root, "source.txt")
	targetPath := filepath.Join(root, "target.txt")

	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	sourceInfo := getFileInfo(t, sourcePath)
	targetInfo := getFileInfo(t, targetPath)

	groups := types.NewDuplicateGroups([]types.DuplicateGroup{
		types.NewDuplicateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{sourceInfo}),
			types.NewSiblingGroup([]*types.FileInfo{targetInfo}),
		}),
	})

	// Run in dry-run mode
	d := New(groups, nil, true, false, false, false, nil)
	d.Run()

	// Files should still be different inodes
	newSourceInfo := getFileInfo(t, sourcePath)
	newTargetInfo := getFileInfo(t, targetPath)

	if newSourceInfo.Ino == newTargetInfo.Ino {
		t.Error("dry run should not modify files")
	}
}

// TestDedupeFileBasic tests basic file deduplication.
func TestDedupeFileBasic(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")
	sourcePath := filepath.Join(root, "source.txt")
	targetPath := filepath.Join(root, "target.txt")

	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	sourceInfo := getFileInfo(t, sourcePath)
	targetInfo := getFileInfo(t, targetPath)

	groups := types.NewDuplicateGroups([]types.DuplicateGroup{
		types.NewDuplicateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{sourceInfo}),
			types.NewSiblingGroup([]*types.FileInfo{targetInfo}),
		}),
	})

	d := New(groups, nil, false, false, false, false, nil)
	d.Run()

	// Verify files are now hardlinked
	newSourceStat := getFileInfo(t, sourcePath)
	newTargetStat := getFileInfo(t, targetPath)

	if newSourceStat.Ino != newTargetStat.Ino {
		t.Error("files should be hardlinked after deduplication")
	}
}

// TestMtimeVerification tests that changed files are skipped.
func TestMtimeVerification(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")
	sourcePath := filepath.Join(root, "source.txt")
	targetPath := filepath.Join(root, "target.txt")

	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	sourceInfo := getFileInfo(t, sourcePath)
	targetInfo := getFileInfo(t, targetPath)

	// Modify target after getting FileInfo (simulates file change during scan)
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(targetPath, []byte("modified"), 0o644); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 10)
	groups := types.NewDuplicateGroups([]types.DuplicateGroup{
		types.NewDuplicateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{sourceInfo}),
			types.NewSiblingGroup([]*types.FileInfo{targetInfo}),
		}),
	})

	d := New(groups, nil, false, false, false, false, errCh)
	d.Run()
	close(errCh)

	// Should report an error (file changed)
	var errCount int
	for range errCh {
		errCount++
	}
	if errCount == 0 {
		t.Error("expected error for modified file")
	}

	// Files should not be hardlinked
	newSourceStat := getFileInfo(t, sourcePath)
	newTargetStat := getFileInfo(t, targetPath)

	if newSourceStat.Ino == newTargetStat.Ino {
		t.Error("modified file should not be deduplicated")
	}
}

// =============================================================================
// Section 6.2: Deduper Error Scenarios
// =============================================================================

// TestSourceDeletedBeforeDedup tests handling of deleted source file.
func TestSourceDeletedBeforeDedup(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")
	sourcePath := filepath.Join(root, "source.txt")
	targetPath := filepath.Join(root, "target.txt")

	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	sourceInfo := getFileInfo(t, sourcePath)
	targetInfo := getFileInfo(t, targetPath)

	// Delete source before deduplication
	if err := os.Remove(sourcePath); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 10)
	groups := types.NewDuplicateGroups([]types.DuplicateGroup{
		types.NewDuplicateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{sourceInfo}),
			types.NewSiblingGroup([]*types.FileInfo{targetInfo}),
		}),
	})

	d := New(groups, nil, false, false, false, false, errCh)
	d.Run()
	close(errCh)

	// Should report an error
	var errCount int
	for range errCh {
		errCount++
	}
	if errCount == 0 {
		t.Error("expected error for deleted source file")
	}
}

// TestTargetDeletedBeforeDedup tests handling of deleted target file.
func TestTargetDeletedBeforeDedup(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")
	sourcePath := filepath.Join(root, "source.txt")
	targetPath := filepath.Join(root, "target.txt")

	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	sourceInfo := getFileInfo(t, sourcePath)
	targetInfo := getFileInfo(t, targetPath)

	// Delete target before deduplication
	if err := os.Remove(targetPath); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 10)
	groups := types.NewDuplicateGroups([]types.DuplicateGroup{
		types.NewDuplicateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{sourceInfo}),
			types.NewSiblingGroup([]*types.FileInfo{targetInfo}),
		}),
	})

	d := New(groups, nil, false, false, false, false, errCh)
	d.Run()
	close(errCh)

	// Should report an error
	var errCount int
	for range errCh {
		errCount++
	}
	if errCount == 0 {
		t.Error("expected error for deleted target file")
	}
}

// =============================================================================
// Section 6.3: Deduper Filesystem Edge Cases
// =============================================================================

// TestSymlinkRelativePath tests that symlinks use relative paths.
func TestSymlinkRelativePath(t *testing.T) {
	root := t.TempDir()

	// Create subdirectories
	subdir := filepath.Join(root, "subdir")
	if err := os.Mkdir(subdir, 0o755); err != nil {
		t.Fatal(err)
	}

	content := []byte("test content")
	source := filepath.Join(root, "source.txt")
	target := filepath.Join(subdir, "target.txt")

	if err := os.WriteFile(source, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := CreateSymlink(source, target); err != nil {
		t.Fatalf("CreateSymlink failed: %v", err)
	}

	// Verify symlink uses relative path
	linkTarget, err := os.Readlink(target)
	if err != nil {
		t.Fatal(err)
	}

	// Should be ../source.txt
	if linkTarget != "../source.txt" {
		t.Errorf("expected relative path ../source.txt, got %s", linkTarget)
	}
}

// TestContainsFile tests the containsFile helper.
func TestContainsFile(t *testing.T) {
	file1 := &types.FileInfo{Path: "/a.txt", Dev: 1, Ino: 100}
	file2 := &types.FileInfo{Path: "/b.txt", Dev: 1, Ino: 100} // Same inode
	file3 := &types.FileInfo{Path: "/c.txt", Dev: 1, Ino: 200} // Different inode

	siblings := types.NewSiblingGroup([]*types.FileInfo{file1, file2})

	if !containsFile(siblings, file1) {
		t.Error("should contain file1")
	}
	if !containsFile(siblings, file2) {
		t.Error("should contain file2 (same inode)")
	}
	if containsFile(siblings, file3) {
		t.Error("should not contain file3 (different inode)")
	}
}

// =============================================================================
// Section 6.4: Deduper Selection Edge Cases
// =============================================================================

// TestSelectSourceAllNlink1 tests fallback when all files have nlink=1.
func TestSelectSourceAllNlink1(t *testing.T) {
	dupeGroup := types.NewDuplicateGroup([]types.SiblingGroup{
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/c.txt", Size: 100, Nlink: 1},
		}),
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/a.txt", Size: 100, Nlink: 1},
		}),
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/b.txt", Size: 100, Nlink: 1},
		}),
	})

	// With all nlink=1, should fall back to lexicographic order
	source := selectSource(dupeGroup, nil)
	if source.Path != "/a.txt" {
		t.Errorf("expected /a.txt (lexicographic first), got %s", source.Path)
	}
}

// TestSelectSourceEmptyPathPriority tests behavior with empty path priority.
func TestSelectSourceEmptyPathPriority(t *testing.T) {
	dupeGroup := types.NewDuplicateGroup([]types.SiblingGroup{
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/b.txt", Size: 100, Nlink: 2},
		}),
		types.NewSiblingGroup([]*types.FileInfo{
			{Path: "/a.txt", Size: 100, Nlink: 1},
		}),
	})

	// Empty path priority should use nlink
	source := selectSource(dupeGroup, []string{})
	if source.Path != "/b.txt" {
		t.Errorf("expected /b.txt (higher nlink), got %s", source.Path)
	}
}

// TestSiblingGroupSkipped tests that source's sibling group is skipped.
func TestSiblingGroupSkipped(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")

	// Create source and its hardlink
	sourcePath := filepath.Join(root, "source.txt")
	sourceLink := filepath.Join(root, "source_link.txt")
	targetPath := filepath.Join(root, "target.txt")

	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(sourcePath, sourceLink); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	sourceInfo := getFileInfo(t, sourcePath)
	sourceLinkInfo := getFileInfo(t, sourceLink)
	targetInfo := getFileInfo(t, targetPath)

	// source and sourceLink are same inode (sibling group)
	// target is different inode
	groups := types.NewDuplicateGroups([]types.DuplicateGroup{
		types.NewDuplicateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{sourceInfo, sourceLinkInfo}), // sibling group
			types.NewSiblingGroup([]*types.FileInfo{targetInfo}),
		}),
	})

	d := New(groups, nil, false, false, false, false, nil)
	d.Run()

	// Only target should be changed, not sourceLink
	newTargetInfo := getFileInfo(t, targetPath)
	if newTargetInfo.Ino != sourceInfo.Ino {
		t.Error("target should be hardlinked to source")
	}
}

// =============================================================================
// Section 7.4: Output Tests (types.go)
// =============================================================================

// TestEscapePath tests path escaping for special characters.
func TestEscapePath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"normal.txt", "normal.txt"},
		{"file\twith\ttabs.txt", "file\\twith\\ttabs.txt"},
		{"file\nwith\nnewlines.txt", "file\\nwith\\nnewlines.txt"},
		{"file\rwith\rreturns.txt", "file\\rwith\\rreturns.txt"},
		{"mixed\t\n\r.txt", "mixed\\t\\n\\r.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := escapePath(tt.input)
			if got != tt.want {
				t.Errorf("escapePath(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// =============================================================================
// P0 Critical Bug Test: Temp File Collision
// =============================================================================

// TestTempFileCollisionFresh tests that a fresh .dupedog.tmp file (< 1 min) blocks CreateHardlink.
// Fresh temp files are not cleaned up to avoid race conditions with active operations.
func TestTempFileCollisionFresh(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")
	source := filepath.Join(root, "source.txt")
	target := filepath.Join(root, "target.txt")
	tmpFile := target + ".dupedog.tmp"

	if err := os.WriteFile(source, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Pre-create the temp file (collision scenario) - it's fresh, so should NOT be cleaned
	if err := os.WriteFile(tmpFile, []byte("collision"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := CreateHardlink(source, target)

	// Expected: error because tmp file is too recent to be cleaned
	if err == nil {
		t.Error("CreateHardlink should fail when fresh .dupedog.tmp exists")
	}

	// Verify target is unchanged
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("failed to read target: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Error("target should be unchanged when CreateHardlink fails")
	}
}

// TestTempFileCollisionOldNlink1 tests that an old .dupedog.tmp with nlink=1 blocks CreateHardlink.
// Files with nlink=1 may be the only copy of data, so they are NEVER deleted.
func TestTempFileCollisionOldNlink1(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")
	source := filepath.Join(root, "source.txt")
	target := filepath.Join(root, "target.txt")
	tmpFile := target + ".dupedog.tmp"

	if err := os.WriteFile(source, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Pre-create the temp file with nlink=1 (no other hardlinks)
	if err := os.WriteFile(tmpFile, []byte("precious data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Set mtime to 2 minutes ago (older than orphanedTmpMaxAge)
	oldTime := time.Now().Add(-2 * time.Minute)
	if err := os.Chtimes(tmpFile, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	err := CreateHardlink(source, target)

	// Expected: error because nlink=1, we won't delete potential data
	if err == nil {
		t.Error("CreateHardlink should fail when .dupedog.tmp has nlink=1")
	}

	// Verify temp file still exists (safety: we didn't delete it)
	if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
		t.Error("temp file with nlink=1 should NOT be deleted")
	}
}

// TestTempFileCollisionOldNlinkGT1 tests that an old .dupedog.tmp with nlink>1 is cleaned up.
// If other hardlinks exist, it's safe to delete the orphaned temp file.
func TestTempFileCollisionOldNlinkGT1(t *testing.T) {
	root := t.TempDir()

	source := filepath.Join(root, "source.txt")
	target := filepath.Join(root, "target.txt")
	tmpFile := target + ".dupedog.tmp"
	tmpBackup := filepath.Join(root, "backup_of_tmp.txt")

	writeFile(t, source, []byte("test content"))
	writeFile(t, target, []byte("test content"))

	// Create temp file with nlink > 1 (has another hardlink, so safe to delete)
	writeFile(t, tmpFile, []byte("orphaned tmp"))
	mustLink(t, tmpFile, tmpBackup)

	// Set mtime to 2 minutes ago (older than orphanedTmpMaxAge)
	setMtime(t, tmpFile, time.Now().Add(-2*time.Minute))

	err := CreateHardlink(source, target)

	// Expected: SUCCESS because old tmp with nlink>1 was cleaned up
	if err != nil {
		t.Errorf("CreateHardlink should succeed after cleaning old tmp with nlink>1: %v", err)
	}

	// Verify target is now a hardlink to source
	if !sameInode(t, source, target) {
		t.Error("target should be hardlinked to source after cleanup")
	}
}

// =============================================================================
// File Locking Tests
// =============================================================================

// TestFileLockedSkipped tests that files locked by another process are skipped.
func TestFileLockedSkipped(t *testing.T) {
	root := t.TempDir()

	content := []byte("test content")
	source := filepath.Join(root, "source.txt")
	target := filepath.Join(root, "target.txt")

	writeFile(t, source, content)
	writeFile(t, target, content)

	sourceInfo := getFileInfo(t, source)
	targetInfo := getFileInfo(t, target)

	// Lock the target file (simulating another process using it)
	f, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}

	// Try to dedupe - should skip because file is locked
	errCh := make(chan error, 10)
	groups := types.NewDuplicateGroups([]types.DuplicateGroup{
		types.NewDuplicateGroup([]types.SiblingGroup{
			types.NewSiblingGroup([]*types.FileInfo{sourceInfo}),
			types.NewSiblingGroup([]*types.FileInfo{targetInfo}),
		}),
	})

	d := New(groups, nil, false, false, false, false, errCh)
	d.Run()
	close(errCh)

	// Verify error was reported (user should know file was skipped)
	var errCount int
	for range errCh {
		errCount++
	}
	if errCount == 0 {
		t.Error("expected error to be reported when file is locked")
	}

	// Verify target is NOT hardlinked (was skipped due to lock)
	if sameInode(t, source, target) {
		t.Error("locked file should NOT be deduplicated")
	}
}

// =============================================================================
// Symlink Source Existence Tests
// =============================================================================

// TestSymlinkSourceMissing tests that CreateSymlink fails if source doesn't exist.
func TestSymlinkSourceMissing(t *testing.T) {
	root := t.TempDir()

	source := filepath.Join(root, "missing.txt") // doesn't exist
	target := filepath.Join(root, "target.txt")

	writeFile(t, target, []byte("target content"))

	err := CreateSymlink(source, target)
	if err == nil {
		t.Error("CreateSymlink should fail when source is missing")
	}

	// Verify target is unchanged (original file still exists)
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("target should still exist: %v", err)
	}
	if string(data) != "target content" {
		t.Error("target content should be unchanged")
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

func getFileInfo(t *testing.T, path string) *types.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat %s: %v", path, err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	return &types.FileInfo{
		Path:    path,
		Size:    info.Size(),
		ModTime: info.ModTime(),
		Dev:     uint64(stat.Dev),
		Ino:     stat.Ino,
		Nlink:   uint32(stat.Nlink),
	}
}

func writeFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustLink(t *testing.T, oldname, newname string) {
	t.Helper()
	if err := os.Link(oldname, newname); err != nil {
		t.Fatal(err)
	}
}

func setMtime(t *testing.T, path string, mtime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func sameInode(t *testing.T, path1, path2 string) bool {
	t.Helper()
	stat1, err := os.Stat(path1)
	if err != nil {
		t.Fatal(err)
	}
	stat2, err := os.Stat(path2)
	if err != nil {
		t.Fatal(err)
	}
	return stat1.Sys().(*syscall.Stat_t).Ino == stat2.Sys().(*syscall.Stat_t).Ino
}
