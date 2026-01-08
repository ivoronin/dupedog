//go:build unix

package scanner

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// =============================================================================
// Section 2.1: Critical Bug Tests (P0) - Invalid Glob Patterns
// =============================================================================

// TestInvalidGlobPatternUnclosedBracket tests that unclosed bracket patterns
// are handled gracefully by the scanner when called directly.
// Note: CLI validates patterns via validateGlobPatterns() before calling scanner.
// This test verifies scanner doesn't crash on invalid patterns if passed directly.
func TestInvalidGlobPatternUnclosedBracket(t *testing.T) {
	root := t.TempDir()

	// Create test files
	createFile(t, filepath.Join(root, "file.txt"), 100)
	createFile(t, filepath.Join(root, "[bracket.txt"), 100)

	// Run scanner with invalid pattern
	// Scanner tolerates invalid patterns (no exclusion applied) since CLI validates upfront
	s := New([]string{root}, 0, []string{"[invalid"}, 2, false, nil)
	files := s.Run()

	// Both files should be returned since invalid pattern doesn't match anything
	if len(files) != 2 {
		t.Errorf("expected 2 files (invalid pattern skipped), got %d", len(files))
	}
}

// TestInvalidGlobPatternTripleAsterisk tests that *** pattern excludes all files.
// *** is a valid pattern in filepath.Match that matches any filename.
func TestInvalidGlobPatternTripleAsterisk(t *testing.T) {
	root := t.TempDir()

	// Create test file
	createFile(t, filepath.Join(root, "file.txt"), 100)

	// *** matches everything, so file should be excluded
	s := New([]string{root}, 0, []string{"***"}, 2, false, nil)
	files := s.Run()

	if len(files) != 0 {
		t.Errorf("expected 0 files (*** excludes all), got %d", len(files))
	}
}

// =============================================================================
// Section 3.1: Core Scanner Tests
// =============================================================================

// TestListDirectoryBasic tests basic directory listing functionality.
func TestListDirectoryBasic(t *testing.T) {
	root := t.TempDir()

	// Create files and subdirectory
	createFile(t, filepath.Join(root, "file1.txt"), 100)
	createFile(t, filepath.Join(root, "file2.txt"), 200)
	if err := os.Mkdir(filepath.Join(root, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	createFile(t, filepath.Join(root, "subdir", "file3.txt"), 300)

	s := New([]string{root}, 0, nil, 2, false, nil)
	files := s.Run()

	if len(files) != 3 {
		t.Errorf("expected 3 files, got %d", len(files))
	}

	// Verify sizes
	sizes := make(map[int64]bool)
	for _, f := range files {
		sizes[f.Size] = true
	}
	for _, expected := range []int64{100, 200, 300} {
		if !sizes[expected] {
			t.Errorf("missing file with size %d", expected)
		}
	}
}

// TestSizeFilteringZeroBytes tests that zero-byte files are handled based on minSize.
func TestSizeFilteringZeroBytes(t *testing.T) {
	root := t.TempDir()

	// Create zero-byte and non-zero files
	createFile(t, filepath.Join(root, "empty.txt"), 0)
	createFile(t, filepath.Join(root, "small.txt"), 1)
	createFile(t, filepath.Join(root, "normal.txt"), 100)

	// Test with minSize=0 (include all)
	s := New([]string{root}, 0, nil, 2, false, nil)
	files := s.Run()
	if len(files) != 3 {
		t.Errorf("minSize=0: expected 3 files, got %d", len(files))
	}

	// Test with minSize=1 (exclude zero-byte)
	s = New([]string{root}, 1, nil, 2, false, nil)
	files = s.Run()
	if len(files) != 2 {
		t.Errorf("minSize=1: expected 2 files, got %d", len(files))
	}

	// Test with minSize=100 (only normal.txt)
	s = New([]string{root}, 100, nil, 2, false, nil)
	files = s.Run()
	if len(files) != 1 {
		t.Errorf("minSize=100: expected 1 file, got %d", len(files))
	}
}

// TestSizeFilteringBoundaryValues tests size filtering at boundary values.
func TestSizeFilteringBoundaryValues(t *testing.T) {
	root := t.TempDir()

	// Create files at exact boundaries
	createFile(t, filepath.Join(root, "size99.txt"), 99)
	createFile(t, filepath.Join(root, "size100.txt"), 100)
	createFile(t, filepath.Join(root, "size101.txt"), 101)

	// minSize=100 should include 100 and 101
	s := New([]string{root}, 100, nil, 2, false, nil)
	files := s.Run()
	if len(files) != 2 {
		t.Errorf("expected 2 files (>=100), got %d", len(files))
	}
}

// TestGlobPatternExclusion tests that glob patterns correctly exclude files.
func TestGlobPatternExclusion(t *testing.T) {
	root := t.TempDir()

	createFile(t, filepath.Join(root, "keep.txt"), 100)
	createFile(t, filepath.Join(root, "exclude.tmp"), 100)
	createFile(t, filepath.Join(root, "exclude.bak"), 100)

	// Exclude *.tmp and *.bak
	s := New([]string{root}, 0, []string{"*.tmp", "*.bak"}, 2, false, nil)
	files := s.Run()

	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}
	if len(files) > 0 && filepath.Base(files[0].Path) != "keep.txt" {
		t.Errorf("wrong file kept: %s", files[0].Path)
	}
}

// TestDirectoryExclusionGit tests that --exclude .git skips .git directories entirely.
func TestDirectoryExclusionGit(t *testing.T) {
	root := t.TempDir()

	// Create regular file
	createFile(t, filepath.Join(root, "main.go"), 100)

	// Create .git directory with files inside
	gitDir := filepath.Join(root, ".git")
	if err := os.Mkdir(gitDir, 0o755); err != nil {
		t.Fatal(err)
	}
	createFile(t, filepath.Join(gitDir, "config"), 50)
	createFile(t, filepath.Join(gitDir, "HEAD"), 30)

	// Create nested .git/objects directory
	objectsDir := filepath.Join(gitDir, "objects")
	if err := os.Mkdir(objectsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	createFile(t, filepath.Join(objectsDir, "pack"), 200)

	// Scan with --exclude .git
	s := New([]string{root}, 0, []string{".git"}, 2, false, nil)
	files := s.Run()

	// Should only find main.go, not any .git files
	if len(files) != 1 {
		t.Errorf("expected 1 file (main.go only), got %d", len(files))
		for _, f := range files {
			t.Logf("  found: %s", f.Path)
		}
	}
	if len(files) > 0 && filepath.Base(files[0].Path) != "main.go" {
		t.Errorf("expected main.go, got %s", files[0].Path)
	}
}

// TestPermissionErrorHandling tests that scanner continues when directories are unreadable.
func TestPermissionErrorHandling(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()

	// Create accessible file
	createFile(t, filepath.Join(root, "accessible.txt"), 100)

	// Create unreadable directory
	unreadable := filepath.Join(root, "unreadable")
	if err := os.Mkdir(unreadable, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(unreadable, 0o755) }() // Cleanup

	errCh := make(chan error, 10)
	s := New([]string{root}, 0, nil, 2, false, errCh)
	files := s.Run()
	close(errCh)

	// Should still find the accessible file
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}

	// Should have reported an error
	var errCount int
	for range errCh {
		errCount++
	}
	if errCount == 0 {
		t.Error("expected permission error to be reported")
	}
}

// =============================================================================
// Section 3.2: Scanner Filesystem Edge Cases
// =============================================================================

// TestZeroBytesFilesHandling tests zero-byte file handling with minSize=0.
func TestZeroBytesFilesHandling(t *testing.T) {
	root := t.TempDir()

	// Create multiple zero-byte files
	createFile(t, filepath.Join(root, "empty1.txt"), 0)
	createFile(t, filepath.Join(root, "empty2.txt"), 0)

	s := New([]string{root}, 0, nil, 2, false, nil)
	files := s.Run()

	if len(files) != 2 {
		t.Errorf("expected 2 zero-byte files, got %d", len(files))
	}
	for _, f := range files {
		if f.Size != 0 {
			t.Errorf("expected size 0, got %d", f.Size)
		}
	}
}

// TestGlobPatternMatchesBasenameOnly verifies patterns match basename, not full path.
// Patterns match both files AND directories by their basename.
func TestGlobPatternMatchesBasenameOnly(t *testing.T) {
	root := t.TempDir()

	// Create directories and files to test basename matching
	keepDir := filepath.Join(root, "keepdir")
	if err := os.Mkdir(keepDir, 0o755); err != nil {
		t.Fatal(err)
	}
	createFile(t, filepath.Join(keepDir, "keep.txt"), 100)

	// Create directory that should be excluded by name
	excludeDir := filepath.Join(root, "skipme")
	if err := os.Mkdir(excludeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	createFile(t, filepath.Join(excludeDir, "hidden.txt"), 100)

	// Create file named "skipme" (should also be excluded)
	createFile(t, filepath.Join(keepDir, "skipme"), 100)

	// Pattern "skipme" excludes both directories AND files named "skipme"
	s := New([]string{root}, 0, []string{"skipme"}, 2, false, nil)
	files := s.Run()

	// Only keepdir/keep.txt should be found
	// - skipme/ directory is excluded (basename matches)
	// - keepdir/skipme file is excluded (basename matches)
	if len(files) != 1 {
		t.Errorf("expected 1 file (keep.txt), got %d", len(files))
		for _, f := range files {
			t.Logf("  found: %s", f.Path)
		}
	}
	if len(files) > 0 && filepath.Base(files[0].Path) != "keep.txt" {
		t.Errorf("expected keep.txt, got %s", files[0].Path)
	}
}

// TestPathIsFileNotDirectory tests scanner behavior when given a file path instead of directory.
// Expected: returns 0 files and reports an error (file is not a directory).
func TestPathIsFileNotDirectory(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "file.txt")
	createFile(t, filePath, 100)

	errCh := make(chan error, 10)
	s := New([]string{filePath}, 0, nil, 2, false, errCh)
	files := s.Run()
	close(errCh)

	// Should return 0 files (file path is not a directory)
	if len(files) != 0 {
		t.Errorf("expected 0 files for file path, got %d", len(files))
	}

	// Should report an error
	var errCount int
	for range errCh {
		errCount++
	}
	if errCount == 0 {
		t.Error("expected error when scanning file path instead of directory")
	}
}

// TestNonExistentPathHandling tests scanner behavior with non-existent paths.
func TestNonExistentPathHandling(t *testing.T) {
	root := t.TempDir()
	nonExistent := filepath.Join(root, "does-not-exist")

	errCh := make(chan error, 10)
	s := New([]string{nonExistent}, 0, nil, 2, false, errCh)
	files := s.Run()
	close(errCh)

	if len(files) != 0 {
		t.Errorf("expected 0 files for non-existent path, got %d", len(files))
	}

	var errCount int
	for range errCh {
		errCount++
	}
	if errCount == 0 {
		t.Error("expected error for non-existent path")
	}
}

// TestOverlappingPaths tests that overlapping paths produce duplicate entries.
// Note: Scanner returns duplicates; screener groups by inode to handle this.
func TestOverlappingPaths(t *testing.T) {
	root := t.TempDir()

	// Create nested structure
	subdir := filepath.Join(root, "subdir")
	if err := os.Mkdir(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	createFile(t, filepath.Join(root, "file1.txt"), 100)
	createFile(t, filepath.Join(subdir, "file2.txt"), 100)

	// Scan both root and subdir (overlapping)
	s := New([]string{root, subdir}, 0, nil, 2, false, nil)
	files := s.Run()

	// file2.txt will be scanned twice - once from root, once from subdir
	// Expected: 3 file entries (file1 + file2 twice)
	if len(files) != 3 {
		t.Errorf("expected 3 file entries (overlapping paths), got %d", len(files))
	}

	// But only 2 unique inodes
	inodes := make(map[uint64]bool)
	for _, f := range files {
		inodes[f.Ino] = true
	}
	if len(inodes) != 2 {
		t.Errorf("expected 2 unique inodes, got %d", len(inodes))
	}
}

// TestDuplicatePaths tests that duplicate paths produce duplicate entries.
// Note: Scanner returns duplicates; screener groups by inode to handle this.
func TestDuplicatePaths(t *testing.T) {
	root := t.TempDir()
	createFile(t, filepath.Join(root, "file.txt"), 100)

	// Scan same path twice
	s := New([]string{root, root}, 0, nil, 2, false, nil)
	files := s.Run()

	// Expected: 2 file entries (same file scanned twice)
	if len(files) != 2 {
		t.Errorf("expected 2 file entries (duplicate paths), got %d", len(files))
	}
}

// TestNonRegularFilesSkipped tests that symlinks, FIFOs, and sockets are skipped.
func TestNonRegularFilesSkipped(t *testing.T) {
	root := t.TempDir()

	// Create regular file
	regularFile := filepath.Join(root, "regular.txt")
	createFile(t, regularFile, 100)

	// Create symlink
	symlink := filepath.Join(root, "symlink.txt")
	if err := os.Symlink(regularFile, symlink); err != nil {
		t.Fatal(err)
	}

	// Create FIFO (named pipe)
	fifo := filepath.Join(root, "fifo")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Logf("Skipping FIFO test: %v", err)
	}

	s := New([]string{root}, 0, nil, 2, false, nil)
	files := s.Run()

	// Should only find regular file
	if len(files) != 1 {
		t.Errorf("expected 1 regular file, got %d", len(files))
	}
	if len(files) > 0 && filepath.Base(files[0].Path) != "regular.txt" {
		t.Errorf("expected regular.txt, got %s", files[0].Path)
	}
}

// TestFilenamesWithSpecialChars tests files with special characters in names.
func TestFilenamesWithSpecialChars(t *testing.T) {
	root := t.TempDir()

	// Create files with special characters
	specialNames := []string{
		"file with spaces.txt",
		"file\twith\ttabs.txt",
		"unicode_日本語.txt",
		"quotes'and\"double.txt",
	}

	for _, name := range specialNames {
		createFile(t, filepath.Join(root, name), 100)
	}

	s := New([]string{root}, 0, nil, 2, false, nil)
	files := s.Run()

	if len(files) != len(specialNames) {
		t.Errorf("expected %d files, got %d", len(specialNames), len(files))
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

func createFile(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	content := make([]byte, size)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

