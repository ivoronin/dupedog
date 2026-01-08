//go:build unix && !e2e

package testfs

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestSowCreatesFilesCorrectly verifies that SowFileTree creates files with correct sizes and content.
func TestSowCreatesFilesCorrectly(t *testing.T) {
	root := t.TempDir()

	spec := FileTree{
		Volumes: []Volume{
			{
				MountPoint: "/vol1",
				Files: []File{
					{Path: []string{"a.txt"}, Chunks: []Chunk{{Pattern: 'A', Size: "100"}}},
					{Path: []string{"b.txt"}, Chunks: []Chunk{{Pattern: 'B', Size: "50"}}},
				},
			},
		},
	}

	if err := SowFileTree(root, spec); err != nil {
		t.Fatalf("SowFileTree failed: %v", err)
	}

	// Verify file a.txt
	pathA := filepath.Join(root, "vol1", "a.txt")
	contentA, err := os.ReadFile(pathA)
	if err != nil {
		t.Fatalf("failed to read a.txt: %v", err)
	}
	if len(contentA) != 100 {
		t.Errorf("a.txt size: got %d, want 100", len(contentA))
	}
	// Content should be all 'A' bytes
	for i, b := range contentA {
		if b != 'A' {
			t.Errorf("a.txt content[%d]: got %q, want 'A'", i, b)
			break
		}
	}

	// Verify file b.txt
	pathB := filepath.Join(root, "vol1", "b.txt")
	contentB, err := os.ReadFile(pathB)
	if err != nil {
		t.Fatalf("failed to read b.txt: %v", err)
	}
	if len(contentB) != 50 {
		t.Errorf("b.txt size: got %d, want 50", len(contentB))
	}
	// Content should be all 'B' bytes
	for i, b := range contentB {
		if b != 'B' {
			t.Errorf("b.txt content[%d]: got %q, want 'B'", i, b)
			break
		}
	}
}

// TestSowCreatesHardlinksCorrectly verifies that multiple paths in a File entry share the same inode.
func TestSowCreatesHardlinksCorrectly(t *testing.T) {
	root := t.TempDir()

	spec := FileTree{
		Volumes: []Volume{
			{
				MountPoint: "/vol1",
				Files: []File{
					{Path: []string{"original.txt", "link1.txt", "subdir/link2.txt"}, Chunks: []Chunk{{Pattern: 'S', Size: "100"}}},
				},
			},
		},
	}

	if err := SowFileTree(root, spec); err != nil {
		t.Fatalf("SowFileTree failed: %v", err)
	}

	// Get inodes for all paths
	paths := []string{
		filepath.Join(root, "vol1", "original.txt"),
		filepath.Join(root, "vol1", "link1.txt"),
		filepath.Join(root, "vol1", "subdir", "link2.txt"),
	}

	var inodes []uint64
	for _, p := range paths {
		info, err := os.Lstat(p)
		if err != nil {
			t.Fatalf("failed to stat %s: %v", p, err)
		}
		stat := info.Sys().(*syscall.Stat_t)
		inodes = append(inodes, stat.Ino)
	}

	// All paths must share the same inode
	for i := 1; i < len(inodes); i++ {
		if inodes[i] != inodes[0] {
			t.Errorf("hardlink mismatch: %s (inode %d) != %s (inode %d)",
				paths[i], inodes[i], paths[0], inodes[0])
		}
	}

	// Verify nlink count
	info, _ := os.Lstat(paths[0])
	stat := info.Sys().(*syscall.Stat_t)
	if stat.Nlink != 3 {
		t.Errorf("nlink: got %d, want 3", stat.Nlink)
	}
}

// TestSowCreatesSymlinksCorrectly verifies that symlinks are created with correct targets.
func TestSowCreatesSymlinksCorrectly(t *testing.T) {
	root := t.TempDir()

	spec := FileTree{
		Volumes: []Volume{
			{
				MountPoint: "/vol1",
				Files: []File{
					{Path: []string{"target.txt"}, Chunks: []Chunk{{Pattern: 'T', Size: "100"}}},
				},
				Symlinks: []Symlink{
					{Path: "link.txt", Target: "target.txt"},
					{Path: "subdir/link2.txt", Target: "../target.txt"},
				},
			},
		},
	}

	if err := SowFileTree(root, spec); err != nil {
		t.Fatalf("SowFileTree failed: %v", err)
	}

	// Verify symlink1
	link1Path := filepath.Join(root, "vol1", "link.txt")
	target1, err := os.Readlink(link1Path)
	if err != nil {
		t.Fatalf("failed to readlink %s: %v", link1Path, err)
	}
	if target1 != "target.txt" {
		t.Errorf("link.txt target: got %q, want %q", target1, "target.txt")
	}

	// Verify symlink2
	link2Path := filepath.Join(root, "vol1", "subdir", "link2.txt")
	target2, err := os.Readlink(link2Path)
	if err != nil {
		t.Fatalf("failed to readlink %s: %v", link2Path, err)
	}
	if target2 != "../target.txt" {
		t.Errorf("subdir/link2.txt target: got %q, want %q", target2, "../target.txt")
	}
}

// TestAssertDetectsMismatches verifies that Assert correctly detects filesystem mismatches.
func TestAssertDetectsMismatches(t *testing.T) {
	// Create a filesystem state
	root := t.TempDir()
	spec := FileTree{
		Volumes: []Volume{
			{
				MountPoint: "/vol1",
				Files: []File{
					{Path: []string{"a.txt"}, Chunks: []Chunk{{Pattern: 'A', Size: "100"}}},
					{Path: []string{"b.txt"}, Chunks: []Chunk{{Pattern: 'B', Size: "100"}}},
				},
			},
		},
	}

	if err := SowFileTree(root, spec); err != nil {
		t.Fatalf("SowFileTree failed: %v", err)
	}

	// Test 1: Assert should pass with correct expectation
	h := &Harness{t: t, root: root, given: spec}
	// This should not fail
	h.Assert(spec)

	// Test 2: Assert should detect missing hardlink
	// We expect a.txt and b.txt to be hardlinked, but they're not
	mockT := &testing.T{}
	mockH := &Harness{t: mockT, root: root, given: spec}

	wrongSpec := FileTree{
		Volumes: []Volume{
			{
				MountPoint: "/vol1",
				Files: []File{
					{Path: []string{"a.txt", "b.txt"}}, // Expect hardlink
				},
			},
		},
	}

	// Run assertion on separate mock T to capture failures
	mockH.Assert(wrongSpec)
	if !mockT.Failed() {
		t.Error("Assert should have failed when expecting hardlink between different inodes")
	}
}

// TestAssertDetectsMissingFile verifies that Assert detects missing files.
func TestAssertDetectsMissingFile(t *testing.T) {
	root := t.TempDir()
	spec := FileTree{
		Volumes: []Volume{
			{
				MountPoint: "/vol1",
				Files: []File{
					{Path: []string{"a.txt"}, Chunks: []Chunk{{Pattern: 'A', Size: "100"}}},
				},
			},
		},
	}

	if err := SowFileTree(root, spec); err != nil {
		t.Fatalf("SowFileTree failed: %v", err)
	}

	// Expect a file that doesn't exist
	mockT := &testing.T{}
	mockH := &Harness{t: mockT, root: root, given: spec}

	wrongSpec := FileTree{
		Volumes: []Volume{
			{
				MountPoint: "/vol1",
				Files: []File{
					{Path: []string{"missing.txt"}}, // Doesn't exist
				},
			},
		},
	}

	mockH.Assert(wrongSpec)
	if !mockT.Failed() {
		t.Error("Assert should have failed when expecting missing file")
	}
}

// TestHarnessNew verifies the Harness constructor creates filesystem correctly.
func TestHarnessNew(t *testing.T) {
	spec := FileTree{
		Volumes: []Volume{
			{
				MountPoint: "/data",
				Files: []File{
					{Path: []string{"file1.txt", "file2.txt"}, Chunks: []Chunk{{Pattern: 'S', Size: "1KiB"}}},
				},
			},
		},
	}

	h := New(t, spec)

	// Verify root exists
	if _, err := os.Stat(h.Root()); err != nil {
		t.Fatalf("root directory should exist: %v", err)
	}

	// Verify files are hardlinked
	path1 := filepath.Join(h.Root(), "data", "file1.txt")
	path2 := filepath.Join(h.Root(), "data", "file2.txt")

	info1, err := os.Lstat(path1)
	if err != nil {
		t.Fatalf("failed to stat file1.txt: %v", err)
	}
	info2, err := os.Lstat(path2)
	if err != nil {
		t.Fatalf("failed to stat file2.txt: %v", err)
	}

	stat1 := info1.Sys().(*syscall.Stat_t)
	stat2 := info2.Sys().(*syscall.Stat_t)

	if stat1.Ino != stat2.Ino {
		t.Error("files should share the same inode (hardlinks)")
	}
}

// TestSowMultiChunkContent verifies that multi-chunk content is generated correctly.
func TestSowMultiChunkContent(t *testing.T) {
	root := t.TempDir()

	spec := FileTree{
		Volumes: []Volume{
			{
				MountPoint: "/vol1",
				Files: []File{
					{Path: []string{"multi.txt"}, Chunks: []Chunk{
						{Pattern: 'A', Size: "100"},
						{Pattern: 'B', Size: "100"},
						{Pattern: 'C', Size: "50"},
					}},
				},
			},
		},
	}

	if err := SowFileTree(root, spec); err != nil {
		t.Fatalf("SowFileTree failed: %v", err)
	}

	// Verify file content
	path := filepath.Join(root, "vol1", "multi.txt")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read multi.txt: %v", err)
	}

	// Total size should be 250
	if len(content) != 250 {
		t.Errorf("multi.txt size: got %d, want 250", len(content))
	}

	// First 100 bytes should be 'A'
	for i := 0; i < 100; i++ {
		if content[i] != 'A' {
			t.Errorf("content[%d]: got %q, want 'A'", i, content[i])
			break
		}
	}

	// Next 100 bytes should be 'B'
	for i := 100; i < 200; i++ {
		if content[i] != 'B' {
			t.Errorf("content[%d]: got %q, want 'B'", i, content[i])
			break
		}
	}

	// Last 50 bytes should be 'C'
	for i := 200; i < 250; i++ {
		if content[i] != 'C' {
			t.Errorf("content[%d]: got %q, want 'C'", i, content[i])
			break
		}
	}
}

// TestFileTotalSize verifies the TotalSize method calculates correctly.
func TestFileTotalSize(t *testing.T) {
	tests := []struct {
		name   string
		chunks []Chunk
		want   int64
	}{
		{
			name:   "empty chunks",
			chunks: nil,
			want:   0,
		},
		{
			name:   "single chunk",
			chunks: []Chunk{{Pattern: 'A', Size: "1KiB"}},
			want:   1024,
		},
		{
			name: "multiple chunks",
			chunks: []Chunk{
				{Pattern: 'A', Size: "1KiB"},
				{Pattern: 'B', Size: "1MiB"},
			},
			want: 1024 + 1048576,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := File{Chunks: tt.chunks}
			got := f.TotalSize()
			if got != tt.want {
				t.Errorf("TotalSize() = %d, want %d", got, tt.want)
			}
		})
	}
}
