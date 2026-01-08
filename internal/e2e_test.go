//go:build e2e

package internal

import (
	"testing"

	"github.com/ivoronin/dupedog/internal/testfs"
)

// =============================================================================
// Section 9.1: Core E2E Tests
// =============================================================================

// TestE2EBasicCLIInvocation tests basic CLI invocation and exit codes.
func TestE2EBasicCLIInvocation(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'A', Size: "1KiB"}}},
					{Path: []string{"b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'A', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run dupedog and check exit code 0
	result := h.RunDupedog("dedupe", "/data")

	expected := testfs.FileTree{
		ExitCode: 0,
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"a.txt", "b.txt"}},
				},
			},
		},
	}
	h.Assert(expected)

	// Verify some output was produced
	if len(result.Stdout) == 0 && len(result.Stderr) == 0 {
		t.Log("Note: no stdout or stderr output")
	}
}

// TestE2EDryRun tests dry-run mode.
func TestE2EDryRun(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'A', Size: "1KiB"}}},
					{Path: []string{"b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'A', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run with --dry-run - files should remain unchanged
	h.RunDupedog("dedupe", "--dry-run", "/data")

	// Files should NOT be hardlinked (different entries = different inodes expected)
	expected := testfs.FileTree{
		ExitCode: 0,
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"a.txt"}},
					{Path: []string{"b.txt"}},
				},
			},
		},
	}
	h.Assert(expected)
}

// =============================================================================
// Section 9.2: Symlink Fallback E2E Tests
// =============================================================================

// TestE2ESymlinkFallbackEnabled tests cross-device deduplication with --symlink-fallback.
func TestE2ESymlinkFallbackEnabled(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/vol1",
				Files: []testfs.File{
					{Path: []string{"source.txt"}, Chunks: []testfs.Chunk{{Pattern: 'A', Size: "1KiB"}}},
				},
			},
			{
				MountPoint: "/vol2",
				Files: []testfs.File{
					{Path: []string{"target.txt"}, Chunks: []testfs.Chunk{{Pattern: 'A', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run with --symlink-fallback flag
	// Note: --trust-device-boundaries is needed because tmpfs volumes have independent inode spaces
	h.RunDupedog("dedupe", "--symlink-fallback", "--trust-device-boundaries", "/vol1", "/vol2")

	// target.txt should be replaced with symlink to source.txt
	expected := testfs.FileTree{
		ExitCode: 0,
		Volumes: []testfs.Volume{
			{
				MountPoint: "/vol1",
				Files: []testfs.File{
					{Path: []string{"source.txt"}},
				},
			},
			{
				MountPoint: "/vol2",
				Symlinks: []testfs.Symlink{
					{Path: "target.txt", Target: "../vol1/source.txt"},
				},
			},
		},
	}
	h.Assert(expected)
}

// TestE2ESymlinkFallbackDisabled tests that cross-device duplicates are skipped without flag.
func TestE2ESymlinkFallbackDisabled(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/vol1",
				Files: []testfs.File{
					{Path: []string{"a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'A', Size: "1KiB"}}},
				},
			},
			{
				MountPoint: "/vol2",
				Files: []testfs.File{
					{Path: []string{"b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'A', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Run WITHOUT --symlink-fallback flag
	h.RunDupedog("dedupe", "/vol1", "/vol2")

	// Files should remain unchanged (cross-device skipped)
	expected := testfs.FileTree{
		ExitCode: 0,
		Volumes: []testfs.Volume{
			{
				MountPoint: "/vol1",
				Files: []testfs.File{
					{Path: []string{"a.txt"}},
				},
			},
			{
				MountPoint: "/vol2",
				Files: []testfs.File{
					{Path: []string{"b.txt"}},
				},
			},
		},
	}
	h.Assert(expected)
}

// =============================================================================
// Section 9.3: Nested Mount E2E Tests (CRITICAL)
// =============================================================================

// TestE2ENestedMounts tests scanning nested mounts without self-dedup.
func TestE2ENestedMounts(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"root.txt"}, Chunks: []testfs.Chunk{{Pattern: 'R', Size: "1KiB"}}},
				},
			},
			{
				MountPoint: "/data/subdir",
				Files: []testfs.File{
					{Path: []string{"nested.txt"}, Chunks: []testfs.Chunk{{Pattern: 'R', Size: "1KiB"}}}, // Same content
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Scan /data which includes /data/subdir
	h.RunDupedog("dedupe", "--symlink-fallback", "/data")

	// Files should be deduplicated (nested.txt should become symlink)
	expected := testfs.FileTree{
		ExitCode: 0,
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"root.txt"}},
				},
			},
			{
				MountPoint: "/data/subdir",
				Symlinks: []testfs.Symlink{
					{Path: "nested.txt", Target: "../root.txt"},
				},
			},
		},
	}
	h.Assert(expected)
}

// =============================================================================
// Section 9.4: Path Priority E2E Tests (CRITICAL)
// =============================================================================

// TestE2EPathPriorityFirstWins tests that first CLI path becomes source.
func TestE2EPathPriorityFirstWins(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/priority",
				Files: []testfs.File{
					{Path: []string{"source.txt"}, Chunks: []testfs.Chunk{{Pattern: 'S', Size: "1KiB"}}},
				},
			},
			{
				MountPoint: "/secondary",
				Files: []testfs.File{
					{Path: []string{"target.txt"}, Chunks: []testfs.Chunk{{Pattern: 'S', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// /priority is first, so source.txt should be kept
	// Note: --trust-device-boundaries is needed because tmpfs volumes have independent inode spaces
	h.RunDupedog("dedupe", "--symlink-fallback", "--trust-device-boundaries", "/priority", "/secondary")

	expected := testfs.FileTree{
		ExitCode: 0,
		Volumes: []testfs.Volume{
			{
				MountPoint: "/priority",
				Files: []testfs.File{
					{Path: []string{"source.txt"}},
				},
			},
			{
				MountPoint: "/secondary",
				Symlinks: []testfs.Symlink{
					{Path: "target.txt", Target: "../priority/source.txt"},
				},
			},
		},
	}
	h.Assert(expected)
}

// TestE2EPathPriorityReversed tests path priority with reversed argument order.
func TestE2EPathPriorityReversed(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/priority",
				Files: []testfs.File{
					{Path: []string{"a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'X', Size: "1KiB"}}},
				},
			},
			{
				MountPoint: "/secondary",
				Files: []testfs.File{
					{Path: []string{"b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'X', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// /secondary is first this time, so b.txt should be kept
	// Note: --trust-device-boundaries is needed because tmpfs volumes have independent inode spaces
	h.RunDupedog("dedupe", "--symlink-fallback", "--trust-device-boundaries", "/secondary", "/priority")

	expected := testfs.FileTree{
		ExitCode: 0,
		Volumes: []testfs.Volume{
			{
				MountPoint: "/priority",
				Symlinks: []testfs.Symlink{
					{Path: "a.txt", Target: "../secondary/b.txt"},
				},
			},
			{
				MountPoint: "/secondary",
				Files: []testfs.File{
					{Path: []string{"b.txt"}},
				},
			},
		},
	}
	h.Assert(expected)
}

// TestE2EMinSizeFlag tests --min-size filtering in E2E.
func TestE2EMinSizeFlag(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"small_a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'S', Size: "100"}}},
					{Path: []string{"small_b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'S', Size: "100"}}},
					{Path: []string{"large_a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'L', Size: "10KiB"}}},
					{Path: []string{"large_b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'L', Size: "10KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Only files >= 1KiB should be deduplicated
	h.RunDupedog("dedupe", "--min-size", "1KiB", "/data")

	expected := testfs.FileTree{
		ExitCode: 0,
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					// Small files unchanged (different inodes)
					{Path: []string{"small_a.txt"}},
					{Path: []string{"small_b.txt"}},
					// Large files hardlinked (same inode)
					{Path: []string{"large_a.txt", "large_b.txt"}},
				},
			},
		},
	}
	h.Assert(expected)
}

// TestE2EExcludePattern tests --exclude pattern filtering.
func TestE2EExcludePattern(t *testing.T) {
	spec := testfs.FileTree{
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					{Path: []string{"keep_a.txt"}, Chunks: []testfs.Chunk{{Pattern: 'K', Size: "1KiB"}}},
					{Path: []string{"keep_b.txt"}, Chunks: []testfs.Chunk{{Pattern: 'K', Size: "1KiB"}}},
					{Path: []string{"skip_a.bak"}, Chunks: []testfs.Chunk{{Pattern: 'K', Size: "1KiB"}}},
					{Path: []string{"skip_b.bak"}, Chunks: []testfs.Chunk{{Pattern: 'K', Size: "1KiB"}}},
				},
			},
		},
	}

	h := testfs.New(t, spec)

	// Exclude *.bak files
	h.RunDupedog("dedupe", "--exclude", "*.bak", "/data")

	expected := testfs.FileTree{
		ExitCode: 0,
		Volumes: []testfs.Volume{
			{
				MountPoint: "/data",
				Files: []testfs.File{
					// .txt files hardlinked
					{Path: []string{"keep_a.txt", "keep_b.txt"}},
					// .bak files unchanged (different inodes)
					{Path: []string{"skip_a.bak"}},
					{Path: []string{"skip_b.bak"}},
				},
			},
		},
	}
	h.Assert(expected)
}

