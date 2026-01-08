//go:build unix

package testfs

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// -----------------------------------------------------------------------------
// Reap Operations - Capture filesystem state
// -----------------------------------------------------------------------------

// ReapPaths captures the filesystem state for the given paths.
//
// Each path becomes a ReapVolume with files grouped by inode (hardlinks)
// and symlinks captured with their targets.
//
// The root parameter specifies the base directory to subtract from paths.
// For E2E tests, root is "" or "/" so paths are used as-is.
// For integration tests, root is t.TempDir() so logical paths are computed.
func ReapPaths(root string, paths []string) (*ReapResult, error) {
	result := &ReapResult{}

	for _, path := range paths {
		// Determine actual path to scan
		actualPath := path
		if root != "" && root != "/" {
			actualPath = filepath.Join(root, path)
		}

		vol, err := reapPath(actualPath, path)
		if err != nil {
			return nil, fmt.Errorf("reap %s: %w", path, err)
		}
		result.Volumes = append(result.Volumes, vol)
	}

	return result, nil
}

// ReapToWriter captures filesystem state and writes JSON to the writer.
// Used by testfs-helper CLI tool to write to stdout.
func ReapToWriter(w io.Writer, paths []string) error {
	result, err := ReapPaths("", paths)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

// reapPath scans a directory and returns its state.
// rootPath is the actual filesystem path to scan.
// logicalPath is the path to report in the result (for volume name).
func reapPath(rootPath, logicalPath string) (ReapVolume, error) {
	vol := ReapVolume{
		Name: logicalPath, // Use logical path for volume name
	}

	// Map inodes to paths for hardlink grouping
	inodeToFile := make(map[uint64]*ReapFile)

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == rootPath {
			return nil // Skip root
		}

		relPath, _ := filepath.Rel(rootPath, path)

		// Handle symlinks - must check before IsDir since Lstat is used
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("readlink %s: %w", path, err)
			}
			vol.Symlinks = append(vol.Symlinks, ReapSymlink{
				Path:   relPath,
				Target: target,
			})
			return nil
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get inode info via syscall
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("cannot get stat for %s", path)
		}

		inode := stat.Ino
		nlink := uint64(stat.Nlink) //nolint:unconvert // platform-dependent type

		// Group by inode (hardlinks)
		if existing, ok := inodeToFile[inode]; ok {
			existing.Path = append(existing.Path, relPath)
		} else {
			rf := &ReapFile{
				Path:  []string{relPath},
				Inode: inode,
				Nlink: nlink,
				Size:  info.Size(),
			}
			inodeToFile[inode] = rf
		}

		return nil
	})

	if err != nil {
		return vol, err
	}

	// Build final files list from inode map
	// This ensures all paths sharing an inode are grouped together
	vol.Files = nil
	for _, rf := range inodeToFile {
		vol.Files = append(vol.Files, *rf)
	}

	return vol, nil
}
