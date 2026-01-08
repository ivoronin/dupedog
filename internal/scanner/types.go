package scanner

import (
	"os"
	"syscall"

	"github.com/ivoronin/dupedog/internal/types"
)

// newFileInfo creates FileInfo from os.FileInfo and path.
func newFileInfo(path string, info os.FileInfo) *types.FileInfo {
	stat := info.Sys().(*syscall.Stat_t)
	return &types.FileInfo{
		Path:    path,
		Size:    info.Size(),
		ModTime: info.ModTime(),
		Dev:     uint64(stat.Dev), //nolint:unconvert // platform-dependent type
		Ino:     stat.Ino,
		Nlink:   uint32(stat.Nlink),
	}
}
