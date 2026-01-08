# dupedog

A high-performance duplicate file finder and deduplicator for Unix systems. Reclaims disk space by replacing duplicate files with hardlinks (or symlinks for cross-device scenarios).

```
$ dupedog dedupe --dry-run /data/ --exclude .git --min-size 1g --verbose
✔ Scanned 2643338 (18 TiB), matched 1099 files (14 TiB) in 290.9s
✔ Selected 946 candidates (13 TiB) in 0.0s
✔ Verified 8.0 TiB + skipped 5.1 TiB out of 13 TiB (100%), confirmed 416 duplicates (5.1 TiB) in 241 sets in 1h13m20.24s
✔ Deduplicated 416/416 files in 241/241 sets (100%), saved 5.1 TiB in 1.9s
```

## Features

- **Parallel scanning** - concurrent directory traversal with configurable worker pool
- **Progressive verification** - eliminates non-duplicates early using staged hashing
- **Atomic operations** - hardlink creation via temp file + rename pattern
- **Cross-device support** - optional symlink fallback when hardlinks aren't possible

## Installation

### Docker

```bash
docker run --rm -v /data:/data ghcr.io/ivoronin/dupedog dedupe --dry-run /data
```

### Binary releases

Download pre-built binaries from [Releases](https://github.com/ivoronin/dupedog/releases).

### Homebrew

```bash
brew install ivoronin/ivoronin/dupedog
```

### From source

```bash
go install github.com/ivoronin/dupedog@latest
```

## Usage

```bash
dupedog dedupe [flags] <paths...>
```

### Examples

```bash
# Preview deduplication
dupedog dedupe --dry-run /data

# Deduplicate with minimum size filter
dupedog dedupe --min-size 1M /backup

# File and subdirectory exclude patterns
dupedog dedupe --exclude "*.tmp" --exclude ".git" /projects

# Cross-device with symlink fallback
dupedog dedupe --symlink-fallback /volume1 /volume2

# Path priority: duplicates found in later paths are replaced with links to earlier paths
dupedog dedupe --symlink-fallback /mnt/important /mnt/archive /mnt/copies
```

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--min-size` | `-m` | `1` | Minimum file size (supports K, M, G suffixes) |
| `--exclude` | `-e` | - | Glob patterns to exclude (repeatable) |
| `--workers` | `-w` | `runtime.NumCPU()` | Parallel workers for scanning and hashing |
| `--dry-run` | `-n` | `false` | Preview changes without executing |
| `--verbose` | `-v` | `false` | Log individual file operations |
| `--no-progress` | - | `false` | Disable progress bar |
| `--symlink-fallback` | - | `false` | Use symlinks for cross-device deduplication |
| `--trust-device-boundaries` | - | `false` | Assume devices have independent inode spaces (unsafe with NFS) |

## How It Works

### Pipeline Overview

```
SCAN → SCREEN → VERIFY → DEDUPE
```

1. **Scan**: Parallel directory traversal, filtering by size and exclude patterns
2. **Screen**: Group files by size, then by inode - only groups with 2+ unique inodes proceed
3. **Verify**: Progressive content verification using SHA-256 (see below)
4. **Dedupe**: Atomic replacement with hardlinks (or symlinks for cross-device)

### Progressive Verification

The verification phase uses a staged hashing strategy to minimize I/O. Instead of reading entire files upfront, dupedog hashes files in stages:

1. **HEAD** - first 1 MB
2. **TAIL** - last 1 MB
3. **CHUNKS** - sequential 1 GB blocks (only if HEAD+TAIL match)

Files are eliminated as soon as their hash diverges from the group. In practice, most non-duplicates are eliminated after just 2 MB of I/O, because files that differ typically differ at the beginning or end.

### Device Boundaries and Inode Grouping

dupedog needs to identify which files are already hardlinked (pointing to the same inode) to avoid redundant hashing. This is controlled by `--trust-device-boundaries`:

**Default behavior (`--trust-device-boundaries=false`):**

Files are grouped by inode number only, ignoring device ID. This is safe for NFS and other network filesystems where the same physical file can appear with different device IDs depending on how it's mounted.

```
/mnt/nfs-a/file.txt  (dev=100, ino=12345)  ─┐
/mnt/nfs-b/file.txt  (dev=200, ino=12345)  ─┴─► Same inode 12345 → treated as same file
```

**With `--trust-device-boundaries`:**

Files are grouped by (device, inode) pair. This assumes each device has an independent inode namespace - inode 12345 on device A is unrelated to inode 12345 on device B.

```
/dev/sda1/file.txt   (dev=100, ino=12345)  ─► device 100, inode 12345
/dev/sdb1/other.txt  (dev=200, ino=12345)  ─► device 200, inode 12345 (different file)
```

**When to use `--trust-device-boundaries`:**
- Multiple local disks with separate filesystems
- USB drives or external storage
- Virtual machine disk images mounted separately

**When NOT to use it (keep default):**
- NFS mounts (same export mounted at multiple paths)
- Network storage with multiple access paths
- Any scenario where the same filesystem might appear as different devices

## Development

```bash
make build       # Build binary
make test        # Run unit tests
make test-e2e    # Run E2E tests (requires Docker)
make lint        # Run linter
```

## License

MIT License - see [LICENSE](LICENSE) for details.
