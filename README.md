# dupedog

Reclaim disk space by replacing duplicate files with hardlinks

[![CI](https://github.com/ivoronin/dupedog/actions/workflows/release.yml/badge.svg)](https://github.com/ivoronin/dupedog/actions/workflows/release.yml)
[![Release](https://img.shields.io/github/v/release/ivoronin/dupedog)](https://github.com/ivoronin/dupedog/releases)

[Overview](#overview) · [Features](#features) · [Installation](#installation) · [Usage](#usage) · [Configuration](#configuration) · [Requirements](#requirements) · [License](#license)

```bash
dupedog dedupe --dry-run /data --exclude .git --min-size 1G --verbose
# Scanned 2643338 (18 TiB), matched 1099 files (14 TiB) in 290.9s
# Selected 946 candidates (13 TiB) in 0.0s
# Verified 8.0 TiB + skipped 5.1 TiB out of 13 TiB (100%), confirmed 416 duplicates (5.1 TiB) in 241 sets in 1h13m20.24s
# Deduplicated 416/416 files in 241/241 sets (100%), saved 5.1 TiB in 1.9s
```

## Overview

dupedog scans directories for duplicate files and replaces them with hardlinks, preserving disk space while maintaining file accessibility at all original paths. Files are identified as duplicates through SHA-256 content verification using a staged hashing strategy that minimizes disk I/O. For cross-device scenarios where hardlinks are not possible, symlink fallback is available.

## Features

- Parallel directory traversal with configurable worker pool (defaults to CPU count)
- Progressive verification: hashes HEAD (1 MB) then TAIL (1 MB) then sequential 1 GB chunks, eliminating non-duplicates early
- Optional hash caching via BoltDB, skipping re-hashing of unchanged files across runs
- Atomic hardlink creation via temp file + rename pattern
- Symlink fallback for cross-device deduplication
- Path priority ordering: duplicates in later paths are replaced with links to files in earlier paths

## Installation

### Docker

```bash
docker run --rm -v /data:/data ghcr.io/ivoronin/dupedog dedupe --dry-run /data
```

### GitHub Releases

Download from [Releases](https://github.com/ivoronin/dupedog/releases).

### Homebrew

```bash
brew install ivoronin/ivoronin/dupedog
```

### From Source

```bash
go install github.com/ivoronin/dupedog@latest
```

## Usage

### Basic Deduplication

```bash
dupedog dedupe /data                          # Deduplicate files in /data
dupedog dedupe --dry-run /data                # Preview changes without executing
dupedog dedupe --min-size 1M /backup          # Only consider files >= 1 MB
```

### Exclude Patterns

```bash
dupedog dedupe --exclude "*.tmp" /data        # Exclude files matching glob pattern
dupedog dedupe --exclude ".git" /projects     # Exclude .git directories
dupedog dedupe -e "*.log" -e "*.tmp" /data    # Multiple patterns (repeatable flag)
```

### Cross-Device Deduplication

```bash
dupedog dedupe --symlink-fallback /volume1 /volume2
```

When deduplicating across different filesystems, hardlinks are not possible. Use `--symlink-fallback` to create symlinks instead.

### Path Priority

```bash
dupedog dedupe --symlink-fallback /mnt/primary /mnt/archive /mnt/copies
```

Path order determines which location keeps the actual data. Duplicates found in later paths are replaced with links pointing to files in earlier paths. In this example, files in `/mnt/primary` are preserved, while duplicates in `/mnt/archive` and `/mnt/copies` become links.

### Hash Caching

```bash
dupedog dedupe --cache-file ~/.cache/dupedog_volume1.db /volume1
```

With `--cache-file`, dupedog remembers file hashes between runs using BoltDB. Unchanged files are verified instantly from cache without disk I/O. Modified files are automatically re-hashed. Use separate cache files for different scan targets.

### Flags Reference

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--min-size` | `-m` | `1` | Minimum file size (supports K, M, G suffixes) |
| `--exclude` | `-e` | - | Glob patterns to exclude (repeatable) |
| `--workers` | `-w` | CPU count | Parallel workers for scanning and hashing |
| `--dry-run` | `-n` | `false` | Preview changes without executing |
| `--verbose` | `-v` | `false` | Log individual file operations |
| `--no-progress` | - | `false` | Disable progress bar |
| `--cache-file` | - | - | Path to hash cache file (enables caching) |
| `--symlink-fallback` | - | `false` | Use symlinks for cross-device deduplication |
| `--trust-device-boundaries` | - | `false` | Assume devices have independent inode spaces |

### Device Boundaries

By default, dupedog groups files by inode number only, ignoring device ID. This is safe for NFS and network filesystems where the same file can appear with different device IDs depending on mount path.

With `--trust-device-boundaries`, files are grouped by (device, inode) pair. Use this when scanning multiple local disks with separate filesystems, USB drives, or VM disk images mounted separately.

Do not use `--trust-device-boundaries` with NFS mounts or network storage where the same filesystem might appear as different devices.

## Configuration

dupedog has no configuration file. All options are passed via command-line flags.

## Requirements

- Linux or macOS
- Go 1.25+ (for building from source)
- Docker (for container usage or E2E tests)

## License

[MIT](LICENSE)
