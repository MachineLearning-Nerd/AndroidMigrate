# AndroidMigrate

Checkpointed Android folder backup and sync over ADB.

AndroidMigrate keeps a local mirror of folders on your Android device, tracks changes with content-addressed blob storage, and creates checkpoints so you can restore any previous state back to the phone.

## Features

- **Incremental sync** -- pulls only changed files from device using SHA-256 content hashing
- **Checkpoints** -- automatic snapshots after every sync, with configurable retention
- **Restore** -- push any checkpoint back to the device, or restore individual files
- **Clone restore** -- migrate a checkpoint to a different device under a new profile
- **Conflict detection** -- detects phone vs local edits and archives local copies for manual resolution
- **Local mirror recovery** -- rebuild a corrupted or lost mirror from checkpoint blobs
- **Backup folder change** -- relocate the mirror directory with automatic rebuild from phone or checkpoint fallback
- **Root management** -- add, disable, or remove sync roots (e.g. DCIM, Documents) per profile
- **Terminal UI** -- curses-based dashboard for managing profiles, running syncs, browsing checkpoints, and resolving conflicts

## Requirements

- Python 3.12+
- ADB (`adb`) installed and on `PATH`
- Android device with USB debugging enabled

## Installation

### PyPI (all platforms)

```bash
pip install androidmigrate
```

### AUR (Arch Linux)

```bash
yay -S androidmigrate
```

### Homebrew (macOS)

```bash
brew tap MachineLearning-Nerd/tap
brew install androidmigrate
```

### From source

```bash
git clone https://github.com/MachineLearning-Nerd/AndroidMigrate.git
cd AndroidMigrate
pip install -e .
```

## Quick Start

```bash
# List connected devices
androidmigrate devices

# Create a profile
androidmigrate profile create myphone \
  --device SERIAL \
  --mirror ~/android-backup

# Add folders to sync
androidmigrate profile add-root myphone /sdcard/DCIM
androidmigrate profile add-root myphone /sdcard/Documents

# Run a sync
androidmigrate sync myphone

# Launch the TUI dashboard
androidmigrate tui
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `devices` | List connected Android devices |
| `profile create` | Create a new backup profile |
| `profile add-root` | Add a device folder to sync |
| `profile list` | List profiles and their roots |
| `sync <profile>` | Sync files from device to local mirror |
| `checkpoints <profile>` | List available checkpoints |
| `restore <profile> <id>` | Restore a checkpoint to the device |
| `repair-local <profile>` | Rebuild local mirror from checkpoint data |
| `clone-restore <profile> <id>` | Clone a checkpoint to a different device |
| `conflicts <profile>` | List unresolved file conflicts |
| `resolve <profile> <id>` | Resolve a conflict (keep phone/local/both) |
| `tui` | Launch the terminal UI |

## TUI Keybindings

| Key | Action |
|-----|--------|
| `j/k` or arrows | Navigate profiles |
| `e` | Edit sync roots |
| `m` | Change backup folder |
| `y` | Run sync |
| `c` | View checkpoints |
| `i` | View issues |
| `l` | View run history |
| `r` | Refresh |
| `?` | Help |
| `q` | Quit |

## Configuration

State is stored in `.androidmigrate/` in the current directory by default. Override with:

```bash
export ANDROIDMIGRATE_HOME=~/path/to/state
```

## Architecture

```
src/androidmigrate/
  cli.py          -- CLI entry point and argument parsing
  tui.py          -- Curses-based terminal UI
  tui_render.py   -- TUI rendering primitives (panels, badges, themes)
  sync_engine.py  -- Core sync, restore, and checkpoint logic
  storage.py      -- SQLite repository and content-addressed blob store
  transport.py    -- ADB transport layer (device communication)
  models.py       -- Domain data classes
  config.py       -- Path helpers and state directory layout
  mirror_path.py  -- Backup folder validation and autocomplete
  root_manager.py -- Interactive root browser and management
```

## License

MIT
