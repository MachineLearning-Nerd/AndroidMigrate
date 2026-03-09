# AndroidMigrate Plan

## Current Status
- Phase: Implementation
- State: Initial working MVP committed in workspace
- Last updated: 2026-03-09
- Owner: Codex + user

## Product Goal
Build a CLI-based Android folder sync and backup tool that:
- connects to an Android phone using `adb`
- backs up user-selected phone folders recursively
- performs incremental sync on later runs
- creates checkpoints for every successful sync
- allows restore of the phone to any earlier checkpoint

## Locked Decisions
- Interface: CLI first
- Access method: `adb` only
- Language: Python
- Host OS: macOS, Windows, Linux
- Sync roots: user-chosen Android folders such as `/sdcard/DCIM`
- Folder behavior: recursive, including all nested folders and files
- Conflict policy: phone wins
- Delete policy: never auto-propagate deletes
- Checkpoint retention: keep last 30 checkpoints
- Scope: shared-storage folders only, not generic private app data on stock Android

## V1 Behavior
- User creates a profile bound to one device serial and one local mirror folder.
- User adds one or more Android roots to that profile.
- First sync copies the full contents of each configured root.
- Later syncs detect deltas and transfer only changed files.
- Every sync creates an immutable checkpoint.
- User can restore:
  - full checkpoint
  - one configured root
  - one relative file path
- If user adds a new root later, only that new root gets a first full ingest.

## Planned Commands
- `androidmigrate devices`
- `androidmigrate profile create <name> --device <serial> --mirror <local_dir>`
- `androidmigrate profile add-root <name> <device_path> [--label <alias>]`
- `androidmigrate profile list`
- `androidmigrate sync <name> [--dry-run]`
- `androidmigrate checkpoints <name>`
- `androidmigrate restore <name> <checkpoint_id> [--root <alias>] [--path <relative_path>] --to-device`
- `androidmigrate conflicts <name>`
- `androidmigrate resolve <name> <relative_path> --keep phone|local|both`

## Architecture
- CLI layer: `argparse`-based command interface for zero-dependency bootstrap
- Sync engine: compares phone state, local mirror state, and last checkpoint
- Metadata store: SQLite
- Content store: SHA-256 blob store
- Checkpoint store: immutable manifests per sync
- Device transport: `adb shell`, `adb pull`, `adb push`

## Core Data Model
- Profile
- SyncRoot
- Checkpoint
- FileState
- ConflictRecord
- DivergenceRecord

## Sync Rules
- Phone-only change: pull to local mirror
- Local-only change: push to phone
- Both changed: phone wins, preserve local edited copy as conflict copy
- Deleted on one side: do not delete the other side automatically
- Added new root: full ingest only for that root

## Restore Rules
- Restore target is the original phone path on the bound device
- Support restoring:
  - entire checkpoint
  - one root
  - one file
- Restore pulls file content from checkpoint blob store and pushes it back to the phone

## Milestones
- [x] Initialize Python project structure
- [x] Add CLI skeleton with profile commands
- [x] Add device detection and capability probe
- [x] Add recursive device scan
- [x] Add local scan
- [x] Add SQLite metadata layer
- [x] Add blob store and checkpoint manifests
- [x] Implement one-way phone-to-local ingest
- [x] Implement local-to-phone push path
- [x] Implement full sync decision engine
- [x] Implement conflict and divergence handling
- [x] Implement checkpoint listing
- [x] Implement restore commands
- [x] Add retention pruning and blob garbage collection
- [x] Add tests
- [ ] Add packaging and install docs

## Test Scenarios
- Recursive backup of `/sdcard/DCIM`
- Incremental sync with one changed file
- Add a new root after initial sync
- Local-only file change
- Phone-only file change
- Same file changed on both sides
- File deleted on one side
- Restore older checkpoint
- Retention pruning after checkpoint 31
- Unauthorized or disconnected device
- Unsupported device shell behavior

## Risks
- Android storage restrictions limit access to private app data
- Device shell behavior may vary across vendors
- Mtime handling may differ by device and host OS
- Two-way sync complexity increases conflict and restore edge cases

## Research Basis
- `adb` supports file transfer and shell access
- modern Android scoped storage restricts private app access
- `adb backup` is not a reliable foundation for modern full-device backup

## Next Step
Start implementation as a greenfield Python CLI with:
1. profile management
2. recursive scan
3. initial full backup
4. delta sync
5. checkpoint creation
