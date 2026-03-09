# AndroidMigrate TUI Plan

## Current Status
- Phase: Planning
- Focus: TUI design and supporting backend changes
- State: decision-complete plan aligned with the current CLI codebase
- Last updated: 2026-03-09

## Goal
Add a keyboard-first terminal UI for AndroidMigrate that makes setup, folder selection, sync, restore, clone restore, issue review, and run-log inspection practical without shell commands.

The TUI must let a user:
- detect and select a connected Android device
- create a profile or edit an existing profile
- enter a local mirror path with validation and autocomplete
- browse Android shared storage under `/sdcard`
- multi-select backup roots without overlapping parent/child selections
- run backup or sync with live progress
- see exactly what succeeded and exactly where failures happened
- restore a checkpoint to the same phone for a full profile or one root
- clone-restore a full checkpoint to a different phone as a new independent profile
- inspect unresolved issues and recent persisted run logs

## Locked Decisions
- TUI stack: stdlib `curses`
- Key style: hybrid
- Arrow keys, Enter, Space, `q`, and `?` are primary; `j/k/h/l` are navigation aliases outside text-entry mode
- `Tab` changes focus only when the focused widget is not a text field
- In text-entry fields, `Tab` is reserved for autocomplete
- Pane-navigation fallback keys `]` and `b` are guaranteed only outside text-entry mode
- Folder browser behavior: `Enter` opens folder, `Space` toggles selection
- Local path entry: text input with validation and directory autocomplete
- Folder selection: browse all folders under `/sdcard`
- Overlapping parent/child selected roots: disallowed
- Sync failure policy:
  - sync continues where safe
  - root-scan failures can skip that root and continue other roots
  - file-level sync failures can mark the run `partial`
- Restore and Clone Restore failure policy:
  - both flows are exact and destructive
  - both flows are fail-fast after preflight
  - after the first destructive mutation, any write or delete failure stops the run immediately
  - no per-file continuation is allowed during restore or clone restore
- Checkpoint policy:
  - successful sync creates a checkpoint
  - partial, failed, or cancelled sync creates no checkpoint
- Restore policy:
  - same-device only
  - full profile means all roots present in the selected checkpoint
  - one-root restore means one root from the selected checkpoint root set
  - exact within the chosen scope
  - updates both phone and local mirror within that scope
  - creates no new checkpoint
  - records one `restore` run entry
  - restoring a currently `disabled` or `removed` root reactivates that root to `active`
- Clone Restore policy:
  - different-device only
  - full profile only in v1
  - full profile means all roots present in the selected checkpoint
  - creates a new independent profile bound to the target phone
  - target roots are created from the selected checkpoint root set and start as `active`
  - creates one initial seeded checkpoint in the new profile on success
  - records one `clone_restore` run entry
  - warns about source and target device differences and requires explicit confirmation
- Mirror-path edit policy:
  - move the existing local backup data to the new path
  - refuse automatic merge into an existing non-empty destination
  - update profile metadata only after the move succeeds
  - rewrite stored conflict archive paths after a successful move
  - leave the old path unchanged if the move fails
- Root lifecycle policy:
  - `active`: included in sync and setup flows
  - `disabled`: excluded from sync, but still visible in restore, history, and issues
  - `removed`: excluded from active setup and sync, but still visible in checkpoint history and historical restore
  - disabling or removing a root never deletes local mirror data automatically
- Profile state policy:
  - `active`: normal sync and restore operations allowed
  - `pending_clone`: target profile exists but clone restore has not completed yet
  - `clone_failed`: clone restore failed or was cancelled after target profile creation; normal sync is blocked
  - `restore_incomplete`: same-device restore failed or was cancelled after destructive mutation; normal sync is blocked
- Issue identity is fixed to `file_states.id`
- Run-log retention: keep last 20 runs per profile
- Minimum terminal size: `90x24`

## Current Codebase Reality
- The repo currently has CLI commands for device listing, profile management, sync, checkpoint listing, restore, issue listing, and issue resolution.
- The transport layer currently supports device listing, probe, full-tree file scan, hash, pull, push, stat, and delete.
- The storage layer currently supports profiles, sync roots, checkpoints, checkpoint entries, and file states.
- The current codebase does not yet support:
  - Android directory browsing
  - profile editing beyond create and add-root
  - root lifecycle states beyond the current boolean flag
  - profile state and clone lineage metadata
  - persisted run-history tables
  - stable issue ids in the model layer
  - per-step sync, restore, or clone-restore event streaming
  - fail-fast restore or clone-restore semantics

## Screen Model
### 1. Dashboard
- Show connected device status
- Show profiles with state badges
- Show unresolved issue count per profile
- Show the last 20 run headers for the highlighted profile
- Block `Sync` on profiles in `pending_clone`, `clone_failed`, or `restore_incomplete`
- For blocked profiles, surface the allowed recovery actions:
  - `clone_failed`: retry clone restore or delete the failed target profile
  - `restore_incomplete`: retry restore
- Show footer legend for the active screen

### 2. Setup Wizard
- Step 1: choose connected device
- Step 2: create a new profile or edit an existing profile
- Step 3: enter local mirror path
- Step 4: browse `/sdcard` and manage roots
- Step 5: review and save

Edit-profile root management:
- left pane: browse available directories under `/sdcard`
- right pane: existing roots with lifecycle badges (`active`, `disabled`, `removed`)
- add new root from left pane with `Space`
- in the right pane:
  - `d`: toggle `active <-> disabled`
  - `R`: mark highlighted existing root as `removed`
  - `a`: reactivate highlighted `disabled` or `removed` root to `active`

Wizard review step:
- `Enter`: save profile changes
- `r`: return to edit selections
- `q`: abort wizard without saving

### 3. Sync / Run View
- Modal while a run is active
- Show operation type, profile, active root, active path, live counters, and scrolling event log
- `x`: request stop at the next safe boundary
- For `sync`, the next safe boundary is file or root boundary
- For `restore` and `clone_restore`, stop requests are honored only before the first destructive mutation; after mutation starts, the run proceeds until success or first failure
- `q`: disabled while a run is active; enabled after completion to return to dashboard

### 4. Restore View
- Step 1: choose action type:
  - `Restore`
  - `Clone Restore`
- Step 2: select source profile
- Step 3: select checkpoint
- Step 4A for `Restore`: choose scope from the selected checkpoint root set:
  - `all checkpoint roots`
  - one checkpoint root
- Step 4B for `Clone Restore`: choose target device, enter new profile name, and enter new mirror path
- Step 5: destructive confirmation

Restore scope chooser:
- list roots from the selected checkpoint, not from the profile's current active-root set
- show each checkpoint root with its current lifecycle badge if that root still exists in the profile
- if the chosen root is currently `disabled` or `removed`, show that restore will reactivate it

Restore confirmation shows:
- source profile name
- checkpoint id and timestamp
- source device serial
- action type
- chosen scope
- warning that exact restore removes newer files in the chosen scope on both phone and local mirror
- warning that restore is fail-fast after destructive mutation begins

Clone Restore confirmation shows:
- source profile name
- checkpoint id and timestamp
- source device serial and model if known
- target device serial and model if known
- new target profile name
- target mirror path
- warning that clone restore will create a new independent target profile
- warning that the target phone and target mirror will be overwritten exactly for all checkpoint roots
- warning that a failed clone target remains quarantined and cannot be normally synced until retried or deleted

### 5. Issues View
- Show unresolved issues only
- Operate on stable `file_states.id` values
- Default action is resolve-to-phone

### 6. Run Logs View
- Show persisted `SyncRun` headers for the selected profile
- Open one run to inspect its ordered event list

## Key Mapping
### Global
- `Arrow keys`: navigate
- `j/k/h/l`: navigation aliases when the focused widget is not a text-entry field
- `q`: back or quit in non-input, non-running screens
- `?`: help overlay
- `r`: refresh device and profile data
- `Tab`: next pane when not in a text-entry widget
- `Shift+Tab`: previous pane when terminal support exists
- `]`: next-pane fallback when not in a text-entry widget
- `b`: previous-pane fallback when not in a text-entry widget
- `Esc`: cancel dialogs or dismiss autocomplete suggestions after escape-timeout handling

### Text Entry and Autocomplete
- Printable keys edit the field and suppress global navigation bindings
- `Tab` on a text field:
  - no match: keep input unchanged and show inline `no matches`
  - one match: accept it immediately
  - multiple matches: complete the longest common prefix and open a suggestion list
- `Up/Down` move inside the suggestion list
- `Enter` accepts the highlighted suggestion or submits the current text if no suggestion list is open
- `Esc` dismisses the suggestion list without clearing typed text
- `Ctrl+U` clears the field

### Folder Browser and Root Management
- `Enter`: open highlighted folder
- `Left`, `Backspace`, `KEY_BACKSPACE`, `127`, or `8`: go to parent folder when the browser pane is focused
- `Space`: add the highlighted folder as a root when the browser pane is focused
- `a`: select all visible folders when the browser pane is focused
- `x`: clear all visible selections when the browser pane is focused
- `s`: save selected roots and continue

Selected-roots pane:
- `Space`: select highlighted existing root entry
- `d`: toggle `active <-> disabled`
- `R`: mark highlighted root `removed`
- `a`: reactivate highlighted `disabled` or `removed` root

### Dashboard Shortcuts
- `n`: new profile wizard
- `e`: edit highlighted profile
- `y`: sync highlighted profile
- `c`: restore or clone restore
- `i`: issues
- `l`: run logs
- `d`: refresh connected devices

### Issues View
- `Enter`: open issue details
- `p`: resolve selected issue to phone
- `o`: show archived conflict-copy path info if present

## Required Backend Changes
### Transport Layer
Add browse-safe transport primitives:
- `list_directories(serial, device_path) -> list[RemoteDirectoryEntry]` for immediate child directories only
- `path_info(serial, device_path)` to distinguish missing, file, and directory
- deterministic host-side sort order for directory entries
- capability probe updates for the browse command path used by `list_directories`

Suggested `RemoteDirectoryEntry` fields:
- `name`
- `absolute_path`
- `parent_path`

### Storage and Model Layer
Replace boolean root enablement with a root lifecycle field:
- `active`
- `disabled`
- `removed`

Add profile-state and lineage metadata:
- `profile_state` with values `active`, `pending_clone`, `clone_failed`, `restore_incomplete`
- `cloned_from_profile_id`
- `cloned_from_checkpoint_id`

Lock issue identity to `file_states.id`:
- add `id` to the `FileState` model
- repository issue queries must return `file_states.id`
- issue resolution APIs must resolve by `issue_id`, not by relative path

Replace ambiguous file-state provenance with explicit fields:
- `last_synced_checkpoint_id`
- `last_restored_from_checkpoint_id`

Add profile-editing operations:
- rename profile
- update mirror path
- add root
- change root lifecycle state
- list roots with stable ids and lifecycle state

Labels:
- keep root labels auto-derived and uniquified in v1
- do not support manual label editing or reordering in v1

Add persisted run-history tables:
- `SyncRun`
- `SyncRunEvent`

Required `SyncRun` fields:
- `id`
- `profile_id`
- `operation_type` (`sync`, `restore`, `clone_restore`, `resolve`)
- `status` (`running`, `completed`, `partial`, `failed`, `cancelled`)
- `started_at`
- `finished_at`
- `source_profile_id`
- `source_checkpoint_id`
- `result_checkpoint_id`
- `summary_json`

Required `SyncRunEvent` fields:
- `id`
- `run_id`
- `seq`
- `created_at`
- `stage`
- `root_id` nullable
- `root_label` nullable
- `relative_path` nullable
- `action` nullable
- `status`
- `message`

Retention behavior:
- prune old run headers and their events in one transaction
- run pruning after a new run is finalized
- keep the newest 20 runs per profile

Mirror-path update behavior:
- move the full profile mirror directory to the new path
- block automatic merge into an existing non-empty destination
- update `profiles.mirror_dir` only after the move succeeds
- rewrite stored `conflict_copy_path` values from the old mirror prefix to the new mirror prefix after the move succeeds
- keep the profile unchanged if the move fails

### Sync, Restore, and Clone-Restore Engine
Add event streaming:
- `sync_profile`, `restore_checkpoint`, clone-restore execution, and `resolve_issue` accept an optional event sink callback
- each event includes enough information for live UI and persisted log rows

Required event boundaries:
- probe start, success, and failure
- browse and preflight validation start, success, and failure
- root scan start, success, and failure
- file pull, push, hash, and delete success or failure
- conflict auto-resolution
- issue creation
- checkpoint create success or failure
- restore start, success, and failure
- clone-restore start, success, and failure
- operator stop requested and honoured

Sync failure boundaries:
- probe failure stops the whole sync run
- root-scan failure marks that root failed and continues other active roots
- file-level pull, push, hash, or delete failure marks that path failed and continues remaining paths in that root when safe
- checkpoint or database-write failure marks the sync run failed and creates no checkpoint

Restore and clone-restore failure boundaries:
- both flows run a full preflight before any destructive mutation
- once destructive mutation starts, the first file write or delete failure aborts the run immediately
- no further roots or files are processed after that failure
- no checkpoint is created for failed or cancelled restore
- no seeded checkpoint is created for failed or cancelled clone restore

Checkpoint and provenance policy:
- `completed` sync: create checkpoint, update `last_synced_checkpoint_id`, clear `last_restored_from_checkpoint_id` for paths finalized by that sync
- `partial`, `failed`, `cancelled` sync: create no checkpoint
- successful restore:
  - create one `restore` run entry
  - update file state only for the restored scope
  - set `last_restored_from_checkpoint_id` to the selected source checkpoint for affected paths
  - leave `last_synced_checkpoint_id` unchanged
  - set reactivated roots to `active`
  - create no checkpoint
- failed or cancelled restore after destructive mutation:
  - create no checkpoint
  - do not persist incremental per-file restore state during the failing mutation phase
  - mark the profile `restore_incomplete`
  - block ordinary sync until the user completes a successful restore retry
- successful clone restore:
  - create the target profile in `pending_clone`
  - create active target roots from the selected checkpoint root set
  - restore exact content to target phone and target mirror
  - create one seeded checkpoint in the new target profile after all content is restored successfully
  - initialize target file states with `last_synced_checkpoint_id` set to the seeded target checkpoint and `last_restored_from_checkpoint_id` set to the selected source checkpoint
  - record one `clone_restore` run in the target profile
  - set target profile state to `active`
- failed or cancelled clone restore after target-profile creation:
  - set target profile state to `clone_failed`
  - keep run history for inspection
  - block ordinary sync on that target profile
  - allow only retry clone restore or explicit deletion of the failed target profile

## Validation and Safety Rules
- Browser starts at `/sdcard`
- Browser shows directories only during root selection
- Parent-child overlapping roots cannot both be saved in a profile
- Local mirror path must be writable or creatable from a writable parent
- If the terminal is smaller than `90x24`, render a blocking resize overlay instead of partial UI
- If the terminal is resized during a run, freeze layout updates, preserve the event buffer, show the resize overlay, and resume rendering once the terminal is large enough again
- Normal restore is blocked when the connected target device does not match the profile's bound serial
- Clone restore is blocked when the chosen target serial matches the source profile serial
- Clone restore requires:
  - an authorized connected target device
  - a unique new profile name
  - a writable target mirror path that is absent or empty
  - all source blobs for the selected checkpoint to exist locally
- Ordinary sync is blocked for profiles in `pending_clone`, `clone_failed`, or `restore_incomplete`

## Restore and Clone-Restore Rules
### Restore
- Restore is same-device only
- Restore supports:
  - full profile restore
  - single-root restore
- Full profile restore means all roots represented by the selected checkpoint
- Single-root restore means one root represented by the selected checkpoint, even if that root is currently `disabled` or `removed`
- Restore is exact within the chosen scope
- Restore updates both phone and local mirror within the chosen scope
- Restore removes extra files on both sides inside the chosen scope when those files are absent from the selected checkpoint
- Restore leaves all checkpoint-external roots untouched
- Restore records its own persisted `restore` run log
- Restore does not trigger an implicit follow-up sync
- Restore creates no new checkpoint

### Clone Restore
- Clone Restore is different-device only
- Clone Restore supports:
  - full profile restore only
- Full profile means all roots represented by the selected checkpoint
- Clone Restore creates a new independent profile bound to the target device
- Clone Restore creates target roots from the selected checkpoint root set and marks them `active`
- Clone Restore restores the selected checkpoint exactly onto the target phone and target mirror across all checkpoint roots
- Clone Restore seeds the new profile with one initial checkpoint after successful content replay
- Clone Restore records one `clone_restore` run in the new target profile
- Clone Restore leaves the source profile unchanged
- Clone Restore does not attempt automatic rollback of already-written phone or local files after a post-mutation failure

## Test Scenarios
- Browse `/sdcard` and navigate nested folders
- Multi-select multiple roots
- Block parent-child overlap at selection time and save time
- Show selected roots persistently while browsing elsewhere
- Autocomplete local path with zero, one, and many matches
- Keep navigation keys inactive while typing in text-entry fields
- Create and edit a profile through the TUI
- Toggle a root between `active` and `disabled`
- Mark a root `removed` and reactivate it later
- Move a profile mirror path successfully and rewrite stored conflict archive paths
- Block mirror-path move into a non-empty target directory
- Keep profile metadata unchanged when a mirror-path move fails
- Sync with live event rendering
- Continue after one file fails during sync and persist the failure in the run log
- Mark a root scan failure during sync, continue other roots, and persist the root-level error
- Honour operator stop requests at a file or root boundary during sync and record `cancelled`
- Auto phone-win a content conflict and archive the local losing copy
- Show delete or divergence issues separately and resolve one by `file_state.id`
- Restore full profile from checkpoint using all checkpoint roots and no follow-up checkpoint
- Restore one checkpoint root that is currently `disabled` and verify it becomes `active`
- Restore one checkpoint root that is currently `removed` and verify it becomes `active`
- Fail restore after destructive mutation and verify the profile becomes `restore_incomplete` and sync is blocked
- Retry restore on a `restore_incomplete` profile and clear the blocked state on success
- Block restore when the target device serial does not match the source profile
- Clone restore a full checkpoint to another phone and create a new independent target profile
- Clone restore from a checkpoint whose root set differs from the source profile's current active-root set
- Block clone restore to the same device serial
- Block clone restore when the target profile name already exists
- Block clone restore when the target mirror path is non-empty
- Block clone restore when source blobs are missing
- Fail clone restore after target-profile creation and verify the target profile becomes `clone_failed` and ordinary sync is blocked
- Persist one seeded checkpoint in the new target profile after successful clone restore
- Keep source profile history unchanged after clone restore
- Show resize overlay on a too-small terminal and recover after resize
- Confirm arrow keys and vim aliases both work in non-input widgets

## Assumptions
- TUI v1 is keyboard-only
- No mouse support in v1
- Single-file restore remains CLI-only in v1
- Existing CLI commands remain supported and unchanged
- The TUI is a direct front end over the existing engine, but it requires new browse, lifecycle, profile-state, lineage, issue-id, run-log, and event-stream capabilities underneath

## Next Step
When implementation starts:
1. add browse-safe transport primitives
2. replace root boolean enablement with lifecycle state and add profile-state plus clone-lineage metadata
3. add model changes for `file_states.id`, explicit file-state provenance, and issue resolution by id
4. add profile-editing repository methods, including mirror-path move handling
5. add run-history schema with source and result checkpoint references
6. add sync event sinks and partial-run boundaries
7. add fail-fast restore and clone-restore flows with quarantined failure states
8. add exact checkpoint-root restore semantics without an implicit follow-up sync
9. add seeded target-profile checkpoint creation for successful clone restore
10. build the curses screen framework
11. implement dashboard, wizard, sync/run, restore, clone-restore, issues, and run-log screens
