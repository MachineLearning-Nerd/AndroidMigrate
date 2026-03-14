from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


IN_SYNC = "in_sync"
CONFLICT = "conflict"
DIVERGED_MISSING_DEVICE = "diverged_missing_device"
DIVERGED_MISSING_LOCAL = "diverged_missing_local"
REMOVED = "removed"

ROOT_ACTIVE = "active"
ROOT_DISABLED = "disabled"
ROOT_REMOVED = "removed"

PROFILE_ACTIVE = "active"
PROFILE_PENDING_CLONE = "pending_clone"
PROFILE_CLONE_FAILED = "clone_failed"
PROFILE_RESTORE_INCOMPLETE = "restore_incomplete"

RUN_SYNC = "sync"
RUN_RESTORE = "restore"
RUN_CLONE_RESTORE = "clone_restore"
RUN_REPAIR_LOCAL = "repair_local"
RUN_CHANGE_MIRROR = "change_mirror"
RUN_RESOLVE = "resolve"

RUN_RUNNING = "running"
RUN_COMPLETED = "completed"
RUN_PARTIAL = "partial"
RUN_FAILED = "failed"
RUN_CANCELLED = "cancelled"


@dataclass(slots=True)
class DeviceInfo:
    serial: str
    state: str
    model: str | None = None
    device: str | None = None


@dataclass(slots=True)
class RemoteDirectoryEntry:
    name: str
    absolute_path: str
    parent_path: str


@dataclass(slots=True)
class Profile:
    id: int
    name: str
    device_serial: str
    mirror_dir: Path
    checkpoint_retention: int
    created_at: str
    profile_state: str = PROFILE_ACTIVE
    cloned_from_profile_id: int | None = None
    cloned_from_checkpoint_id: int | None = None

    @property
    def can_sync(self) -> bool:
        return self.profile_state == PROFILE_ACTIVE


@dataclass(slots=True)
class SyncRoot:
    id: int
    profile_id: int
    device_path: str
    label: str
    lifecycle: str = ROOT_ACTIVE

    @property
    def enabled(self) -> bool:
        return self.lifecycle == ROOT_ACTIVE


@dataclass(slots=True)
class CheckpointRoot:
    id: int
    profile_id: int
    device_path: str
    label: str
    lifecycle: str | None = None


@dataclass(slots=True)
class FileMetadata:
    relative_path: str
    size: int
    mtime: int
    absolute_path: str


@dataclass(slots=True)
class FileState:
    id: int | None
    profile_id: int
    root_id: int
    relative_path: str
    status: str
    device_present: bool
    device_hash: str | None
    device_size: int | None
    device_mtime: int | None
    local_present: bool
    local_hash: str | None
    local_size: int | None
    local_mtime: int | None
    conflict_copy_path: str | None
    updated_at: str
    last_synced_checkpoint_id: int | None = None
    last_restored_from_checkpoint_id: int | None = None

    @property
    def has_issue(self) -> bool:
        return self.status in {CONFLICT, DIVERGED_MISSING_DEVICE, DIVERGED_MISSING_LOCAL}


@dataclass(slots=True)
class Checkpoint:
    id: int
    profile_id: int
    created_at: str
    status: str
    summary_json: str


@dataclass(slots=True)
class SyncRun:
    id: int
    profile_id: int
    operation_type: str
    status: str
    started_at: str
    finished_at: str | None
    source_profile_id: int | None
    source_checkpoint_id: int | None
    result_checkpoint_id: int | None
    summary_json: str


@dataclass(slots=True)
class SyncRunEvent:
    id: int
    run_id: int
    seq: int
    created_at: str
    stage: str
    root_id: int | None
    root_label: str | None
    relative_path: str | None
    action: str | None
    status: str
    message: str


@dataclass(slots=True)
class SyncSummary:
    profile_name: str
    dry_run: bool = False
    checkpoint_id: int | None = None
    pulled: int = 0
    pushed: int = 0
    conflicts: int = 0
    divergences: int = 0
    unchanged: int = 0
    removed: int = 0
    roots_scanned: int = 0
    files_seen: int = 0

    def to_dict(self) -> dict[str, int | str | bool | None]:
        return {
            "profile_name": self.profile_name,
            "dry_run": self.dry_run,
            "checkpoint_id": self.checkpoint_id,
            "pulled": self.pulled,
            "pushed": self.pushed,
            "conflicts": self.conflicts,
            "divergences": self.divergences,
            "unchanged": self.unchanged,
            "removed": self.removed,
            "roots_scanned": self.roots_scanned,
            "files_seen": self.files_seen,
        }
