from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .config import conflict_copy_path, local_root_path
from .models import (
    CONFLICT,
    DIVERGED_MISSING_DEVICE,
    DIVERGED_MISSING_LOCAL,
    IN_SYNC,
    PROFILE_ACTIVE,
    PROFILE_CLONE_FAILED,
    PROFILE_PENDING_CLONE,
    PROFILE_RESTORE_INCOMPLETE,
    REMOVED,
    ROOT_ACTIVE,
    RUN_CLONE_RESTORE,
    RUN_CHANGE_MIRROR,
    RUN_COMPLETED,
    RUN_FAILED,
    RUN_REPAIR_LOCAL,
    RUN_RESOLVE,
    RUN_RESTORE,
    RUN_SYNC,
    FileMetadata,
    FileState,
    Profile,
    SyncRoot,
    SyncSummary,
)
from .storage import BlobStore, Repository, utc_now
from .transport import ADBTransport, DeviceTransport

EventSink = Callable[[dict[str, object]], None]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scan_local_root(root_path: Path) -> dict[str, FileMetadata]:
    files: dict[str, FileMetadata] = {}
    if not root_path.exists():
        return files
    for current_root, _, filenames in os.walk(root_path):
        for filename in filenames:
            absolute = Path(current_root) / filename
            stat_result = absolute.stat()
            relative = absolute.relative_to(root_path).as_posix()
            files[relative] = FileMetadata(
                relative_path=relative,
                size=stat_result.st_size,
                mtime=int(stat_result.st_mtime),
                absolute_path=str(absolute),
            )
    return files


@dataclass(slots=True)
class RootSyncResult:
    states: list[FileState]
    checkpoint_entries: list[dict[str, object]]


@dataclass(slots=True)
class RestoreScopeState:
    states: list[FileState]
    reactivated_root_ids: set[int]


class RunLogger:
    def __init__(
        self,
        repository: Repository,
        run_id: int | None,
        operation: str,
        event_sink: EventSink | None = None,
    ) -> None:
        self.repository = repository
        self.run_id = run_id
        self.operation = operation
        self.event_sink = event_sink
        self.seq = 0

    def emit(
        self,
        stage: str,
        status: str,
        message: str,
        *,
        root: SyncRoot | None = None,
        relative_path: str | None = None,
        action: str | None = None,
    ) -> None:
        self.seq += 1
        payload = {
            "seq": self.seq,
            "operation": self.operation,
            "stage": stage,
            "status": status,
            "message": message,
            "root_id": root.id if root else None,
            "root_label": root.label if root else None,
            "relative_path": relative_path,
            "action": action,
        }
        if self.run_id is not None:
            self.repository.append_run_event(
                self.run_id,
                self.seq,
                stage,
                status,
                message,
                root_id=root.id if root else None,
                root_label=root.label if root else None,
                relative_path=relative_path,
                action=action,
            )
        if self.event_sink is not None:
            self.event_sink(payload)


class SyncEngine:
    def __init__(
        self,
        repository: Repository,
        blob_store: BlobStore,
        transport: DeviceTransport | None = None,
    ) -> None:
        self.repository = repository
        self.blob_store = blob_store
        self.transport = transport or ADBTransport()

    def sync_profile(
        self,
        profile_name: str,
        dry_run: bool = False,
        event_sink: EventSink | None = None,
    ) -> SyncSummary:
        profile = self.repository.get_profile(profile_name)
        if not profile.can_sync:
            raise ValueError(f"Profile {profile.name} is blocked for sync (state={profile.profile_state})")
        roots = self.repository.list_active_roots(profile.id)
        if not roots:
            raise ValueError(f"Profile {profile.name} has no active roots")

        summary = SyncSummary(profile_name=profile.name, dry_run=dry_run)
        run_id = None if dry_run else self.repository.start_run(profile.id, RUN_SYNC, summary=summary.to_dict())
        logger = RunLogger(self.repository, run_id, RUN_SYNC, event_sink)
        root_results: list[RootSyncResult] = []
        missing_local_roots = self._roots_missing_local_history(profile, roots)

        try:
            logger.emit("probe", "running", f"Probing device {profile.device_serial}")
            try:
                self.transport.probe_device(profile.device_serial)
            except Exception as exc:
                if missing_local_roots:
                    labels = ", ".join(root.label for root in missing_local_roots)
                    raise ValueError(
                        f"Local backup root(s) missing for profile {profile.name}: {labels}. "
                        f"Device is unavailable. Run 'androidmigrate repair-local {profile.name}' "
                        "to rebuild from the latest checkpoint."
                    ) from exc
                raise
            logger.emit("probe", "completed", f"Device {profile.device_serial} is ready")

            for root in roots:
                summary.roots_scanned += 1
                logger.emit("root_scan", "running", f"Scanning root {root.label}", root=root)
                root_result = self._sync_root(profile, root, summary, dry_run=dry_run, logger=logger)
                root_results.append(root_result)
                logger.emit("root_scan", "completed", f"Scanned root {root.label}", root=root)

            if dry_run:
                return summary

            checkpoint_entries = [entry for result in root_results for entry in result.checkpoint_entries]
            states = [state for result in root_results for state in result.states]
            checkpoint_id = self.repository.create_checkpoint(profile.id, RUN_COMPLETED, summary.to_dict())
            summary.checkpoint_id = checkpoint_id
            self.repository.update_checkpoint_summary(checkpoint_id, summary.to_dict())
            self.repository.insert_checkpoint_entries(checkpoint_id, checkpoint_entries)
            self.repository.finalize_synced_file_states(states, checkpoint_id)
            self.repository.prune_checkpoints(profile.id, profile.checkpoint_retention)
            self.blob_store.gc(self.repository.referenced_blob_hashes())
            self.repository.finalize_run(run_id, RUN_COMPLETED, result_checkpoint_id=checkpoint_id, summary=summary.to_dict())
            self.repository.prune_runs(profile.id)
            logger.emit("checkpoint", "completed", f"Created checkpoint {checkpoint_id}")
            return summary
        except Exception as exc:
            if run_id is not None:
                logger.emit("run", "failed", str(exc))
                self.repository.finalize_run(run_id, RUN_FAILED, summary={"error": str(exc), **summary.to_dict()})
                self.repository.prune_runs(profile.id)
            raise

    def repair_local(
        self,
        profile_name: str,
        checkpoint_id: int | None = None,
        root_label: str | None = None,
        event_sink: EventSink | None = None,
    ) -> SyncSummary:
        profile = self.repository.get_profile(profile_name)
        checkpoints = self.repository.list_checkpoints(profile.id)
        if not checkpoints:
            raise ValueError(f"Profile {profile.name} has no checkpoints to repair from")
        if checkpoint_id is None:
            checkpoint_id = checkpoints[0].id
        self.repository.get_checkpoint(profile.id, checkpoint_id)
        checkpoint_roots = self.repository.list_checkpoint_roots(profile.id, checkpoint_id)
        if not checkpoint_roots:
            raise ValueError(f"Checkpoint {checkpoint_id} has no roots")

        selected_checkpoint_roots = checkpoint_roots
        if root_label is not None:
            selected_checkpoint_roots = [root for root in checkpoint_roots if root.label == root_label]
            if not selected_checkpoint_roots:
                raise ValueError(f"Checkpoint {checkpoint_id} has no root labeled {root_label}")

        selected_roots = [self.repository.get_root_by_id(root.id) for root in selected_checkpoint_roots]
        run_id = self.repository.start_run(
            profile.id,
            RUN_REPAIR_LOCAL,
            source_profile_id=profile.id,
            source_checkpoint_id=checkpoint_id,
            summary={"checkpoint_id": checkpoint_id, "root_label": root_label},
        )
        logger = RunLogger(self.repository, run_id, RUN_REPAIR_LOCAL, event_sink)
        summary = SyncSummary(profile_name=profile.name)

        try:
            logger.emit("preflight", "running", f"Preparing local repair from checkpoint {checkpoint_id}")
            desired_entries_by_root: dict[int, list[dict[str, object]]] = {}
            for root in selected_roots:
                entries = list(self.repository.list_checkpoint_entries(checkpoint_id, root.id))
                for entry in entries:
                    blob_path = self.blob_store.path_for_hash(entry["blob_hash"])
                    if not blob_path.exists():
                        raise ValueError(f"Missing blob for local repair: {entry['blob_hash']}")
                desired_entries_by_root[root.id] = entries
            logger.emit("preflight", "completed", f"Local repair preflight passed for checkpoint {checkpoint_id}")

            all_states: list[FileState] = []
            for root in selected_roots:
                summary.roots_scanned += 1
                logger.emit("repair_root", "running", f"Repairing local root {root.label}", root=root)
                root_states = self._repair_local_root(
                    profile,
                    root,
                    checkpoint_id,
                    desired_entries_by_root[root.id],
                    summary,
                    logger,
                )
                all_states.extend(root_states)
                logger.emit("repair_root", "completed", f"Repaired local root {root.label}", root=root)

            self.repository.save_file_states(all_states)
            self.repository.finalize_run(
                run_id,
                RUN_COMPLETED,
                summary={"checkpoint_id": checkpoint_id, "root_label": root_label, **summary.to_dict()},
            )
            self.repository.prune_runs(profile.id)
            return summary
        except Exception as exc:
            logger.emit("repair_local", "failed", str(exc))
            self.repository.finalize_run(
                run_id,
                RUN_FAILED,
                summary={"checkpoint_id": checkpoint_id, "root_label": root_label, "error": str(exc), **summary.to_dict()},
            )
            self.repository.prune_runs(profile.id)
            raise

    def preview_change_mirror_source(self, profile_name: str) -> str:
        profile = self.repository.get_profile(profile_name)
        if profile.profile_state == PROFILE_PENDING_CLONE:
            raise ValueError(f"Profile {profile.name} cannot change backup folder while clone restore is pending")
        source, checkpoint_id = self._determine_change_mirror_source(profile, self.repository.list_active_roots(profile.id))
        if source == "phone":
            return "Phone"
        if source == "checkpoint":
            return f"Latest checkpoint #{checkpoint_id}"
        return "Local history only"

    def change_mirror_path(
        self,
        profile_name: str,
        target_mirror_dir: Path,
        event_sink: EventSink | None = None,
    ) -> SyncSummary:
        profile = self.repository.get_profile(profile_name)
        if profile.profile_state == PROFILE_PENDING_CLONE:
            raise ValueError(f"Profile {profile.name} cannot change backup folder while clone restore is pending")

        target_mirror_dir = target_mirror_dir.expanduser().resolve()
        self._validate_target_mirror_dir(profile.mirror_dir, target_mirror_dir)
        roots = self.repository.list_roots(profile.id)
        active_roots = [root for root in roots if root.lifecycle == ROOT_ACTIVE]
        inactive_roots = [root for root in roots if root.lifecycle != ROOT_ACTIVE]
        source, checkpoint_id = self._determine_change_mirror_source(profile, active_roots)
        summary = SyncSummary(profile_name=profile.name)
        run_id = self.repository.start_run(
            profile.id,
            RUN_CHANGE_MIRROR,
            source_profile_id=profile.id,
            source_checkpoint_id=checkpoint_id,
            summary={
                "old_mirror_dir": str(profile.mirror_dir),
                "target_mirror_dir": str(target_mirror_dir),
                "rebuild_source": source,
            },
        )
        logger = RunLogger(self.repository, run_id, RUN_CHANGE_MIRROR, event_sink)
        target_existed = target_mirror_dir.exists()
        target_profile = self._profile_with_mirror(profile, target_mirror_dir)

        try:
            logger.emit("preflight", "running", f"Preparing backup-folder change to {target_mirror_dir}")
            target_mirror_dir.mkdir(parents=True, exist_ok=True)
            logger.emit("preflight", "completed", f"Using {source.replace('_', ' ')} source for {profile.name}")

            for root in inactive_roots:
                self._copy_local_history_root(profile, target_profile, root, logger)

            active_states: list[FileState] = []
            if source == "phone":
                for root in active_roots:
                    summary.roots_scanned += 1
                    logger.emit("mirror_root", "running", f"Rebuilding active root {root.label} from phone", root=root)
                    active_states.extend(self._rebuild_active_root_from_phone(profile, target_profile, root, summary, logger))
                    logger.emit("mirror_root", "completed", f"Rebuilt active root {root.label} from phone", root=root)
            elif source == "checkpoint":
                assert checkpoint_id is not None
                for root in active_roots:
                    summary.roots_scanned += 1
                    logger.emit(
                        "mirror_root",
                        "running",
                        f"Rebuilding active root {root.label} from checkpoint {checkpoint_id}",
                        root=root,
                    )
                    active_states.extend(
                        self._restore_active_root_from_checkpoint(profile, target_profile, root, checkpoint_id, summary, logger)
                    )
                    logger.emit(
                        "mirror_root",
                        "completed",
                        f"Rebuilt active root {root.label} from checkpoint {checkpoint_id}",
                        root=root,
                    )

            self.repository.update_profile_mirror_dir(profile.id, target_mirror_dir)
            if active_states:
                self.repository.save_file_states(active_states)
            logger.emit("change_mirror", "completed", f"Changed backup folder to {target_mirror_dir}")
            self.repository.finalize_run(
                run_id,
                RUN_COMPLETED,
                summary={
                    "old_mirror_dir": str(profile.mirror_dir),
                    "target_mirror_dir": str(target_mirror_dir),
                    "rebuild_source": source,
                    **summary.to_dict(),
                },
            )
            self.repository.prune_runs(profile.id)
            return summary
        except Exception as exc:
            self._cleanup_target_mirror_dir(target_mirror_dir, existed_before=target_existed)
            logger.emit("change_mirror", "failed", str(exc))
            self.repository.finalize_run(
                run_id,
                RUN_FAILED,
                summary={
                    "old_mirror_dir": str(profile.mirror_dir),
                    "target_mirror_dir": str(target_mirror_dir),
                    "rebuild_source": source,
                    "error": str(exc),
                    **summary.to_dict(),
                },
            )
            self.repository.prune_runs(profile.id)
            raise

    def list_issues(self, profile_name: str) -> list[tuple[SyncRoot, FileState]]:
        profile = self.repository.get_profile(profile_name)
        return self.repository.list_open_issues(profile.id)

    def resolve_issue(
        self,
        profile_name: str,
        issue_id: int,
        keep: str,
        event_sink: EventSink | None = None,
    ) -> SyncSummary:
        profile = self.repository.get_profile(profile_name)
        if not profile.can_sync:
            raise ValueError(f"Profile {profile.name} is blocked for sync (state={profile.profile_state})")
        run_id = self.repository.start_run(profile.id, RUN_RESOLVE, summary={"issue_id": issue_id, "keep": keep})
        logger = RunLogger(self.repository, run_id, RUN_RESOLVE, event_sink)
        try:
            root, state = self.repository.get_issue(profile.id, issue_id)
            local_path = local_root_path(profile, root) / state.relative_path
            remote_path = ADBTransport.join_remote(root.device_path, state.relative_path)
            logger.emit("resolve", "running", f"Resolving issue {issue_id}", root=root, relative_path=state.relative_path)

            if keep == "phone":
                if state.device_present:
                    self.transport.pull_file(profile.device_serial, remote_path, local_path)
                elif local_path.exists():
                    local_path.unlink()
            elif keep == "local":
                source = local_path
                if state.status == CONFLICT and state.conflict_copy_path:
                    source = Path(state.conflict_copy_path)
                    if not source.exists():
                        raise ValueError(f"Missing conflict archive: {source}")
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, local_path)
                if source.exists():
                    self.transport.push_file(profile.device_serial, source, remote_path)
                else:
                    self.transport.delete_file(profile.device_serial, remote_path)
            elif keep == "both":
                if state.status == DIVERGED_MISSING_DEVICE and local_path.exists():
                    self.transport.push_file(profile.device_serial, local_path, remote_path)
                elif state.status == DIVERGED_MISSING_LOCAL and state.device_present:
                    self.transport.pull_file(profile.device_serial, remote_path, local_path)
            else:
                raise ValueError("keep must be one of: phone, local, both")

            self.repository.mark_issue_resolved(issue_id)
            logger.emit("resolve", "completed", f"Resolved issue {issue_id}", root=root, relative_path=state.relative_path)
            self.repository.finalize_run(run_id, RUN_COMPLETED, summary={"issue_id": issue_id, "keep": keep})
            self.repository.prune_runs(profile.id)
            return self.sync_profile(profile.name, event_sink=event_sink)
        except Exception as exc:
            logger.emit("resolve", "failed", str(exc))
            self.repository.finalize_run(run_id, RUN_FAILED, summary={"issue_id": issue_id, "keep": keep, "error": str(exc)})
            self.repository.prune_runs(profile.id)
            raise

    def restore_checkpoint(
        self,
        profile_name: str,
        checkpoint_id: int,
        root_label: str | None = None,
        relative_path: str | None = None,
        event_sink: EventSink | None = None,
    ) -> SyncSummary:
        profile = self.repository.get_profile(profile_name)
        if profile.profile_state not in {PROFILE_ACTIVE, PROFILE_RESTORE_INCOMPLETE}:
            raise ValueError(f"Profile {profile.name} cannot be restored in state {profile.profile_state}")
        self.repository.get_checkpoint(profile.id, checkpoint_id)
        checkpoint_roots = self.repository.list_checkpoint_roots(profile.id, checkpoint_id)
        if not checkpoint_roots:
            raise ValueError(f"Checkpoint {checkpoint_id} has no roots")

        selected_roots = checkpoint_roots
        if root_label is not None:
            selected_roots = [root for root in checkpoint_roots if root.label == root_label]
            if not selected_roots:
                raise ValueError(f"Checkpoint {checkpoint_id} has no root labeled {root_label}")
        if relative_path is not None and root_label is None:
            raise ValueError("--path requires --root")

        run_id = self.repository.start_run(
            profile.id,
            RUN_RESTORE,
            source_profile_id=profile.id,
            source_checkpoint_id=checkpoint_id,
            summary={"checkpoint_id": checkpoint_id, "root_label": root_label, "relative_path": relative_path},
        )
        logger = RunLogger(self.repository, run_id, RUN_RESTORE, event_sink)
        summary = SyncSummary(profile_name=profile.name)
        mutated = False

        try:
            logger.emit("probe", "running", f"Probing device {profile.device_serial}")
            self.transport.probe_device(profile.device_serial)
            logger.emit("probe", "completed", f"Device {profile.device_serial} is ready")

            if relative_path is not None:
                source_root = selected_roots[0]
                target_root = self.repository.get_root_by_id(source_root.id)
                desired_entries = self.repository.list_checkpoint_entries(checkpoint_id, target_root.id, relative_path)
                if desired_entries:
                    blob_path = self.blob_store.path_for_hash(desired_entries[0]["blob_hash"])
                    if not blob_path.exists():
                        raise ValueError(f"Missing blob for restore: {desired_entries[0]['blob_hash']}")
                mutated = True
                state = self._restore_single_path(profile, target_root, checkpoint_id, relative_path, logger)
                self.repository.finalize_restored_file_states([state], checkpoint_id)
                if target_root.lifecycle != ROOT_ACTIVE:
                    self.repository.set_root_lifecycle(target_root.id, ROOT_ACTIVE)
                self.repository.update_profile_state(profile.id, PROFILE_ACTIVE)
                logger.emit("restore", "completed", f"Restored {target_root.label}/{relative_path}", root=target_root, relative_path=relative_path)
                self.repository.finalize_run(run_id, RUN_COMPLETED, summary={"checkpoint_id": checkpoint_id, "root_label": root_label, "relative_path": relative_path})
                self.repository.prune_runs(profile.id)
                return summary

            logger.emit("preflight", "running", f"Preparing restore from checkpoint {checkpoint_id}")
            root_scope = [self.repository.get_root_by_id(root.id) for root in selected_roots]
            for root in root_scope:
                for entry in self.repository.list_checkpoint_entries(checkpoint_id, root.id):
                    blob_path = self.blob_store.path_for_hash(entry["blob_hash"])
                    if not blob_path.exists():
                        raise ValueError(f"Missing blob for restore: {entry['blob_hash']}")
            logger.emit("preflight", "completed", f"Restore preflight passed for checkpoint {checkpoint_id}")
            mutated = True
            restore_states = self._restore_roots(profile, root_scope, checkpoint_id, logger)
            self.repository.finalize_restored_file_states(restore_states.states, checkpoint_id)
            for root_id in restore_states.reactivated_root_ids:
                self.repository.set_root_lifecycle(root_id, ROOT_ACTIVE)
            self.repository.update_profile_state(profile.id, PROFILE_ACTIVE)
            logger.emit("restore", "completed", f"Restored checkpoint {checkpoint_id}")
            self.repository.finalize_run(run_id, RUN_COMPLETED, summary={"checkpoint_id": checkpoint_id, "root_label": root_label})
            self.repository.prune_runs(profile.id)
            return summary
        except Exception as exc:
            if mutated:
                self.repository.update_profile_state(profile.id, PROFILE_RESTORE_INCOMPLETE)
            logger.emit("restore", "failed", str(exc))
            self.repository.finalize_run(run_id, RUN_FAILED, summary={"checkpoint_id": checkpoint_id, "error": str(exc)})
            self.repository.prune_runs(profile.id)
            raise

    def clone_restore(
        self,
        source_profile_name: str,
        checkpoint_id: int,
        target_device_serial: str,
        target_profile_name: str,
        target_mirror_dir: Path,
        event_sink: EventSink | None = None,
    ) -> SyncSummary:
        source_profile = self.repository.get_profile(source_profile_name)
        self.repository.get_checkpoint(source_profile.id, checkpoint_id)
        if target_device_serial == source_profile.device_serial:
            raise ValueError("clone restore requires a different target device")
        checkpoint_roots = self.repository.list_checkpoint_roots(source_profile.id, checkpoint_id)
        if not checkpoint_roots:
            raise ValueError(f"Checkpoint {checkpoint_id} has no roots")
        target_mirror_dir = target_mirror_dir.expanduser().resolve()
        if target_mirror_dir.exists() and any(target_mirror_dir.iterdir()):
            raise ValueError(f"Target mirror path must be absent or empty: {target_mirror_dir}")
        try:
            self.repository.get_profile(target_profile_name)
        except ValueError:
            pass
        else:
            raise ValueError(f"Profile already exists: {target_profile_name}")

        self.transport.probe_device(target_device_serial)
        for root in checkpoint_roots:
            for entry in self.repository.list_checkpoint_entries(checkpoint_id, root.id):
                blob_path = self.blob_store.path_for_hash(entry["blob_hash"])
                if not blob_path.exists():
                    raise ValueError(f"Missing blob for clone restore: {entry['blob_hash']}")

        target_profile = self.repository.create_profile(
            target_profile_name,
            target_device_serial,
            target_mirror_dir,
            profile_state=PROFILE_PENDING_CLONE,
            cloned_from_profile_id=source_profile.id,
            cloned_from_checkpoint_id=checkpoint_id,
        )
        target_profile.mirror_dir.mkdir(parents=True, exist_ok=True)
        run_id = self.repository.start_run(
            target_profile.id,
            RUN_CLONE_RESTORE,
            source_profile_id=source_profile.id,
            source_checkpoint_id=checkpoint_id,
            summary={"target_device_serial": target_device_serial},
        )
        logger = RunLogger(self.repository, run_id, RUN_CLONE_RESTORE, event_sink)
        summary = SyncSummary(profile_name=target_profile.name)

        try:
            logger.emit("probe", "completed", f"Target device {target_device_serial} is ready")
            root_map: dict[int, SyncRoot] = {}
            for source_root in checkpoint_roots:
                created = self.repository.add_root(target_profile.id, source_root.device_path, source_root.label, lifecycle=ROOT_ACTIVE)
                (target_profile.mirror_dir / created.label).mkdir(parents=True, exist_ok=True)
                root_map[source_root.id] = created

            restore_states = self._restore_roots(target_profile, list(root_map.values()), checkpoint_id, logger, source_root_id_map=root_map)

            checkpoint_entries = [
                {
                    "root_id": state.root_id,
                    "relative_path": state.relative_path,
                    "blob_hash": state.device_hash,
                    "size": state.device_size,
                    "device_mtime": state.device_mtime,
                }
                for state in restore_states.states
                if state.device_present and state.device_hash and state.device_size is not None and state.device_mtime is not None
            ]
            checkpoint_summary = {"profile_name": target_profile.name, "source_checkpoint_id": checkpoint_id, "operation": RUN_CLONE_RESTORE}
            target_checkpoint_id = self.repository.create_checkpoint(target_profile.id, RUN_COMPLETED, checkpoint_summary)
            self.repository.insert_checkpoint_entries(target_checkpoint_id, checkpoint_entries)
            for state in restore_states.states:
                state.last_synced_checkpoint_id = target_checkpoint_id
                state.last_restored_from_checkpoint_id = checkpoint_id
            self.repository.save_file_states(restore_states.states)
            self.repository.update_profile_state(target_profile.id, PROFILE_ACTIVE)
            summary.checkpoint_id = target_checkpoint_id
            logger.emit("checkpoint", "completed", f"Created target checkpoint {target_checkpoint_id}")
            self.repository.finalize_run(
                run_id,
                RUN_COMPLETED,
                result_checkpoint_id=target_checkpoint_id,
                summary={"source_checkpoint_id": checkpoint_id, "result_checkpoint_id": target_checkpoint_id},
            )
            self.repository.prune_runs(target_profile.id)
            self.repository.prune_checkpoints(target_profile.id, target_profile.checkpoint_retention)
            return summary
        except Exception as exc:
            self.repository.update_profile_state(target_profile.id, PROFILE_CLONE_FAILED)
            logger.emit("clone_restore", "failed", str(exc))
            self.repository.finalize_run(run_id, RUN_FAILED, summary={"source_checkpoint_id": checkpoint_id, "error": str(exc)})
            self.repository.prune_runs(target_profile.id)
            raise

    def _restore_roots(
        self,
        profile: Profile,
        roots: list[SyncRoot],
        checkpoint_id: int,
        logger: RunLogger,
        *,
        source_root_id_map: dict[int, SyncRoot] | None = None,
    ) -> RestoreScopeState:
        all_states: list[FileState] = []
        reactivated_root_ids: set[int] = set()

        for root in roots:
            source_root_id = root.id
            if source_root_id_map is not None:
                matches = [source_id for source_id, target_root in source_root_id_map.items() if target_root.id == root.id]
                if not matches:
                    raise ValueError(f"Missing source checkpoint root mapping for {root.label}")
                source_root_id = matches[0]

            logger.emit("restore_root", "running", f"Restoring root {root.label}", root=root)
            local_dir = local_root_path(profile, root)
            desired_entries = self.repository.list_checkpoint_entries(checkpoint_id, source_root_id)
            desired_map = {row["relative_path"]: row for row in desired_entries}
            current_device = self._safe_scan_root(profile.device_serial, root.device_path)
            current_local = scan_local_root(local_dir)
            previous_states = self.repository.list_file_states(profile.id, root.id)

            for row in desired_entries:
                blob_path = self.blob_store.path_for_hash(row["blob_hash"])
                if not blob_path.exists():
                    raise ValueError(f"Missing blob for restore: {row['blob_hash']}")

            for relpath in sorted(set(current_device) - set(desired_map)):
                logger.emit("delete", "running", f"Deleting extra remote file {relpath}", root=root, relative_path=relpath, action="delete_remote")
                self.transport.delete_file(profile.device_serial, ADBTransport.join_remote(root.device_path, relpath))
            for relpath in sorted(set(current_local) - set(desired_map)):
                logger.emit("delete", "running", f"Deleting extra local file {relpath}", root=root, relative_path=relpath, action="delete_local")
                target = local_dir / relpath
                if target.exists():
                    target.unlink()

            root_states: dict[str, FileState] = {}
            for relpath, row in desired_map.items():
                blob_path = self.blob_store.path_for_hash(row["blob_hash"])
                target = local_dir / relpath
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(blob_path, target)
                self.transport.push_file(profile.device_serial, blob_path, ADBTransport.join_remote(root.device_path, relpath))
                remote_stat = self.transport.stat_file(profile.device_serial, ADBTransport.join_remote(root.device_path, relpath))
                remote_stat.relative_path = relpath
                local_stat = FileMetadata(
                    relative_path=relpath,
                    size=target.stat().st_size,
                    mtime=int(target.stat().st_mtime),
                    absolute_path=str(target),
                )
                root_states[relpath] = self._build_state(
                    profile,
                    root,
                    relpath,
                    IN_SYNC,
                    remote_stat,
                    row["blob_hash"],
                    local_stat,
                    row["blob_hash"],
                    None,
                )
                summary_message = f"Restored {root.label}/{relpath}"
                logger.emit("restore_file", "completed", summary_message, root=root, relative_path=relpath, action="push")

            removed_paths = sorted((set(previous_states) | set(current_device) | set(current_local)) - set(desired_map))
            for relpath in removed_paths:
                root_states[relpath] = FileState(
                    id=previous_states.get(relpath).id if relpath in previous_states else None,
                    profile_id=profile.id,
                    root_id=root.id,
                    relative_path=relpath,
                    status=REMOVED,
                    device_present=False,
                    device_hash=None,
                    device_size=None,
                    device_mtime=None,
                    local_present=False,
                    local_hash=None,
                    local_size=None,
                    local_mtime=None,
                    conflict_copy_path=None,
                    updated_at=utc_now(),
                    last_synced_checkpoint_id=previous_states.get(relpath).last_synced_checkpoint_id if relpath in previous_states else None,
                    last_restored_from_checkpoint_id=checkpoint_id,
                )

            all_states.extend(root_states.values())
            if root.lifecycle != ROOT_ACTIVE:
                reactivated_root_ids.add(root.id)
            logger.emit("restore_root", "completed", f"Restored root {root.label}", root=root)

        return RestoreScopeState(states=all_states, reactivated_root_ids=reactivated_root_ids)

    def _restore_single_path(
        self,
        profile: Profile,
        root: SyncRoot,
        checkpoint_id: int,
        relative_path: str,
        logger: RunLogger,
    ) -> FileState:
        desired_entries = self.repository.list_checkpoint_entries(checkpoint_id, root.id, relative_path)
        previous_states = self.repository.list_file_states(profile.id, root.id)
        remote_path = ADBTransport.join_remote(root.device_path, relative_path)
        local_path = local_root_path(profile, root) / relative_path

        if not desired_entries:
            logger.emit("delete", "running", f"Deleting {root.label}/{relative_path}", root=root, relative_path=relative_path, action="delete")
            if local_path.exists():
                local_path.unlink()
            self.transport.delete_file(profile.device_serial, remote_path)
            state = FileState(
                id=previous_states.get(relative_path).id if relative_path in previous_states else None,
                profile_id=profile.id,
                root_id=root.id,
                relative_path=relative_path,
                status=REMOVED,
                device_present=False,
                device_hash=None,
                device_size=None,
                device_mtime=None,
                local_present=False,
                local_hash=None,
                local_size=None,
                local_mtime=None,
                conflict_copy_path=None,
                updated_at=utc_now(),
                last_synced_checkpoint_id=previous_states.get(relative_path).last_synced_checkpoint_id if relative_path in previous_states else None,
                last_restored_from_checkpoint_id=checkpoint_id,
            )
            return state

        row = desired_entries[0]
        blob_path = self.blob_store.path_for_hash(row["blob_hash"])
        if not blob_path.exists():
            raise ValueError(f"Missing blob for restore: {row['blob_hash']}")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(blob_path, local_path)
        self.transport.push_file(profile.device_serial, blob_path, remote_path)
        remote_stat = self.transport.stat_file(profile.device_serial, remote_path)
        remote_stat.relative_path = relative_path
        local_stat = FileMetadata(
            relative_path=relative_path,
            size=local_path.stat().st_size,
            mtime=int(local_path.stat().st_mtime),
            absolute_path=str(local_path),
        )
        return self._build_state(
            profile,
            root,
            relative_path,
            IN_SYNC,
            remote_stat,
            row["blob_hash"],
            local_stat,
            row["blob_hash"],
            None,
        )

    def _safe_scan_root(self, serial: str, root_path: str) -> dict[str, FileMetadata]:
        try:
            return self.transport.scan_root(serial, root_path)
        except Exception:
            return {}

    def _copy_local_history_root(
        self,
        source_profile: Profile,
        target_profile: Profile,
        root: SyncRoot,
        logger: RunLogger,
    ) -> None:
        source_dir = local_root_path(source_profile, root)
        if not source_dir.exists():
            return
        target_dir = local_root_path(target_profile, root)
        logger.emit("copy_history", "running", f"Copying local history for {root.label}", root=root, action="copy_local_history")
        shutil.copytree(source_dir, target_dir, dirs_exist_ok=True, copy_function=shutil.copy2)
        logger.emit("copy_history", "completed", f"Copied local history for {root.label}", root=root, action="copy_local_history")

    def _rebuild_active_root_from_phone(
        self,
        source_profile: Profile,
        target_profile: Profile,
        root: SyncRoot,
        summary: SyncSummary,
        logger: RunLogger,
    ) -> list[FileState]:
        previous_states = self.repository.list_file_states(source_profile.id, root.id)
        device_files = self.transport.scan_root(source_profile.device_serial, root.device_path)
        target_dir = local_root_path(target_profile, root)
        target_dir.mkdir(parents=True, exist_ok=True)

        result_states: list[FileState] = []
        for relative_path in sorted(set(previous_states) | set(device_files)):
            summary.files_seen += 1
            previous = previous_states.get(relative_path)
            if relative_path in device_files:
                state = self._pull_from_device(
                    target_profile,
                    root,
                    relative_path,
                    device_files[relative_path],
                    False,
                    summary,
                    logger,
                    previous,
                )
            else:
                summary.removed += 1
                logger.emit(
                    "mirror_file",
                    "completed",
                    f"Marked {root.label}/{relative_path} removed in new mirror",
                    root=root,
                    relative_path=relative_path,
                    action="remove_from_new_mirror",
                )
                state = FileState(
                    id=previous.id if previous else None,
                    profile_id=source_profile.id,
                    root_id=root.id,
                    relative_path=relative_path,
                    status=REMOVED,
                    device_present=False,
                    device_hash=None,
                    device_size=None,
                    device_mtime=None,
                    local_present=False,
                    local_hash=None,
                    local_size=None,
                    local_mtime=None,
                    conflict_copy_path=None,
                    updated_at=utc_now(),
                    last_synced_checkpoint_id=previous.last_synced_checkpoint_id if previous else None,
                    last_restored_from_checkpoint_id=previous.last_restored_from_checkpoint_id if previous else None,
                )
            result_states.append(self._carry_forward_mirror_change_metadata(state, previous))
        return result_states

    def _restore_active_root_from_checkpoint(
        self,
        source_profile: Profile,
        target_profile: Profile,
        root: SyncRoot,
        checkpoint_id: int,
        summary: SyncSummary,
        logger: RunLogger,
    ) -> list[FileState]:
        previous_states = self.repository.list_file_states(source_profile.id, root.id)
        target_dir = local_root_path(target_profile, root)
        target_dir.mkdir(parents=True, exist_ok=True)
        desired_entries = list(self.repository.list_checkpoint_entries(checkpoint_id, root.id))
        desired_map = {row["relative_path"]: row for row in desired_entries}
        root_states: dict[str, FileState] = {}

        for relpath, row in desired_map.items():
            blob_path = self.blob_store.path_for_hash(row["blob_hash"])
            target = target_dir / relpath
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(blob_path, target)
            previous = previous_states.get(relpath)
            local_stat = FileMetadata(
                relative_path=relpath,
                size=target.stat().st_size,
                mtime=int(target.stat().st_mtime),
                absolute_path=str(target),
            )
            state = FileState(
                id=previous.id if previous else None,
                profile_id=source_profile.id,
                root_id=root.id,
                relative_path=relpath,
                status=IN_SYNC,
                device_present=True,
                device_hash=row["blob_hash"],
                device_size=row["size"],
                device_mtime=row["device_mtime"],
                local_present=True,
                local_hash=row["blob_hash"],
                local_size=local_stat.size,
                local_mtime=local_stat.mtime,
                conflict_copy_path=None,
                updated_at=utc_now(),
                last_synced_checkpoint_id=checkpoint_id,
                last_restored_from_checkpoint_id=None,
            )
            root_states[relpath] = self._carry_forward_mirror_change_metadata(state, previous)
            summary.files_seen += 1
            logger.emit(
                "mirror_file",
                "completed",
                f"Rebuilt {root.label}/{relpath} from checkpoint {checkpoint_id}",
                root=root,
                relative_path=relpath,
                action="copy_checkpoint_blob",
            )

        for relpath in sorted(set(previous_states) - set(desired_map)):
            previous = previous_states.get(relpath)
            state = FileState(
                id=previous.id if previous else None,
                profile_id=source_profile.id,
                root_id=root.id,
                relative_path=relpath,
                status=REMOVED,
                device_present=False,
                device_hash=None,
                device_size=None,
                device_mtime=None,
                local_present=False,
                local_hash=None,
                local_size=None,
                local_mtime=None,
                conflict_copy_path=None,
                updated_at=utc_now(),
                last_synced_checkpoint_id=previous.last_synced_checkpoint_id if previous else checkpoint_id,
                last_restored_from_checkpoint_id=previous.last_restored_from_checkpoint_id if previous else None,
            )
            root_states[relpath] = self._carry_forward_mirror_change_metadata(state, previous)
            summary.removed += 1

        return list(root_states.values())

    @staticmethod
    def _profile_with_mirror(profile: Profile, mirror_dir: Path) -> Profile:
        return Profile(
            id=profile.id,
            name=profile.name,
            device_serial=profile.device_serial,
            mirror_dir=mirror_dir,
            checkpoint_retention=profile.checkpoint_retention,
            created_at=profile.created_at,
            profile_state=profile.profile_state,
            cloned_from_profile_id=profile.cloned_from_profile_id,
            cloned_from_checkpoint_id=profile.cloned_from_checkpoint_id,
        )

    def _determine_change_mirror_source(self, profile: Profile, active_roots: list[SyncRoot]) -> tuple[str, int | None]:
        if not active_roots:
            return "local_history_only", None
        try:
            self.transport.probe_device(profile.device_serial)
            return "phone", None
        except Exception:
            checkpoints = self.repository.list_checkpoints(profile.id)
            if not checkpoints:
                raise ValueError(
                    f"Device {profile.device_serial} is unavailable and profile {profile.name} has no checkpoint fallback"
                )
            checkpoint_id = checkpoints[0].id
            self._preflight_checkpoint_blobs(checkpoint_id, active_roots)
            return "checkpoint", checkpoint_id

    def _preflight_checkpoint_blobs(self, checkpoint_id: int, roots: list[SyncRoot]) -> None:
        for root in roots:
            for entry in self.repository.list_checkpoint_entries(checkpoint_id, root.id):
                blob_path = self.blob_store.path_for_hash(entry["blob_hash"])
                if not blob_path.exists():
                    raise ValueError(f"Missing blob for checkpoint rebuild: {entry['blob_hash']}")

    @staticmethod
    def _validate_target_mirror_dir(current_mirror_dir: Path, target_mirror_dir: Path) -> None:
        if target_mirror_dir == current_mirror_dir:
            raise ValueError("Backup folder is already set to that path")
        if target_mirror_dir.exists():
            if not target_mirror_dir.is_dir():
                raise ValueError(f"Target backup folder must be a directory: {target_mirror_dir}")
            if any(target_mirror_dir.iterdir()):
                raise ValueError(f"Target backup folder must be empty: {target_mirror_dir}")
            return

        parent = target_mirror_dir.parent
        while not parent.exists() and parent != parent.parent:
            parent = parent.parent
        if not parent.exists() or not parent.is_dir():
            raise ValueError(f"Target backup folder has no valid parent directory: {target_mirror_dir}")
        if not os.access(parent, os.W_OK):
            raise ValueError(f"Parent directory is not writable: {parent}")

    @staticmethod
    def _carry_forward_mirror_change_metadata(state: FileState, previous: FileState | None) -> FileState:
        if previous is None:
            return state
        state.last_synced_checkpoint_id = previous.last_synced_checkpoint_id
        state.last_restored_from_checkpoint_id = previous.last_restored_from_checkpoint_id
        if previous.status == CONFLICT and state.device_present and state.local_present:
            state.status = CONFLICT
            state.conflict_copy_path = previous.conflict_copy_path
        return state

    @staticmethod
    def _cleanup_target_mirror_dir(target_mirror_dir: Path, *, existed_before: bool) -> None:
        if not target_mirror_dir.exists():
            return
        if existed_before:
            for child in list(target_mirror_dir.iterdir()):
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    try:
                        child.unlink()
                    except FileNotFoundError:
                        pass
            return
        shutil.rmtree(target_mirror_dir, ignore_errors=True)

    def _sync_root(
        self,
        profile: Profile,
        root: SyncRoot,
        summary: SyncSummary,
        dry_run: bool,
        logger: RunLogger,
    ) -> RootSyncResult:
        previous_states = self.repository.list_file_states(profile.id, root.id)
        device_files = self.transport.scan_root(profile.device_serial, root.device_path)
        local_dir = local_root_path(profile, root)
        if previous_states and not local_dir.exists():
            return self._reseed_missing_local_root(
                profile,
                root,
                previous_states,
                device_files,
                dry_run,
                summary,
                logger,
            )
        local_files = scan_local_root(local_dir)
        result_states: list[FileState] = []

        for relative_path in sorted(set(previous_states) | set(device_files) | set(local_files)):
            summary.files_seen += 1
            state = self._sync_path(
                profile=profile,
                root=root,
                previous=previous_states.get(relative_path),
                device_meta=device_files.get(relative_path),
                local_meta=local_files.get(relative_path),
                dry_run=dry_run,
                summary=summary,
                logger=logger,
            )
            result_states.append(state)

        checkpoint_entries = [
            {
                "root_id": root.id,
                "relative_path": state.relative_path,
                "blob_hash": state.device_hash,
                "size": state.device_size,
                "device_mtime": state.device_mtime,
            }
            for state in result_states
            if state.device_present and state.device_hash and state.device_size is not None and state.device_mtime is not None
        ]
        return RootSyncResult(states=result_states, checkpoint_entries=checkpoint_entries)

    def _reseed_missing_local_root(
        self,
        profile: Profile,
        root: SyncRoot,
        previous_states: dict[str, FileState],
        device_files: dict[str, FileMetadata],
        dry_run: bool,
        summary: SyncSummary,
        logger: RunLogger,
    ) -> RootSyncResult:
        local_dir = local_root_path(profile, root)
        logger.emit(
            "missing_local_root_detected",
            "running",
            f"Local root {root.label} is missing; rebuilding from phone",
            root=root,
            action="reseed_local_from_phone",
        )
        if not dry_run:
            local_dir.mkdir(parents=True, exist_ok=True)

        result_states: list[FileState] = []
        for relative_path in sorted(set(previous_states) | set(device_files)):
            summary.files_seen += 1
            if relative_path in device_files:
                state = self._pull_from_device(
                    profile,
                    root,
                    relative_path,
                    device_files[relative_path],
                    dry_run,
                    summary,
                    logger,
                    previous_states.get(relative_path),
                )
            else:
                summary.removed += 1
                logger.emit(
                    "reseed_local_from_phone",
                    "completed",
                    f"Marked {relative_path} removed during local rebuild",
                    root=root,
                    relative_path=relative_path,
                    action="reseed_remove",
                )
                previous = previous_states.get(relative_path)
                state = FileState(
                    id=previous.id if previous else None,
                    profile_id=profile.id,
                    root_id=root.id,
                    relative_path=relative_path,
                    status=REMOVED,
                    device_present=False,
                    device_hash=None,
                    device_size=None,
                    device_mtime=None,
                    local_present=False,
                    local_hash=None,
                    local_size=None,
                    local_mtime=None,
                    conflict_copy_path=None,
                    updated_at=utc_now(),
                    last_synced_checkpoint_id=previous.last_synced_checkpoint_id if previous else None,
                    last_restored_from_checkpoint_id=previous.last_restored_from_checkpoint_id if previous else None,
                )
            result_states.append(state)

        logger.emit(
            "reseed_local_from_phone",
            "completed",
            f"Rebuilt local root {root.label} from phone",
            root=root,
            action="reseed_local_from_phone",
        )
        checkpoint_entries = [
            {
                "root_id": root.id,
                "relative_path": state.relative_path,
                "blob_hash": state.device_hash,
                "size": state.device_size,
                "device_mtime": state.device_mtime,
            }
            for state in result_states
            if state.device_present and state.device_hash and state.device_size is not None and state.device_mtime is not None
        ]
        return RootSyncResult(states=result_states, checkpoint_entries=checkpoint_entries)

    def _repair_local_root(
        self,
        profile: Profile,
        root: SyncRoot,
        checkpoint_id: int,
        desired_entries: list[dict[str, object]],
        summary: SyncSummary,
        logger: RunLogger,
    ) -> list[FileState]:
        local_dir = local_root_path(profile, root)
        previous_states = self.repository.list_file_states(profile.id, root.id)
        current_local = scan_local_root(local_dir)
        desired_map = {row["relative_path"]: row for row in desired_entries}
        root_states: dict[str, FileState] = {}

        local_dir.mkdir(parents=True, exist_ok=True)
        for relpath in sorted(set(current_local) - set(desired_map)):
            logger.emit(
                "repair_local_from_checkpoint",
                "running",
                f"Deleting extra local file {relpath}",
                root=root,
                relative_path=relpath,
                action="delete_local",
            )
            target = local_dir / relpath
            if target.exists():
                target.unlink()
            summary.removed += 1

        for relpath, row in desired_map.items():
            blob_path = self.blob_store.path_for_hash(row["blob_hash"])
            target = local_dir / relpath
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(blob_path, target)
            local_stat = FileMetadata(
                relative_path=relpath,
                size=target.stat().st_size,
                mtime=int(target.stat().st_mtime),
                absolute_path=str(target),
            )
            previous = previous_states.get(relpath)
            root_states[relpath] = FileState(
                id=previous.id if previous else None,
                profile_id=profile.id,
                root_id=root.id,
                relative_path=relpath,
                status=IN_SYNC,
                device_present=True,
                device_hash=row["blob_hash"],
                device_size=row["size"],
                device_mtime=row["device_mtime"],
                local_present=True,
                local_hash=row["blob_hash"],
                local_size=local_stat.size,
                local_mtime=local_stat.mtime,
                conflict_copy_path=None,
                updated_at=utc_now(),
                last_synced_checkpoint_id=checkpoint_id,
                last_restored_from_checkpoint_id=checkpoint_id,
            )
            summary.files_seen += 1
            logger.emit(
                "repair_local_from_checkpoint",
                "completed",
                f"Repaired {root.label}/{relpath}",
                root=root,
                relative_path=relpath,
                action="copy_blob",
            )

        for relpath in sorted((set(previous_states) | set(current_local)) - set(desired_map)):
            previous = previous_states.get(relpath)
            root_states[relpath] = FileState(
                id=previous.id if previous else None,
                profile_id=profile.id,
                root_id=root.id,
                relative_path=relpath,
                status=REMOVED,
                device_present=False,
                device_hash=None,
                device_size=None,
                device_mtime=None,
                local_present=False,
                local_hash=None,
                local_size=None,
                local_mtime=None,
                conflict_copy_path=None,
                updated_at=utc_now(),
                last_synced_checkpoint_id=checkpoint_id,
                last_restored_from_checkpoint_id=checkpoint_id,
            )

        return list(root_states.values())

    def _roots_missing_local_history(self, profile: Profile, roots: list[SyncRoot]) -> list[SyncRoot]:
        missing: list[SyncRoot] = []
        for root in roots:
            if local_root_path(profile, root).exists():
                continue
            if self.repository.list_file_states(profile.id, root.id):
                missing.append(root)
        return missing

    def _sync_path(
        self,
        profile: Profile,
        root: SyncRoot,
        previous: FileState | None,
        device_meta: FileMetadata | None,
        local_meta: FileMetadata | None,
        dry_run: bool,
        summary: SyncSummary,
        logger: RunLogger,
    ) -> FileState:
        stamp = utc_now().replace(":", "").replace("-", "")
        relative_path = device_meta.relative_path if device_meta else local_meta.relative_path if local_meta else previous.relative_path
        remote_path = ADBTransport.join_remote(root.device_path, relative_path)
        prev_known = previous is not None and (previous.device_present or previous.local_present)
        prev_status = previous.status if previous is not None else None
        device_hash_cache = previous.device_hash if self._same_meta(device_meta, previous, "device") else None
        local_hash_cache = previous.local_hash if self._same_meta(local_meta, previous, "local") else None

        def get_device_hash() -> str | None:
            nonlocal device_hash_cache
            if device_meta is None:
                return None
            if device_hash_cache is None:
                device_hash_cache = self.transport.hash_remote_file(profile.device_serial, remote_path)
            return device_hash_cache

        def get_local_hash() -> str | None:
            nonlocal local_hash_cache
            if local_meta is None:
                return None
            if local_hash_cache is None:
                local_hash_cache = sha256_file(Path(local_meta.absolute_path))
            return local_hash_cache

        device_changed = self._side_changed(device_meta, previous, "device", get_device_hash)
        local_changed = self._side_changed(local_meta, previous, "local", get_local_hash)

        if device_meta and local_meta:
            equal_now = False
            if previous is None or prev_status != IN_SYNC or device_changed or local_changed:
                equal_now = get_device_hash() == get_local_hash()
            elif previous.device_hash and previous.local_hash:
                equal_now = previous.device_hash == previous.local_hash

            if previous is None:
                if equal_now:
                    summary.unchanged += 1
                    return self._build_state(profile, root, relative_path, IN_SYNC, device_meta, get_device_hash(), local_meta, get_local_hash(), None, previous)
                return self._handle_conflict(profile, root, relative_path, device_meta, local_meta, dry_run, summary, stamp, logger)

            if prev_status in {CONFLICT, DIVERGED_MISSING_DEVICE, DIVERGED_MISSING_LOCAL} and equal_now:
                summary.unchanged += 1
                return self._build_state(profile, root, relative_path, IN_SYNC, device_meta, get_device_hash(), local_meta, get_local_hash(), None, previous)

            if not device_changed and not local_changed:
                summary.unchanged += 1
                conflict_copy = previous.conflict_copy_path if prev_status == CONFLICT else None
                status = prev_status if prev_status == CONFLICT else IN_SYNC
                return self._build_state(profile, root, relative_path, status, device_meta, get_device_hash(), local_meta, get_local_hash(), conflict_copy, previous)

            if equal_now:
                summary.unchanged += 1
                return self._build_state(profile, root, relative_path, IN_SYNC, device_meta, get_device_hash(), local_meta, get_local_hash(), None, previous)

            if device_changed and not local_changed:
                return self._pull_from_device(profile, root, relative_path, device_meta, dry_run, summary, logger, previous)
            if local_changed and not device_changed:
                return self._push_to_device(profile, root, relative_path, local_meta, dry_run, summary, logger, previous)
            return self._handle_conflict(profile, root, relative_path, device_meta, local_meta, dry_run, summary, stamp, logger, previous)

        if device_meta and not local_meta:
            if not prev_known:
                return self._pull_from_device(profile, root, relative_path, device_meta, dry_run, summary, logger, previous)
            summary.divergences += 1
            logger.emit("issue", "completed", f"Device-only divergence at {relative_path}", root=root, relative_path=relative_path, action="divergence")
            return self._build_state(
                profile,
                root,
                relative_path,
                DIVERGED_MISSING_LOCAL,
                device_meta,
                get_device_hash(),
                None,
                None,
                previous.conflict_copy_path if previous else None,
                previous,
            )

        if local_meta and not device_meta:
            if not prev_known:
                return self._push_to_device(profile, root, relative_path, local_meta, dry_run, summary, logger, previous)
            summary.divergences += 1
            logger.emit("issue", "completed", f"Local-only divergence at {relative_path}", root=root, relative_path=relative_path, action="divergence")
            return self._build_state(
                profile,
                root,
                relative_path,
                DIVERGED_MISSING_DEVICE,
                None,
                None,
                local_meta,
                get_local_hash(),
                previous.conflict_copy_path if previous else None,
                previous,
            )

        summary.removed += 1
        return FileState(
            id=previous.id if previous else None,
            profile_id=profile.id,
            root_id=root.id,
            relative_path=relative_path,
            status=REMOVED,
            device_present=False,
            device_hash=None,
            device_size=None,
            device_mtime=None,
            local_present=False,
            local_hash=None,
            local_size=None,
            local_mtime=None,
            conflict_copy_path=None,
            updated_at=utc_now(),
            last_synced_checkpoint_id=previous.last_synced_checkpoint_id if previous else None,
            last_restored_from_checkpoint_id=previous.last_restored_from_checkpoint_id if previous else None,
        )

    def _pull_from_device(
        self,
        profile: Profile,
        root: SyncRoot,
        relative_path: str,
        device_meta: FileMetadata,
        dry_run: bool,
        summary: SyncSummary,
        logger: RunLogger,
        previous: FileState | None,
    ) -> FileState:
        local_path = local_root_path(profile, root) / relative_path
        summary.pulled += 1
        logger.emit("file_transfer", "running", f"Pulling {relative_path}", root=root, relative_path=relative_path, action="pull")
        if dry_run:
            return self._build_state(profile, root, relative_path, IN_SYNC, device_meta, None, None, None, None, previous)
        self.transport.pull_file(profile.device_serial, device_meta.absolute_path, local_path)
        local_stat = FileMetadata(
            relative_path=relative_path,
            size=local_path.stat().st_size,
            mtime=int(local_path.stat().st_mtime),
            absolute_path=str(local_path),
        )
        blob_hash = sha256_file(local_path)
        self.blob_store.store_path(local_path, blob_hash)
        logger.emit("file_transfer", "completed", f"Pulled {relative_path}", root=root, relative_path=relative_path, action="pull")
        return self._build_state(profile, root, relative_path, IN_SYNC, device_meta, blob_hash, local_stat, blob_hash, None, previous)

    def _push_to_device(
        self,
        profile: Profile,
        root: SyncRoot,
        relative_path: str,
        local_meta: FileMetadata,
        dry_run: bool,
        summary: SyncSummary,
        logger: RunLogger,
        previous: FileState | None,
    ) -> FileState:
        local_path = Path(local_meta.absolute_path)
        remote_path = ADBTransport.join_remote(root.device_path, relative_path)
        summary.pushed += 1
        blob_hash = sha256_file(local_path)
        logger.emit("file_transfer", "running", f"Pushing {relative_path}", root=root, relative_path=relative_path, action="push")
        if dry_run:
            return self._build_state(profile, root, relative_path, IN_SYNC, None, blob_hash, local_meta, blob_hash, None, previous)
        self.blob_store.store_path(local_path, blob_hash)
        self.transport.push_file(profile.device_serial, local_path, remote_path)
        device_stat = self.transport.stat_file(profile.device_serial, remote_path)
        device_stat.relative_path = relative_path
        logger.emit("file_transfer", "completed", f"Pushed {relative_path}", root=root, relative_path=relative_path, action="push")
        return self._build_state(profile, root, relative_path, IN_SYNC, device_stat, blob_hash, local_meta, blob_hash, None, previous)

    def _handle_conflict(
        self,
        profile: Profile,
        root: SyncRoot,
        relative_path: str,
        device_meta: FileMetadata,
        local_meta: FileMetadata,
        dry_run: bool,
        summary: SyncSummary,
        stamp: str,
        logger: RunLogger,
        previous: FileState | None = None,
    ) -> FileState:
        summary.conflicts += 1
        local_path = Path(local_meta.absolute_path)
        archive_path = conflict_copy_path(profile, root, relative_path, stamp)
        logger.emit("conflict", "running", f"Resolving conflict at {relative_path}", root=root, relative_path=relative_path, action="phone_wins")
        if dry_run:
            return self._build_state(profile, root, relative_path, CONFLICT, device_meta, None, local_meta, None, str(archive_path), previous)
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, archive_path)
        self.transport.pull_file(profile.device_serial, device_meta.absolute_path, local_path)
        local_stat = FileMetadata(
            relative_path=relative_path,
            size=local_path.stat().st_size,
            mtime=int(local_path.stat().st_mtime),
            absolute_path=str(local_path),
        )
        blob_hash = sha256_file(local_path)
        self.blob_store.store_path(local_path, blob_hash)
        logger.emit("conflict", "completed", f"Archived local conflict copy for {relative_path}", root=root, relative_path=relative_path, action="phone_wins")
        return self._build_state(
            profile,
            root,
            relative_path,
            CONFLICT,
            device_meta,
            blob_hash,
            local_stat,
            blob_hash,
            str(archive_path),
            previous,
        )

    @staticmethod
    def _same_meta(current: FileMetadata | None, previous: FileState | None, side: str) -> bool:
        if current is None or previous is None:
            return False
        if side == "device":
            return previous.device_present and previous.device_size == current.size and previous.device_mtime == current.mtime
        return previous.local_present and previous.local_size == current.size and previous.local_mtime == current.mtime

    @staticmethod
    def _side_changed(
        current: FileMetadata | None,
        previous: FileState | None,
        side: str,
        hash_getter,
    ) -> bool:
        if current is None:
            return bool(previous.device_present if side == "device" else previous.local_present) if previous else False
        if previous is None:
            hash_getter()
            return True
        was_present = previous.device_present if side == "device" else previous.local_present
        if not was_present:
            hash_getter()
            return True
        previous_size = previous.device_size if side == "device" else previous.local_size
        previous_mtime = previous.device_mtime if side == "device" else previous.local_mtime
        previous_hash = previous.device_hash if side == "device" else previous.local_hash
        if previous_size == current.size and previous_mtime == current.mtime:
            return False
        current_hash = hash_getter()
        return current_hash != previous_hash

    @staticmethod
    def _build_state(
        profile: Profile,
        root: SyncRoot,
        relative_path: str,
        status: str,
        device_meta: FileMetadata | None,
        device_hash: str | None,
        local_meta: FileMetadata | None,
        local_hash: str | None,
        conflict_path: str | None,
        previous: FileState | None = None,
    ) -> FileState:
        return FileState(
            id=previous.id if previous else None,
            profile_id=profile.id,
            root_id=root.id,
            relative_path=relative_path,
            status=status,
            device_present=device_meta is not None,
            device_hash=device_hash,
            device_size=device_meta.size if device_meta else None,
            device_mtime=device_meta.mtime if device_meta else None,
            local_present=local_meta is not None,
            local_hash=local_hash,
            local_size=local_meta.size if local_meta else None,
            local_mtime=local_meta.mtime if local_meta else None,
            conflict_copy_path=conflict_path,
            updated_at=utc_now(),
            last_synced_checkpoint_id=previous.last_synced_checkpoint_id if previous else None,
            last_restored_from_checkpoint_id=previous.last_restored_from_checkpoint_id if previous else None,
        )


def summary_to_text(summary: SyncSummary) -> str:
    return json.dumps(summary.to_dict(), indent=2, sort_keys=True)
