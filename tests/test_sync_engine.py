from __future__ import annotations

import hashlib
import os
from pathlib import Path

from androidmigrate.models import CONFLICT, PROFILE_PENDING_CLONE, ROOT_DISABLED
from androidmigrate.storage import BlobStore, Repository
from androidmigrate.sync_engine import SyncEngine
from androidmigrate.transport import ADBTransport


class FakeTransport:
    def __init__(self, files: dict[str, tuple[bytes, int]] | None = None) -> None:
        self.files: dict[str, dict[str, object]] = {}
        if files:
            for path, (content, mtime) in files.items():
                self.files[path] = {"content": content, "mtime": mtime}
        self.push_calls = 0
        self.pull_calls = 0
        self.probe_error: Exception | None = None

    def list_devices(self):
        return []

    def probe_device(self, serial: str) -> None:
        if self.probe_error is not None:
            raise self.probe_error
        return None

    def scan_root(self, serial: str, device_path: str):
        prefix = device_path.rstrip("/")
        result = {}
        for path, entry in self.files.items():
            if path.startswith(prefix + "/"):
                rel = path[len(prefix) + 1 :]
                result[rel] = type(
                    "Meta",
                    (),
                    {
                        "relative_path": rel,
                        "size": len(entry["content"]),
                        "mtime": entry["mtime"],
                        "absolute_path": path,
                    },
                )()
        return result

    def hash_remote_file(self, serial: str, remote_path: str) -> str:
        entry = self.files[remote_path]
        return hashlib.sha256(entry["content"]).hexdigest()

    def pull_file(self, serial: str, remote_path: str, local_path: Path) -> None:
        self.pull_calls += 1
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(self.files[remote_path]["content"])

    def push_file(self, serial: str, local_path: Path, remote_path: str) -> None:
        self.push_calls += 1
        self.files[remote_path] = {
            "content": local_path.read_bytes(),
            "mtime": int(local_path.stat().st_mtime),
        }

    def stat_file(self, serial: str, remote_path: str):
        entry = self.files[remote_path]
        return type(
            "Meta",
            (),
            {
                "relative_path": Path(remote_path).name,
                "size": len(entry["content"]),
                "mtime": entry["mtime"],
                "absolute_path": remote_path,
            },
        )()

    def list_directories(self, serial: str, device_path: str):
        prefix = device_path.rstrip("/")
        seen = {}
        for path in self.files:
            if not path.startswith(prefix + "/"):
                continue
            rel = path[len(prefix) + 1 :]
            head = rel.split("/", 1)[0]
            if "/" not in rel:
                continue
            child = f"{prefix}/{head}"
            seen[child] = type(
                "RemoteDir",
                (),
                {
                    "name": head,
                    "absolute_path": child,
                    "parent_path": prefix,
                },
            )()
        return sorted(seen.values(), key=lambda entry: entry.name.lower())

    def path_info(self, serial: str, device_path: str) -> str:
        prefix = device_path.rstrip("/")
        if prefix in self.files:
            return "file"
        for path in self.files:
            if path.startswith(prefix + "/"):
                return "directory"
        return "missing"

    def delete_file(self, serial: str, remote_path: str) -> None:
        self.files.pop(remote_path, None)


def setup_engine(tmp_path: Path, transport: FakeTransport) -> tuple[Repository, SyncEngine]:
    state_dir = tmp_path / "state"
    mirror_dir = tmp_path / "mirror"
    repository = Repository(state_dir)
    repository.create_profile("demo", "SER123", mirror_dir)
    repository.add_root(1, "/sdcard/DCIM", "dcim")
    engine = SyncEngine(repository, BlobStore(state_dir), transport)
    return repository, engine


def test_initial_sync_and_second_delta_run(tmp_path: Path) -> None:
    transport = FakeTransport(
        {
            "/sdcard/DCIM/Camera/a.jpg": (b"a1", 100),
            "/sdcard/DCIM/Screenshots/b.png": (b"b1", 120),
        }
    )
    repository, engine = setup_engine(tmp_path, transport)

    first = engine.sync_profile("demo")
    assert first.pulled == 2
    assert first.pushed == 0
    assert (tmp_path / "mirror" / "dcim" / "Camera" / "a.jpg").read_bytes() == b"a1"
    assert len(repository.list_checkpoints(1)) == 1

    second = engine.sync_profile("demo")
    assert second.pulled == 0
    assert second.pushed == 0
    assert second.unchanged == 2
    assert len(repository.list_checkpoints(1)) == 2


def test_local_change_pushes_back_to_phone(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"a1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")

    local_file = tmp_path / "mirror" / "dcim" / "Camera" / "a.jpg"
    local_file.write_bytes(b"a2")
    os.utime(local_file, (200, 200))

    summary = engine.sync_profile("demo")
    assert summary.pushed == 1
    assert transport.files["/sdcard/DCIM/Camera/a.jpg"]["content"] == b"a2"


def test_conflict_archives_local_copy_and_phone_wins(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"phone-v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")

    local_file = tmp_path / "mirror" / "dcim" / "Camera" / "a.jpg"
    local_file.write_bytes(b"local-v2")
    os.utime(local_file, (200, 200))
    transport.files["/sdcard/DCIM/Camera/a.jpg"] = {"content": b"phone-v2", "mtime": 201}

    summary = engine.sync_profile("demo")
    issues = engine.list_issues("demo")
    assert summary.conflicts == 1
    assert local_file.read_bytes() == b"phone-v2"
    assert len(issues) == 1
    conflict_copy = Path(issues[0][1].conflict_copy_path)
    assert conflict_copy.read_bytes() == b"local-v2"


def test_restore_brings_back_older_checkpoint(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")

    transport.files["/sdcard/DCIM/Camera/a.jpg"] = {"content": b"v2", "mtime": 300}
    engine.sync_profile("demo")

    summary = engine.restore_checkpoint("demo", 1, root_label="dcim", relative_path="Camera/a.jpg")
    assert summary.checkpoint_id is None
    assert transport.files["/sdcard/DCIM/Camera/a.jpg"]["content"] == b"v1"
    assert (tmp_path / "mirror" / "dcim" / "Camera" / "a.jpg").read_bytes() == b"v1"
    assert len(repository.list_checkpoints(1)) == 2


def test_deleted_on_phone_becomes_divergence_and_resolve_keep_local_pushes_back(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")

    transport.delete_file("SER123", "/sdcard/DCIM/Camera/a.jpg")
    summary = engine.sync_profile("demo")
    assert summary.divergences == 1

    issue_id = engine.list_issues("demo")[0][1].id
    assert issue_id is not None
    engine.resolve_issue("demo", issue_id, keep="local")
    assert transport.files["/sdcard/DCIM/Camera/a.jpg"]["content"] == b"v1"


def test_checkpoint_summary_persists_real_checkpoint_id(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)

    summary = engine.sync_profile("demo")
    checkpoint = repository.list_checkpoints(1)[0]

    assert summary.checkpoint_id == 1
    assert '"checkpoint_id": 1' in checkpoint.summary_json


def test_missing_local_root_reseeds_from_phone(tmp_path: Path) -> None:
    transport = FakeTransport(
        {
            "/sdcard/DCIM/Camera/a.jpg": (b"a1", 100),
            "/sdcard/DCIM/Screenshots/b.png": (b"b1", 120),
        }
    )
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")

    local_root = tmp_path / "mirror" / "dcim"
    for child in sorted(local_root.rglob("*"), reverse=True):
        if child.is_file():
            child.unlink()
        else:
            child.rmdir()
    local_root.rmdir()

    summary = engine.sync_profile("demo")
    issues = engine.list_issues("demo")

    assert summary.pulled == 2
    assert summary.divergences == 0
    assert issues == []
    assert (local_root / "Camera" / "a.jpg").read_bytes() == b"a1"
    assert (local_root / "Screenshots" / "b.png").read_bytes() == b"b1"


def test_existing_empty_root_directory_does_not_auto_reseed(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"a1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")

    local_file = tmp_path / "mirror" / "dcim" / "Camera" / "a.jpg"
    local_file.unlink()

    summary = engine.sync_profile("demo")

    assert summary.pulled == 0
    assert summary.divergences == 1


def test_missing_local_root_with_unavailable_device_suggests_repair(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"a1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")

    local_root = tmp_path / "mirror" / "dcim"
    for child in sorted(local_root.rglob("*"), reverse=True):
        if child.is_file():
            child.unlink()
        else:
            child.rmdir()
    local_root.rmdir()
    transport.probe_error = RuntimeError("device offline")

    try:
        engine.sync_profile("demo")
    except ValueError as exc:
        assert "repair-local demo" in str(exc)
    else:
        raise AssertionError("expected sync failure with repair guidance")


def test_repair_local_rebuilds_from_latest_checkpoint_without_touching_phone(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")
    before_checkpoints = len(repository.list_checkpoints(1))

    transport.files["/sdcard/DCIM/Camera/a.jpg"] = {"content": b"v2", "mtime": 200}
    local_root = tmp_path / "mirror" / "dcim"
    for child in sorted(local_root.rglob("*"), reverse=True):
        if child.is_file():
            child.unlink()
        else:
            child.rmdir()
    local_root.rmdir()

    summary = engine.repair_local("demo")

    assert summary.checkpoint_id is None
    assert (local_root / "Camera" / "a.jpg").read_bytes() == b"v1"
    assert transport.files["/sdcard/DCIM/Camera/a.jpg"]["content"] == b"v2"
    assert len(repository.list_checkpoints(1)) == before_checkpoints
    runs = repository.list_recent_runs(1)
    assert runs[0].operation_type == "repair_local"
    assert runs[0].status == "completed"


def test_repair_local_fails_when_required_blob_is_missing(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")

    checkpoint_id = repository.list_checkpoints(1)[0].id
    entry = repository.list_checkpoint_entries(checkpoint_id, 1)[0]
    blob_path = BlobStore(tmp_path / "state").path_for_hash(entry["blob_hash"])
    blob_path.unlink()

    local_root = tmp_path / "mirror" / "dcim"
    for child in sorted(local_root.rglob("*"), reverse=True):
        if child.is_file():
            child.unlink()
        else:
            child.rmdir()
    local_root.rmdir()

    try:
        engine.repair_local("demo")
    except ValueError as exc:
        assert "Missing blob for local repair" in str(exc)
    else:
        raise AssertionError("expected repair-local failure when blob is missing")
    assert not local_root.exists()


def test_change_mirror_path_rebuilds_active_roots_and_copies_disabled_history(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    repository.add_root(1, "/sdcard/Documents", "docs", lifecycle=ROOT_DISABLED)
    engine.sync_profile("demo")

    old_mirror = tmp_path / "mirror"
    docs_file = old_mirror / "docs" / "old.txt"
    docs_file.parent.mkdir(parents=True, exist_ok=True)
    docs_file.write_text("history")
    before_checkpoints = len(repository.list_checkpoints(1))

    target_mirror = tmp_path / "mirror-new"
    engine.change_mirror_path("demo", target_mirror)

    profile = repository.get_profile("demo")
    assert profile.mirror_dir == target_mirror
    assert (target_mirror / "dcim" / "Camera" / "a.jpg").read_bytes() == b"v1"
    assert (target_mirror / "docs" / "old.txt").read_text() == "history"
    assert docs_file.read_text() == "history"
    assert (old_mirror / "dcim" / "Camera" / "a.jpg").read_bytes() == b"v1"
    assert len(repository.list_checkpoints(1)) == before_checkpoints
    latest_run = repository.list_recent_runs(1)[0]
    assert latest_run.operation_type == "change_mirror"
    assert latest_run.status == "completed"


def test_change_mirror_path_falls_back_to_checkpoint(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")
    before_checkpoints = len(repository.list_checkpoints(1))
    transport.files["/sdcard/DCIM/Camera/a.jpg"] = {"content": b"v2", "mtime": 200}
    transport.probe_error = RuntimeError("device offline")

    target_mirror = tmp_path / "checkpoint-fallback"
    engine.change_mirror_path("demo", target_mirror)

    assert (target_mirror / "dcim" / "Camera" / "a.jpg").read_bytes() == b"v1"
    assert transport.files["/sdcard/DCIM/Camera/a.jpg"]["content"] == b"v2"
    assert len(repository.list_checkpoints(1)) == before_checkpoints


def test_change_mirror_path_blocks_pending_clone_profile(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    repository.update_profile_state(1, PROFILE_PENDING_CLONE)

    try:
        engine.change_mirror_path("demo", tmp_path / "new-mirror")
    except ValueError as exc:
        assert "pending" in str(exc)
    else:
        raise AssertionError("expected pending-clone profile to block mirror change")


def test_change_mirror_path_rejects_non_empty_target(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")
    target_mirror = tmp_path / "non-empty-target"
    target_mirror.mkdir(parents=True)
    (target_mirror / "junk.txt").write_text("junk")

    try:
        engine.change_mirror_path("demo", target_mirror)
    except ValueError as exc:
        assert "must be empty" in str(exc)
    else:
        raise AssertionError("expected non-empty target to be rejected")
    assert repository.get_profile("demo").mirror_dir == tmp_path / "mirror"


def test_change_mirror_path_preserves_conflict_archive_for_active_conflict(tmp_path: Path) -> None:
    transport = FakeTransport({"/sdcard/DCIM/Camera/a.jpg": (b"phone-v1", 100)})
    repository, engine = setup_engine(tmp_path, transport)
    engine.sync_profile("demo")

    local_file = tmp_path / "mirror" / "dcim" / "Camera" / "a.jpg"
    local_file.write_bytes(b"local-v2")
    os.utime(local_file, (200, 200))
    transport.files["/sdcard/DCIM/Camera/a.jpg"] = {"content": b"phone-v2", "mtime": 201}
    engine.sync_profile("demo")

    issue_id = engine.list_issues("demo")[0][1].id
    old_issue = repository.get_issue(1, issue_id)[1]
    assert old_issue.status == CONFLICT
    old_archive = old_issue.conflict_copy_path

    target_mirror = tmp_path / "conflict-target"
    engine.change_mirror_path("demo", target_mirror)

    new_issue = repository.get_issue(1, issue_id)[1]
    assert new_issue.status == CONFLICT
    assert new_issue.conflict_copy_path == old_archive
    assert Path(old_archive).read_bytes() == b"local-v2"
    assert (target_mirror / "dcim" / "Camera" / "a.jpg").read_bytes() == b"phone-v2"
