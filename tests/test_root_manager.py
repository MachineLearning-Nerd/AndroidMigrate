from __future__ import annotations

import sqlite3
from pathlib import Path

from androidmigrate.models import ROOT_ACTIVE, ROOT_DISABLED, ROOT_REMOVED, RemoteDirectoryEntry
from androidmigrate.root_manager import ROOT_HOME, RootManagerController
from androidmigrate.storage import Repository
from androidmigrate.transport import TransportError


class BrowserTransport:
    def __init__(self, directories: dict[str, list[str]]) -> None:
        self.directories = directories

    def list_directories(self, serial: str, device_path: str) -> list[RemoteDirectoryEntry]:
        children = self.directories.get(device_path, [])
        return [
            RemoteDirectoryEntry(name=Path(child).name, absolute_path=child, parent_path=device_path)
            for child in children
        ]


class FailingBrowserTransport:
    def list_directories(self, serial: str, device_path: str) -> list[RemoteDirectoryEntry]:
        raise TransportError("device not found")


def setup_profile(tmp_path: Path):
    repository = Repository(tmp_path / "state")
    mirror_dir = tmp_path / "mirror"
    profile = repository.create_profile("demo", "SER123", mirror_dir)
    return repository, profile, mirror_dir


def test_stage_new_root_and_save_creates_root_and_mirror_dir(tmp_path: Path) -> None:
    repository, profile, mirror_dir = setup_profile(tmp_path)
    transport = BrowserTransport({ROOT_HOME: ["/sdcard/Test"]})
    controller = RootManagerController(repository, transport, profile)

    controller.toggle_browser_selection()
    assert "/sdcard/Test" in controller.state.staged_additions

    assert controller.save() is True
    new_root = repository.get_root_by_device_path(profile.id, "/sdcard/Test")
    assert new_root is not None
    assert new_root.lifecycle == ROOT_ACTIVE
    assert (mirror_dir / new_root.label).is_dir()
    assert controller.state.status_message == "Saved root changes"


def test_refresh_sets_empty_browser_message(tmp_path: Path) -> None:
    repository, profile, _ = setup_profile(tmp_path)
    transport = BrowserTransport({ROOT_HOME: []})
    controller = RootManagerController(repository, transport, profile)

    assert controller.browser_entries == []
    assert controller.state.status_message == "No subfolders under /sdcard"


def test_refresh_surfaces_transport_error(tmp_path: Path) -> None:
    repository, profile, _ = setup_profile(tmp_path)
    controller = RootManagerController(repository, FailingBrowserTransport(), profile)

    assert controller.browser_entries == []
    assert controller.state.status_message == "Unable to browse /sdcard: device not found"


def test_browser_reactivates_removed_root_without_duplicate_insert(tmp_path: Path) -> None:
    repository, profile, _ = setup_profile(tmp_path)
    removed_root = repository.add_root(profile.id, "/sdcard/Test", "test", lifecycle=ROOT_REMOVED)
    transport = BrowserTransport({ROOT_HOME: ["/sdcard/Test"]})
    controller = RootManagerController(repository, transport, profile)

    controller.toggle_browser_selection()
    assert controller.state.staged_lifecycle[removed_root.id] == ROOT_ACTIVE

    assert controller.save() is True
    roots = repository.list_roots(profile.id)
    assert len(roots) == 1
    assert roots[0].device_path == "/sdcard/Test"
    assert roots[0].lifecycle == ROOT_ACTIVE


def test_clear_staged_additions_clears_browser_reactivation(tmp_path: Path) -> None:
    repository, profile, _ = setup_profile(tmp_path)
    removed_root = repository.add_root(profile.id, "/sdcard/Test", "test", lifecycle=ROOT_REMOVED)
    transport = BrowserTransport({ROOT_HOME: ["/sdcard/Test"]})
    controller = RootManagerController(repository, transport, profile)

    controller.toggle_browser_selection()
    assert controller.state.staged_lifecycle[removed_root.id] == ROOT_ACTIVE

    controller.clear_staged_additions()

    assert removed_root.id not in controller.state.staged_lifecycle
    assert "Cleared staged additions" in controller.state.status_message


def test_overlap_against_disabled_root_is_blocked(tmp_path: Path) -> None:
    repository, profile, _ = setup_profile(tmp_path)
    repository.add_root(profile.id, "/sdcard/DCIM", "dcim", lifecycle=ROOT_DISABLED)
    transport = BrowserTransport(
        {
            ROOT_HOME: ["/sdcard/DCIM"],
            "/sdcard/DCIM": ["/sdcard/DCIM/Camera"],
        }
    )
    controller = RootManagerController(repository, transport, profile)
    controller.open_selected_directory()

    controller.toggle_browser_selection()

    assert not controller.state.staged_additions
    assert "Blocked by existing root" in controller.state.status_message


def test_overlap_against_removed_root_is_blocked(tmp_path: Path) -> None:
    repository, profile, _ = setup_profile(tmp_path)
    repository.add_root(profile.id, "/sdcard/Archive", "archive", lifecycle=ROOT_REMOVED)
    transport = BrowserTransport(
        {
            ROOT_HOME: ["/sdcard/Archive"],
            "/sdcard/Archive": ["/sdcard/Archive/Old"],
        }
    )
    controller = RootManagerController(repository, transport, profile)
    controller.open_selected_directory()

    controller.toggle_browser_selection()

    assert not controller.state.staged_additions
    assert "Blocked by existing root" in controller.state.status_message


def test_controller_save_failure_preserves_staged_changes(tmp_path: Path) -> None:
    repository, profile, _ = setup_profile(tmp_path)
    transport = BrowserTransport({ROOT_HOME: ["/sdcard/Test"]})
    controller = RootManagerController(repository, transport, profile)
    controller.toggle_browser_selection()

    def broken_apply_root_changes(*args, **kwargs):
        raise sqlite3.IntegrityError("boom")

    repository.apply_root_changes = broken_apply_root_changes  # type: ignore[method-assign]

    assert controller.save() is False
    assert "/sdcard/Test" in controller.state.staged_additions
    assert "Save failed" in controller.state.status_message


def test_apply_root_changes_rolls_back_on_error(tmp_path: Path) -> None:
    repository, profile, _ = setup_profile(tmp_path)
    root = repository.add_root(profile.id, "/sdcard/DCIM", "dcim", lifecycle=ROOT_ACTIVE)

    try:
        repository.apply_root_changes(
            profile.id,
            additions=[("/sdcard/Documents", "dcim", ROOT_ACTIVE)],
            lifecycle_updates={root.id: ROOT_DISABLED},
        )
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError("Expected IntegrityError")

    current = repository.get_root_by_id(root.id)
    assert current.lifecycle == ROOT_ACTIVE
    assert repository.get_root_by_device_path(profile.id, "/sdcard/Documents") is None
