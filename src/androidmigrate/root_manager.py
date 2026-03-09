from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from .config import derive_label, local_root_path, unique_label
from .models import ROOT_ACTIVE, ROOT_DISABLED, ROOT_REMOVED, Profile, RemoteDirectoryEntry, SyncRoot
from .storage import Repository
from .transport import DeviceTransport, TransportError


ROOT_BROWSER = "browser"
ROOT_LIST = "roots"
ROOT_HOME = "/sdcard"


def normalize_device_path(device_path: str) -> str:
    normalized = PurePosixPath(device_path).as_posix()
    if not normalized:
        return "/"
    if normalized != "/" and normalized.endswith("/"):
        normalized = normalized.rstrip("/")
    return normalized or "/"


def paths_overlap(left: str, right: str) -> bool:
    left_path = PurePosixPath(normalize_device_path(left))
    right_path = PurePosixPath(normalize_device_path(right))
    return left_path == right_path or left_path in right_path.parents or right_path in left_path.parents


@dataclass(slots=True)
class StagedRootAdd:
    device_path: str
    label: str


@dataclass(slots=True)
class RootManagerState:
    current_path: str = ROOT_HOME
    browser_index: int = 0
    roots_index: int = 0
    active_pane: str = ROOT_BROWSER
    status_message: str = "Browse /sdcard and press Space to stage folders"
    staged_additions: dict[str, StagedRootAdd] = field(default_factory=dict)
    staged_lifecycle: dict[int, str] = field(default_factory=dict)
    staged_browser_reactivations: set[int] = field(default_factory=set)


class RootManagerController:
    def __init__(self, repository: Repository, transport: DeviceTransport, profile: Profile) -> None:
        self.repository = repository
        self.transport = transport
        self.profile = profile
        self.state = RootManagerState()
        self._browser_entries: list[RemoteDirectoryEntry] = []
        self._roots: list[SyncRoot] = []
        self.refresh()

    @property
    def browser_entries(self) -> list[RemoteDirectoryEntry]:
        return self._browser_entries

    @property
    def roots(self) -> list[SyncRoot]:
        return self._roots

    @property
    def has_pending_changes(self) -> bool:
        return bool(self.state.staged_additions or self.state.staged_lifecycle)

    def refresh(self) -> None:
        self._roots = self.repository.list_roots(self.profile.id)
        try:
            self._browser_entries = self.transport.list_directories(self.profile.device_serial, self.state.current_path)
            if self._browser_entries:
                self.state.status_message = f"Loaded {len(self._browser_entries)} folder(s) from {self.state.current_path}"
            else:
                self.state.status_message = f"No subfolders under {self.state.current_path}"
        except TransportError as exc:
            self._browser_entries = []
            self.state.status_message = f"Unable to browse {self.state.current_path}: {exc}"
        self.state.browser_index = self._clamp_index(self.state.browser_index, len(self._browser_entries))
        self.state.roots_index = self._clamp_index(self.state.roots_index, len(self._roots))

    def switch_pane(self, direction: int = 1) -> None:
        panes = [ROOT_BROWSER, ROOT_LIST]
        current = panes.index(self.state.active_pane)
        self.state.active_pane = panes[(current + direction) % len(panes)]

    def move_selection(self, delta: int) -> None:
        if self.state.active_pane == ROOT_BROWSER:
            self.state.browser_index = self._clamp_index(self.state.browser_index + delta, len(self._browser_entries))
        else:
            self.state.roots_index = self._clamp_index(self.state.roots_index + delta, len(self._roots))

    def selected_entry(self) -> RemoteDirectoryEntry | None:
        if not self._browser_entries:
            return None
        return self._browser_entries[self.state.browser_index]

    def selected_root(self) -> SyncRoot | None:
        if not self._roots:
            return None
        return self._roots[self.state.roots_index]

    def current_root_state(self, root: SyncRoot) -> str:
        return self.state.staged_lifecycle.get(root.id, root.lifecycle)

    def stage_all_visible(self) -> None:
        changed = 0
        blocked = 0
        for entry in self._browser_entries:
            result = self._stage_path(entry.absolute_path, allow_unstage=False)
            if result:
                changed += 1
            else:
                blocked += 1
        if changed:
            self.state.status_message = f"Staged {changed} folder(s)"
        elif blocked:
            self.state.status_message = "No visible folders could be staged"

    def clear_staged_additions(self) -> None:
        if not self.state.staged_additions and not self.state.staged_browser_reactivations:
            self.state.status_message = "No staged additions to clear"
            return
        for root_id in list(self.state.staged_browser_reactivations):
            root = self._root_by_id(root_id)
            if root is not None:
                self._set_root_stage(root, root.lifecycle)
        self.state.staged_browser_reactivations.clear()
        self.state.staged_additions.clear()
        self.state.status_message = "Cleared staged additions"

    def open_selected_directory(self) -> None:
        entry = self.selected_entry()
        if entry is None:
            self.state.status_message = "No folder selected"
            return
        self.state.current_path = entry.absolute_path
        self.state.browser_index = 0
        self.refresh()

    def go_to_parent(self) -> None:
        current = PurePosixPath(self.state.current_path)
        home = PurePosixPath(ROOT_HOME)
        if current == home:
            self.state.status_message = "Already at /sdcard"
            return
        parent = current.parent
        if home not in {parent, *parent.parents} and parent != home:
            parent = home
        self.state.current_path = str(parent)
        self.state.browser_index = 0
        self.refresh()

    def toggle_browser_selection(self) -> None:
        entry = self.selected_entry()
        if entry is None:
            self.state.status_message = "No folder selected"
            return
        if self._stage_path(entry.absolute_path, allow_unstage=True):
            return
        if entry.absolute_path in self.state.staged_additions:
            self.state.staged_additions.pop(entry.absolute_path, None)
            self.state.status_message = f"Unstaged {entry.absolute_path}"
            return
        root = self._root_by_device_path(entry.absolute_path)
        if root and root.lifecycle in {ROOT_DISABLED, ROOT_REMOVED} and self.state.staged_lifecycle.get(root.id) == ROOT_ACTIVE:
            self.state.staged_lifecycle.pop(root.id, None)
            self.state.status_message = f"Unstaged reactivation for {root.label}"

    def toggle_selected_root_disabled(self) -> None:
        root = self.selected_root()
        if root is None:
            self.state.status_message = "No root selected"
            return
        self.state.staged_browser_reactivations.discard(root.id)
        current = self.current_root_state(root)
        if current == ROOT_REMOVED:
            self.state.status_message = "Use a to reactivate removed roots"
            return
        target = ROOT_DISABLED if current == ROOT_ACTIVE else ROOT_ACTIVE
        self._set_root_stage(root, target)
        self.state.status_message = f"Staged {root.label} -> {target}"

    def reactivate_selected_root(self) -> None:
        root = self.selected_root()
        if root is None:
            self.state.status_message = "No root selected"
            return
        self.state.staged_browser_reactivations.discard(root.id)
        if self.current_root_state(root) == ROOT_ACTIVE:
            self.state.status_message = f"{root.label} is already active"
            return
        self._set_root_stage(root, ROOT_ACTIVE)
        self.state.status_message = f"Staged {root.label} -> active"

    def remove_selected_root(self) -> None:
        root = self.selected_root()
        if root is None:
            self.state.status_message = "No root selected"
            return
        self.state.staged_browser_reactivations.discard(root.id)
        if self.current_root_state(root) == ROOT_REMOVED:
            self.state.status_message = f"{root.label} is already removed"
            return
        self._set_root_stage(root, ROOT_REMOVED)
        self.state.status_message = f"Staged {root.label} -> removed"

    def save(self) -> bool:
        additions = list(self.state.staged_additions.values())
        lifecycle_updates = {
            root.id: target
            for root in self._roots
            if (target := self.state.staged_lifecycle.get(root.id)) is not None and target != root.lifecycle
        }
        if not additions and not lifecycle_updates:
            self.state.status_message = "No changes to save"
            return True
        error = self.validate_plan()
        if error:
            self.state.status_message = error
            return False

        created_dirs: list[Path] = []
        try:
            for addition in additions:
                mirror_path = local_root_path(
                    self.profile,
                    SyncRoot(id=0, profile_id=self.profile.id, device_path=addition.device_path, label=addition.label),
                )
                if not mirror_path.exists():
                    mirror_path.mkdir(parents=True, exist_ok=False)
                    created_dirs.append(mirror_path)
            for root in self._roots:
                if lifecycle_updates.get(root.id) == ROOT_ACTIVE and root.lifecycle != ROOT_ACTIVE:
                    mirror_path = local_root_path(self.profile, root)
                    if not mirror_path.exists():
                        mirror_path.mkdir(parents=True, exist_ok=False)
                        created_dirs.append(mirror_path)
            self.repository.apply_root_changes(
                self.profile.id,
                additions=[(item.device_path, item.label, ROOT_ACTIVE) for item in additions],
                lifecycle_updates=lifecycle_updates,
            )
        except (OSError, sqlite3.DatabaseError, ValueError) as exc:
            for path in reversed(created_dirs):
                try:
                    path.rmdir()
                except OSError:
                    pass
            self.state.status_message = f"Save failed: {exc}"
            return False

        self.state.staged_additions.clear()
        self.state.staged_lifecycle.clear()
        self.state.staged_browser_reactivations.clear()
        self.refresh()
        self.state.status_message = "Saved root changes"
        return True

    def validate_plan(self) -> str | None:
        planned = [(root.id, root.label, normalize_device_path(root.device_path)) for root in self._roots]
        planned.extend((None, addition.label, normalize_device_path(addition.device_path)) for addition in self.state.staged_additions.values())
        for index, (_, left_label, left_path) in enumerate(planned):
            for _, right_label, right_path in planned[index + 1 :]:
                if paths_overlap(left_path, right_path):
                    return (
                        "Overlapping roots are not allowed: "
                        f"{left_label} ({left_path}) and {right_label} ({right_path})"
                    )
        return None

    def staged_summary(self) -> str:
        additions = ", ".join(sorted(item.label for item in self.state.staged_additions.values())) or "none"
        lifecycle_changes = []
        for root in self._roots:
            target = self.state.staged_lifecycle.get(root.id)
            if target is not None and target != root.lifecycle:
                lifecycle_changes.append(f"{root.label}={target}")
        lifecycle = ", ".join(lifecycle_changes) or "none"
        return f"adds: {additions} | lifecycle: {lifecycle}"

    def browser_marker(self, entry: RemoteDirectoryEntry) -> str:
        path = entry.absolute_path
        if path in self.state.staged_additions:
            return "+"
        root = self._root_by_device_path(path)
        if root is None:
            return " "
        lifecycle = self.current_root_state(root)
        return {
            ROOT_ACTIVE: "A",
            ROOT_DISABLED: "D",
            ROOT_REMOVED: "R",
        }[lifecycle]

    def root_marker(self, root: SyncRoot) -> str:
        lifecycle = self.current_root_state(root)
        staged = self.state.staged_lifecycle.get(root.id)
        marker = {
            ROOT_ACTIVE: "A",
            ROOT_DISABLED: "D",
            ROOT_REMOVED: "R",
        }[lifecycle]
        if staged is not None and staged != root.lifecycle:
            marker = f"{marker}+"
        return marker

    def _stage_path(self, device_path: str, *, allow_unstage: bool) -> bool:
        if allow_unstage and device_path in self.state.staged_additions:
            self.state.staged_additions.pop(device_path, None)
            self.state.status_message = f"Unstaged {device_path}"
            return True
        root = self._root_by_device_path(device_path)
        if root is not None:
            current = self.current_root_state(root)
            if root.lifecycle == ROOT_ACTIVE and current == ROOT_ACTIVE:
                self.state.status_message = f"{root.label} is already configured"
                return False
            if allow_unstage and current == ROOT_ACTIVE:
                self.state.staged_lifecycle.pop(root.id, None)
                self.state.staged_browser_reactivations.discard(root.id)
                self.state.status_message = f"Unstaged reactivation for {root.label}"
                return True
            if current == ROOT_ACTIVE:
                self.state.status_message = f"{root.label} is already staged active"
                return False
            self._set_root_stage(root, ROOT_ACTIVE)
            if allow_unstage:
                self.state.staged_browser_reactivations.add(root.id)
            self.state.status_message = f"Staged reactivation for {root.label}"
            return True

        conflict = self._find_path_conflict(device_path)
        if conflict:
            self.state.status_message = conflict
            return False

        label = self._derive_staged_label(device_path)
        self.state.staged_additions[device_path] = StagedRootAdd(device_path=device_path, label=label)
        self.state.status_message = f"Staged add: {label} -> {device_path}"
        return True

    def _root_by_device_path(self, device_path: str) -> SyncRoot | None:
        for root in self._roots:
            if root.device_path == device_path:
                return root
        return None

    def _root_by_id(self, root_id: int) -> SyncRoot | None:
        for root in self._roots:
            if root.id == root_id:
                return root
        return None

    def _find_path_conflict(self, device_path: str) -> str | None:
        for root in self._roots:
            if paths_overlap(device_path, root.device_path):
                return f"Blocked by existing root {root.label}: {root.device_path}"
        for staged in self.state.staged_additions.values():
            if paths_overlap(device_path, staged.device_path):
                return f"Blocked by staged root: {staged.device_path}"
        return None

    def _derive_staged_label(self, device_path: str) -> str:
        existing_labels = {root.label for root in self._roots}
        existing_labels.update(item.label for item in self.state.staged_additions.values())
        return unique_label(existing_labels, derive_label(device_path))

    def _set_root_stage(self, root: SyncRoot, lifecycle: str) -> None:
        if lifecycle == root.lifecycle:
            self.state.staged_lifecycle.pop(root.id, None)
        else:
            self.state.staged_lifecycle[root.id] = lifecycle

    @staticmethod
    def _clamp_index(index: int, length: int) -> int:
        if length <= 0:
            return 0
        return max(0, min(index, length - 1))
