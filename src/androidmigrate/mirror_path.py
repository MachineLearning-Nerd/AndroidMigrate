from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class MirrorPathValidation:
    target_path: Path | None
    message: str
    ok: bool
    is_noop: bool = False


@dataclass(slots=True)
class MirrorPathAutocomplete:
    updated_text: str
    suggestions: list[str]
    message: str


def validate_target_mirror_path(raw_text: str, current_path: Path) -> MirrorPathValidation:
    candidate = raw_text.strip()
    if not candidate:
        return MirrorPathValidation(None, "Enter a backup folder path", False)

    target_path = Path(candidate).expanduser().resolve()
    if target_path == current_path:
        return MirrorPathValidation(target_path, "Backup folder is already set to that path", True, is_noop=True)

    if target_path.exists():
        if not target_path.is_dir():
            return MirrorPathValidation(None, f"Target is not a directory: {target_path}", False)
        if any(target_path.iterdir()):
            return MirrorPathValidation(None, f"Target folder must be empty: {target_path}", False)
        return MirrorPathValidation(target_path, f"Ready to switch to {target_path}", True)

    parent = _nearest_existing_parent(target_path)
    if parent is None or not parent.is_dir():
        return MirrorPathValidation(None, f"Target path has no valid parent directory: {target_path}", False)
    if not os.access(parent, os.W_OK):
        return MirrorPathValidation(None, f"Parent directory is not writable: {parent}", False)
    return MirrorPathValidation(target_path, f"Ready to create {target_path}", True)


def autocomplete_directory_input(raw_text: str) -> MirrorPathAutocomplete:
    candidate = raw_text or ""
    expanded = Path(candidate).expanduser()

    if candidate.endswith(os.sep) or candidate.endswith("/"):
        search_root = expanded
        prefix = ""
    else:
        search_root = expanded.parent if str(expanded.parent) else Path(".")
        prefix = expanded.name

    try:
        search_root = search_root.resolve()
    except OSError:
        return MirrorPathAutocomplete(candidate, [], "No matches")

    if not search_root.exists() or not search_root.is_dir():
        return MirrorPathAutocomplete(candidate, [], "No matches")

    suggestions = sorted(
        [str(child) for child in search_root.iterdir() if child.is_dir() and child.name.startswith(prefix)],
        key=str.casefold,
    )
    if not suggestions:
        return MirrorPathAutocomplete(candidate, [], "No matches")
    if len(suggestions) == 1:
        return MirrorPathAutocomplete(suggestions[0], suggestions, f"Completed to {suggestions[0]}")

    common_prefix = os.path.commonprefix(suggestions)
    updated_text = common_prefix if len(common_prefix) > len(candidate) else candidate
    return MirrorPathAutocomplete(updated_text, suggestions, f"{len(suggestions)} matches")


def _nearest_existing_parent(path: Path) -> Path | None:
    current = path.parent
    while True:
        if current.exists():
            return current
        if current == current.parent:
            return None
        current = current.parent
