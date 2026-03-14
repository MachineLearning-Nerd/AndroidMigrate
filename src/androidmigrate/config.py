from __future__ import annotations

import os
import re
import shutil
from pathlib import Path, PurePosixPath

from .models import Profile, SyncRoot


ENV_STATE_DIR = "ANDROIDMIGRATE_HOME"
POINTER_DIR = Path.home() / ".config" / "androidmigrate"
POINTER_FILE = POINTER_DIR / "home"
STATE_SUBDIR = ".androidmigrate"


def read_pointer_file() -> Path | None:
    try:
        text = POINTER_FILE.read_text().strip()
        if text:
            return Path(text)
    except (OSError, ValueError):
        pass
    return None


def write_pointer_file(base_path: Path) -> None:
    POINTER_DIR.mkdir(parents=True, exist_ok=True)
    POINTER_FILE.write_text(str(base_path.resolve()) + "\n")


def state_dir_for_base(base_path: Path) -> Path:
    return base_path / STATE_SUBDIR


def relocate_state(old_state_dir: Path, new_state_dir: Path) -> None:
    old_state_dir = old_state_dir.resolve()
    new_state_dir = new_state_dir.resolve()
    if old_state_dir == new_state_dir:
        return
    old_db = old_state_dir / "state.db"
    if not old_db.exists():
        return
    new_db = new_state_dir / "state.db"
    if new_db.exists():
        raise FileExistsError(f"State database already exists at {new_db}")
    new_state_dir.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm", "-journal"):
        src = old_state_dir / f"state.db{suffix}"
        if src.exists():
            shutil.move(str(src), str(new_state_dir / src.name))
    old_blobs = old_state_dir / "blobs"
    if old_blobs.exists():
        shutil.move(str(old_blobs), str(new_state_dir / "blobs"))
    try:
        old_state_dir.rmdir()
    except OSError:
        pass


def get_state_dir() -> Path:
    raw = os.environ.get(ENV_STATE_DIR)
    if raw:
        return Path(raw).expanduser()
    pointer = read_pointer_file()
    if pointer:
        return state_dir_for_base(pointer)
    return Path.cwd() / ".androidmigrate"


def ensure_state_layout(state_dir: Path) -> Path:
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "blobs").mkdir(parents=True, exist_ok=True)
    return state_dir


def derive_label(device_path: str) -> str:
    candidate = PurePosixPath(device_path).name or "root"
    candidate = re.sub(r"[^A-Za-z0-9._-]+", "-", candidate).strip("-._")
    return candidate or "root"


def unique_label(existing_labels: set[str], desired: str) -> str:
    if desired not in existing_labels:
        return desired
    suffix = 2
    while f"{desired}-{suffix}" in existing_labels:
        suffix += 1
    return f"{desired}-{suffix}"


def local_root_path(profile: Profile, root: SyncRoot) -> Path:
    return profile.mirror_dir / root.label


def conflict_copy_path(profile: Profile, root: SyncRoot, relative_path: str, stamp: str) -> Path:
    rel = Path(relative_path)
    target = profile.mirror_dir / ".androidmigrate_conflicts" / root.label / rel
    suffix = "".join(rel.suffixes)
    stem = rel.name[: -len(suffix)] if suffix else rel.name
    filename = f"{stem}.local-conflict-{stamp}{suffix}"
    return target.with_name(filename)
