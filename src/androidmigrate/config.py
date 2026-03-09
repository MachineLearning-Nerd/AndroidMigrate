from __future__ import annotations

import os
import re
from pathlib import Path, PurePosixPath

from .models import Profile, SyncRoot


ENV_STATE_DIR = "ANDROIDMIGRATE_HOME"


def get_state_dir() -> Path:
    raw = os.environ.get(ENV_STATE_DIR)
    if raw:
        return Path(raw).expanduser()
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
