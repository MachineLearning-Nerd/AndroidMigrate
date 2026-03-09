from __future__ import annotations

import argparse
from pathlib import Path

from androidmigrate.config import derive_label, get_state_dir, unique_label
from androidmigrate.storage import BlobStore, Repository
from androidmigrate.sync_engine import SyncEngine, summary_to_text
from androidmigrate.transport import ADBTransport


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create or reuse an AndroidMigrate profile and sync one folder.")
    parser.add_argument("--profile", required=True, help="Profile name to create or reuse")
    parser.add_argument("--device", required=True, help="ADB device serial")
    parser.add_argument("--mirror", required=True, help="Local mirror directory")
    parser.add_argument("--root", required=True, help="Device folder path, for example /sdcard/Test")
    parser.add_argument("--label", help="Optional local label for this root")
    parser.add_argument(
        "--state-dir",
        default=str(get_state_dir()),
        help="State directory for SQLite and blob storage",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    state_dir = Path(args.state_dir).expanduser()
    mirror_dir = Path(args.mirror).expanduser().resolve()
    repository = Repository(state_dir)
    engine = SyncEngine(repository, BlobStore(state_dir), ADBTransport())

    try:
        try:
            profile = repository.get_profile(args.profile)
        except ValueError:
            profile = repository.create_profile(args.profile, args.device, mirror_dir)
            profile.mirror_dir.mkdir(parents=True, exist_ok=True)

        roots = repository.list_roots(profile.id)
        if not any(root.device_path == args.root for root in roots):
            existing = {root.label for root in roots}
            desired = args.label or derive_label(args.root)
            label = unique_label(existing, desired)
            root = repository.add_root(profile.id, args.root, label)
            (profile.mirror_dir / root.label).mkdir(parents=True, exist_ok=True)

        summary = engine.sync_profile(profile.name)
        print(summary_to_text(summary))
        return 0
    finally:
        repository.close()


if __name__ == "__main__":
    raise SystemExit(main())
