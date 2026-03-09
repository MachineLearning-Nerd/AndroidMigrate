from __future__ import annotations

import argparse
from pathlib import Path

from .config import derive_label, get_state_dir, unique_label
from .storage import BlobStore, Repository
from .sync_engine import SyncEngine, summary_to_text
from .transport import ADBTransport, TransportError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="androidmigrate")
    parser.add_argument("--state-dir", default=str(get_state_dir()), help="State directory for SQLite and blobs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("devices", help="List connected devices")

    profile = subparsers.add_parser("profile", help="Manage profiles")
    profile_sub = profile.add_subparsers(dest="profile_command", required=True)

    create = profile_sub.add_parser("create", help="Create a profile")
    create.add_argument("name")
    create.add_argument("--device", required=True, dest="device_serial")
    create.add_argument("--mirror", required=True, dest="mirror_dir")
    create.add_argument("--retention", type=int, default=30, dest="checkpoint_retention")

    add_root = profile_sub.add_parser("add-root", help="Add a sync root")
    add_root.add_argument("name")
    add_root.add_argument("device_path")
    add_root.add_argument("--label")

    list_profiles = profile_sub.add_parser("list", help="List profiles")
    list_profiles.add_argument("name", nargs="?")

    sync = subparsers.add_parser("sync", help="Run sync")
    sync.add_argument("name")
    sync.add_argument("--dry-run", action="store_true")

    checkpoints = subparsers.add_parser("checkpoints", help="List checkpoints")
    checkpoints.add_argument("name")

    runs = subparsers.add_parser("runs", help="List recent runs")
    runs.add_argument("name")

    restore = subparsers.add_parser("restore", help="Restore a checkpoint to the phone")
    restore.add_argument("name")
    restore.add_argument("checkpoint_id", type=int)
    restore.add_argument("--root")
    restore.add_argument("--path")
    restore.add_argument("--to-device", action="store_true")

    clone_restore = subparsers.add_parser("clone-restore", help="Clone a checkpoint to a different device")
    clone_restore.add_argument("name")
    clone_restore.add_argument("checkpoint_id", type=int)
    clone_restore.add_argument("--target-device", required=True, dest="target_device")
    clone_restore.add_argument("--new-profile", required=True, dest="new_profile")
    clone_restore.add_argument("--mirror", required=True, dest="mirror_dir")

    conflicts = subparsers.add_parser("conflicts", help="List unresolved issues")
    conflicts.add_argument("name")

    resolve = subparsers.add_parser("resolve", help="Resolve an unresolved path")
    resolve.add_argument("name")
    resolve.add_argument("issue_id", type=int)
    resolve.add_argument("--keep", choices=["phone", "local", "both"], required=True)

    subparsers.add_parser("tui", help="Launch the terminal UI")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    state_dir = Path(args.state_dir).expanduser()
    repository = Repository(state_dir)
    blob_store = BlobStore(state_dir)
    transport = ADBTransport()
    engine = SyncEngine(repository, blob_store, transport)

    try:
        if args.command == "devices":
            return _cmd_devices(transport)
        if args.command == "profile":
            return _cmd_profile(repository, args)
        if args.command == "sync":
            summary = engine.sync_profile(args.name, dry_run=args.dry_run)
            print(summary_to_text(summary))
            return 0
        if args.command == "checkpoints":
            return _cmd_checkpoints(repository, args.name)
        if args.command == "runs":
            return _cmd_runs(repository, args.name)
        if args.command == "restore":
            if not args.to_device:
                raise ValueError("restore currently requires --to-device")
            summary = engine.restore_checkpoint(args.name, args.checkpoint_id, root_label=args.root, relative_path=args.path)
            print(summary_to_text(summary))
            return 0
        if args.command == "clone-restore":
            summary = engine.clone_restore(
                args.name,
                args.checkpoint_id,
                target_device_serial=args.target_device,
                target_profile_name=args.new_profile,
                target_mirror_dir=Path(args.mirror_dir),
            )
            print(summary_to_text(summary))
            return 0
        if args.command == "conflicts":
            return _cmd_conflicts(engine, args.name)
        if args.command == "resolve":
            summary = engine.resolve_issue(args.name, args.issue_id, keep=args.keep)
            print(summary_to_text(summary))
            return 0
        if args.command == "tui":
            from .tui import run_tui

            return run_tui(repository, blob_store, transport)
    except (ValueError, TransportError) as exc:
        parser.exit(status=2, message=f"error: {exc}\n")
    finally:
        repository.close()
    return 0


def _cmd_devices(transport: ADBTransport) -> int:
    devices = transport.list_devices()
    if not devices:
        print("No devices detected")
        return 0
    for device in devices:
        extras = []
        if device.model:
            extras.append(f"model={device.model}")
        if device.device:
            extras.append(f"device={device.device}")
        suffix = f" ({', '.join(extras)})" if extras else ""
        print(f"{device.serial}\t{device.state}{suffix}")
    return 0


def _cmd_profile(repository: Repository, args: argparse.Namespace) -> int:
    if args.profile_command == "create":
        mirror_dir = Path(args.mirror_dir).expanduser().resolve()
        profile = repository.create_profile(
            name=args.name,
            device_serial=args.device_serial,
            mirror_dir=mirror_dir,
            checkpoint_retention=args.checkpoint_retention,
        )
        profile.mirror_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created profile {profile.name} -> {profile.mirror_dir}")
        return 0

    if args.profile_command == "add-root":
        profile = repository.get_profile(args.name)
        existing = {root.label for root in repository.list_roots(profile.id)}
        desired = args.label or derive_label(args.device_path)
        label = unique_label(existing, desired)
        root = repository.add_root(profile.id, args.device_path, label)
        (profile.mirror_dir / root.label).mkdir(parents=True, exist_ok=True)
        print(f"Added root {root.label}: {root.device_path}")
        return 0

    if args.profile_command == "list":
        profiles = [repository.get_profile(args.name)] if args.name else repository.list_profiles()
        for profile in profiles:
            print(
                f"{profile.name}\tdevice={profile.device_serial}\tmirror={profile.mirror_dir}"
                f"\tstate={profile.profile_state}"
            )
            for root in repository.list_roots(profile.id):
                print(f"  - {root.label}: {root.device_path}\tstate={root.lifecycle}")
        return 0

    raise ValueError(f"Unsupported profile command {args.profile_command}")


def _cmd_checkpoints(repository: Repository, profile_name: str) -> int:
    profile = repository.get_profile(profile_name)
    checkpoints = repository.list_checkpoints(profile.id)
    if not checkpoints:
        print("No checkpoints yet")
        return 0
    for checkpoint in checkpoints:
        print(f"{checkpoint.id}\t{checkpoint.created_at}\t{checkpoint.status}\t{checkpoint.summary_json}")
    return 0


def _cmd_conflicts(engine: SyncEngine, profile_name: str) -> int:
    issues = engine.list_issues(profile_name)
    if not issues:
        print("No unresolved issues")
        return 0
    for root, state in issues:
        extra = f"\tarchive={state.conflict_copy_path}" if state.conflict_copy_path else ""
        print(f"{state.id}\t{root.label}/{state.relative_path}\t{state.status}{extra}")
    return 0


def _cmd_runs(repository: Repository, profile_name: str) -> int:
    profile = repository.get_profile(profile_name)
    runs = repository.list_recent_runs(profile.id)
    if not runs:
        print("No runs yet")
        return 0
    for run in runs:
        print(
            f"{run.id}\t{run.operation_type}\t{run.status}"
            f"\tstarted={run.started_at}\tfinished={run.finished_at}"
            f"\tsource_checkpoint={run.source_checkpoint_id}\tresult_checkpoint={run.result_checkpoint_id}"
        )
    return 0
