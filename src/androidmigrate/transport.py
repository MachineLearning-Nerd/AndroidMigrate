from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path, PurePosixPath
from typing import Protocol

from .models import DeviceInfo, FileMetadata, RemoteDirectoryEntry


class TransportError(RuntimeError):
    pass


class DeviceTransport(Protocol):
    def list_devices(self) -> list[DeviceInfo]:
        ...

    def probe_device(self, serial: str) -> None:
        ...

    def scan_root(self, serial: str, device_path: str) -> dict[str, FileMetadata]:
        ...

    def list_directories(self, serial: str, device_path: str) -> list[RemoteDirectoryEntry]:
        ...

    def path_info(self, serial: str, device_path: str) -> str:
        ...

    def hash_remote_file(self, serial: str, remote_path: str) -> str:
        ...

    def pull_file(self, serial: str, remote_path: str, local_path: Path) -> None:
        ...

    def push_file(self, serial: str, local_path: Path, remote_path: str) -> None:
        ...

    def stat_file(self, serial: str, remote_path: str) -> FileMetadata:
        ...

    def delete_file(self, serial: str, remote_path: str) -> None:
        ...


class ADBTransport:
    def __init__(self, adb_path: str = "adb") -> None:
        self.adb_path = adb_path

    def list_devices(self) -> list[DeviceInfo]:
        completed = self._run([self.adb_path, "devices", "-l"], text=True)
        devices: list[DeviceInfo] = []
        for line in completed.stdout.splitlines():
            line = line.strip()
            if not line or line.startswith("List of devices"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            model = None
            device = None
            for token in parts[2:]:
                if token.startswith("model:"):
                    model = token.split(":", 1)[1]
                elif token.startswith("device:"):
                    device = token.split(":", 1)[1]
            devices.append(DeviceInfo(serial=parts[0], state=parts[1], model=model, device=device))
        return devices

    def probe_device(self, serial: str) -> None:
        self._run(
            [
                self.adb_path,
                "-s",
                serial,
                "shell",
                "sh",
                "-c",
                "command -v find >/dev/null && command -v stat >/dev/null && command -v mkdir >/dev/null && command -v rm >/dev/null && command -v cat >/dev/null",
            ],
            text=True,
        )

    def scan_root(self, serial: str, device_path: str) -> dict[str, FileMetadata]:
        script = (
            'root="$1"; '
            'if [ ! -d "$root" ]; then echo "root-not-found:$root" >&2; exit 44; fi; '
            'start="$root"/.; '
            'find "$start" -type f -exec stat -c "%n\t%s\t%Y" {} +'
        )
        completed = self._run(
            [self.adb_path, "-s", serial, "exec-out", "sh", "-c", script, "sh", device_path],
            text=True,
        )
        files: dict[str, FileMetadata] = {}
        root = PurePosixPath(device_path)
        for line in completed.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            absolute_path, size, mtime = line.rsplit("\t", 2)
            normalized_path = PurePosixPath(absolute_path).as_posix()
            rel = PurePosixPath(normalized_path).relative_to(root).as_posix()
            files[rel] = FileMetadata(
                relative_path=rel,
                size=int(size),
                mtime=int(mtime),
                absolute_path=normalized_path,
            )
        return files

    def list_directories(self, serial: str, device_path: str) -> list[RemoteDirectoryEntry]:
        script = (
            'root="$1"; '
            'if [ ! -d "$root" ]; then echo "root-not-found:$root" >&2; exit 44; fi; '
            'for path in "$root"/* "$root"/.[!.]* "$root"/..?*; do '
            '  [ -e "$path" ] || continue; '
            '  [ -d "$path" ] || continue; '
            '  printf "%s\\n" "$path"; '
            'done'
        )
        completed = self._run(
            [self.adb_path, "-s", serial, "exec-out", "sh", "-c", script, "sh", device_path],
            text=True,
        )
        parent = PurePosixPath(device_path).as_posix()
        entries = []
        for line in completed.stdout.splitlines():
            path = line.strip()
            if not path:
                continue
            normalized_path = PurePosixPath(path).as_posix()
            entries.append(
                RemoteDirectoryEntry(
                    name=PurePosixPath(normalized_path).name,
                    absolute_path=normalized_path,
                    parent_path=parent,
                )
            )
        return sorted(entries, key=lambda entry: entry.name.lower())

    def path_info(self, serial: str, device_path: str) -> str:
        completed = self._run(
            [
                self.adb_path,
                "-s",
                serial,
                "shell",
                "sh",
                "-c",
                'if [ -d "$1" ]; then echo directory; elif [ -f "$1" ]; then echo file; else echo missing; fi',
                "sh",
                device_path,
            ],
            text=True,
        )
        return completed.stdout.strip() or "missing"

    def hash_remote_file(self, serial: str, remote_path: str) -> str:
        args = [self.adb_path, "-s", serial, "exec-out", "sh", "-c", 'cat "$1"', "sh", remote_path]
        try:
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except FileNotFoundError as exc:
            raise TransportError(f"adb not found at {self.adb_path}") from exc
        digest = hashlib.sha256()
        assert process.stdout is not None
        while True:
            chunk = process.stdout.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        stderr = b""
        if process.stderr is not None:
            stderr = process.stderr.read()
        if process.wait() != 0:
            raise TransportError(stderr.decode("utf-8", errors="replace").strip() or f"Failed to hash {remote_path}")
        return digest.hexdigest()

    def pull_file(self, serial: str, remote_path: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self._run([self.adb_path, "-s", serial, "pull", remote_path, str(local_path)], text=True)

    def push_file(self, serial: str, local_path: Path, remote_path: str) -> None:
        self._ensure_remote_dir(serial, str(PurePosixPath(remote_path).parent))
        self._run([self.adb_path, "-s", serial, "push", str(local_path), remote_path], text=True)

    def stat_file(self, serial: str, remote_path: str) -> FileMetadata:
        completed = self._run(
            [
                self.adb_path,
                "-s",
                serial,
                "exec-out",
                "sh",
                "-c",
                'stat -c "%n\t%s\t%Y" "$1"',
                "sh",
                remote_path,
            ],
            text=True,
        )
        line = completed.stdout.strip()
        if not line:
            raise TransportError(f"Unable to stat remote file {remote_path}")
        absolute_path, size, mtime = line.rsplit("\t", 2)
        rel = PurePosixPath(absolute_path).name
        return FileMetadata(relative_path=rel, size=int(size), mtime=int(mtime), absolute_path=absolute_path)

    def delete_file(self, serial: str, remote_path: str) -> None:
        self._run(
            [self.adb_path, "-s", serial, "shell", "sh", "-c", 'rm -f "$1"', "sh", remote_path],
            text=True,
        )

    def _ensure_remote_dir(self, serial: str, remote_dir: str) -> None:
        self._run(
            [self.adb_path, "-s", serial, "shell", "sh", "-c", 'mkdir -p "$1"', "sh", remote_dir],
            text=True,
        )

    @staticmethod
    def join_remote(root_path: str, relative_path: str) -> str:
        if not relative_path:
            return root_path
        parts = [part for part in relative_path.split("/") if part]
        return str(PurePosixPath(root_path).joinpath(*parts))

    @staticmethod
    def _run(args: list[str], text: bool) -> subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]:
        try:
            completed = subprocess.run(
                args,
                check=False,
                capture_output=True,
                text=text,
            )
        except FileNotFoundError as exc:
            raise TransportError(f"adb not found at {args[0]}") from exc
        if completed.returncode != 0:
            stderr = completed.stderr.decode() if isinstance(completed.stderr, bytes) else completed.stderr
            raise TransportError(stderr.strip() or "adb command failed")
        return completed
