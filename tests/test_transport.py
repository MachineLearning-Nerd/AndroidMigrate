from __future__ import annotations

import subprocess

from androidmigrate.transport import ADBTransport


class StubADBTransport(ADBTransport):
    def __init__(self, stdout: str) -> None:
        super().__init__(adb_path="adb")
        self.stdout = stdout
        self.calls: list[list[str]] = []

    def _run(self, args: list[str], text: bool):
        self.calls.append(args)
        return subprocess.CompletedProcess(args=args, returncode=0, stdout=self.stdout, stderr="")


def test_list_directories_parses_and_sorts_entries() -> None:
    transport = StubADBTransport("/sdcard/Test\n/sdcard/DCIM\n")

    entries = transport.list_directories("SER123", "/sdcard")

    assert [entry.name for entry in entries] == ["DCIM", "Test"]
    assert [entry.absolute_path for entry in entries] == ["/sdcard/DCIM", "/sdcard/Test"]
    assert '"$root"/* "$root"/.[!.]* "$root"/..?*' in transport.calls[0][6]
