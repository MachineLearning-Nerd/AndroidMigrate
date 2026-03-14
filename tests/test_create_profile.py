from __future__ import annotations

from pathlib import Path

from androidmigrate.models import DeviceInfo
from androidmigrate.storage import Repository
from androidmigrate.tui import CreateProfileScreen


class CreateProfileTransport:
    def __init__(self, devices: list[DeviceInfo] | None = None) -> None:
        self.devices = devices or []

    def list_devices(self) -> list[DeviceInfo]:
        return self.devices


ONLINE_DEVICE = DeviceInfo(serial="ABC123", state="device", model="Pixel")


def make_screen(tmp_path: Path, devices: list[DeviceInfo] | None = None):
    repository = Repository(tmp_path / "state")
    transport = CreateProfileTransport(devices)
    screen = CreateProfileScreen(stdscr=None, repository=repository, transport=transport, theme=None)
    return screen, repository


# --- Validation (_validate_all) ---


def test_validate_all_rejects_empty_name(tmp_path: Path) -> None:
    screen, _ = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.name_text = ""
    screen.state.mirror_text = str(tmp_path / "mirror")
    error = screen._validate_all()
    assert error is not None
    assert "empty" in error.lower()
    assert screen.state.active_field == screen.FIELD_NAME


def test_validate_all_rejects_duplicate_name(tmp_path: Path) -> None:
    screen, _ = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.name_text = "taken"
    screen.state.existing_names.add("taken")
    screen.state.mirror_text = str(tmp_path / "mirror")
    error = screen._validate_all()
    assert error is not None
    assert "already" in error.lower()
    assert screen.state.active_field == screen.FIELD_NAME


def test_validate_all_rejects_no_devices(tmp_path: Path) -> None:
    screen, _ = make_screen(tmp_path, [])
    screen.state.name_text = "myprofile"
    screen.state.mirror_text = str(tmp_path / "mirror")
    error = screen._validate_all()
    assert error is not None
    assert "device" in error.lower()
    assert screen.state.active_field == screen.FIELD_DEVICE


def test_validate_all_rejects_empty_mirror(tmp_path: Path) -> None:
    screen, _ = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.name_text = "myprofile"
    screen.state.mirror_text = ""
    error = screen._validate_all()
    assert error is not None
    assert "mirror" in error.lower() or "empty" in error.lower()
    assert screen.state.active_field == screen.FIELD_MIRROR


def test_validate_all_rejects_nonexistent_mirror_parent(tmp_path: Path) -> None:
    screen, _ = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.name_text = "myprofile"
    screen.state.mirror_text = "/nonexistent/deeply/nested/mirror"
    error = screen._validate_all()
    assert error is not None
    assert screen.state.active_field == screen.FIELD_MIRROR


def test_validate_all_rejects_invalid_retention(tmp_path: Path) -> None:
    screen, _ = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.name_text = "myprofile"
    screen.state.mirror_text = str(tmp_path / "mirror")

    for bad_value in ("", "0", "abc"):
        screen.state.retention_text = bad_value
        error = screen._validate_all()
        assert error is not None, f"Expected error for retention={bad_value!r}"
        assert screen.state.active_field == screen.FIELD_RETENTION


def test_validate_all_accepts_valid_input(tmp_path: Path) -> None:
    screen, _ = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.name_text = "myprofile"
    screen.state.mirror_text = str(tmp_path / "mirror")
    screen.state.retention_text = "30"
    error = screen._validate_all()
    assert error is None


# --- Mirror validation (_validate_mirror_message) ---


def test_mirror_validation_accepts_existing_directory(tmp_path: Path) -> None:
    mirror = tmp_path / "existing_mirror"
    mirror.mkdir()
    screen, _ = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.mirror_text = str(mirror)
    message = screen._validate_mirror_message()
    assert message.startswith("Ready:")


def test_mirror_validation_accepts_creatable_path(tmp_path: Path) -> None:
    screen, _ = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.mirror_text = str(tmp_path / "new_mirror")
    message = screen._validate_mirror_message()
    assert "ready to create" in message.lower()


def test_mirror_validation_rejects_file_as_mirror(tmp_path: Path) -> None:
    file_path = tmp_path / "a_file"
    file_path.write_text("not a directory")
    screen, _ = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.mirror_text = str(file_path)
    message = screen._validate_mirror_message()
    assert "not a directory" in message.lower()


# --- Submit flow ---


def test_submit_creates_profile_and_mirror_dir(tmp_path: Path) -> None:
    screen, repository = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.name_text = "newprof"
    screen.state.mirror_text = str(tmp_path / "mirror")
    screen.state.retention_text = "14"
    screen._show_confirmation = lambda *a: True

    result = screen._submit()

    assert result is not None
    assert "saved" in result.lower()
    assert "newprof" in result
    profiles = repository.list_profiles()
    assert len(profiles) == 1
    assert profiles[0].name == "newprof"
    assert profiles[0].device_serial == "ABC123"
    assert profiles[0].checkpoint_retention == 14
    assert (tmp_path / "mirror" / "newprof").is_dir()


def test_submit_returns_none_when_cancelled(tmp_path: Path) -> None:
    screen, repository = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.name_text = "newprof"
    screen.state.mirror_text = str(tmp_path / "mirror")
    screen.state.retention_text = "14"
    screen._show_confirmation = lambda *a: False

    result = screen._submit()

    assert result is None
    assert len(repository.list_profiles()) == 0


def test_submit_handles_integrity_error(tmp_path: Path) -> None:
    screen, repository = make_screen(tmp_path, [ONLINE_DEVICE])
    # Pre-create a profile with the same name to trigger IntegrityError
    repository.create_profile("clash", "OTHER", tmp_path / "other")
    screen.state.name_text = "clash"
    screen.state.mirror_text = str(tmp_path / "mirror")
    screen.state.retention_text = "30"
    # Bypass the cached existing_names check so we hit the DB constraint
    screen.state.existing_names = set()
    screen._show_confirmation = lambda *a: True

    result = screen._submit()

    assert result is None
    assert "already taken" in screen.state.status_message.lower()
    assert screen.state.active_field == screen.FIELD_NAME
    assert "clash" in screen.state.existing_names


def test_submit_rolls_back_on_mkdir_failure(tmp_path: Path, monkeypatch) -> None:
    screen, repository = make_screen(tmp_path, [ONLINE_DEVICE])
    screen.state.name_text = "failprof"
    screen.state.mirror_text = str(tmp_path / "mirror")
    screen.state.retention_text = "30"
    screen._show_confirmation = lambda *a: True

    original_mkdir = Path.mkdir

    def broken_mkdir(self, *args, **kwargs):
        if "mirror" in str(self):
            raise OSError("Permission denied")
        return original_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", broken_mkdir)

    result = screen._submit()

    assert result is None
    assert "unable to create" in screen.state.status_message.lower()
    assert len(repository.list_profiles()) == 0


# --- Device refresh ---


def test_refresh_devices_filters_non_device_state(tmp_path: Path) -> None:
    devices = [
        DeviceInfo(serial="A", state="device", model="Pixel"),
        DeviceInfo(serial="B", state="unauthorized"),
        DeviceInfo(serial="C", state="offline"),
        DeviceInfo(serial="D", state="device", model="Galaxy"),
    ]
    screen, _ = make_screen(tmp_path, devices)
    assert len(screen.state.devices) == 2
    serials = [d.serial for d in screen.state.devices]
    assert serials == ["A", "D"]


# --- Field navigation ---


def test_advance_field_clamps_at_bounds(tmp_path: Path) -> None:
    screen, _ = make_screen(tmp_path, [ONLINE_DEVICE])

    screen.state.active_field = 0
    screen._advance_field(-1)
    assert screen.state.active_field == 0

    screen.state.active_field = 3
    screen._advance_field(1)
    assert screen.state.active_field == 3
