from __future__ import annotations

from pathlib import Path

import pytest

from androidmigrate import config


@pytest.fixture(autouse=True)
def _isolate_pointer(tmp_path, monkeypatch):
    pointer_dir = tmp_path / "config" / "androidmigrate"
    monkeypatch.setattr(config, "POINTER_DIR", pointer_dir)
    monkeypatch.setattr(config, "POINTER_FILE", pointer_dir / "home")


def test_read_pointer_file_returns_none_when_missing():
    assert config.read_pointer_file() is None


def test_write_and_read_pointer_file_roundtrip(tmp_path):
    base = tmp_path / "backups"
    base.mkdir()
    config.write_pointer_file(base)
    result = config.read_pointer_file()
    assert result == base.resolve()


def test_get_state_dir_prefers_env_over_pointer(tmp_path, monkeypatch):
    env_dir = tmp_path / "env_state"
    pointer_base = tmp_path / "pointer_base"
    pointer_base.mkdir()
    config.write_pointer_file(pointer_base)
    monkeypatch.setenv("ANDROIDMIGRATE_HOME", str(env_dir))
    assert config.get_state_dir() == env_dir


def test_get_state_dir_uses_pointer_when_no_env(tmp_path, monkeypatch):
    monkeypatch.delenv("ANDROIDMIGRATE_HOME", raising=False)
    base = tmp_path / "backups"
    base.mkdir()
    config.write_pointer_file(base)
    assert config.get_state_dir() == base / ".androidmigrate"


def test_get_state_dir_falls_back_to_cwd(monkeypatch):
    monkeypatch.delenv("ANDROIDMIGRATE_HOME", raising=False)
    cwd = Path.cwd()
    assert config.get_state_dir() == cwd / ".androidmigrate"


def test_state_dir_for_base(tmp_path):
    assert config.state_dir_for_base(tmp_path / "backups") == tmp_path / "backups" / ".androidmigrate"


def test_relocate_state_moves_db_and_blobs(tmp_path):
    old = tmp_path / "old_state"
    old.mkdir()
    (old / "state.db").write_text("db content")
    (old / "state.db-wal").write_text("wal content")
    blobs = old / "blobs" / "ab"
    blobs.mkdir(parents=True)
    (blobs / "abcdef").write_text("blob data")

    new = tmp_path / "new_state"
    config.relocate_state(old, new)

    assert (new / "state.db").read_text() == "db content"
    assert (new / "state.db-wal").read_text() == "wal content"
    assert (new / "blobs" / "ab" / "abcdef").read_text() == "blob data"
    assert not (old / "state.db").exists()
    assert not (old / "blobs").exists()


def test_relocate_state_noop_same_dir(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "state.db").write_text("db content")
    config.relocate_state(state, state)
    assert (state / "state.db").read_text() == "db content"


def test_relocate_state_noop_no_db(tmp_path):
    old = tmp_path / "old"
    old.mkdir()
    new = tmp_path / "new"
    config.relocate_state(old, new)
    assert not new.exists()


def test_relocate_state_raises_when_dest_has_db(tmp_path):
    old = tmp_path / "old"
    old.mkdir()
    (old / "state.db").write_text("old db")
    new = tmp_path / "new"
    new.mkdir()
    (new / "state.db").write_text("existing db")
    with pytest.raises(FileExistsError):
        config.relocate_state(old, new)
