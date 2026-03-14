from __future__ import annotations

from pathlib import Path

from androidmigrate.mirror_path import autocomplete_directory_input, validate_target_mirror_path


def test_validate_target_mirror_path_accepts_empty_or_missing_target(tmp_path: Path) -> None:
    current = tmp_path / "current"
    current.mkdir()

    missing = validate_target_mirror_path(str(tmp_path / "missing"), current)
    empty_target = tmp_path / "empty"
    empty_target.mkdir()
    empty = validate_target_mirror_path(str(empty_target), current)

    assert missing.ok is True
    assert empty.ok is True


def test_validate_target_mirror_path_rejects_non_empty_target(tmp_path: Path) -> None:
    current = tmp_path / "current"
    current.mkdir()
    target = tmp_path / "target"
    target.mkdir()
    (target / "file.txt").write_text("junk")

    validation = validate_target_mirror_path(str(target), current)

    assert validation.ok is False
    assert "must be empty" in validation.message


def test_validate_target_mirror_path_marks_same_path_as_noop(tmp_path: Path) -> None:
    current = tmp_path / "current"
    current.mkdir()

    validation = validate_target_mirror_path(str(current), current)

    assert validation.ok is True
    assert validation.is_noop is True


def test_autocomplete_directory_input_returns_matches(tmp_path: Path) -> None:
    (tmp_path / "backups").mkdir()
    (tmp_path / "backup-archive").mkdir()
    result = autocomplete_directory_input(str(tmp_path / "back"))

    assert result.updated_text == str(tmp_path / "backup")
    assert len(result.suggestions) == 2
    assert result.message == "2 matches"


def test_autocomplete_directory_input_completes_single_match(tmp_path: Path) -> None:
    target = tmp_path / "only-one"
    target.mkdir()

    result = autocomplete_directory_input(str(tmp_path / "only"))

    assert result.updated_text == str(target)
    assert result.suggestions == [str(target)]
