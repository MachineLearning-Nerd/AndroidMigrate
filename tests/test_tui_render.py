from __future__ import annotations

from pathlib import Path

from androidmigrate.models import DeviceInfo, PROFILE_ACTIVE, PROFILE_CLONE_FAILED, Profile
from androidmigrate.tui_render import (
    build_dashboard_banner,
    badge_tone,
    format_badge,
    format_scroll_label,
    infer_message_tone,
    truncate_left,
    truncate_right,
)


def make_profile(state: str = PROFILE_ACTIVE) -> Profile:
    return Profile(
        id=1,
        name="demo",
        device_serial="SER123",
        mirror_dir=Path("/tmp/demo"),
        checkpoint_retention=30,
        created_at="2026-03-09T00:00:00+00:00",
        profile_state=state,
    )


def test_truncate_helpers_keep_expected_side() -> None:
    assert truncate_right("abcdefgh", 5) == "ab..."
    assert truncate_left("abcdefgh", 5) == "...gh"
    assert truncate_right("abc", 5) == "abc"
    assert truncate_left("abc", 5) == "abc"


def test_scroll_label_shows_visible_range() -> None:
    assert format_scroll_label(0, 0, 0) == "0/0"
    assert format_scroll_label(0, 3, 8) == "1-3/8"
    assert format_scroll_label(4, 3, 8) == "5-7/8"


def test_badges_and_tones_are_semantic() -> None:
    assert format_badge("active") == "[ACTIVE]"
    assert badge_tone("active") == "success"
    assert badge_tone("partial") == "warning"
    assert badge_tone("failed") == "error"


def test_dashboard_banner_prefers_profile_and_device_truth() -> None:
    profile = make_profile()
    connected = DeviceInfo(serial="SER123", state="device", model="moto_g13")

    assert build_dashboard_banner(profile, connected).tone == "success"
    assert "connected and ready" in build_dashboard_banner(profile, connected).text
    assert build_dashboard_banner(profile, None).tone == "warning"
    assert build_dashboard_banner(make_profile(PROFILE_CLONE_FAILED), connected).tone == "warning"


def test_message_tone_is_inferred_from_operator_text() -> None:
    assert infer_message_tone("Sync completed") == "success"
    assert infer_message_tone("No runs yet") == "warning"
    assert infer_message_tone("Unable to browse /sdcard") == "error"
