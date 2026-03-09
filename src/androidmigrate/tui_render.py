from __future__ import annotations

import curses
from dataclasses import dataclass
from datetime import datetime

from .models import (
    PROFILE_ACTIVE,
    PROFILE_CLONE_FAILED,
    PROFILE_PENDING_CLONE,
    PROFILE_RESTORE_INCOMPLETE,
    ROOT_ACTIVE,
    ROOT_DISABLED,
    ROOT_REMOVED,
    RUN_CANCELLED,
    RUN_COMPLETED,
    RUN_FAILED,
    RUN_PARTIAL,
)


@dataclass(slots=True)
class Banner:
    text: str
    tone: str


@dataclass(slots=True)
class Theme:
    title: int
    header: int
    panel_border: int
    panel_focus: int
    text: int
    muted: int
    selected: int
    success: int
    warning: int
    error: int
    footer: int


def init_theme() -> Theme:
    title = curses.A_BOLD
    header = curses.A_BOLD
    panel_border = curses.A_DIM
    panel_focus = curses.A_BOLD
    text = curses.A_NORMAL
    muted = curses.A_DIM
    selected = curses.A_REVERSE | curses.A_BOLD
    success = curses.A_BOLD
    warning = curses.A_BOLD
    error = curses.A_BOLD
    footer = curses.A_DIM

    if curses.has_colors():
        curses.start_color()
        try:
            curses.use_default_colors()
        except curses.error:
            pass
        colors = [
            (1, curses.COLOR_CYAN),
            (2, curses.COLOR_GREEN),
            (3, curses.COLOR_YELLOW),
            (4, curses.COLOR_RED),
            (5, curses.COLOR_WHITE),
        ]
        for pair_id, fg in colors:
            curses.init_pair(pair_id, fg, -1)
        title = curses.color_pair(1) | curses.A_BOLD
        header = curses.color_pair(1) | curses.A_BOLD
        panel_border = curses.color_pair(5)
        panel_focus = curses.color_pair(1) | curses.A_BOLD
        success = curses.color_pair(2) | curses.A_BOLD
        warning = curses.color_pair(3) | curses.A_BOLD
        error = curses.color_pair(4) | curses.A_BOLD
        footer = curses.color_pair(5) | curses.A_DIM

    return Theme(
        title=title,
        header=header,
        panel_border=panel_border,
        panel_focus=panel_focus,
        text=text,
        muted=muted,
        selected=selected,
        success=success,
        warning=warning,
        error=error,
        footer=footer,
    )


def tone_attr(theme: Theme, tone: str) -> int:
    if tone == "success":
        return theme.success
    if tone == "warning":
        return theme.warning
    if tone == "error":
        return theme.error
    if tone == "selected":
        return theme.selected
    return theme.text


def truncate_right(text: str, width: int) -> str:
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    return f"{text[: width - 3]}..."


def truncate_left(text: str, width: int) -> str:
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width <= 3:
        return text[-width:]
    return f"...{text[-(width - 3):]}"


def format_scroll_label(start: int, visible_count: int, total_count: int) -> str:
    if total_count <= 0:
        return "0/0"
    end = min(total_count, start + visible_count)
    return f"{start + 1}-{end}/{total_count}"


def format_badge(value: str) -> str:
    return f"[{value.replace('_', ' ').upper()}]"


def badge_tone(value: str) -> str:
    if value in {ROOT_ACTIVE, PROFILE_ACTIVE, RUN_COMPLETED}:
        return "success"
    if value in {ROOT_DISABLED, PROFILE_PENDING_CLONE, RUN_PARTIAL, RUN_CANCELLED}:
        return "warning"
    if value in {ROOT_REMOVED, PROFILE_CLONE_FAILED, PROFILE_RESTORE_INCOMPLETE, RUN_FAILED}:
        return "error"
    return "muted"


def format_clock(value: datetime | None) -> str:
    if value is None:
        return "--:--:--"
    return value.strftime("%H:%M:%S")


def infer_message_tone(message: str) -> str:
    lowered = message.lower()
    if any(token in lowered for token in ("failed", "unable", "error", "not connected", "blocked")):
        return "error"
    if any(token in lowered for token in ("no ", "already", "cancelled", "disabled", "removed")):
        return "warning"
    if any(token in lowered for token in ("saved", "loaded", "completed", "ready", "connected", "refreshed")):
        return "success"
    return "muted"


def build_dashboard_banner(profile, matching_device) -> Banner:
    if profile is None:
        return Banner("No profile selected", "warning")
    if profile.profile_state != PROFILE_ACTIVE:
        return Banner(f"Profile {profile.name} is blocked: {profile.profile_state}", "warning")
    if matching_device is None:
        return Banner(f"Bound device {profile.device_serial} not connected", "warning")
    if matching_device.state != "device":
        return Banner(f"Bound device {profile.device_serial} is {matching_device.state}", "error")
    model = f" ({matching_device.model})" if matching_device.model else ""
    return Banner(f"Bound device {matching_device.serial}{model} connected and ready", "success")


def safe_addstr(window, y: int, x: int, text: str, attr: int = 0) -> None:
    if not text:
        return
    height, width = window.getmaxyx()
    if y < 0 or y >= height or x >= width:
        return
    clipped_x = max(0, x)
    max_width = width - clipped_x
    if max_width <= 0:
        return
    try:
        window.addnstr(y, clipped_x, text, max_width, attr)
    except curses.error:
        pass


def fill_line(window, y: int, x: int, width: int, attr: int = 0) -> None:
    if width <= 0:
        return
    safe_addstr(window, y, x, " " * width, attr)


def draw_box(window, y: int, x: int, height: int, width: int, attr: int) -> None:
    if height < 2 or width < 2:
        return
    for col in range(x + 1, x + width - 1):
        try:
            window.addch(y, col, curses.ACS_HLINE, attr)
            window.addch(y + height - 1, col, curses.ACS_HLINE, attr)
        except curses.error:
            pass
    for row in range(y + 1, y + height - 1):
        try:
            window.addch(row, x, curses.ACS_VLINE, attr)
            window.addch(row, x + width - 1, curses.ACS_VLINE, attr)
        except curses.error:
            pass
    for row, col, corner in (
        (y, x, curses.ACS_ULCORNER),
        (y, x + width - 1, curses.ACS_URCORNER),
        (y + height - 1, x, curses.ACS_LLCORNER),
        (y + height - 1, x + width - 1, curses.ACS_LRCORNER),
    ):
        try:
            window.addch(row, col, corner, attr)
        except curses.error:
            pass


def draw_panel(window, theme: Theme, y: int, x: int, height: int, width: int, title: str, *, focused: bool = False, note: str | None = None) -> tuple[int, int, int, int]:
    border_attr = theme.panel_focus if focused else theme.panel_border
    draw_box(window, y, x, height, width, border_attr)
    safe_addstr(window, y, x + 2, f" {truncate_right(title, max(1, width - 8))} ", theme.title if focused else theme.header)
    if note:
        safe_addstr(window, y, x + width - len(note) - 2, note, theme.muted)
    return y + 1, x + 1, max(0, height - 2), max(0, width - 2)


def draw_centered_placeholder(window, y: int, x: int, height: int, width: int, text: str, attr: int) -> None:
    line = truncate_right(text, max(0, width - 2))
    target_y = y + max(0, height // 2)
    target_x = x + max(0, (width - len(line)) // 2)
    safe_addstr(window, target_y, target_x, line, attr)


def draw_key_value(window, theme: Theme, y: int, x: int, width: int, label: str, value: str, *, value_mode: str = "right") -> None:
    label_text = f"{label}:"
    safe_addstr(window, y, x, label_text, theme.muted)
    value_width = max(0, width - len(label_text) - 1)
    if value_mode == "left":
        rendered = truncate_left(value, value_width)
    else:
        rendered = truncate_right(value, value_width)
    safe_addstr(window, y, x + len(label_text) + 1, rendered, theme.text)
