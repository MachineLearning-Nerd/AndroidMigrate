from __future__ import annotations

import curses
from dataclasses import dataclass
from datetime import datetime

from .models import PROFILE_ACTIVE
from .root_manager import ROOT_BROWSER, RootManagerController
from .storage import BlobStore, Repository
from .sync_engine import SyncEngine
from .transport import ADBTransport, TransportError
from .tui_render import (
    Banner,
    badge_tone,
    build_dashboard_banner,
    draw_centered_placeholder,
    draw_key_value,
    draw_panel,
    fill_line,
    format_badge,
    format_clock,
    format_scroll_label,
    infer_message_tone,
    init_theme,
    safe_addstr,
    tone_attr,
    truncate_left,
    truncate_right,
)


MIN_HEIGHT = 24
MIN_WIDTH = 90


@dataclass(slots=True)
class DashboardState:
    selected_profile: int = 0
    status_message: str = "Press ? for help"
    last_refresh_at: datetime | None = None


def _visible_bounds(selected: int, total: int, max_rows: int) -> tuple[int, int]:
    if max_rows <= 0 or total <= max_rows:
        return 0, total
    start = max(0, min(selected - (max_rows // 2), total - max_rows))
    return start, start + max_rows


def _draw_resize_overlay(stdscr, height: int, width: int, message: str) -> None:
    stdscr.erase()
    box_height = 5
    box_width = min(max(len(message) + 8, 36), max(36, width - 4))
    top = max(0, (height - box_height) // 2)
    left = max(0, (width - box_width) // 2)
    theme = init_theme()
    draw_panel(stdscr, theme, top, left, box_height, box_width, "Resize Needed", focused=True)
    safe_addstr(stdscr, top + 2, left + 2, truncate_right(message, box_width - 4), theme.warning)
    stdscr.refresh()


class RootManagerScreen:
    def __init__(self, stdscr, controller: RootManagerController, theme) -> None:
        self.stdscr = stdscr
        self.controller = controller
        self.theme = theme

    def run(self) -> str:
        while True:
            self.draw()
            ch = self.stdscr.getch()
            if ch in (ord("q"), 27):
                return "Cancelled root changes"
            if ch in (ord("r"),):
                self.controller.refresh()
                self.controller.state.status_message = "Refreshed folders and roots"
                continue
            if ch in (9, ord("]")):
                self.controller.switch_pane(1)
                continue
            if ch in (curses.KEY_BTAB, ord("b")):
                self.controller.switch_pane(-1)
                continue
            if ch in (curses.KEY_DOWN, ord("j")):
                self.controller.move_selection(1)
                continue
            if ch in (curses.KEY_UP, ord("k")):
                self.controller.move_selection(-1)
                continue
            if ch == ord("s"):
                if self.controller.save():
                    return self.controller.state.status_message
                continue

            if self.controller.state.active_pane == ROOT_BROWSER:
                if ch in (curses.KEY_ENTER, 10, 13):
                    self.controller.open_selected_directory()
                    continue
                if ch in (curses.KEY_LEFT, curses.KEY_BACKSPACE, 127):
                    self.controller.go_to_parent()
                    continue
                if ch == ord(" "):
                    self.controller.toggle_browser_selection()
                    continue
                if ch == ord("x"):
                    self.controller.clear_staged_additions()
                    continue
                if ch == ord("a"):
                    self.controller.stage_all_visible()
                    continue
            else:
                if ch == ord("d"):
                    self.controller.toggle_selected_root_disabled()
                    continue
                if ch == ord("R"):
                    self.controller.remove_selected_root()
                    continue
                if ch == ord("a"):
                    self.controller.reactivate_selected_root()
                    continue

    def draw(self) -> None:
        self.stdscr.erase()
        height, width = self.stdscr.getmaxyx()
        if height < MIN_HEIGHT or width < MIN_WIDTH:
            _draw_resize_overlay(self.stdscr, height, width, f"Resize terminal to at least {MIN_WIDTH}x{MIN_HEIGHT}")
            return

        profile = self.controller.profile
        state = self.controller.state
        content_y = 3
        content_height = max(8, height - 6)
        panel_width = (width - 3) // 2
        right_x = panel_width + 2

        self._draw_header(
            f"Root Manager  {profile.name}",
            f"device={profile.device_serial}  mirror={truncate_left(str(profile.mirror_dir), max(10, width // 3))}",
        )
        self._draw_banner(Banner(state.status_message, infer_message_tone(state.status_message)))
        summary = f"path={truncate_left(state.current_path, max(10, width - 28))}  staged_add={len(state.staged_additions)}  staged_state={len(state.staged_lifecycle)}"
        fill_line(self.stdscr, 2, 0, width, self.theme.muted)
        safe_addstr(self.stdscr, 2, 2, truncate_right(summary, width - 4), self.theme.muted)

        browser_start, browser_end = _visible_bounds(state.browser_index, len(self.controller.browser_entries), content_height - 2)
        roots_start, roots_end = _visible_bounds(state.roots_index, len(self.controller.roots), content_height - 2)

        browser_y, browser_x, browser_h, browser_w = draw_panel(
            self.stdscr,
            self.theme,
            content_y,
            1,
            content_height,
            panel_width,
            "Phone Folders",
            focused=state.active_pane == ROOT_BROWSER,
            note=format_scroll_label(browser_start, browser_end - browser_start, len(self.controller.browser_entries)),
        )
        roots_y, roots_x, roots_h, roots_w = draw_panel(
            self.stdscr,
            self.theme,
            content_y,
            right_x,
            content_height,
            width - right_x - 1,
            "Configured Roots",
            focused=state.active_pane != ROOT_BROWSER,
            note=format_scroll_label(roots_start, roots_end - roots_start, len(self.controller.roots)),
        )

        if not self.controller.browser_entries:
            message = state.status_message if state.status_message.startswith("Unable to browse") else f"No folders under {state.current_path}"
            draw_centered_placeholder(self.stdscr, browser_y, browser_x, browser_h, browser_w, message, self.theme.warning)
        for row, entry in enumerate(self.controller.browser_entries[browser_start:browser_end]):
            actual_index = browser_start + row
            y = browser_y + row
            row_attr = self.theme.selected if state.active_pane == ROOT_BROWSER and actual_index == state.browser_index else self.theme.text
            fill_line(self.stdscr, y, browser_x, browser_w, row_attr if row_attr == self.theme.selected else self.theme.text)
            marker = f"[{self.controller.browser_marker(entry)}]"
            safe_addstr(self.stdscr, y, browser_x + 1, marker, row_attr)
            safe_addstr(
                self.stdscr,
                y,
                browser_x + 6,
                truncate_right(entry.name, max(0, browser_w - 7)),
                row_attr,
            )

        if not self.controller.roots:
            draw_centered_placeholder(self.stdscr, roots_y, roots_x, roots_h, roots_w, "No roots configured", self.theme.muted)
        for row, root in enumerate(self.controller.roots[roots_start:roots_end]):
            actual_index = roots_start + row
            y = roots_y + row
            row_attr = self.theme.selected if state.active_pane != ROOT_BROWSER and actual_index == state.roots_index else self.theme.text
            fill_line(self.stdscr, y, roots_x, roots_w, row_attr if row_attr == self.theme.selected else self.theme.text)
            marker = f"[{self.controller.root_marker(root)}]"
            label = truncate_right(root.label, max(8, roots_w // 3))
            path = truncate_left(root.device_path, max(0, roots_w - len(marker) - len(label) - 6))
            safe_addstr(self.stdscr, y, roots_x + 1, marker, row_attr)
            safe_addstr(self.stdscr, y, roots_x + 6, label, row_attr)
            safe_addstr(self.stdscr, y, roots_x + 6 + len(label) + 1, truncate_right(path, max(0, roots_w - len(label) - 8)), row_attr)

        fill_line(self.stdscr, height - 2, 0, width, self.theme.muted)
        safe_addstr(self.stdscr, height - 2, 2, truncate_right(self.controller.staged_summary(), width - 4), self.theme.muted)
        self._draw_footer(
            "Tab/] next pane  b prev pane  Enter open  Left/backspace parent  Space stage  a all/reactivate  d disable  R remove  x clear  s save  q back"
        )
        self.stdscr.refresh()

    def _draw_header(self, title: str, subtitle: str) -> None:
        width = self.stdscr.getmaxyx()[1]
        fill_line(self.stdscr, 0, 0, width, self.theme.header)
        safe_addstr(self.stdscr, 0, 2, truncate_right(title, width // 2), self.theme.title)
        safe_addstr(self.stdscr, 0, max(2, width - len(subtitle) - 2), truncate_left(subtitle, width // 2), self.theme.muted)

    def _draw_banner(self, banner: Banner) -> None:
        width = self.stdscr.getmaxyx()[1]
        fill_line(self.stdscr, 1, 0, width, tone_attr(self.theme, banner.tone))
        safe_addstr(self.stdscr, 1, 2, truncate_right(banner.text, width - 4), tone_attr(self.theme, banner.tone))

    def _draw_footer(self, text: str) -> None:
        height, width = self.stdscr.getmaxyx()
        fill_line(self.stdscr, height - 1, 0, width, self.theme.footer)
        safe_addstr(self.stdscr, height - 1, 2, truncate_right(text, width - 4), self.theme.footer)


class DashboardApp:
    def __init__(self, stdscr, repository: Repository, blob_store: BlobStore, transport: ADBTransport) -> None:
        self.stdscr = stdscr
        self.repository = repository
        self.transport = transport
        self.engine = SyncEngine(repository, blob_store, transport)
        self.state = DashboardState()
        self.devices = []
        self.profiles = []
        self.theme = None
        self.refresh()

    def refresh(self) -> None:
        self.devices = self.transport.list_devices()
        self.profiles = self.repository.list_profiles()
        self.state.last_refresh_at = datetime.now()
        if self.profiles:
            self.state.selected_profile = max(0, min(self.state.selected_profile, len(self.profiles) - 1))
        else:
            self.state.selected_profile = 0

    def run(self) -> int:
        curses.curs_set(0)
        self.stdscr.keypad(True)
        self.theme = init_theme()
        while True:
            self.draw()
            ch = self.stdscr.getch()
            if ch in (ord("q"), 27):
                return 0
            if ch in (ord("r"),):
                self.refresh()
                self.state.status_message = "Refreshed dashboard"
                continue
            if ch in (curses.KEY_DOWN, ord("j")):
                if self.profiles:
                    self.state.selected_profile = min(self.state.selected_profile + 1, len(self.profiles) - 1)
                continue
            if ch in (curses.KEY_UP, ord("k")):
                if self.profiles:
                    self.state.selected_profile = max(self.state.selected_profile - 1, 0)
                continue
            if ch == ord("?"):
                self.show_popup(
                    "Help",
                    [
                        "Dashboard",
                        "  j/k or arrows   move between profiles",
                        "  e               edit roots for selected profile",
                        "  y               sync selected profile",
                        "  c               view checkpoints",
                        "  i               view issues",
                        "  l               view recent runs",
                        "  r               refresh devices and profiles",
                        "",
                        "Root Manager",
                        "  Tab/] / b       switch panes",
                        "  Space           stage folder",
                        "  d / R / a       disable, remove, reactivate",
                        "  s               save root changes",
                        "  q               back",
                    ],
                )
                continue
            if ch == ord("e"):
                self.open_root_manager()
                continue
            if ch == ord("c"):
                self.show_checkpoints()
                continue
            if ch == ord("i"):
                self.show_issues()
                continue
            if ch == ord("l"):
                self.show_runs()
                continue
            if ch == ord("y"):
                self.run_sync()
                continue

    def selected_profile(self):
        if not self.profiles:
            return None
        return self.profiles[self.state.selected_profile]

    def matching_device(self, profile):
        if profile is None:
            return None
        return next((device for device in self.devices if device.serial == profile.device_serial), None)

    def draw(self) -> None:
        assert self.theme is not None
        self.stdscr.erase()
        height, width = self.stdscr.getmaxyx()
        if height < MIN_HEIGHT or width < MIN_WIDTH:
            _draw_resize_overlay(self.stdscr, height, width, f"Resize terminal to at least {MIN_WIDTH}x{MIN_HEIGHT}")
            return

        selected = self.selected_profile()
        matching_device = self.matching_device(selected)
        banner = build_dashboard_banner(selected, matching_device)
        content_y = 3
        content_height = max(8, height - 6)
        left_width = max(30, min(36, width // 3 + 2))
        right_x = left_width + 2
        right_width = width - right_x - 1
        summary_height = 7
        roots_height = max(5, (content_height - summary_height) // 2)
        runs_height = max(5, content_height - summary_height - roots_height)
        roots_height = content_height - summary_height - runs_height

        self._draw_header("AndroidMigrate", f"Refreshed {format_clock(self.state.last_refresh_at)}")
        self._draw_banner(banner)

        profile_start, profile_end = _visible_bounds(self.state.selected_profile, len(self.profiles), content_height - 2)
        profiles_y, profiles_x, profiles_h, profiles_w = draw_panel(
            self.stdscr,
            self.theme,
            content_y,
            1,
            content_height,
            left_width,
            "Profiles",
            focused=True,
            note=format_scroll_label(profile_start, profile_end - profile_start, len(self.profiles)),
        )
        summary_y, summary_x, summary_h, summary_w = draw_panel(
            self.stdscr,
            self.theme,
            content_y,
            right_x,
            summary_height,
            right_width,
            "Device + Profile Summary",
        )
        roots_y, roots_x, roots_h, roots_w = draw_panel(
            self.stdscr,
            self.theme,
            content_y + summary_height,
            right_x,
            roots_height,
            right_width,
            "Roots",
        )
        runs_y, runs_x, runs_h, runs_w = draw_panel(
            self.stdscr,
            self.theme,
            content_y + summary_height + roots_height,
            right_x,
            runs_height,
            right_width,
            "Recent Runs",
        )

        if not self.profiles:
            draw_centered_placeholder(self.stdscr, profiles_y, profiles_x, profiles_h, profiles_w, "No profiles configured", self.theme.warning)
        for row, profile in enumerate(self.profiles[profile_start:profile_end]):
            actual_index = profile_start + row
            y = profiles_y + row
            row_attr = self.theme.selected if actual_index == self.state.selected_profile else self.theme.text
            fill_line(self.stdscr, y, profiles_x, profiles_w, row_attr if row_attr == self.theme.selected else self.theme.text)
            badge = format_badge(profile.profile_state)
            badge_width = len(badge)
            safe_addstr(self.stdscr, y, profiles_x + 1, truncate_right(profile.name, max(0, profiles_w - badge_width - 4)), row_attr)
            safe_addstr(
                self.stdscr,
                y,
                profiles_x + max(1, profiles_w - badge_width - 1),
                badge,
                row_attr if row_attr == self.theme.selected else tone_attr(self.theme, badge_tone(profile.profile_state)),
            )

        if selected is None:
            draw_centered_placeholder(self.stdscr, summary_y, summary_x, summary_h, summary_w, "Select a profile to inspect", self.theme.muted)
            draw_centered_placeholder(self.stdscr, roots_y, roots_x, roots_h, roots_w, "No roots to show", self.theme.muted)
            draw_centered_placeholder(self.stdscr, runs_y, runs_x, runs_h, runs_w, "No runs to show", self.theme.muted)
            self._draw_status_and_footer()
            self.stdscr.refresh()
            return

        device_label = selected.device_serial
        device_attr = self.theme.warning
        if matching_device is not None and matching_device.state == "device":
            device_label = f"{matching_device.serial} {format_badge('connected')}"
            device_attr = self.theme.success
        elif matching_device is not None:
            device_label = f"{matching_device.serial} {format_badge(matching_device.state)}"
            device_attr = self.theme.error
        else:
            device_label = f"{selected.device_serial} {format_badge('missing')}"
            device_attr = self.theme.warning
        safe_addstr(self.stdscr, summary_y, summary_x + 1, truncate_right(device_label, summary_w - 2), device_attr)
        draw_key_value(self.stdscr, self.theme, summary_y + 1, summary_x + 1, summary_w - 2, "Profile", f"{selected.name} {format_badge(selected.profile_state)}")
        draw_key_value(self.stdscr, self.theme, summary_y + 2, summary_x + 1, summary_w - 2, "Mirror", str(selected.mirror_dir), value_mode="left")
        draw_key_value(
            self.stdscr,
            self.theme,
            summary_y + 3,
            summary_x + 1,
            summary_w - 2,
            "Issues",
            str(self.repository.count_open_issues(selected.id)),
        )
        latest_checkpoint = self.repository.list_checkpoints(selected.id)[:1]
        latest_checkpoint_text = f"#{latest_checkpoint[0].id}" if latest_checkpoint else "none"
        draw_key_value(self.stdscr, self.theme, summary_y + 4, summary_x + 1, summary_w - 2, "Checkpoint", latest_checkpoint_text)
        latest_run = self.repository.list_recent_runs(selected.id, limit=1)
        if latest_run:
            run_value = f"{latest_run[0].operation_type} {format_badge(latest_run[0].status)}"
        else:
            run_value = "none"
        draw_key_value(self.stdscr, self.theme, summary_y + 5, summary_x + 1, summary_w - 2, "Last Run", run_value)

        roots = self.repository.list_roots(selected.id)
        roots_start, roots_end = _visible_bounds(0, len(roots), roots_h)
        if not roots:
            draw_centered_placeholder(self.stdscr, roots_y, roots_x, roots_h, roots_w, "No roots configured", self.theme.muted)
        for row, root in enumerate(roots[roots_start:roots_end]):
            y = roots_y + row
            fill_line(self.stdscr, y, roots_x, roots_w, self.theme.text)
            badge = format_badge(root.lifecycle)
            badge_attr = tone_attr(self.theme, badge_tone(root.lifecycle))
            label = truncate_right(root.label, max(8, roots_w // 4))
            safe_addstr(self.stdscr, y, roots_x + 1, label, self.theme.text)
            safe_addstr(self.stdscr, y, roots_x + len(label) + 2, badge, badge_attr)
            safe_addstr(
                self.stdscr,
                y,
                roots_x + len(label) + len(badge) + 4,
                truncate_left(root.device_path, max(0, roots_w - len(label) - len(badge) - 6)),
                self.theme.muted,
            )

        runs = self.repository.list_recent_runs(selected.id, limit=max(1, runs_h))
        if not runs:
            draw_centered_placeholder(self.stdscr, runs_y, runs_x, runs_h, runs_w, "No runs yet", self.theme.muted)
        for row, run in enumerate(runs[:runs_h]):
            y = runs_y + row
            fill_line(self.stdscr, y, runs_x, runs_w, self.theme.text)
            badge = format_badge(run.status)
            safe_addstr(self.stdscr, y, runs_x + 1, truncate_right(run.operation_type, 14), self.theme.text)
            safe_addstr(self.stdscr, y, runs_x + 16, badge, tone_attr(self.theme, badge_tone(run.status)))
            source = f"cp={run.result_checkpoint_id}" if run.result_checkpoint_id else "no checkpoint"
            safe_addstr(self.stdscr, y, runs_x + 16 + len(badge) + 2, truncate_right(source, max(0, runs_w - 20 - len(badge))), self.theme.muted)

        self._draw_status_and_footer()
        self.stdscr.refresh()

    def _draw_header(self, title: str, right_text: str) -> None:
        width = self.stdscr.getmaxyx()[1]
        fill_line(self.stdscr, 0, 0, width, self.theme.header)
        safe_addstr(self.stdscr, 0, 2, truncate_right(title, width // 2), self.theme.title)
        safe_addstr(self.stdscr, 0, max(2, width - len(right_text) - 2), right_text, self.theme.muted)

    def _draw_banner(self, banner: Banner) -> None:
        width = self.stdscr.getmaxyx()[1]
        attr = tone_attr(self.theme, banner.tone)
        fill_line(self.stdscr, 1, 0, width, attr)
        safe_addstr(self.stdscr, 1, 2, truncate_right(banner.text, width - 4), attr)

    def _draw_status_and_footer(self) -> None:
        height, width = self.stdscr.getmaxyx()
        status_attr = tone_attr(self.theme, infer_message_tone(self.state.status_message))
        fill_line(self.stdscr, height - 2, 0, width, status_attr)
        safe_addstr(self.stdscr, height - 2, 2, truncate_right(self.state.status_message, width - 4), status_attr)
        fill_line(self.stdscr, height - 1, 0, width, self.theme.footer)
        safe_addstr(
            self.stdscr,
            height - 1,
            2,
            truncate_right("j/k move  e edit roots  y sync  c checkpoints  i issues  l runs  r refresh  ? help  q quit", width - 4),
            self.theme.footer,
        )

    def open_root_manager(self) -> None:
        profile = self.selected_profile()
        if profile is None:
            self.state.status_message = "No profile selected"
            return
        if profile.profile_state != PROFILE_ACTIVE:
            self.state.status_message = f"Profile {profile.name} is not editable in state {profile.profile_state}"
            return
        self.devices = self.transport.list_devices()
        matching_device = self.matching_device(profile)
        if matching_device is None:
            self.state.status_message = f"Device {profile.device_serial} is not connected"
            return
        if matching_device.state != "device":
            self.state.status_message = f"Device {profile.device_serial} is in state {matching_device.state}"
            return
        controller = RootManagerController(self.repository, self.transport, profile)
        status = RootManagerScreen(self.stdscr, controller, self.theme).run()
        self.refresh()
        self.state.status_message = status

    def run_sync(self) -> None:
        profile = self.selected_profile()
        if profile is None:
            self.state.status_message = "No profile selected"
            return
        logs: list[str] = []
        stats = {"events": 0, "completed": 0, "failed": 0, "latest": "Waiting for sync events"}

        def sink(event: dict[str, object]) -> None:
            stats["events"] += 1
            if event["status"] == "completed":
                stats["completed"] += 1
            elif event["status"] == "failed":
                stats["failed"] += 1
            stats["latest"] = str(event["message"])
            logs.append(f"{event['stage']} [{event['status']}] {event['message']}")
            self.draw_run_screen(f"Sync: {profile.name}", logs, stats, run_state="running")

        try:
            summary = self.engine.sync_profile(profile.name, event_sink=sink)
            stats["latest"] = f"Created checkpoint {summary.checkpoint_id}"
            self.draw_run_screen(f"Sync: {profile.name}", logs, stats, run_state="completed")
            self.state.status_message = f"Sync completed for {profile.name} checkpoint={summary.checkpoint_id}"
        except (ValueError, TransportError) as exc:
            logs.append(f"run [failed] {exc}")
            stats["failed"] += 1
            stats["latest"] = str(exc)
            self.draw_run_screen(f"Sync: {profile.name}", logs, stats, run_state="failed")
            self.state.status_message = f"Sync failed for {profile.name}: {exc}"
            self.stdscr.getch()
        finally:
            self.refresh()

    def draw_run_screen(self, title: str, logs: list[str], stats: dict[str, object], *, run_state: str) -> None:
        height, width = self.stdscr.getmaxyx()
        if height < MIN_HEIGHT or width < MIN_WIDTH:
            _draw_resize_overlay(self.stdscr, height, width, f"Resize terminal to at least {MIN_WIDTH}x{MIN_HEIGHT}")
            return

        self.stdscr.erase()
        banner = Banner(f"{title} {format_badge(run_state)}", "success" if run_state == "completed" else "error" if run_state == "failed" else "warning")
        self._draw_header(title, f"events={stats['events']}")
        self._draw_banner(banner)
        summary_y, summary_x, summary_h, summary_w = draw_panel(self.stdscr, self.theme, 3, 1, 6, width - 2, "Run Summary")
        logs_y, logs_x, logs_h, logs_w = draw_panel(
            self.stdscr,
            self.theme,
            9,
            1,
            max(6, height - 12),
            width - 2,
            "Event Log",
            note=format_scroll_label(max(0, len(logs) - max(1, height - 14)), min(len(logs), max(1, height - 14)), len(logs)),
        )

        draw_key_value(self.stdscr, self.theme, summary_y, summary_x + 1, summary_w - 2, "Events", str(stats["events"]))
        draw_key_value(self.stdscr, self.theme, summary_y + 1, summary_x + 1, summary_w - 2, "Completed", str(stats["completed"]))
        draw_key_value(self.stdscr, self.theme, summary_y + 2, summary_x + 1, summary_w - 2, "Failed", str(stats["failed"]))
        draw_key_value(self.stdscr, self.theme, summary_y + 3, summary_x + 1, summary_w - 2, "Latest", str(stats["latest"]), value_mode="left")

        if not logs:
            draw_centered_placeholder(self.stdscr, logs_y, logs_x, logs_h, logs_w, "Waiting for sync events", self.theme.muted)
        visible_logs = logs[-logs_h:]
        for row, line in enumerate(visible_logs):
            safe_addstr(self.stdscr, logs_y + row, logs_x + 1, truncate_right(line, logs_w - 2), self.theme.text)

        fill_line(self.stdscr, height - 1, 0, width, self.theme.footer)
        footer = "Running..." if run_state == "running" else "Press any key to return"
        safe_addstr(self.stdscr, height - 1, 2, truncate_right(footer, width - 4), self.theme.footer)
        self.stdscr.refresh()

    def show_checkpoints(self) -> None:
        profile = self.selected_profile()
        if profile is None:
            self.state.status_message = "No profile selected"
            return
        checkpoints = self.repository.list_checkpoints(profile.id)
        lines = [f"{cp.id}  {cp.created_at}  {cp.status}" for cp in checkpoints] or ["No checkpoints yet"]
        self.show_popup(f"Checkpoints: {profile.name}", lines)

    def show_issues(self) -> None:
        profile = self.selected_profile()
        if profile is None:
            self.state.status_message = "No profile selected"
            return
        issues = self.engine.list_issues(profile.name)
        lines = [f"{state.id}  {root.label}/{state.relative_path}  {state.status}" for root, state in issues] or [
            "No unresolved issues"
        ]
        self.show_popup(f"Issues: {profile.name}", lines)

    def show_runs(self) -> None:
        profile = self.selected_profile()
        if profile is None:
            self.state.status_message = "No profile selected"
            return
        runs = self.repository.list_recent_runs(profile.id)
        lines = [
            f"{run.id}  {run.operation_type}  {run.status}  src={run.source_checkpoint_id}  out={run.result_checkpoint_id}"
            for run in runs
        ] or ["No runs yet"]
        self.show_popup(f"Runs: {profile.name}", lines)

    def show_popup(self, title: str, lines: list[str]) -> None:
        height, width = self.stdscr.getmaxyx()
        popup_height = min(height - 4, max(8, len(lines) + 4))
        popup_width = min(width - 6, max(46, max(len(title) + 6, *(len(line) + 4 for line in lines))))
        top = max(1, (height - popup_height) // 2)
        left = max(2, (width - popup_width) // 2)
        win = curses.newwin(popup_height, popup_width, top, left)
        win.keypad(True)
        draw_panel(win, self.theme, 0, 0, popup_height, popup_width, title, focused=True)
        max_lines = popup_height - 3
        for idx, line in enumerate(lines[:max_lines]):
            safe_addstr(win, 1 + idx, 2, truncate_right(line, popup_width - 4), self.theme.text)
        safe_addstr(win, popup_height - 2, 2, truncate_right("Press any key to close", popup_width - 4), self.theme.footer)
        win.refresh()
        win.getch()
        del win


def run_tui(repository: Repository, blob_store: BlobStore, transport: ADBTransport) -> int:
    def _wrapped(stdscr) -> int:
        app = DashboardApp(stdscr, repository, blob_store, transport)
        return app.run()

    return curses.wrapper(_wrapped)
