from __future__ import annotations

import curses
from dataclasses import dataclass

from .models import PROFILE_ACTIVE
from .root_manager import ROOT_BROWSER, RootManagerController
from .storage import BlobStore, Repository
from .sync_engine import SyncEngine
from .transport import ADBTransport, TransportError


MIN_HEIGHT = 24
MIN_WIDTH = 90


@dataclass(slots=True)
class DashboardState:
    selected_profile: int = 0
    status_message: str = "Press ? for help"


def _visible_bounds(selected: int, total: int, max_rows: int) -> tuple[int, int]:
    if max_rows <= 0 or total <= max_rows:
        return 0, total
    start = max(0, min(selected - (max_rows // 2), total - max_rows))
    return start, start + max_rows


class RootManagerScreen:
    def __init__(self, stdscr, controller: RootManagerController) -> None:
        self.stdscr = stdscr
        self.controller = controller

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
            self.stdscr.addstr(0, 0, f"Resize terminal to at least {MIN_WIDTH}x{MIN_HEIGHT}")
            self.stdscr.refresh()
            return

        divider = width // 2
        browser_width = divider - 3
        roots_width = width - divider - 4
        profile = self.controller.profile
        state = self.controller.state

        self.stdscr.addstr(0, 2, f"Root Manager: {profile.name}", curses.A_BOLD)
        subtitle = (
            f"device={profile.device_serial}  mirror={profile.mirror_dir}  "
            f"staged_add={len(state.staged_additions)}  staged_state={len(state.staged_lifecycle)}"
        )
        self.stdscr.addstr(1, 2, subtitle[: width - 4], curses.A_DIM)

        browser_attr = curses.A_BOLD if state.active_pane == ROOT_BROWSER else curses.A_UNDERLINE
        roots_attr = curses.A_BOLD if state.active_pane != ROOT_BROWSER else curses.A_UNDERLINE
        self.stdscr.addstr(3, 2, f"Browser: {state.current_path}", browser_attr)
        self.stdscr.addstr(3, divider + 2, "Profile Roots", roots_attr)
        self.stdscr.vline(2, divider, curses.ACS_VLINE, height - 5)

        visible_rows = height - 8
        browser_start, browser_end = _visible_bounds(state.browser_index, len(self.controller.browser_entries), visible_rows)
        roots_start, roots_end = _visible_bounds(state.roots_index, len(self.controller.roots), visible_rows)

        if not self.controller.browser_entries:
            empty_message = state.status_message
            if not empty_message.startswith("Unable to browse"):
                empty_message = f"No subfolders found under {state.current_path}"
            self.stdscr.addstr(5, 4, empty_message[:browser_width])
        for row, entry in enumerate(self.controller.browser_entries[browser_start:browser_end], start=5):
            actual_index = browser_start + (row - 5)
            attr = curses.A_REVERSE if state.active_pane == ROOT_BROWSER and actual_index == state.browser_index else curses.A_NORMAL
            marker = self.controller.browser_marker(entry)
            text = f"[{marker}] {entry.name}"
            self.stdscr.addstr(row, 4, text[:browser_width], attr)

        if not self.controller.roots:
            self.stdscr.addstr(5, divider + 4, "No roots configured")
        for row, root in enumerate(self.controller.roots[roots_start:roots_end], start=5):
            actual_index = roots_start + (row - 5)
            attr = curses.A_REVERSE if state.active_pane != ROOT_BROWSER and actual_index == state.roots_index else curses.A_NORMAL
            marker = self.controller.root_marker(root)
            text = f"[{marker}] {root.label} -> {root.device_path}"
            self.stdscr.addstr(row, divider + 4, text[:roots_width], attr)

        footer = (
            "Tab/] next pane  b prev pane  Enter open  Left/backspace parent  "
            "Space stage  a all/reactivate  d disable  R remove  x clear staged adds  s save  q back"
        )
        self.stdscr.addstr(height - 2, 2, state.status_message[: width - 4])
        self.stdscr.addstr(height - 1, 2, footer[: width - 4], curses.A_DIM)
        self.stdscr.refresh()


class DashboardApp:
    def __init__(self, stdscr, repository: Repository, blob_store: BlobStore, transport: ADBTransport) -> None:
        self.stdscr = stdscr
        self.repository = repository
        self.transport = transport
        self.engine = SyncEngine(repository, blob_store, transport)
        self.state = DashboardState()
        self.devices = []
        self.profiles = []
        self.refresh()

    def refresh(self) -> None:
        self.devices = self.transport.list_devices()
        self.profiles = self.repository.list_profiles()
        if self.profiles:
            self.state.selected_profile = max(0, min(self.state.selected_profile, len(self.profiles) - 1))
        else:
            self.state.selected_profile = 0

    def run(self) -> int:
        curses.curs_set(0)
        self.stdscr.keypad(True)
        while True:
            self.draw()
            ch = self.stdscr.getch()
            if ch in (ord("q"), 27):
                return 0
            if ch in (ord("r"),):
                self.refresh()
                self.state.status_message = "Refreshed"
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
                        "j/k or arrows: move between profiles",
                        "e: edit roots for selected active profile",
                        "r: refresh devices and profiles",
                        "y: sync selected profile",
                        "c: view checkpoints",
                        "i: view issues",
                        "l: view recent runs",
                        "q or Esc: quit",
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

    def draw(self) -> None:
        self.stdscr.erase()
        height, width = self.stdscr.getmaxyx()
        if height < MIN_HEIGHT or width < MIN_WIDTH:
            self.stdscr.addstr(0, 0, f"Resize terminal to at least {MIN_WIDTH}x{MIN_HEIGHT}")
            self.stdscr.refresh()
            return

        divider = max(30, width // 3)
        selected = self.selected_profile()

        self.stdscr.addstr(0, 2, "AndroidMigrate TUI", curses.A_BOLD)
        self.stdscr.addstr(height - 2, 2, self.state.status_message[: width - 4])
        self.stdscr.addstr(
            height - 1,
            2,
            "j/k arrows move  e edit roots  y sync  c checkpoints  i issues  l runs  r refresh  ? help  q quit"[
                : width - 4
            ],
            curses.A_DIM,
        )

        self.stdscr.addstr(2, 2, "Profiles", curses.A_UNDERLINE)
        if not self.profiles:
            self.stdscr.addstr(4, 4, "No profiles configured")
        profile_rows = height - 8
        start, end = _visible_bounds(self.state.selected_profile, len(self.profiles), profile_rows)
        for row, profile in enumerate(self.profiles[start:end], start=4):
            actual_index = start + (row - 4)
            attr = curses.A_REVERSE if actual_index == self.state.selected_profile else curses.A_NORMAL
            line = f"{profile.name} [{profile.profile_state}]"
            self.stdscr.addstr(row, 4, line[: divider - 6], attr)

        self.stdscr.vline(1, divider, curses.ACS_VLINE, height - 4)

        self.stdscr.addstr(2, divider + 2, "Devices", curses.A_UNDERLINE)
        if not self.devices:
            self.stdscr.addstr(4, divider + 4, "No devices detected")
        for idx, device in enumerate(self.devices[:4]):
            extras = f" model={device.model}" if device.model else ""
            self.stdscr.addstr(4 + idx, divider + 4, f"{device.serial} [{device.state}]{extras}"[: width - divider - 6])

        if selected is None:
            self.stdscr.refresh()
            return

        line = 10
        self.stdscr.addstr(line, divider + 2, "Selected Profile", curses.A_UNDERLINE)
        line += 2
        details = [
            f"name: {selected.name}",
            f"device: {selected.device_serial}",
            f"mirror: {selected.mirror_dir}",
            f"state: {selected.profile_state}",
            f"issues: {self.repository.count_open_issues(selected.id)}",
        ]
        for item in details:
            self.stdscr.addstr(line, divider + 4, item[: width - divider - 6])
            line += 1

        line += 1
        self.stdscr.addstr(line, divider + 2, "Roots", curses.A_UNDERLINE)
        line += 2
        roots = self.repository.list_roots(selected.id)
        for root in roots[: max(1, height - line - 10)]:
            self.stdscr.addstr(line, divider + 4, f"{root.label} [{root.lifecycle}] -> {root.device_path}"[: width - divider - 6])
            line += 1

        if line < height - 7:
            line += 1
            self.stdscr.addstr(line, divider + 2, "Recent Runs", curses.A_UNDERLINE)
            line += 2
            for run in self.repository.list_recent_runs(selected.id, limit=max(1, height - line - 3)):
                text = f"{run.operation_type} [{run.status}] source={run.source_checkpoint_id} result={run.result_checkpoint_id}"
                self.stdscr.addstr(line, divider + 4, text[: width - divider - 6])
                line += 1
                if line >= height - 2:
                    break

        self.stdscr.refresh()

    def open_root_manager(self) -> None:
        profile = self.selected_profile()
        if profile is None:
            self.state.status_message = "No profile selected"
            return
        if profile.profile_state != PROFILE_ACTIVE:
            self.state.status_message = f"Profile {profile.name} is not editable in state {profile.profile_state}"
            return
        self.devices = self.transport.list_devices()
        matching_device = next((device for device in self.devices if device.serial == profile.device_serial), None)
        if matching_device is None:
            self.state.status_message = f"Device {profile.device_serial} is not connected"
            return
        if matching_device.state != "device":
            self.state.status_message = f"Device {profile.device_serial} is in state {matching_device.state}"
            return
        controller = RootManagerController(self.repository, self.transport, profile)
        status = RootManagerScreen(self.stdscr, controller).run()
        self.refresh()
        self.state.status_message = status

    def run_sync(self) -> None:
        profile = self.selected_profile()
        if profile is None:
            self.state.status_message = "No profile selected"
            return
        logs: list[str] = []

        def sink(event: dict[str, object]) -> None:
            text = f"{event['stage']} [{event['status']}] {event['message']}"
            logs.append(text)
            self.draw_run_screen(f"Sync: {profile.name}", logs)

        try:
            summary = self.engine.sync_profile(profile.name, event_sink=sink)
            self.state.status_message = f"Sync completed for {profile.name} checkpoint={summary.checkpoint_id}"
        except (ValueError, TransportError) as exc:
            logs.append(f"failed: {exc}")
            self.draw_run_screen(f"Sync: {profile.name}", logs)
            self.state.status_message = f"Sync failed for {profile.name}: {exc}"
            self.stdscr.getch()
        finally:
            self.refresh()

    def draw_run_screen(self, title: str, logs: list[str]) -> None:
        self.stdscr.erase()
        height, width = self.stdscr.getmaxyx()
        self.stdscr.addstr(0, 2, title, curses.A_BOLD)
        max_lines = max(1, height - 4)
        for idx, line in enumerate(logs[-max_lines:]):
            self.stdscr.addstr(2 + idx, 2, line[: width - 4])
        self.stdscr.addstr(height - 1, 2, "Running..."[: width - 4], curses.A_DIM)
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
        popup_width = min(width - 8, max(40, max(len(title) + 4, *(len(line) + 4 for line in lines))))
        top = max(1, (height - popup_height) // 2)
        left = max(2, (width - popup_width) // 2)
        win = curses.newwin(popup_height, popup_width, top, left)
        win.keypad(True)
        win.box()
        win.addstr(0, 2, f" {title} ", curses.A_BOLD)
        max_lines = popup_height - 3
        for idx, line in enumerate(lines[:max_lines]):
            win.addstr(1 + idx, 2, line[: popup_width - 4])
        win.addstr(popup_height - 1, 2, "Press any key to close"[: popup_width - 4], curses.A_DIM)
        win.refresh()
        win.getch()
        del win


def run_tui(repository: Repository, blob_store: BlobStore, transport: ADBTransport) -> int:
    def _wrapped(stdscr) -> int:
        app = DashboardApp(stdscr, repository, blob_store, transport)
        return app.run()

    return curses.wrapper(_wrapped)
