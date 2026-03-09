from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .config import ensure_state_layout
from .models import (
    Checkpoint,
    CheckpointRoot,
    FileState,
    PROFILE_ACTIVE,
    Profile,
    ROOT_ACTIVE,
    SyncRoot,
    SyncRun,
    SyncRunEvent,
)


def utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class BlobStore:
    def __init__(self, state_dir: Path) -> None:
        self.root = ensure_state_layout(state_dir) / "blobs"

    def path_for_hash(self, blob_hash: str) -> Path:
        return self.root / blob_hash[:2] / blob_hash

    def store_path(self, source: Path, blob_hash: str) -> Path:
        import shutil

        target = self.path_for_hash(blob_hash)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            shutil.copy2(source, target)
        return target

    def gc(self, referenced_hashes: set[str]) -> None:
        if not self.root.exists():
            return
        for subdir in self.root.iterdir():
            if not subdir.is_dir():
                continue
            for blob in subdir.iterdir():
                if blob.name not in referenced_hashes:
                    blob.unlink()
            try:
                subdir.rmdir()
            except OSError:
                pass


class Repository:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = ensure_state_layout(state_dir)
        self.db_path = self.state_dir / "state.db"
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                device_serial TEXT NOT NULL,
                mirror_dir TEXT NOT NULL,
                checkpoint_retention INTEGER NOT NULL DEFAULT 30,
                created_at TEXT NOT NULL,
                profile_state TEXT NOT NULL DEFAULT 'active',
                cloned_from_profile_id INTEGER,
                cloned_from_checkpoint_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS sync_roots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
                device_path TEXT NOT NULL,
                label TEXT NOT NULL,
                lifecycle TEXT NOT NULL DEFAULT 'active',
                UNIQUE(profile_id, device_path),
                UNIQUE(profile_id, label)
            );

            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                summary_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS checkpoint_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                checkpoint_id INTEGER NOT NULL REFERENCES checkpoints(id) ON DELETE CASCADE,
                root_id INTEGER NOT NULL REFERENCES sync_roots(id) ON DELETE CASCADE,
                relative_path TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                size INTEGER NOT NULL,
                device_mtime INTEGER NOT NULL,
                UNIQUE(checkpoint_id, root_id, relative_path)
            );

            CREATE TABLE IF NOT EXISTS file_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
                root_id INTEGER NOT NULL REFERENCES sync_roots(id) ON DELETE CASCADE,
                relative_path TEXT NOT NULL,
                status TEXT NOT NULL,
                device_present INTEGER NOT NULL,
                device_hash TEXT,
                device_size INTEGER,
                device_mtime INTEGER,
                local_present INTEGER NOT NULL,
                local_hash TEXT,
                local_size INTEGER,
                local_mtime INTEGER,
                conflict_copy_path TEXT,
                updated_at TEXT NOT NULL,
                last_synced_checkpoint_id INTEGER,
                last_restored_from_checkpoint_id INTEGER,
                UNIQUE(profile_id, root_id, relative_path)
            );

            CREATE TABLE IF NOT EXISTS sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
                operation_type TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                source_profile_id INTEGER,
                source_checkpoint_id INTEGER,
                result_checkpoint_id INTEGER,
                summary_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_run_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL REFERENCES sync_runs(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                stage TEXT NOT NULL,
                root_id INTEGER,
                root_label TEXT,
                relative_path TEXT,
                action TEXT,
                status TEXT NOT NULL,
                message TEXT NOT NULL
            );
            """
        )
        self._migrate_schema()
        self.conn.commit()

    def _migrate_schema(self) -> None:
        self._ensure_column("profiles", "profile_state", "TEXT NOT NULL DEFAULT 'active'")
        self._ensure_column("profiles", "cloned_from_profile_id", "INTEGER")
        self._ensure_column("profiles", "cloned_from_checkpoint_id", "INTEGER")
        self.conn.execute(
            """
            UPDATE profiles
            SET profile_state = ?
            WHERE profile_state IS NULL OR profile_state = ''
            """,
            (PROFILE_ACTIVE,),
        )

        self._ensure_column("sync_roots", "lifecycle", "TEXT NOT NULL DEFAULT 'active'")
        if "enabled" in self._table_columns("sync_roots"):
            self.conn.execute(
                """
                UPDATE sync_roots
                SET lifecycle = CASE
                    WHEN lifecycle IS NULL OR lifecycle = '' THEN CASE
                        WHEN enabled = 1 THEN 'active'
                        ELSE 'disabled'
                    END
                    ELSE lifecycle
                END
                """
            )
        else:
            self.conn.execute(
                """
                UPDATE sync_roots
                SET lifecycle = ?
                WHERE lifecycle IS NULL OR lifecycle = ''
                """,
                (ROOT_ACTIVE,),
            )

        self._ensure_column("file_states", "last_synced_checkpoint_id", "INTEGER")
        self._ensure_column("file_states", "last_restored_from_checkpoint_id", "INTEGER")
        if "last_checkpoint_id" in self._table_columns("file_states"):
            self.conn.execute(
                """
                UPDATE file_states
                SET last_synced_checkpoint_id = COALESCE(last_synced_checkpoint_id, last_checkpoint_id)
                """
            )

    def _table_columns(self, table: str) -> set[str]:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {row["name"] for row in rows}

    def _ensure_column(self, table: str, name: str, definition: str) -> None:
        if name in self._table_columns(table):
            return
        self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

    def create_profile(
        self,
        name: str,
        device_serial: str,
        mirror_dir: Path,
        checkpoint_retention: int = 30,
        profile_state: str = PROFILE_ACTIVE,
        cloned_from_profile_id: int | None = None,
        cloned_from_checkpoint_id: int | None = None,
    ) -> Profile:
        created_at = utc_now()
        cursor = self.conn.execute(
            """
            INSERT INTO profiles (
                name, device_serial, mirror_dir, checkpoint_retention, created_at,
                profile_state, cloned_from_profile_id, cloned_from_checkpoint_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                device_serial,
                str(mirror_dir),
                checkpoint_retention,
                created_at,
                profile_state,
                cloned_from_profile_id,
                cloned_from_checkpoint_id,
            ),
        )
        self.conn.commit()
        return Profile(
            id=int(cursor.lastrowid),
            name=name,
            device_serial=device_serial,
            mirror_dir=mirror_dir,
            checkpoint_retention=checkpoint_retention,
            created_at=created_at,
            profile_state=profile_state,
            cloned_from_profile_id=cloned_from_profile_id,
            cloned_from_checkpoint_id=cloned_from_checkpoint_id,
        )

    def delete_profile(self, profile_id: int) -> None:
        self.conn.execute("DELETE FROM profiles WHERE id = ?", (profile_id,))
        self.conn.commit()

    def rename_profile(self, profile_id: int, new_name: str) -> None:
        self.conn.execute("UPDATE profiles SET name = ? WHERE id = ?", (new_name, profile_id))
        self.conn.commit()

    def update_profile_mirror_dir(self, profile_id: int, mirror_dir: Path) -> None:
        self.conn.execute("UPDATE profiles SET mirror_dir = ? WHERE id = ?", (str(mirror_dir), profile_id))
        self.conn.commit()

    def update_profile_state(self, profile_id: int, profile_state: str) -> None:
        self.conn.execute("UPDATE profiles SET profile_state = ? WHERE id = ?", (profile_state, profile_id))
        self.conn.commit()

    def list_profiles(self) -> list[Profile]:
        rows = self.conn.execute(
            """
            SELECT id, name, device_serial, mirror_dir, checkpoint_retention, created_at,
                   profile_state, cloned_from_profile_id, cloned_from_checkpoint_id
            FROM profiles
            ORDER BY name
            """
        ).fetchall()
        return [self._row_to_profile(row) for row in rows]

    def get_profile(self, name: str) -> Profile:
        row = self.conn.execute(
            """
            SELECT id, name, device_serial, mirror_dir, checkpoint_retention, created_at,
                   profile_state, cloned_from_profile_id, cloned_from_checkpoint_id
            FROM profiles
            WHERE name = ?
            """,
            (name,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown profile: {name}")
        return self._row_to_profile(row)

    def get_profile_by_id(self, profile_id: int) -> Profile:
        row = self.conn.execute(
            """
            SELECT id, name, device_serial, mirror_dir, checkpoint_retention, created_at,
                   profile_state, cloned_from_profile_id, cloned_from_checkpoint_id
            FROM profiles
            WHERE id = ?
            """,
            (profile_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown profile id: {profile_id}")
        return self._row_to_profile(row)

    def add_root(self, profile_id: int, device_path: str, label: str, lifecycle: str = ROOT_ACTIVE) -> SyncRoot:
        cursor = self.conn.execute(
            """
            INSERT INTO sync_roots (profile_id, device_path, label, lifecycle)
            VALUES (?, ?, ?, ?)
            """,
            (profile_id, device_path, label, lifecycle),
        )
        self.conn.commit()
        return SyncRoot(
            id=int(cursor.lastrowid),
            profile_id=profile_id,
            device_path=device_path,
            label=label,
            lifecycle=lifecycle,
        )

    def set_root_lifecycle(self, root_id: int, lifecycle: str) -> None:
        self.conn.execute("UPDATE sync_roots SET lifecycle = ? WHERE id = ?", (lifecycle, root_id))
        self.conn.commit()

    def list_roots(self, profile_id: int, *, include_removed: bool = True) -> list[SyncRoot]:
        query = """
            SELECT id, profile_id, device_path, label, lifecycle
            FROM sync_roots
            WHERE profile_id = ?
        """
        params: list[object] = [profile_id]
        if not include_removed:
            query += " AND lifecycle != 'removed'"
        query += " ORDER BY label"
        rows = self.conn.execute(query, params).fetchall()
        return [self._row_to_root(row) for row in rows]

    def list_active_roots(self, profile_id: int) -> list[SyncRoot]:
        rows = self.conn.execute(
            """
            SELECT id, profile_id, device_path, label, lifecycle
            FROM sync_roots
            WHERE profile_id = ? AND lifecycle = 'active'
            ORDER BY label
            """,
            (profile_id,),
        ).fetchall()
        return [self._row_to_root(row) for row in rows]

    def get_root_by_label(self, profile_id: int, label: str) -> SyncRoot:
        row = self.conn.execute(
            """
            SELECT id, profile_id, device_path, label, lifecycle
            FROM sync_roots
            WHERE profile_id = ? AND label = ?
            """,
            (profile_id, label),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown root label: {label}")
        return self._row_to_root(row)

    def get_root_by_id(self, root_id: int) -> SyncRoot:
        row = self.conn.execute(
            """
            SELECT id, profile_id, device_path, label, lifecycle
            FROM sync_roots
            WHERE id = ?
            """,
            (root_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown root id: {root_id}")
        return self._row_to_root(row)

    def get_root_by_device_path(self, profile_id: int, device_path: str) -> SyncRoot | None:
        row = self.conn.execute(
            """
            SELECT id, profile_id, device_path, label, lifecycle
            FROM sync_roots
            WHERE profile_id = ? AND device_path = ?
            """,
            (profile_id, device_path),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_root(row)

    def apply_root_changes(
        self,
        profile_id: int,
        *,
        additions: list[tuple[str, str, str]] | None = None,
        lifecycle_updates: dict[int, str] | None = None,
    ) -> None:
        additions = additions or []
        lifecycle_updates = lifecycle_updates or {}
        with self.conn:
            for root_id, lifecycle in lifecycle_updates.items():
                cursor = self.conn.execute(
                    """
                    UPDATE sync_roots
                    SET lifecycle = ?
                    WHERE id = ? AND profile_id = ?
                    """,
                    (lifecycle, root_id, profile_id),
                )
                if cursor.rowcount != 1:
                    raise ValueError(f"Unknown root id for profile {profile_id}: {root_id}")
            for device_path, label, lifecycle in additions:
                self.conn.execute(
                    """
                    INSERT INTO sync_roots (profile_id, device_path, label, lifecycle)
                    VALUES (?, ?, ?, ?)
                    """,
                    (profile_id, device_path, label, lifecycle),
                )

    def list_checkpoint_roots(self, profile_id: int, checkpoint_id: int) -> list[CheckpointRoot]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT sr.id, sr.profile_id, sr.device_path, sr.label, sr.lifecycle
            FROM checkpoint_entries ce
            JOIN checkpoints cp ON cp.id = ce.checkpoint_id
            JOIN sync_roots sr ON sr.id = ce.root_id
            WHERE cp.profile_id = ? AND ce.checkpoint_id = ?
            ORDER BY sr.label
            """,
            (profile_id, checkpoint_id),
        ).fetchall()
        return [
            CheckpointRoot(
                id=row["id"],
                profile_id=row["profile_id"],
                device_path=row["device_path"],
                label=row["label"],
                lifecycle=row["lifecycle"],
            )
            for row in rows
        ]

    def list_file_states(self, profile_id: int, root_id: int) -> dict[str, FileState]:
        rows = self.conn.execute(
            """
            SELECT id, profile_id, root_id, relative_path, status,
                   device_present, device_hash, device_size, device_mtime,
                   local_present, local_hash, local_size, local_mtime,
                   conflict_copy_path, updated_at,
                   last_synced_checkpoint_id, last_restored_from_checkpoint_id
            FROM file_states
            WHERE profile_id = ? AND root_id = ?
            """,
            (profile_id, root_id),
        ).fetchall()
        return {row["relative_path"]: self._row_to_file_state(row) for row in rows}

    def list_open_issues(self, profile_id: int) -> list[tuple[SyncRoot, FileState]]:
        rows = self.conn.execute(
            """
            SELECT fs.id, fs.profile_id, fs.root_id, fs.relative_path, fs.status,
                   fs.device_present, fs.device_hash, fs.device_size, fs.device_mtime,
                   fs.local_present, fs.local_hash, fs.local_size, fs.local_mtime,
                   fs.conflict_copy_path, fs.updated_at,
                   fs.last_synced_checkpoint_id, fs.last_restored_from_checkpoint_id,
                   sr.id AS sync_root_id, sr.profile_id AS sync_root_profile_id,
                   sr.device_path, sr.label, sr.lifecycle
            FROM file_states fs
            JOIN sync_roots sr ON sr.id = fs.root_id
            WHERE fs.profile_id = ?
              AND fs.status IN ('conflict', 'diverged_missing_device', 'diverged_missing_local')
            ORDER BY sr.label, fs.relative_path
            """,
            (profile_id,),
        ).fetchall()
        return [
            (
                SyncRoot(
                    id=row["sync_root_id"],
                    profile_id=row["sync_root_profile_id"],
                    device_path=row["device_path"],
                    label=row["label"],
                    lifecycle=row["lifecycle"],
                ),
                self._row_to_file_state(row),
            )
            for row in rows
        ]

    def count_open_issues(self, profile_id: int) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM file_states
            WHERE profile_id = ?
              AND status IN ('conflict', 'diverged_missing_device', 'diverged_missing_local')
            """,
            (profile_id,),
        ).fetchone()
        return int(row["count"])

    def get_issue(self, profile_id: int, issue_id: int) -> tuple[SyncRoot, FileState]:
        row = self.conn.execute(
            """
            SELECT fs.id, fs.profile_id, fs.root_id, fs.relative_path, fs.status,
                   fs.device_present, fs.device_hash, fs.device_size, fs.device_mtime,
                   fs.local_present, fs.local_hash, fs.local_size, fs.local_mtime,
                   fs.conflict_copy_path, fs.updated_at,
                   fs.last_synced_checkpoint_id, fs.last_restored_from_checkpoint_id,
                   sr.id AS sync_root_id, sr.profile_id AS sync_root_profile_id,
                   sr.device_path, sr.label, sr.lifecycle
            FROM file_states fs
            JOIN sync_roots sr ON sr.id = fs.root_id
            WHERE fs.profile_id = ?
              AND fs.id = ?
              AND fs.status IN ('conflict', 'diverged_missing_device', 'diverged_missing_local')
            """,
            (profile_id, issue_id),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown unresolved issue id: {issue_id}")
        return (
            SyncRoot(
                id=row["sync_root_id"],
                profile_id=row["sync_root_profile_id"],
                device_path=row["device_path"],
                label=row["label"],
                lifecycle=row["lifecycle"],
            ),
            self._row_to_file_state(row),
        )

    def upsert_file_state(self, state: FileState) -> None:
        self.conn.execute(
            """
            INSERT INTO file_states (
                profile_id, root_id, relative_path, status,
                device_present, device_hash, device_size, device_mtime,
                local_present, local_hash, local_size, local_mtime,
                conflict_copy_path, updated_at,
                last_synced_checkpoint_id, last_restored_from_checkpoint_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(profile_id, root_id, relative_path) DO UPDATE SET
                status = excluded.status,
                device_present = excluded.device_present,
                device_hash = excluded.device_hash,
                device_size = excluded.device_size,
                device_mtime = excluded.device_mtime,
                local_present = excluded.local_present,
                local_hash = excluded.local_hash,
                local_size = excluded.local_size,
                local_mtime = excluded.local_mtime,
                conflict_copy_path = excluded.conflict_copy_path,
                updated_at = excluded.updated_at,
                last_synced_checkpoint_id = excluded.last_synced_checkpoint_id,
                last_restored_from_checkpoint_id = excluded.last_restored_from_checkpoint_id
            """,
            (
                state.profile_id,
                state.root_id,
                state.relative_path,
                state.status,
                int(state.device_present),
                state.device_hash,
                state.device_size,
                state.device_mtime,
                int(state.local_present),
                state.local_hash,
                state.local_size,
                state.local_mtime,
                state.conflict_copy_path,
                state.updated_at,
                state.last_synced_checkpoint_id,
                state.last_restored_from_checkpoint_id,
            ),
        )

    def save_file_states(self, states: list[FileState]) -> None:
        for state in states:
            self.upsert_file_state(state)
        self.conn.commit()

    def finalize_synced_file_states(self, states: list[FileState], checkpoint_id: int) -> None:
        for state in states:
            state.last_synced_checkpoint_id = checkpoint_id
            state.last_restored_from_checkpoint_id = None
            self.upsert_file_state(state)
        self.conn.commit()

    def finalize_restored_file_states(self, states: list[FileState], checkpoint_id: int) -> None:
        for state in states:
            state.last_restored_from_checkpoint_id = checkpoint_id
            self.upsert_file_state(state)
        self.conn.commit()

    def create_checkpoint(self, profile_id: int, status: str, summary: dict[str, object]) -> int:
        cursor = self.conn.execute(
            """
            INSERT INTO checkpoints (profile_id, created_at, status, summary_json)
            VALUES (?, ?, ?, ?)
            """,
            (profile_id, utc_now(), status, json.dumps(summary, sort_keys=True)),
        )
        return int(cursor.lastrowid)

    def update_checkpoint_summary(self, checkpoint_id: int, summary: dict[str, object]) -> None:
        self.conn.execute(
            "UPDATE checkpoints SET summary_json = ? WHERE id = ?",
            (json.dumps(summary, sort_keys=True), checkpoint_id),
        )
        self.conn.commit()

    def insert_checkpoint_entries(self, checkpoint_id: int, entries: list[dict[str, object]]) -> None:
        self.conn.executemany(
            """
            INSERT INTO checkpoint_entries (checkpoint_id, root_id, relative_path, blob_hash, size, device_mtime)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    checkpoint_id,
                    entry["root_id"],
                    entry["relative_path"],
                    entry["blob_hash"],
                    entry["size"],
                    entry["device_mtime"],
                )
                for entry in entries
            ],
        )

    def list_checkpoints(self, profile_id: int) -> list[Checkpoint]:
        rows = self.conn.execute(
            """
            SELECT id, profile_id, created_at, status, summary_json
            FROM checkpoints
            WHERE profile_id = ?
            ORDER BY id DESC
            """,
            (profile_id,),
        ).fetchall()
        return [self._row_to_checkpoint(row) for row in rows]

    def get_checkpoint(self, profile_id: int, checkpoint_id: int) -> Checkpoint:
        row = self.conn.execute(
            """
            SELECT id, profile_id, created_at, status, summary_json
            FROM checkpoints
            WHERE profile_id = ? AND id = ?
            """,
            (profile_id, checkpoint_id),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown checkpoint: {checkpoint_id}")
        return self._row_to_checkpoint(row)

    def list_checkpoint_entries(
        self,
        checkpoint_id: int,
        root_id: int | None = None,
        relative_path: str | None = None,
    ) -> list[sqlite3.Row]:
        query = """
            SELECT checkpoint_id, root_id, relative_path, blob_hash, size, device_mtime
            FROM checkpoint_entries
            WHERE checkpoint_id = ?
        """
        params: list[object] = [checkpoint_id]
        if root_id is not None:
            query += " AND root_id = ?"
            params.append(root_id)
        if relative_path is not None:
            query += " AND relative_path = ?"
            params.append(relative_path)
        query += " ORDER BY relative_path"
        return self.conn.execute(query, params).fetchall()

    def prune_checkpoints(self, profile_id: int, keep_count: int) -> None:
        rows = self.conn.execute(
            """
            SELECT id
            FROM checkpoints
            WHERE profile_id = ?
            ORDER BY id DESC
            """,
            (profile_id,),
        ).fetchall()
        stale_ids = [row["id"] for row in rows[keep_count:]]
        if stale_ids:
            self.conn.executemany("DELETE FROM checkpoints WHERE id = ?", [(checkpoint_id,) for checkpoint_id in stale_ids])
            self.conn.commit()

    def referenced_blob_hashes(self) -> set[str]:
        rows = self.conn.execute("SELECT DISTINCT blob_hash FROM checkpoint_entries").fetchall()
        return {row["blob_hash"] for row in rows}

    def mark_issue_resolved(self, issue_id: int) -> None:
        self.conn.execute(
            """
            UPDATE file_states
            SET status = 'in_sync', conflict_copy_path = NULL, updated_at = ?
            WHERE id = ?
            """,
            (utc_now(), issue_id),
        )
        self.conn.commit()

    def start_run(
        self,
        profile_id: int,
        operation_type: str,
        *,
        source_profile_id: int | None = None,
        source_checkpoint_id: int | None = None,
        result_checkpoint_id: int | None = None,
        summary: dict[str, object] | None = None,
    ) -> int:
        cursor = self.conn.execute(
            """
            INSERT INTO sync_runs (
                profile_id, operation_type, status, started_at, finished_at,
                source_profile_id, source_checkpoint_id, result_checkpoint_id, summary_json
            )
            VALUES (?, ?, 'running', ?, NULL, ?, ?, ?, ?)
            """,
            (
                profile_id,
                operation_type,
                utc_now(),
                source_profile_id,
                source_checkpoint_id,
                result_checkpoint_id,
                json.dumps(summary or {}, sort_keys=True),
            ),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def append_run_event(
        self,
        run_id: int,
        seq: int,
        stage: str,
        status: str,
        message: str,
        *,
        root_id: int | None = None,
        root_label: str | None = None,
        relative_path: str | None = None,
        action: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO sync_run_events (
                run_id, seq, created_at, stage, root_id, root_label, relative_path, action, status, message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, seq, utc_now(), stage, root_id, root_label, relative_path, action, status, message),
        )
        self.conn.commit()

    def finalize_run(
        self,
        run_id: int,
        status: str,
        *,
        result_checkpoint_id: int | None = None,
        summary: dict[str, object] | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE sync_runs
            SET status = ?, finished_at = ?, result_checkpoint_id = ?, summary_json = ?
            WHERE id = ?
            """,
            (status, utc_now(), result_checkpoint_id, json.dumps(summary or {}, sort_keys=True), run_id),
        )
        self.conn.commit()

    def prune_runs(self, profile_id: int, keep_count: int = 20) -> None:
        rows = self.conn.execute(
            """
            SELECT id
            FROM sync_runs
            WHERE profile_id = ?
            ORDER BY id DESC
            """,
            (profile_id,),
        ).fetchall()
        stale_ids = [row["id"] for row in rows[keep_count:]]
        if stale_ids:
            self.conn.executemany("DELETE FROM sync_runs WHERE id = ?", [(run_id,) for run_id in stale_ids])
            self.conn.commit()

    def list_recent_runs(self, profile_id: int, limit: int = 20) -> list[SyncRun]:
        rows = self.conn.execute(
            """
            SELECT id, profile_id, operation_type, status, started_at, finished_at,
                   source_profile_id, source_checkpoint_id, result_checkpoint_id, summary_json
            FROM sync_runs
            WHERE profile_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (profile_id, limit),
        ).fetchall()
        return [self._row_to_run(row) for row in rows]

    def list_run_events(self, run_id: int) -> list[SyncRunEvent]:
        rows = self.conn.execute(
            """
            SELECT id, run_id, seq, created_at, stage, root_id, root_label, relative_path, action, status, message
            FROM sync_run_events
            WHERE run_id = ?
            ORDER BY seq
            """,
            (run_id,),
        ).fetchall()
        return [self._row_to_run_event(row) for row in rows]

    @staticmethod
    def _row_to_profile(row: sqlite3.Row) -> Profile:
        return Profile(
            id=row["id"],
            name=row["name"],
            device_serial=row["device_serial"],
            mirror_dir=Path(row["mirror_dir"]),
            checkpoint_retention=row["checkpoint_retention"],
            created_at=row["created_at"],
            profile_state=row["profile_state"],
            cloned_from_profile_id=row["cloned_from_profile_id"],
            cloned_from_checkpoint_id=row["cloned_from_checkpoint_id"],
        )

    @staticmethod
    def _row_to_root(row: sqlite3.Row) -> SyncRoot:
        lifecycle = row["lifecycle"] if "lifecycle" in row.keys() else (ROOT_ACTIVE if row["enabled"] else "disabled")
        return SyncRoot(
            id=row["id"],
            profile_id=row["profile_id"],
            device_path=row["device_path"],
            label=row["label"],
            lifecycle=lifecycle,
        )

    @staticmethod
    def _row_to_checkpoint(row: sqlite3.Row) -> Checkpoint:
        return Checkpoint(
            id=row["id"],
            profile_id=row["profile_id"],
            created_at=row["created_at"],
            status=row["status"],
            summary_json=row["summary_json"],
        )

    @staticmethod
    def _row_to_file_state(row: sqlite3.Row) -> FileState:
        keys = set(row.keys())
        return FileState(
            id=row["id"] if "id" in keys else None,
            profile_id=row["profile_id"],
            root_id=row["root_id"],
            relative_path=row["relative_path"],
            status=row["status"],
            device_present=bool(row["device_present"]),
            device_hash=row["device_hash"],
            device_size=row["device_size"],
            device_mtime=row["device_mtime"],
            local_present=bool(row["local_present"]),
            local_hash=row["local_hash"],
            local_size=row["local_size"],
            local_mtime=row["local_mtime"],
            conflict_copy_path=row["conflict_copy_path"],
            updated_at=row["updated_at"],
            last_synced_checkpoint_id=row["last_synced_checkpoint_id"] if "last_synced_checkpoint_id" in keys else row["last_checkpoint_id"],
            last_restored_from_checkpoint_id=row["last_restored_from_checkpoint_id"] if "last_restored_from_checkpoint_id" in keys else None,
        )

    @staticmethod
    def _row_to_run(row: sqlite3.Row) -> SyncRun:
        return SyncRun(
            id=row["id"],
            profile_id=row["profile_id"],
            operation_type=row["operation_type"],
            status=row["status"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            source_profile_id=row["source_profile_id"],
            source_checkpoint_id=row["source_checkpoint_id"],
            result_checkpoint_id=row["result_checkpoint_id"],
            summary_json=row["summary_json"],
        )

    @staticmethod
    def _row_to_run_event(row: sqlite3.Row) -> SyncRunEvent:
        return SyncRunEvent(
            id=row["id"],
            run_id=row["run_id"],
            seq=row["seq"],
            created_at=row["created_at"],
            stage=row["stage"],
            root_id=row["root_id"],
            root_label=row["root_label"],
            relative_path=row["relative_path"],
            action=row["action"],
            status=row["status"],
            message=row["message"],
        )
