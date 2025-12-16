#!/usr/bin/env python3
"""
Database helpers for ReplayModCenter Archive.

Table schema:
- id INTEGER PRIMARY KEY AUTOINCREMENT
- replay_id INTEGER UNIQUE NOT NULL
- sha256 TEXT NULL
- downloaded_at TEXT NULL
"""
import sqlite3
import datetime
from typing import Optional

class Database:
    def __init__(self, path: str = "./replays.db"):
        self.path = path
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS replays (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                replay_id INTEGER UNIQUE NOT NULL,
                sha256 TEXT,  
                downloaded_at TEXT
            );
            """
        )
        self._conn.commit()

    def get_max_replay_id(self) -> Optional[int]:
        cur = self._conn.cursor()
        cur.execute("SELECT MAX(replay_id) as m FROM replays")
        row = cur.fetchone()
        if not row:
            return None
        m = row["m"]
        return None if m is None else int(m)

    def has_replay(self, replay_id: int) -> bool:
        cur = self._conn.cursor()
        cur.execute("SELECT 1 FROM replays WHERE replay_id = ? LIMIT 1", (replay_id,))
        return cur.fetchone() is not None

    def insert_nonexistent(self, replay_id: int) -> None:
        cur = self._conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO replays (replay_id, sha256, downloaded_at) VALUES (?, NULL, NULL)",
            (replay_id,),
        )
        self._conn.commit()

    def upsert_replay(self, replay_id: int, sha256: Optional[str]) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        cur = self._conn.cursor()
        cur.execute(
            "SELECT id FROM replays WHERE replay_id = ?",
            (replay_id,),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE replays SET sha256 = ?, downloaded_at = ? WHERE replay_id = ?",
                (sha256, now, replay_id),
            )
        else:
            cur.execute(
                "INSERT INTO replays (replay_id, sha256, downloaded_at) VALUES (?, ?, ?)",
                (replay_id, sha256, now),
            )
        self._conn.commit()

    def close(self) -> None:
        try:
            self._conn.commit()
        finally:
            self._conn.close()


# if __name__ == "__main__":
#     db = Database(":memory:")
#     db.upsert_replay(1, "deadbeef", "1_deadbeef.mcpr")
#     print(db.get_max_replay_id())
#     db.close()
