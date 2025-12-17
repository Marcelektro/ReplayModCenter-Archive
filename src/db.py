#!/usr/bin/env python3
"""
Database helpers for ReplayModCenter Archive.

Table schema:
- id INTEGER PRIMARY KEY AUTOINCREMENT
- replay_id INTEGER UNIQUE NOT NULL
- sha256 TEXT NULL
- filesize INTEGER NULL
- downloaded_at TEXT NULL
"""
import sqlite3
import datetime
from typing import Optional, List, Iterator, Tuple

class Database:
    def __init__(self, path: str = "./replays.db"):
        self.path = path
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS replays (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                replay_id INTEGER UNIQUE NOT NULL,
                sha256 TEXT,  
                filesize INTEGER,  
                downloaded_at TEXT
            );
            """
        )
        self._conn.commit()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS replay_metadata (
                replays_id INTEGER PRIMARY KEY,
                singleplayer TEXT(1),
                serverName TEXT(128),
                generator TEXT(128),
                duration INTEGER,
                date INTEGER,
                mcversion TEXT(16),
                FOREIGN KEY(replays_id) REFERENCES replays(id) ON DELETE CASCADE
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS replays_metadata_players (
                replays_id INTEGER NOT NULL,
                player_uuid TEXT(36) NOT NULL,
                FOREIGN KEY(replays_id) REFERENCES replays(id) ON DELETE CASCADE
                PRIMARY KEY (replays_id, player_uuid)
            );
            """
        )

        self._conn.commit()

        # quick migration
        cur.execute("PRAGMA table_info(replays)")
        cols = [r[1] if isinstance(r, tuple) else r["name"] for r in cur.fetchall()]
        if "filesize" not in cols:
            cur.execute("ALTER TABLE replays ADD COLUMN filesize INTEGER")
            self._conn.commit()
            from logger_config import get_logger
            logger = get_logger("db")
            logger.info("Migrated database: added filesize column to replays table")

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
            "INSERT OR IGNORE INTO replays (replay_id, sha256, filesize, downloaded_at) VALUES (?, NULL, NULL, NULL)",
            (replay_id,),
        )
        self._conn.commit()

    def upsert_replay(self, replay_id: int, sha256: Optional[str], filesize: Optional[int] = None) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        cur = self._conn.cursor()
        cur.execute(
            "SELECT id FROM replays WHERE replay_id = ?",
            (replay_id,),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE replays SET sha256 = ?, filesize = ?, downloaded_at = ? WHERE replay_id = ?",
                (sha256, filesize, now, replay_id),
            )
        else:
            cur.execute(
                "INSERT INTO replays (replay_id, sha256, filesize, downloaded_at) VALUES (?, ?, ?, ?)",
                (replay_id, sha256, filesize, now),
            )
        self._conn.commit()

    def stream_replays(self, start_id: int = 0, end_id: Optional[int] = None) -> Iterator[Tuple[int, Optional[str], Optional[int]]]:
        """
        Stream replay rows (id, sha256, filesize) ordered by id.

        Yields tuples: (id, sha256, filesize). If end_id is None, streams from start_id upwards.
        """
        cur = self._conn.cursor()
        if end_id is None:
            cur.execute("SELECT id, sha256, filesize FROM replays WHERE id >= ? ORDER BY id ASC",
                        (start_id,))
        else:
            cur.execute(
                "SELECT id, sha256, filesize FROM replays WHERE id BETWEEN ? AND ? ORDER BY id ASC",
                (start_id, end_id),
            )
        for row in cur:
            yield int(row["id"]), row["sha256"], row["filesize"]

    def has_replay_metadata(self, replay_id: int) -> bool:
        """
        Return True if metadata exists for the given id.
        """
        cur = self._conn.cursor()
        cur.execute("SELECT 1 FROM replay_metadata WHERE replays_id = ? LIMIT 1", (replay_id,))
        return cur.fetchone() is not None

    def upsert_replay_metadata(
        self,
        replay_id: int,
        singleplayer: Optional[bool] = None,
        server_name: Optional[str] = None,
        generator: Optional[str] = None,
        duration: Optional[int] = None,
        date: Optional[int] = None,
        mc_version: Optional[str] = None,
    ) -> None:
        """
        Insert or update metadata for a given replay_id.

        replay_id refers to the `id` column in `replays` (not the replay center id).
        singleplayer is stored as '1' for True, '0' for False, or NULL if None.
        """
        sp_val = None
        if singleplayer:
            sp_val = '1'
        elif singleplayer is False:
            sp_val = '0'

        cur = self._conn.cursor()
        cur.execute("SELECT replays_id FROM replay_metadata WHERE replays_id = ?", (replay_id,))
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE replay_metadata SET singleplayer = ?, serverName = ?, generator = ?, duration = ?, date = ?, mcversion = ? WHERE replays_id = ?",
                (sp_val, server_name, generator, duration, date, mc_version, replay_id),
            )
        else:
            cur.execute(
                "INSERT INTO replay_metadata (replays_id, singleplayer, serverName, generator, duration, date, mcversion) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (replay_id, sp_val, server_name, generator, duration, date, mc_version),
            )
        self._conn.commit()

    def replace_replay_players(self, replay_id: int, players: List[str]) -> None:
        """
        Replace the players list for a replay. Removes existing rows and inserts provided UUIDs.

        player UUIDs should be full 36-character strings (with hyphens).
        """
        cur = self._conn.cursor()
        cur.execute("DELETE FROM replays_metadata_players WHERE replays_id = ?", (replay_id,))
        if players:
            cur.executemany(
                "INSERT INTO replays_metadata_players (replays_id, player_uuid) VALUES (?, ?)",
                [(replay_id, p) for p in players],
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
