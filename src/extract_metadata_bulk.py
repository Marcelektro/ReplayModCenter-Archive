#!/usr/bin/env python3
"""
Migrations script for extracting and storing metadata of locally stored replays into the database.

Requires the existing database and replay files to be present.

Arguments:
--db: Path to the sqlite DB file (default: ./replays.db)
--input: Path to the output directory of the downloader script (where replays are stored) (default: ./output/replays)
--start-id: Replay ID to start processing from (inclusive, default: 0)
--end-id: Replay ID to end processing at (inclusive, default: 20000)

The script will iterate over the specified range of replay IDs (replaymod center IDs).
Then for each replay ID, it will check if the replay file exists in the specified input directory.
If the file exists, it will parse the .mcpr file to extract its metadata.
The extracted metadata will then be stored in the database in the replay_metadata and replays_metadata_players for easy querying later.
"""
import argparse
import os
from pathlib import Path
from typing import Optional
from tqdm import tqdm
from db import Database
from mcpr_parser import parse_mcpr

from logger_config import get_logger

logger = get_logger("extract_metadata_bulk")


def find_file_by_sha(input_dir: Path, sha256: str) -> Optional[Path]:
    """
    Construct the expected path from sha256 and return Path if exists.
    """
    if not sha256 or len(sha256) != 64:
        logger.debug("Invalid sha256 provided: %s", sha256)
        return None
    first = sha256[0:2]
    second = sha256[2:4]
    remainder = sha256[4:]
    exts = ["mcpr", "zip", "bin"] # extensions during development
    for ext in exts:
        p = input_dir / first / second / f"{remainder}.{ext}"
        if p.exists():
            return p
    return None


def process_range(db_path: str, input_dir: str, start_id: int, end_id: Optional[int], skip_existing: bool = True) -> None:
    db = Database(db_path)
    inpt = Path(input_dir)

    # if database has entries, stream them to get sha256 and replay ids
    # otherwise iterate numeric range
    streamed = list(db.stream_replays(start_id=start_id, end_id=end_id))
    if streamed:
        iterator = streamed
    else:
        # build simple numeric iterator
        if end_id is None:
            raise ValueError("Database empty and end_id is required when no streamed rows available")
        iterator = [(i, None, None) for i in range(start_id, end_id + 1)]

    total = len(iterator)
    logger.info("Processing %s replay entries (start=%s end=%s)", total, start_id, end_id)

    for replay_id, sha256, filesize in tqdm(iterator, total=total):

        # skip if metadata already present
        if skip_existing and db.has_replay_metadata(replay_id):
            logger.debug("Skipping %s: metadata already present", replay_id)
            continue

        if sha256:
            file_path = find_file_by_sha(inpt, sha256)
        else:
            logger.debug("No sha256 for replay %s, cannot locate file", replay_id)
            continue

        if not file_path:
            logger.debug("No file found for replay %s (sha256=%s)", replay_id, sha256)
            continue

        metadata = parse_mcpr(file_path)
        if metadata is None:
            logger.info("No metadata parsed for replay %s from file %s", replay_id, file_path)
            continue

        # map fields and write to db
        singleplayer = metadata.get("singleplayer") if isinstance(metadata.get("singleplayer"), bool) else None
        server_name = metadata.get("serverName") or metadata.get("servername") or None
        generator = metadata.get("generator")
        duration = metadata.get("duration")
        date = metadata.get("date")
        mcversion = metadata.get("mcversion") or metadata.get("mcVersion") or None
        players = metadata.get("players") if isinstance(metadata.get("players"), list) else []

        try:
            db.upsert_replay_metadata(
                replay_id=replay_id,
                singleplayer=singleplayer,
                server_name=server_name,
                generator=generator,
                duration=duration,
                date=date,
                mc_version=mcversion,
            )
            if players:
                players_str = [str(p) for p in players]
                db.replace_replay_players(replay_id, players_str)
            logger.info("Stored metadata for replay %s", replay_id)
        except Exception as e:
            logger.exception("Failed to store metadata for replay %s: %s", replay_id, e)

    db.close()


def main():
    parser = argparse.ArgumentParser(description="Extract metadata from local .mcpr files and store in database")
    parser.add_argument("--db", type=str, default="./replays.db", help="Path to sqlite DB file")
    parser.add_argument("--input", type=str, default="./output/replays", help="Path to output directory containing replays")
    parser.add_argument("--start-id", type=int, default=0, help="Start replay id (inclusive)")
    parser.add_argument("--end-id", type=int, default=None, help="End replay id (inclusive). If omitted, will stream from start_id upwards if DB has entries")
    parser.add_argument("--no-skip", dest="skip", action="store_false", help="Do not skip replays that already have metadata")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        logger.error("Database file does not exist: %s", args.db)
        return

    if not os.path.isdir(args.input):
        logger.error("Input directory does not exist or is not a directory: %s", args.input)
        return

    process_range(args.db, args.input, args.start_id, args.end_id, skip_existing=args.skip)


if __name__ == "__main__":
    main()
