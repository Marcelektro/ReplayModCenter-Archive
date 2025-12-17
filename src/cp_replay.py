#!/usr/bin/env python3
"""
A script to copy a replay file from one location to another.

Useful for e.g. quickly retrieving a replay file by its hash or replay id.

E.g.:
./src/cp_replay.py --output "/home/marcelektro/.local/share/multimc/instances/1.8.9-replayTesting/.minecraft/replay_recordings" --replay-id 2717
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Optional

from db import Database

from logger_config import get_logger
logger = get_logger("cp_replay")

def copy_replay_file(source: Path, destination: Path) -> None:
    """
    Copy the replay file from source to destination.
    """
    try:
        shutil.copy2(source, destination)
        logger.info("Successfully copied replay file from %s to %s", str(source), str(destination))
    except Exception as e:
        logger.error("Failed to copy replay file from %s to %s: %s", str(source), str(destination), str(e))

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

def get_file_by_replay_id(db: Database, replay_id: int, replays_dir: Path) -> Path | None:
    """
    Get the file path of the replay by its replay ID.
    """
    _, sha256, _ = db.get_by_replay_id(replay_id)
    if sha256 is None:
        logger.error("No replay found with ID %d", replay_id)
        return None

    return find_file_by_sha(replays_dir, sha256)

def main():
    parser = argparse.ArgumentParser(description="Copy a replay file by its replay ID or SHA256 hash")
    parser.add_argument("--db", type=str, default="./replays.db", help="Path to sqlite DB file")
    parser.add_argument("--input", type=str, default="./output/replays", help="Path to output directory containing replays")
    parser.add_argument("--replay-id", type=int, help="Replay ID to copy")
    parser.add_argument("--sha256", type=str, help="SHA256 hash of the replay to copy")
    parser.add_argument("--output-file", type=str, help="Output path for the copied replay file")
    parser.add_argument("--output", type=str, help="Output directory for the copied replay file")
    args = parser.parse_args()

    db = Database(args.db)
    replays_dir = Path(args.input)

    if args.replay_id is not None:
        source_file = get_file_by_replay_id(db, args.replay_id, replays_dir)
        if source_file is None:
            logger.error("Could not find replay file for ID %d", args.replay_id)
            return
    elif args.sha256 is not None:
        source_file = find_file_by_sha(replays_dir, args.sha256)
        if source_file is None:
            logger.error("Could not find replay file for SHA256 %s", args.sha256)
            return
    else:
        logger.error("Either --replay-id or --sha256 must be provided")
        return


    if args.output_file:
        output_path = Path(args.output_file)
    elif args.output:
        output_path = Path(args.output) / source_file.name
    else:
        logger.error("Either --output-file or --output must be provided")
        return

    if output_path.exists():
        logger.error("Output file %s already exists, aborting to avoid overwrite", str(output_path))
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    copy_replay_file(source_file, output_path)

if __name__ == "__main__":
    main()