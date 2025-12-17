#!/usr/bin/env python3
"""
Parser module for ReplayMod files (.mcpr).

Exposes a method to extract metadata as json from a .mcpr file.


Metadata json schema (probably incomplete):
- singleplayer: bool — true if singleplayer, false if multiplayer
- serverName: str — server name if multiplayer, world name if singleplayer
- generator: str — program that generated the replay, usually "ReplayMod vX.Y.Z-commitHash"
- duration: int — duration of the replay in milliseconds
- date: int — unix timestamp (milliseconds since epoch) when the replay was recorded
- players: str[] — array of player UUIDs that appeared in the replay
- mcversion: str — Minecraft version the replay was recorded in, usually "1.8"
"""
import zipfile
import json
from typing import Optional, Dict
from pathlib import Path

from logger_config import get_logger

logger = get_logger("mcpr_parser")


def parse_mcpr(file_path: Path) -> Optional[Dict]:
    """
    Parse a .mcpr file and extract metadata as a dictionary.
    Returns None if parsing fails.
    """
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            if 'metaData.json' in z.namelist():
                with z.open('metaData.json') as f:
                    data = json.load(f)
                    logger.debug("Parsed mcpr file %s successfully", str(file_path))
                    return data
            else:
                logger.warning("No metaData.json found in mcpr file %s", str(file_path))
                return None
    except zipfile.BadZipFile:
        logger.error("Bad zip file: %s", str(file_path))
        return None
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON in mcpr file: %s", str(file_path))
        return None
    except Exception as e:
        logger.error("Unexpected error parsing mcpr file %s: %s", str(file_path), str(e))
        return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse a .mcpr file and extract metadata.")
    parser.add_argument("file", type=str, help="Path to the .mcpr file to parse.")
    parser.add_argument("--full", action="store_true", help="Display full players array without truncation.")
    args = parser.parse_args()

    mcpr_path = Path(args.file)
    full = args.full

    if not mcpr_path.is_file():
        logger.error("File does not exist: %s", str(mcpr_path))
    else:
        metadata = parse_mcpr(mcpr_path)
        if metadata is not None:
            # truncate players array length for display
            if not full and "players" in metadata and isinstance(metadata["players"], list):
                # show only first 5 players and add ... if longer
                if len(metadata["players"]) > 5:
                    players_size = len(metadata["players"])
                    metadata["players"] = metadata["players"][:5]
                    metadata["players"].append(f"... and {players_size - 5} more")
            print(json.dumps(metadata, indent=4))
        else:
            logger.info("No metadata extracted from file: %s", str(mcpr_path))
