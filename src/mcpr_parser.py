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
import shutil
import subprocess
import zipfile
import json
from typing import Optional, Dict
from pathlib import Path

from logger_config import get_logger

logger = get_logger("mcpr_parser")

# we seem to have crashed on several replays due to zipfile.BadZipFile
# this is the easiest fallback without reimplementing zip parsing ourselves probably (idk im not a python pro) xD
# https://stackoverflow.com/questions/3083235/unzipping-file-results-in-badzipfile-file-is-not-a-zip-file
def _extract_with_system_tool(file_path: Path, member: str) -> Optional[str]:

    if shutil.which("7z"):
        cmd = ["7z", "x", "-so", str(file_path), member]

        try:
            proc = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if proc.returncode != 0:
                logger.error("7z failed: %s (rc=%s) stderr=%s", " ".join(cmd), proc.returncode, proc.stderr.decode(errors="replace"))
                return None
            return proc.stdout.decode("utf-8")
        except Exception as e:
            logger.exception("Exception running external tool for %s: %s", str(file_path), str(e))
            logger.info("Attempting unzip fallback...")

    if shutil.which("unzip"):
        cmd = ["unzip", "-p", str(file_path), member]

        try:
            proc = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # unzip can crash with 2 and it still works, just read stdout only
            if cmd[0] == "unzip" and proc.returncode == 2:
                logger.debug("Unzip return code 2. Trying anyway...")
                return proc.stdout.decode("utf-8")

            if proc.returncode != 0:
                logger.error("Unzip failed: %s (rc=%s) stderr=%s", " ".join(cmd), proc.returncode, proc.stderr.decode(errors="replace"))
                return None
            return proc.stdout.decode("utf-8")
        except Exception as e:
            logger.exception("Exception running external tool for %s: %s", str(file_path), str(e))
            logger.info("No more fallback options available...")
        return None

    else:
        logger.error("No external unzip/7z tool found for fallback extraction")
        return None


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
        logger.warning("zipfile.BadZipFile for %s, attempting external extractor", str(file_path))
        content = _extract_with_system_tool(file_path, "metaData.json")
        if content is None:
            logger.error("Fallback extraction failed for %s", str(file_path))
            return None
        try:
            data = json.loads(content)
            logger.debug("Parsed mcpr file %s successfully (external tool)", str(file_path))
            return data
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON from fallback extraction for %s", str(file_path))
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
