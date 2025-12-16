#!/usr/bin/env python3
"""
Bruteforce script to download replays sequentially.
"""
import signal
import argparse
from tqdm import tqdm

from db import Database
from downloader import Downloader
from logger_config import get_logger

logger = get_logger("bruteforce")


def main():
    parser = argparse.ArgumentParser(description="Bruteforce-download ReplayMod Center replays")
    parser.add_argument("--start", type=int, default=0, help="Start replay id (inclusive)")
    parser.add_argument("--max-id", type=int, default=20000, help="Maximum replay id to attempt (inclusive)")
    parser.add_argument("--db", type=str, default="./replays.db", help="Path to sqlite DB file")
    parser.add_argument("--output", type=str, default="./output/replays", help="Output directory for replays")
    parser.add_argument("--base-url", type=str, default="https://www.replaymod.com/api/download_file?id=$id$", help="Url for replay downloads, with $id$ placeholder")
    args = parser.parse_args()

    db = Database(args.db)
    dl = Downloader(base_url=args.base_url, output_dir=args.output)

    # determine starting point: if DB has a max id greater than start, continue after that
    db_max = db.get_max_replay_id()
    if db_max is not None and db_max >= args.start:
        start = db_max + 1
    else:
        start = args.start

    # prepare graceful shutdown
    stop_flag = {"stop": False}

    def _handle_sigint(signum, frame):
        logger.info("Received interrupt, finishing current download and exiting...")
        stop_flag["stop"] = True

    signal.signal(signal.SIGINT, _handle_sigint)

    total = args.max_id - start + 1
    if total <= 0:
        logger.info("Nothing to do: start=%s max_id=%s", start, args.max_id)
        return

    logger.info("Starting downloads from %s to %s -> total=%s", start, args.max_id, total)

    pbar = tqdm(total=total, unit="file", desc="Downloading")
    try:
        for rid in range(start, args.max_id + 1):
            if stop_flag["stop"]:
                break

            res = dl.download(rid)

            if res.ok:
                db.upsert_replay(rid, res.sha256)
            elif res.status_code == 400: # weirdly enough they return 400 for non-existing replays
                # store a record marking confirmed non-existence at download time (NULL hash)
                db.insert_nonexistent(rid)
                logger.warning("Replay id %s does not exist (400)", rid)
            else:
                logger.warning("Download failed for replay id %s with status code %s", rid, res.status_code)

            pbar.update(1)
    finally:
        pbar.close()
        db.close()
        logger.info("Exiting")


if __name__ == "__main__":
    main()
