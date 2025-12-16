#!/usr/bin/env python3
"""
Downloader module for ReplayModCenter Archive.

Exposes a Downloader class which handles:
- HTTP requests
- saving files
- computing sha256
- returning metadata
"""

import hashlib
from typing import Optional, Dict
from pathlib import Path

import requests

from logger_config import get_logger

logger = get_logger("downloader")


class DownloadResult:
    def __init__(self, replay_id: int, ok: bool, sha256: Optional[str], filename: Optional[str], status_code: int, headers: Dict[str, str], filesize: Optional[int]):
        self.replay_id = replay_id
        self.ok = ok
        self.sha256 = sha256
        self.filename = filename
        self.status_code = status_code
        self.headers = headers
        self.filesize = filesize


class Downloader:
    def __init__(self, base_url: str = "https://www.replaymod.com/api/download_file?id=$id$", output_dir: str = "./output/replays"):
        self.base_url = base_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.debug("Downloader initialized: base_url=%s, output_dir=%s", self.base_url, str(self.output_dir))

    def _get_extension(self, headers: Dict[str, str], replay_id: int) -> str:
        # they return content type application/force-download
        # so we look at Content-disposition and extract extension from there
        cd = headers.get("Content-Disposition", "")
        if "filename=" in cd:
            parts = cd.split("filename=")
            if len(parts) > 1:
                filename_part = parts[1].strip().strip('"').strip("'")
                if "." in filename_part:
                    ext = filename_part.split(".")[-1]
                    return ext
        # fallback to content-type

        # ct = headers.get("Content-Type", "application/octet-stream")
        # if "zip" in ct:
        #     return "mcpr"
        # if "octet-stream" in ct:
        #     return "mcpr"
        return "mcpr"

    def _unique_filename(self, base: Path) -> Path:
        if not base.exists():
            return base
        stem = base.stem
        suffix = base.suffix
        i = 1
        while True:
            candidate = base.with_name(f"{stem}_{i}{suffix}")
            if not candidate.exists():
                return candidate
            i += 1

    def download(self, replay_id: int, timeout: int = 30) -> DownloadResult:

        url = self.base_url.replace("$id$", str(replay_id))
        logger.info("Requesting %s", url)
        try:
            resp = requests.get(url, timeout=timeout, stream=True)
        except Exception as e:
            logger.exception("Request failed for %s: %s", url, e)
            return DownloadResult(replay_id, False, None, None, -1, {}, None)

        # log headers and status
        headers = {k: v for k, v in resp.headers.items()}
        logger.debug("Response status=%s for id=%s", resp.status_code, replay_id)
        logger.debug("Response headers: %s", headers)

        if resp.status_code != 200:
            # save a small body to logs if available, for debugging purposes
            try:
                body = resp.content[:1024]
                logger.debug("Response body (truncated): %s", body)
            except Exception:
                pass
            return DownloadResult(replay_id, False, None, None, resp.status_code, headers, None)

        # compute sha256 while streaming to file
        h = hashlib.sha256()
        # guess extension
        ext = self._get_extension(headers, replay_id)
        temp_name = f"{replay_id}_tmp"
        temp_path = self.output_dir / temp_name

        try:
            with open(temp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    f.write(chunk)
                    h.update(chunk)
        except Exception as e:
            logger.exception("Error writing file for id=%s: %s", replay_id, e)
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            # perhaps we throw here instead?
            return DownloadResult(replay_id, False, None, None, resp.status_code, headers, None)

        hexsum = h.hexdigest()
        first = hexsum[:2]
        second = hexsum[2:4]
        remainder = hexsum[4:]
        target_dir = self.output_dir / first / second
        target_dir.mkdir(parents=True, exist_ok=True)
        fname = f"{remainder}.{ext}"
        final_path = target_dir / fname
        final_path = self._unique_filename(final_path)
        temp_path.rename(final_path)
        try:
            filesize = int(final_path.stat().st_size) # compute filesize in bytes
        except Exception:
            filesize = None
        logger.info("Saved replay %s -> %s (sha256=%s, size=%s)", replay_id, final_path, hexsum, filesize)
        return DownloadResult(replay_id, True, hexsum, str(final_path), resp.status_code, headers, filesize)


# if __name__ == "__main__":
#     dl = Downloader()
#     r = dl.download(0)
#     print(r.ok, r.sha256, r.filename)
