#!/usr/bin/env python3
"""
Configure structured logging for the downloader.
Use stdout for INFO+ logs and a rotating file for DEBUG+ logs.
"""
import logging
from logging.handlers import RotatingFileHandler
import os

LOG_DIR = os.environ.get("RMC_LOG_DIR", "./logs")
LOG_FILE = os.path.join(LOG_DIR, "downloader.log")


def get_logger(name: str = "replaymod") -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)

    # stdout handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(ch)

    # rotating file handler (10 MB)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(fh)

    # avoid duplicate logs
    logger.propagate = False
    return logger

