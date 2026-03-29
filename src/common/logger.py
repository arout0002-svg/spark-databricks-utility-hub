"""Logging utilities for PySpark jobs."""

import logging
import sys
from typing import Optional

from pythonjsonlogger import jsonlogger


def get_logger(name: str, level: str = "INFO", request_id: Optional[str] = None) -> logging.Logger:
    """Create a JSON-structured logger suitable for production job runs."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level.upper())
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s %(request_id)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    if request_id:
        logger = logging.LoggerAdapter(logger, {"request_id": request_id})  # type: ignore[assignment]
    return logger  # type: ignore[return-value]
