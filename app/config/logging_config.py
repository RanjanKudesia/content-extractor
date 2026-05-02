"""Logging configuration for the Content Extractor service."""
import logging
import os


def setup_logging() -> None:
    """Configure root logger level and format from the LOG_LEVEL environment variable."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        level = logging.INFO
        level_name = "INFO"

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )
    logging.getLogger(__name__).info(
        "Logging configured",
        extra={"log_level": level_name},
    )
