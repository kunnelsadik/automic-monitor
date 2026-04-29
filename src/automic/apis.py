"""Automic log normalization utilities."""
import html
import logging
import re

logger = logging.getLogger(__name__)


def normalize_automic_log(log_text: str) -> str:
    """Normalize raw Automic API log output to clean plain text."""
    if not log_text:
        return ""

    log_text = html.unescape(log_text)
    log_text = log_text.replace("\r\n", "\n").replace("\r", "\n")
    log_text = log_text.lstrip("﻿")
    log_text = "\n".join(line.rstrip() for line in log_text.splitlines())
    log_text = re.sub(r"[^\x09\x0A\x20-\x7E]", "", log_text)

    # Ensure copy/move commands start on a fresh line when the API splits them
    log_text = re.sub(
        r'(?<!\n)([A-Za-z]:\\?>\\s*(copy|move)\\s+")',
        r"\n\1",
        log_text,
        flags=re.IGNORECASE,
    )

    return log_text
