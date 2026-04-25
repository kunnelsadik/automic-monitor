"""Automic API utility functions for log processing and data manipulation."""
import html
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


def normalize_automic_log(log_text: str) -> str:
    """Normalize Automic job log output.
    
    Handles:
    - HTML entity decoding (&gt; &lt; &amp; etc.)
    - Line ending normalization
    - BOM removal
    - Control character removal
    - Command prompt fixing
    
    Args:
        log_text: Raw log text from Automic API
        
    Returns:
        Normalized log text
        
    Example:
        normalized = normalize_automic_log(raw_log)
    """
    if not log_text:
        return ""

    # 1. Decode HTML entities (&gt; &lt; &amp; etc.)
    log_text = html.unescape(log_text)

    # 2. Normalize line endings (CRLF → LF)
    log_text = log_text.replace("\r\n", "\n").replace("\r", "\n")

    # 3. Remove BOM (Byte Order Mark) if present
    log_text = log_text.lstrip("\ufeff")

    # 4. Remove trailing whitespace on each line
    log_text = "\n".join(line.rstrip() for line in log_text.splitlines())

    # 5. Remove non-printable control characters (except newline & tab)
    log_text = re.sub(r"[^\x09\x0A\x20-\x7E]", "", log_text)

    # 6. Fix broken command prompts (API sometimes splits lines)
    # Ensures "c:>copy" is on a fresh line
    log_text = re.sub(
        r'(?<!\n)([A-Za-z]:\\?>\s*(copy|move)\s+")',
        r"\n\1",
        log_text,
        flags=re.IGNORECASE,
    )

    return log_text


def extract_return_code(log_text: str) -> Optional[int]:
    """Extract return code from Automic log.
    
    Looks for pattern like "RET=0", "RET=123", etc.
    
    Args:
        log_text: Log text to search
        
    Returns:
        Return code as int or None if not found
    """
    match = re.search(r"\bRET=(\d+)\b", log_text)
    if match:
        return int(match.group(1))
    return None


def extract_external_log_path(log_text: str) -> Optional[str]:
    """Extract external log file path from Automic log.
    
    Looks for pattern like: Log "C:\path\to\log.log"
    
    Args:
        log_text: Log text to search
        
    Returns:
        Log file path or None if not found
    """
    match = re.search(r'\bLog\s+"(?P<path>[^"]+\.log)"', log_text, re.IGNORECASE)
    if match:
        return match.group("path")
    return None


def detect_file_transfer(log_text: str) -> dict:
    """Detect and extract file transfer information from logs.
    
    Identifies copy/move commands and file counts.
    
    Args:
        log_text: Log text to analyze
        
    Returns:
        Dict with transfer details:
            - command: "copy" or "move"
            - source: Source directory
            - destination: Destination directory
            - extension: File extension
            - count: Number of files transferred
            - transfer_ok: Whether transfer succeeded
            - has_failure: Whether failure was detected
    """
    result = {
        "command": None,
        "source": None,
        "destination": None,
        "extension": None,
        "count": None,
        "transfer_ok": False,
        "has_failure": False,
    }

    # Pattern for copy/move commands
    cmd_pattern = r'^(copy|move)\s+"(?P<src>.+?)\\\*\.(?P<ext>[^"]+)"\s+"(?P<dst>[^"]+)"'
    match = re.search(cmd_pattern, log_text, re.MULTILINE | re.IGNORECASE)

    if match:
        result["command"] = match.group(1).lower()
        result["source"] = match.group("src")
        result["extension"] = match.group("ext")
        result["destination"] = match.group("dst")

    # File count pattern
    count_pattern = r"(?P<count>\d+)\s+file\(s\)\s+(copied|moved)"
    match = re.search(count_pattern, log_text, re.IGNORECASE)
    if match:
        result["count"] = int(match.group("count"))

    # Transfer success/failure
    result["transfer_ok"] = bool(re.search(r"transfer succeeded", log_text, re.IGNORECASE))
    result["has_failure"] = bool(re.search(r"Failure in command", log_text, re.IGNORECASE))

    return result


def detect_errors(log_text: str) -> list:
    """Detect error patterns in log text.
    
    Identifies common error types:
    - DUPLICATE: Duplicate file name
    - NOT_FOUND: File/path not found
    - ACCESS_DENIED: Permission denied
    - PATH_NOT_FOUND: Directory not found
    
    Args:
        log_text: Log text to analyze
        
    Returns:
        List of dicts with detected errors:
            - type: Error type
            - severity: CRITICAL, ERROR, or WARNING
            - retryable: Whether the operation can be retried
            - line: Matching log line
    """
    error_patterns = [
        {
            "type": "DUPLICATE",
            "pattern": re.compile(r"duplicate file name exists", re.IGNORECASE),
            "severity": "WARNING",
            "retryable": False,
        },
        {
            "type": "NOT_FOUND",
            "pattern": re.compile(
                r"cannot be found|file not found", re.IGNORECASE
            ),
            "severity": "WARNING",
            "retryable": True,
        },
        {
            "type": "ACCESS_DENIED",
            "pattern": re.compile(
                r"access is denied|permission denied", re.IGNORECASE
            ),
            "severity": "CRITICAL",
            "retryable": False,
        },
        {
            "type": "PATH_NOT_FOUND",
            "pattern": re.compile(
                r"path not found|system cannot find the path", re.IGNORECASE
            ),
            "severity": "ERROR",
            "retryable": False,
        },
    ]

    errors = []
    for line in log_text.splitlines():
        for error_def in error_patterns:
            if error_def["pattern"].search(line):
                errors.append(
                    {
                        "type": error_def["type"],
                        "severity": error_def["severity"],
                        "retryable": error_def["retryable"],
                        "line": line.strip(),
                    }
                )
                break  # Only match first error pattern per line

    return errors


def write_to_file(file_path: str, content: str, mode: str = "w") -> None:
    """Write content to a file.
    
    Args:
        file_path: Path to file to write
        content: Content to write
        mode: File open mode ("w" for write, "a" for append)
    """
    try:
        with open(file_path, mode) as f:
            f.write(content)
        logger.info(f"Data written successfully to {file_path}")
    except Exception as e:
        logger.error(f"Failed to write to {file_path}: {e}")
        raise
