"""Log parsing utilities for Automic job logs."""
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# Regex patterns for log parsing
RET_RE = re.compile(r"\bRET=(\d+)\b")
EXTERNAL_LOG_RE = re.compile(
    r'\bLog\s+"(?P<path>[^"]+\.log)"', re.IGNORECASE
)
DOWNLOAD_RE = re.compile(
    r'Downloading to local file\s+"(?P<local>[^"]+)"', re.IGNORECASE
)
TRANSFER_OK_RE = re.compile(r"transfer succeeded", re.IGNORECASE)
FAILURE_RE = re.compile(r"Failure in command", re.IGNORECASE)

COPY_MOVE_CMD_RE = re.compile(
    r"^(copy|move)\s+\"(?P<src>.+?)\\\*\.(?P<ext>[^\"]+)\"\s+\"(?P<dst>[^\"]+)\"",
    re.IGNORECASE | re.MULTILINE,
)

COPY_MOVE_COUNT_RE = re.compile(
    r"(?P<count>\d+)\s+file\(s\)\s+(copied|moved)", re.IGNORECASE
)

ERROR_PATTERNS = [
    ("DUPLICATE", re.compile(r"duplicate file name exists", re.IGNORECASE)),
    ("NOT_FOUND", re.compile(r"cannot be found|file not found", re.IGNORECASE)),
    ("ACCESS_DENIED", re.compile(r"access is denied|permission denied", re.IGNORECASE)),
    ("PATH_NOT_FOUND", re.compile(r"path not found|system cannot find the path", re.IGNORECASE)),
]

ERROR_SEVERITY_MAP = {
    "DUPLICATE": "WARNING",
    "NOT_FOUND": "WARNING",
    "PATH_NOT_FOUND": "ERROR",
    "ACCESS_DENIED": "CRITICAL",
    "UNKNOWN": "ERROR",
}

ERROR_RETRYABLE_MAP = {
    "DUPLICATE": False,
    "NOT_FOUND": True,
    "PATH_NOT_FOUND": False,
    "ACCESS_DENIED": False,
    "UNKNOWN": False,
}


def read_log_from_shared_drive(log_path: str) -> Optional[str]:
    """Read log file from shared drive.
    
    Args:
        log_path: Path to log file on shared drive
        
    Returns:
        Log content as string or None if file not found
    """
    try:
        path = Path(log_path)
        if path.exists():
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                return f.read()
        else:
            logger.warning(f"Log file not found: {log_path}")
            return None
    except Exception as e:
        logger.error(f"Failed to read log file {log_path}: {e}")
        return None


def parse_job_log(
    log_text: str,
    external_log_loader: Optional[Callable[[str], Optional[str]]] = None,
) -> Dict:
    """Parse Automic job log and extract key information.
    
    Args:
        log_text: Job log text
        external_log_loader: Optional function to load external logs
        
    Returns:
        Dict with parsed log information:
            - return_code: Job return code
            - files_transferred: List of transferred files
            - errors: List of detected errors
            - external_logs: List of external log paths found
            - transfer_details: File transfer statistics
            - raw_log: Original log text
    """
    result = {
        "return_code": None,
        "files_transferred": [],
        "errors": [],
        "external_logs": [],
        "transfer_details": {},
        "raw_log": log_text,
    }

    if not log_text:
        return result

    # Extract return code
    ret_match = RET_RE.search(log_text)
    if ret_match:
        result["return_code"] = int(ret_match.group(1))

    # Extract external log paths
    for match in EXTERNAL_LOG_RE.finditer(log_text):
        log_path = match.group("path")
        result["external_logs"].append(log_path)

        # Load external log if loader provided
        if external_log_loader:
            external_content = external_log_loader(log_path)
            if external_content:
                logger.debug(f"Loaded external log from: {log_path}")
                log_text += "\n" + external_content

    # Extract download paths
    for match in DOWNLOAD_RE.finditer(log_text):
        result["files_transferred"].append(match.group("local"))

    # Parse copy/move commands
    cmd_match = COPY_MOVE_CMD_RE.search(log_text)
    if cmd_match:
        result["transfer_details"]["command"] = cmd_match.group(1).lower()
        result["transfer_details"]["source"] = cmd_match.group("src")
        result["transfer_details"]["extension"] = cmd_match.group("ext")
        result["transfer_details"]["destination"] = cmd_match.group("dst")

        # Get file count
        count_match = COPY_MOVE_COUNT_RE.search(log_text)
        if count_match:
            result["transfer_details"]["count"] = int(count_match.group("count"))

    # Check for transfer success/failure
    result["transfer_details"]["transfer_ok"] = bool(TRANSFER_OK_RE.search(log_text))
    result["transfer_details"]["has_failure"] = bool(FAILURE_RE.search(log_text))

    # Detect errors
    errors_by_type = defaultdict(list)
    for line in log_text.splitlines():
        for error_type, pattern in ERROR_PATTERNS:
            if pattern.search(line):
                errors_by_type[error_type].append(line.strip())
                break

    # Format errors with severity and retryable info
    for error_type, lines in errors_by_type.items():
        for line in lines:
            result["errors"].append(
                {
                    "type": error_type,
                    "severity": ERROR_SEVERITY_MAP.get(error_type, "ERROR"),
                    "retryable": ERROR_RETRYABLE_MAP.get(error_type, False),
                    "line": line,
                }
            )

    return result


def extract_file_counts(log_text: str) -> Dict[str, int]:
    """Extract file operation counts from log.
    
    Args:
        log_text: Job log text
        
    Returns:
        Dict with file counts:
            - copied: Number of files copied
            - moved: Number of files moved
            - failed: Number of failed operations
    """
    counts = {"copied": 0, "moved": 0, "failed": 0}

    # Count copied files
    copy_matches = re.findall(
        r"(\d+)\s+file\(s\)\s+copied", log_text, re.IGNORECASE
    )
    if copy_matches:
        counts["copied"] = sum(int(m) for m in copy_matches)

    # Count moved files
    move_matches = re.findall(
        r"(\d+)\s+file\(s\)\s+moved", log_text, re.IGNORECASE
    )
    if move_matches:
        counts["moved"] = sum(int(m) for m in move_matches)

    # Count failures
    failure_matches = re.findall(r"(\d+)\s+file\(s\)\s+failed", log_text, re.IGNORECASE)
    if failure_matches:
        counts["failed"] = sum(int(m) for m in failure_matches)

    return counts


def get_error_summary(errors: List[Dict]) -> Dict:
    """Summarize errors from parsed log.
    
    Args:
        errors: List of error dicts from parse_job_log()
        
    Returns:
        Summary dict:
            - total_errors: Total number of errors
            - by_type: Count by error type
            - critical_count: Number of critical errors
            - retryable_count: Number of retryable errors
    """
    summary = {
        "total_errors": len(errors),
        "by_type": defaultdict(int),
        "critical_count": 0,
        "retryable_count": 0,
    }

    for error in errors:
        summary["by_type"][error["type"]] += 1
        if error["severity"] == "CRITICAL":
            summary["critical_count"] += 1
        if error["retryable"]:
            summary["retryable_count"] += 1

    return dict(summary)
