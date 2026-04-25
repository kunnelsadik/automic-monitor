"""File-based business rule validation."""
import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def validate_files(rules_df: pd.DataFrame) -> list[dict]:
    """Check each rule's path has at least the required number of files."""
    failures = []
    for _, rule in rules_df.iterrows():
        path = rule["path"]
        min_files = rule["min_files"]
        count = len(os.listdir(path)) if os.path.exists(path) else 0
        logger.debug(f"Rule check {path}: required={min_files}, found={count}")
        if count < min_files:
            logger.warning(f"Rule failure {path}: required={min_files}, found={count}")
            failures.append({"path": path, "expected": min_files, "actual": count})
    logger.info(f"File validation complete: {len(failures)} failure(s)")
    return failures
