import os
import logging

logger = logging.getLogger(__name__)

def validate_files(rules_df):
    logger.info("Starting file validation")
    failures = []
    for _, rule in rules_df.iterrows():
        path = rule["path"]
        min_files = rule["min_files"]

        count = len(os.listdir(path)) if os.path.exists(path) else 0
        logger.debug(f"Validated path {path}: expected {min_files}, found {count}")
        if count < min_files:
            logger.warning(f"Validation failure for {path}: expected {min_files}, found {count}")
            failures.append({
                "path": path,
                "expected": min_files,
                "actual": count
            })
    logger.info(f"File validation completed with {len(failures)} failures")
    return failures
