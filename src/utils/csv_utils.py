"""CSV file utilities for reading and writing configuration and result data."""
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_csv_locks: dict[str, threading.Lock] = {}
_locks_meta = threading.Lock()


def _get_file_lock(file_path: str) -> threading.Lock:
    with _locks_meta:
        if file_path not in _csv_locks:
            _csv_locks[file_path] = threading.Lock()
        return _csv_locks[file_path]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_csv(
    file_path: str,
    columns: Optional[list[str]] = None,
    dtypes: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    try:
        path = Path(file_path)
        if not path.exists():
            logger.warning(f"CSV not found: {file_path}, returning empty DataFrame")
            return pd.DataFrame(columns=columns) if columns else pd.DataFrame()
        df = pd.read_csv(file_path, usecols=columns, dtype=dtypes)
        logger.info(f"Read {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read {file_path}: {e}")
        return pd.DataFrame(columns=columns) if columns else pd.DataFrame()


def write_csv(
    file_path: str,
    data: pd.DataFrame,
    index: bool = False,
    mode: str = "w",
) -> None:
    try:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if mode == "a" and path.exists():
            data.to_csv(file_path, mode="a", header=False, index=index)
        else:
            data.to_csv(file_path, mode="w", index=index)
        logger.info(f"Wrote {len(data)} rows to {file_path}")
    except Exception as e:
        logger.error(f"Failed to write {file_path}: {e}")
        raise


def append_csv(file_path: str, row_data: dict[str, Any]) -> None:
    try:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df_new = pd.DataFrame([row_data])
        if path.exists() and path.stat().st_size > 0:
            existing = pd.read_csv(file_path, skipinitialspace=True)
            existing.columns = existing.columns.str.strip()
            # Drop pandas duplicate-artifact columns (e.g. "col.1", "col.2")
            expected = set(df_new.columns)
            existing = existing[[c for c in existing.columns if c in expected]]
            df = pd.concat([existing, df_new], ignore_index=True)
        else:
            df = df_new
        df.to_csv(file_path, index=False)
        logger.debug(f"Appended row to {file_path}")
    except Exception as e:
        logger.error(f"Failed to append to {file_path}: {e}")
        raise


def load_config_workflows(file_path: str = "data/config_workflows.csv") -> pd.DataFrame:
    return read_csv(
        file_path, columns=["workflow_name", "object_type", "is_active", "last_polled_at"]
    )


def load_processed_runs(file_path: str = "data/processed_runs.csv") -> pd.DataFrame:
    return read_csv(file_path, columns=["run_id", "workflow_name", "processed_timestamp"])


def load_workflow_results(file_path: str = "data/workflow_results.csv") -> pd.DataFrame:
    return read_csv(
        file_path, columns=["run_id", "workflow_name", "status", "start_time", "end_time"]
    )


def _match_key(series: pd.Series, value: Any) -> pd.Series:
    """Compare a series to value tolerating int/float differences (e.g. 86116768 vs 86116768.0)."""
    try:
        numeric_val = float(value)
        numeric_series = pd.to_numeric(series, errors="coerce")
        return numeric_series == numeric_val
    except (ValueError, TypeError):
        return series.astype(str) == str(value)


def upsert_csv(file_path: str, row_data: dict[str, Any], key_col: str) -> None:
    """Insert row if key not found; update only if status has changed.
    
    - If record exists and status is unchanged: skip write (no CSV change)
    - If record exists and status changed: update the record and write
    - If record doesn't exist: insert new record and write
    - Automatically deduplicates when writing.
    """
    lock = _get_file_lock(file_path)
    with lock:
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            should_write = False  # Track if we need to write changes
            
            if path.exists() and path.stat().st_size > 0:
                df = pd.read_csv(file_path)
                
                # Check for and remove duplicates
                has_dupes = key_col in df.columns and df.duplicated(subset=[key_col]).any()
                if has_dupes:
                    df = df.drop_duplicates(subset=[key_col], keep="last").reset_index(drop=True)
                    logger.debug(f"Removed duplicate rows in {file_path} for key={key_col}")
                    should_write = True  # Need to write if we deduplicated
                
                # Check if record already exists
                mask = _match_key(df[key_col], row_data[key_col])
                if mask.any():
                    # Record exists - check if status changed
                    existing_status = str(df.loc[mask, "status"].iloc[0]) if "status" in df.columns else None
                    new_status = str(row_data.get("status", ""))
                    
                    if existing_status == new_status:
                        # Status unchanged - skip update
                        if should_write:
                            # But we still need to write deduplicated data
                            df.to_csv(file_path, index=False)
                            logger.debug(f"Wrote deduped {file_path}; skipped {key_col}={row_data[key_col]} (status unchanged)")
                        else:
                            logger.debug(f"Skipping upsert for {key_col}={row_data[key_col]}: status unchanged ({existing_status})")
                        return
                    
                    # Status changed - update all columns in the matching row
                    for col, val in row_data.items():
                        if col in df.columns:
                            df.loc[mask, col] = val
                    should_write = True
                    logger.debug(f"Updated {key_col}={row_data[key_col]}: status changed to {new_status}")
                else:
                    # New record - insert it
                    df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)
                    should_write = True
                    logger.debug(f"Inserted new record {key_col}={row_data[key_col]}")
            else:
                # File doesn't exist - create it
                df = pd.DataFrame([row_data])
                should_write = True
                logger.debug(f"Created new {file_path} with {key_col}={row_data[key_col]}")
            
            if should_write:
                df.to_csv(file_path, index=False)
                logger.debug(f"Upserted {file_path} by {key_col}={row_data[key_col]}")
                
        except Exception as e:
            logger.error(f"Failed to upsert {file_path}: {e}")
            raise


def add_active_jobp(run_id: str, workflow_name: str) -> None:
    append_csv("data/active_jobps.csv", {
        "run_id": str(run_id),
        "workflow_name": workflow_name,
        "first_seen_at": now(),
    })


def load_active_jobps(file_path: str = "data/active_jobps.csv") -> pd.DataFrame:
    return read_csv(file_path, columns=["run_id", "workflow_name", "first_seen_at"])


def remove_active_jobp(run_id: str) -> None:
    lock = _get_file_lock("data/active_jobps.csv")
    with lock:
        try:
            path = Path("data/active_jobps.csv")
            if not path.exists():
                return
            df = pd.read_csv("data/active_jobps.csv")
            df = df[~_match_key(df["run_id"], run_id)]
            df.to_csv("data/active_jobps.csv", index=False)
            logger.debug(f"Removed active JOBP run_id={run_id}")
        except Exception as e:
            logger.error(f"Failed to remove active JOBP run_id={run_id}: {e}")


def load_business_rules(file_path: str = "data/business_rules.csv") -> pd.DataFrame:
    return read_csv(file_path)


def save_workflow_result(
    run_id: str,
    workflow_name: str,
    status: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    file_path: str = "data/workflow_results.csv",
) -> None:
    append_csv(file_path, {
        "run_id": run_id,
        "workflow_name": workflow_name,
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
    })
