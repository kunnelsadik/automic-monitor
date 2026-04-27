"""CSV file utilities for reading and writing configuration and result data."""
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)


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
