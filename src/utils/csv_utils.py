"""CSV file utilities for reading and writing configuration data."""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def read_csv(
    file_path: str,
    columns: Optional[List[str]] = None,
    dtypes: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Read CSV file into a pandas DataFrame.
    
    Args:
        file_path: Path to CSV file
        columns: Optional list of column names to read
        dtypes: Optional dict mapping column names to data types
        
    Returns:
        pandas.DataFrame with CSV data
        
    Example:
        df = read_csv("config_workflows.csv", columns=["workflow_name", "is_active"])
    """
    try:
        path = Path(file_path)
        if not path.exists():
            logger.warning(f"CSV file not found: {file_path}, returning empty DataFrame")
            return pd.DataFrame(columns=columns) if columns else pd.DataFrame()

        df = pd.read_csv(file_path, usecols=columns, dtype=dtypes)
        logger.info(f"Read {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read CSV file {file_path}: {e}")
        return pd.DataFrame(columns=columns) if columns else pd.DataFrame()


def write_csv(
    file_path: str,
    data: pd.DataFrame,
    index: bool = False,
    mode: str = "w",
) -> None:
    """Write DataFrame to CSV file.
    
    Args:
        file_path: Path to CSV file
        data: DataFrame to write
        index: Whether to include index column
        mode: File write mode ("w" for write, "a" for append)
        
    Example:
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        write_csv("output.csv", df)
    """
    try:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if mode == "a" and path.exists():
            data.to_csv(file_path, mode="a", header=False, index=index)
        else:
            data.to_csv(file_path, mode="w", index=index)

        logger.info(f"Wrote {len(data)} rows to {file_path}")
    except Exception as e:
        logger.error(f"Failed to write CSV file {file_path}: {e}")
        raise


def append_csv(file_path: str, row_data: Dict[str, Any]) -> None:
    """Append a single row to CSV file.
    
    Creates file if it doesn't exist. Headers are automatically added on creation.
    
    Args:
        file_path: Path to CSV file
        row_data: Dict with column names and values
        
    Example:
        append_csv("processed_runs.csv", {
            "run_id": "12345",
            "workflow_name": "JOBS.TEST",
            "processed_timestamp": datetime.now()
        })
    """
    try:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        df_new = pd.DataFrame([row_data])

        if path.exists() and path.stat().st_size > 0:
            # File exists and has content
            df_existing = pd.read_csv(file_path)
            df = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            # New file
            df = df_new

        df.to_csv(file_path, index=False)
        logger.debug(f"Appended row to {file_path}")
    except Exception as e:
        logger.error(f"Failed to append to CSV file {file_path}: {e}")
        raise


def load_config_workflows(file_path: str = "data/config_workflows.csv") -> pd.DataFrame:
    """Load workflow configuration from CSV.
    
    Expected columns: workflow_name, object_type, is_active, last_polled_at
    
    Args:
        file_path: Path to workflow config CSV
        
    Returns:
        DataFrame with workflow configurations
    """
    columns = ["workflow_name", "object_type", "is_active", "last_polled_at"]
    return read_csv(file_path, columns=columns)


def load_processed_runs(file_path: str = "data/processed_runs.csv") -> pd.DataFrame:
    """Load processed run IDs from CSV.
    
    Expected columns: run_id, workflow_name, processed_timestamp
    
    Args:
        file_path: Path to processed runs CSV
        
    Returns:
        DataFrame with processed run IDs
    """
    columns = ["run_id", "workflow_name", "processed_timestamp"]
    return read_csv(file_path, columns=columns)


def load_workflow_results(file_path: str = "data/workflow_results.csv") -> pd.DataFrame:
    """Load workflow execution results from CSV.
    
    Expected columns: run_id, workflow_name, status, start_time, end_time
    
    Args:
        file_path: Path to workflow results CSV
        
    Returns:
        DataFrame with workflow results
    """
    columns = ["run_id", "workflow_name", "status", "start_time", "end_time"]
    return read_csv(file_path, columns=columns)


def load_business_rules(file_path: str = "data/business_rules.csv") -> pd.DataFrame:
    """Load business rules from CSV.
    
    Args:
        file_path: Path to business rules CSV
        
    Returns:
        DataFrame with business rules
    """
    return read_csv(file_path)


def save_workflow_results(
    run_id: str,
    workflow_name: str,
    status: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    file_path: str = "data/workflow_results.csv",
) -> None:
    """Save a workflow execution result.
    
    Args:
        run_id: Execution run ID
        workflow_name: Name of workflow
        status: Execution status (COMPLETED, FAILED, etc.)
        start_time: Execution start time
        end_time: Execution end time
        file_path: Path to results CSV
    """
    row_data = {
        "run_id": run_id,
        "workflow_name": workflow_name,
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
    }
    append_csv(file_path, row_data)
