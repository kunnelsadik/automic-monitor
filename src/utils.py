import pandas as pd
import os
import logging
from datetime import datetime

DATA_DIR = "data"
logger = logging.getLogger(__name__)

def read_csv(name, columns=None):
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        logger.warning(f"CSV file {path} does not exist, returning empty DataFrame")
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path)
    logger.debug(f"Read {len(df)} rows from {path}")
    return df

def append_csv(name, row: dict):
    path = os.path.join(DATA_DIR, name)
    df = pd.DataFrame([row])
    header = not os.path.exists(path)
    df.to_csv(path, mode="a", header=header, index=False)
    logger.debug(f"Appended row to {path}")

def now():
    return datetime.utcnow().isoformat()