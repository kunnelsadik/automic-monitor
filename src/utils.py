import pandas as pd
import os
from datetime import datetime

DATA_DIR = "data"

def read_csv(name, columns=None):
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        return pd.DataFrame(columns=columns)
    return pd.read_csv(path)

def append_csv(name, row: dict):
    path = os.path.join(DATA_DIR, name)
    df = pd.DataFrame([row])
    header = not os.path.exists(path)
    df.to_csv(path, mode="a", header=header, index=False)

def now():
    return datetime.utcnow().isoformat()