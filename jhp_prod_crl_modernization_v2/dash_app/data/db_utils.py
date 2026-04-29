import pandas as pd
import pyodbc
from dash import html, dcc, dash_table, Input, Output, State, ctx
import dash_bootstrap_components as dbc

def get_conn():
    return pyodbc.connect(
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        r"DBQ=C:\data\workflow_rules.accdb;"
    )

def fetch_workflows():
    with get_conn() as conn:
        return pd.read_sql(
            "SELECT workflow_id, workflow_name FROM Workflows", conn
        )

def fetch_rules(workflow_id):
    with get_conn() as conn:
        return pd.read_sql(
            "SELECT * FROM WorkflowRules WHERE workflow_id=?",
            conn, params=[workflow_id]
        )

def fetch_default_rules(workflow_id):
    return pd.DataFrame([
        {
            "rule_id": None,
            "workflow_id": workflow_id,
            "job_name": "",
            "rule_name": "",
            "rule_value": "",
            "enabled": True
        }
    ])