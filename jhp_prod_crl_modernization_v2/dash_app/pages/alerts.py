import dash
from dash import html, dash_table, Input, Output, State, dcc
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd

from data.data_loader import get_data

dash.register_page(__name__, path="/alerts", title="Alerts")
# ---------------- LAYOUT ----------------

layout = dbc.Container(
    fluid=True,
    className="pt-3",
    children=[

        # ✅ STORE FOR RAW DATA
        dcc.Store(id="alert-raw-data"),

        html.H2("🚨 Job Failure Alerts", className="mb-3"),

        # ---------------- FILTER PANEL ----------------
        dbc.Card(
            dbc.CardBody([
                dbc.Row([

                    dbc.Col([
                        dbc.Label("Workflow"),
                        dbc.Input(
                            id="filter-workflow",
                            type="text",
                            placeholder="Workflow name"
                        ),
                    ], md=2),

                    dbc.Col([
                        dbc.Label("Job Name"),
                        dbc.Input(
                            id="filter-job",
                            type="text",
                            placeholder="Job name"
                        ),
                    ], md=2),

                    dbc.Col([
                        dbc.Label("Severity"),
                        dbc.Select(
                            id="filter-severity",
                            options=[
                                {"label": "CRITICAL", "value": "CRITICAL"},
                                {"label": "HIGH", "value": "HIGH"},
                                {"label": "MEDIUM", "value": "MEDIUM"},
                            ],
                            placeholder="Select"
                        ),
                    ], md=2),

                    dbc.Col([
                        dbc.Label("Failure Reason"),
                        dbc.Input(
                            id="filter-reason",
                            type="text",
                            placeholder="Failure reason"
                        ),
                    ], md=3),

                    dbc.Col([
                        dbc.Label("Start Time"),
                        dbc.Input(
                            id="filter-start",
                            type="datetime-local"
                        ),
                    ], md=1),

                    dbc.Col([
                        dbc.Label("End Time"),
                        dbc.Input(
                            id="filter-end",
                            type="datetime-local"
                        ),
                    ], md=1),

                ])
            ]),
            className="mb-3"
        ),

        # ---------------- DATA TABLE ----------------
        dbc.Row(
            dbc.Col(
                dash_table.DataTable(
                    id="alert-table",
                    columns=[
                        {"name": "Workflow", "id": "workflow_name"},
                        {"name": "Job Name", "id": "job_name"},
                        {"name": "Run ID", "id": "run_id"},
                        {"name": "Severity", "id": "severity"},
                        {"name": "Status", "id": "status"},
                        {"name": "Status Text", "id": "status_text"},
                        {"name": "Start Time", "id": "start_time"},
                        {"name": "Failure Reason", "id": "failure_reason"},
                    ],
                    data=[],
                    row_selectable="single",
                    page_size=20,

                    style_table={
                        "overflowX": "auto",
                        "width": "100%",
                        "border": "1px solid #dee2e6",
                    },

                    style_cell={
                        "padding": "8px",
                        "whiteSpace": "normal",
                        "textAlign": "left",
                        "fontFamily": "Segoe UI, Arial",
                        "fontSize": "13px",
                        "border": "1px solid #dee2e6",
                    },

                    style_header={
                        "backgroundColor": "#212529",
                        "color": "white",
                        "fontWeight": "bold",
                    },

                    style_data_conditional=[
                        {
                            "if": {"filter_query": "{severity} = 'CRITICAL'"},
                            "backgroundColor": "#f8d7da",
                            "color": "#721c24",
                        },
                        {
                            "if": {"filter_query": "{severity} = 'HIGH'"},
                            "backgroundColor": "#fff3cd",
                            "color": "#856404",
                        },
                        {
                            "if": {"filter_query": "{severity} = 'MEDIUM'"},
                            "backgroundColor": "#e2e3e5",
                        },
                    ],
                ),
                width=12,
            ),
        ),

        dbc.Button(
            "Create Incident",
            id="create-incident-btn",
            color="danger",
            className="mt-3",
            disabled=True,
        ),

        html.Div(id="alert-selected-msg", className="mt-2"),
    ],
)


# ---------------- LOAD DATA (PAGE INIT) ----------------
# @dash.callback(
#     Output("alert-table", "data"),
#     Input("alert-table", "id")
# )
# def load_alert_data(_):
    
#     df = get_data()
    
#     #1800–1899   ANY_ABEND (FAILURE)Job ended 
#     df['status'] = pd.to_numeric(df['status'], errors='coerce')

#     failed_jobs = df.loc[
#     (df['status'].between(1800, 1899)) ]

#     failed_jobs = failed_jobs[["workflow_name","business_function","object_name","run_id","start_time","status_text","status"]]
#     failed_jobs = failed_jobs.rename(columns={'object_name': 'job_name'})
#     # moke up data
#     severity_list = ["CRITICAL","MEDIUM","HIGH"]
#     failure_reason = ['Missing - Source File', 'Failure - Duplicate source record', 'Missing - Valid Zip File', 'Error - Data issue', 'Root Cause - Pending', 'Resource - DB Connection', 'Invalid - Source Data', 'Failure - Command Task', 'Error - Webservice Invoker', 'Failure - Terminated Unexpectedly', 'completed on restart', 'Error - Socket Closed', 'Error - File Archival', 'Error - Parameter File', 'Normal', 'Error - File Validation', 'Resource - File System Down', 'Root Cause - File move issue', 'Failure - PK Violation error', 'Failure - Webservice issue', 'Error - Data Validation', 'Failure  - Database unavailability', 'Failure - Code Error', 'Error -  reading data from the source.', 'Failure - Automic Error', 'Failure - Parameter issue', 'Informatica - Data Refresh', 'Resource - DB Locking', 'Resource - Server Down', 'Root Cause - Duplicate primary key issue.', 'Root Cause - Unavailability of DIW_PROD_BI', 'None', 'DB driver error', 'Downtime - Automic Agent', 'Downtime - Sever Patching', 'Error - Incorrect file name', 'Failed -  Informatica reboot', 'Failure  - first run after migration issue', 'Failure - Audit Log', 'Failure - Done file issue', 'Failure - Due to delay in the Excel monthly load', 'Failure - Infra session kill state', 'Failure - SQL Error', 'Failure - Timeout issue', 'Invalid - File Path', 'Resource - Deadlock Issue', 'Root Cause - Data refresh', 'Root Cause - Duplicate records for claim line', 'Root Cause - File rename issue', 'Root Cause - Integration Failure']
#     failed_jobs['severity'] = np.random.choice(severity_list, size=len(failed_jobs))
#     failed_jobs['failure_reason'] = np.random.choice(failure_reason, size=len(failed_jobs))
#     return failed_jobs.to_dict('records')
#     #return alert_data.to_dict("records")


@dash.callback(
    Output("alert-raw-data", "data"),
    Input("alert-table", "id")
)
def load_alert_data(_):

    df = get_data()

    df["status"] = pd.to_numeric(df["status"], errors="coerce")

    failed_jobs = df.loc[df["status"].between(1800, 1899)]
    failed_jobs = failed_jobs[
        ["workflow_name", "business_function", "object_name", "run_id",
         "start_time", "status_text", "status"]
    ]

    failed_jobs = failed_jobs.rename(columns={"object_name": "job_name"})

    # ✅ mock severity & reason
    severity_list = ["CRITICAL", "HIGH", "MEDIUM"]
    failure_reason = ["Missing File", "DB Error", "Timeout", "Infra Issue"]

    failed_jobs["severity"] = np.random.choice(severity_list, len(failed_jobs))
    failed_jobs["failure_reason"] = np.random.choice(failure_reason, len(failed_jobs))

    # ✅ FIX #1: Convert datetime to STRING
    failed_jobs["start_time"] = pd.to_datetime(
        failed_jobs["start_time"], errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # ✅ FIX #2: Convert numpy types to pure Python
    clean_records = failed_jobs.replace({np.nan: None}).to_dict("records")

    return clean_records
# ---------------- ENABLE INCIDENT BUTTON ----------------
@dash.callback(
    Output("create-incident-btn", "disabled"),
    Output("alert-selected-msg", "children"),
    Input("alert-table", "selected_rows")
)
def enable_incident_button(selected):
    if not selected:
        return True, "Select an alert to create an incident."
    return False, "Alert selected. Click Create Incident."

# ---------------- NAVIGATE TO INCIDENT ----------------
@dash.callback(
    Output("selected-alert", "data"),
    Output("url", "pathname"),
    Input("create-incident-btn", "n_clicks"),
    State("alert-table", "selected_rows"),
    State("alert-table", "data"),
)
def go_to_incident(n, selected, table_data):
    if not n or not selected:
        raise dash.exceptions.PreventUpdate

    return table_data[selected[0]], "/incident"


@dash.callback(
    Output("alert-table", "data"),
    Input("alert-raw-data", "data"),
    Input("filter-workflow", "value"),
    Input("filter-job", "value"),
    Input("filter-severity", "value"),
    Input("filter-reason", "value"),
    Input("filter-start", "value"),
    Input("filter-end", "value"),
)
def apply_filters(
    raw_data,
    workflow,
    job,
    severity,
    reason,
    start_time,
    end_time
):
    if not raw_data:
        return []

    df = pd.DataFrame(raw_data)

    # ✅ CRITICAL FIX:
    # Convert start_time STRING back to datetime
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")

    # -------- TEXT FILTERS --------
    if workflow:
        df = df[df["workflow_name"].str.contains(workflow, case=False, na=False)]

    if job:
        df = df[df["job_name"].str.contains(job, case=False, na=False)]

    if severity:
        df = df[df["severity"] == severity]

    if reason:
        df = df[df["failure_reason"].str.contains(reason, case=False, na=False)]

    # -------- DATE RANGE FILTER --------
    if start_time:
        df = df[df["start_time"] >= pd.to_datetime(start_time)]

    if end_time:
        df = df[df["start_time"] <= pd.to_datetime(end_time)]

    # ✅ Convert datetime back to string for DataTable
    df["start_time"] = df["start_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    return df.to_dict("records")