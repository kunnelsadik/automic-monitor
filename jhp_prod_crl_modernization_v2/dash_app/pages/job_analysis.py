import dash
from dash import html, dcc, dash_table, Input, Output, State, callback
from dash.exceptions import PreventUpdate

from simulate_filetransfer import get_filetransfer_cmd, process_jobs

# -------------------------------------------------------
# Register page
# -------------------------------------------------------
dash.register_page(
    __name__,
    name="Job Analysis Tables",
    path="/job-analysis"
)

# -------------------------------------------------------
# EXTERNAL METHOD (YOU ALREADY HAVE THIS)
# -------------------------------------------------------

def get_workflow():
    # 🔁 Replace with your real logic
    return [
       "JOBP.DAILY_IPLUS_837_INBOUND_OUTBOUND_PROCESS"
,"JOBP.DAILY_MEDICAID_834UPD_MEMBERSP_REDESIGNED"
,"JOBP.DAILY_CHIP_834_MEMBERSHIP_NEW_REDESIGN"
,"JOBP.DAILY_MHK_EXTRACTS_DATA_UPLOAD_PROCESS"
,"JOBP.DAILY_TD_BANK_PROCESS"
,"JOBP.DAILY_MEDICARE_ENCOUNTER_PROCESSING_PROCESS"
,"JOBP.MEDICIAD_ENCOUNTER_PROCESS"
,"JOBP.CVS_HEALTH_DAILY_CET_PCT_LOAD_PROCESSES"
,"JOBP.DAILY_ACA_SALESFORCE_ENROLLMENT"
,"JOBP.DAILY_BUILD_HEALTHTRIO_PROVIDER_DIRECTORY_DATA"
,"JOBP.DAILY_IPLUS_ZELIS_CLAIM_REPRICER_INBOUND_PROCESS"
,"JOBP.DAILY_IPLUS_ZELIS_CLAIM_REPRICER_OUTBOUND_PROCESS"
,"JOBP.DAILY_IPLUS_SECONDARY_ZELIS_UPLOAD_PROCESS"
,"JOBP.DAILY_IPLUS_SECONDARY_ZELIS_DOWNLOAD_PROCESS"
,"JOBP.WEEKLY_MATERNITY_BILLING_PROCESS"
,"JOBP.WEEKLY_MATERNITY_REMIT_INFORMATICA"
,"JOBP.MONTHLY_CAID_CHIP_820_REMITTANCE_MASTER"
,"JOBP.MONTHLY_CHIP_PREMIUM_BILLING_PROCESS"
,"JOBP.MANUAL_MONTHLY_MEDICARE_PREMIUM_BILLING_INVOICE_PROCESS"
,"JOBP.DPW_MONTHLY_MEMBERSHIP_REDESIGN"
,"JOBP.MONTHLY_CHIP_834_MEMBERSHIP_NEW_REDESIGN"
,"JOBP.MONTHLY_CAPITATION_EXTRACT_PROCESS"
,"JOBP.MONTHLY_BONUS_PAYMENT_ECHO_MASTER"
    ]


# -------------------------------------------------------
# MOCK BACKEND LOGIC (wire real logic later)
# -------------------------------------------------------

def get_job_command_data(workflow_name):

    # wf_details_file=r"C:\Users\afe3356\Code\jhp_prod_crl_modernization\all_workflow_details.csv"
    wf_details_file=r"all_workflow_details.csv"
    ls_job_data = get_filetransfer_cmd(workflow_name,wf_details_file)
     
    return ls_job_data


def get_final_folder_status_data(workflow_name):
    # Replace with real final status logic
    # wf_details_file=r"C:\Users\afe3356\Code\jhp_prod_crl_modernization\all_workflow_details.csv"
    wf_details_file=r"all_workflow_details.csv"
    ls_job_data = get_filetransfer_cmd(workflow_name,wf_details_file)
    out = process_jobs(ls_job_data)
    
    return out


# -------------------------------------------------------
# HELPERS
# -------------------------------------------------------

def flatten_job_commands(data):
    rows = []
    for job in data:
        for cmd in job.get("commands", []):
            rows.append({
                "job_name": job["job_name"],
                "command": cmd.get("command", ""),
                "source": cmd.get("source", ""),
                "target": cmd.get("target", ""),
            })
    return rows


def flatten_folder_status(data):
    rows = []
    for job in data:
        for folder, info in job.get("final_folders_status", {}).items():
            rows.append({
                "job_name": job["job_name"],
                "folder": folder,
                "has_files": "YES" if info.get("has_job_files") else "NO",
                "files": ", ".join(info.get("job_files", [])),
                "other_job_files": ", ".join(info.get("other_job_files", []))
            })
    return rows


# -------------------------------------------------------
# LAYOUT
# -------------------------------------------------------

layout = html.Div(
    style={"padding": "20px"},
    children=[

        # ==========================
        # WORKFLOW SELECTION + BUTTONS
        # ==========================
        html.Div(
            style={"marginBottom": "20px"},
            children=[
                html.Label("Select Workflow:", style={"fontWeight": "bold"}),

                dcc.Dropdown(
                    id="workflow-dropdown",
                    options=[{"label": wf, "value": wf} for wf in get_workflow()],
                    placeholder="Choose a workflow",
                    style={"width": "600px", "marginBottom": "10px"}
                ),

                html.Button(
                    "Analyse Job",
                    id="analyse-job-btn",
                    style={"marginRight": "10px"}
                ),

                html.Button(
                    "Find Final Status",
                    id="final-status-btn"
                ),
            ]
        ),

        # ========================
        # TABLE 1 : JOB COMMANDS
        # ========================
        html.H3("Job Command Details"),

        dash_table.DataTable(
            id="job-command-table",
            columns=[
                {"name": "Job Name", "id": "job_name"},
                {"name": "Command", "id": "command"},
                {"name": "Source", "id": "source"},
                {"name": "Target", "id": "target"},
            ],
            fixed_rows={"headers": True},   # ✅ freeze header
            data=[],
            filter_action="native",
            sort_action="native",
            page_action="none",
            style_table={"maxHeight": "350px", "overflowY": "auto"},
            style_cell={
                "textAlign": "left",
                "fontFamily": "monospace",
                "fontSize": "13px",
                "whiteSpace": "normal",
                "padding": "6px"
            },
            style_header={
                "fontWeight": "bold",
                "backgroundColor": "#f0f0f0",
                "borderBottom": "2px solid black"
            }
        ),

        html.Hr(style={"margin": "30px 0"}),

        # ========================
        # TABLE 2 : FINAL STATUS
        # ========================
        html.H3("Final Folder Status"),

        dash_table.DataTable(
            id="final-folder-table",
            columns=[
                {"name": "Job Name", "id": "job_name"},
                {"name": "Folder Path", "id": "folder"},
                {"name": "Has Files", "id": "has_files"},
                {"name": "Files", "id": "files"},
                {"name": "Other Job Files", "id": "other_job_files"},
            ],
            
            fixed_rows={"headers": True},   # ✅ freeze header

            data=[],
            filter_action="native",
            sort_action="native",
            page_action="none",
            style_table={"maxHeight": "400px", "overflowY": "auto"},
            style_cell={
                "textAlign": "left",
                "fontFamily": "monospace",
                "fontSize": "13px",
                "whiteSpace": "normal",
                "padding": "6px"
            },
            style_header={
                "fontWeight": "bold",
                "backgroundColor": "#f0f0f0",
                "borderBottom": "2px solid black"
            },
            style_data_conditional=[
                {
                    "if": {"filter_query": '{has_files} = "YES"'},
                    "backgroundColor": "#E6FFFA",
                },
                {
                    "if": {"filter_query": '{has_files} = "NO"'},
                    "backgroundColor": "#FFF1F2",
                },
            ]
        ),
    ]
)

# -------------------------------------------------------
# CALLBACKS
# -------------------------------------------------------

@callback(
    Output("job-command-table", "data"),
    Input("analyse-job-btn", "n_clicks"),
    State("workflow-dropdown", "value"),
    prevent_initial_call=True
)
def analyse_job(_, workflow_name):
    if not workflow_name:
        raise PreventUpdate

    job_data = get_job_command_data(workflow_name)
    return flatten_job_commands(job_data)


@callback(
    Output("final-folder-table", "data"),
    Input("final-status-btn", "n_clicks"),
    State("workflow-dropdown", "value"),
    prevent_initial_call=True
)
def find_final_status(_, workflow_name):
    if not workflow_name:
        raise PreventUpdate

    status_data = get_final_folder_status_data(workflow_name)
    return flatten_folder_status(status_data)
