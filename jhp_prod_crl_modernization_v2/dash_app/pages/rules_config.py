import pandas as pd
from database_util import configur_jobs_n_rules
import pyodbc
from dash import html, dcc, dash_table, Input, Output, State, ctx
import dash_bootstrap_components as dbc
import dash


dash.register_page(__name__, path="/rules_config", title="Rule Config")

def get_conn():
    db_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_crl_modern.accdb"
    return pyodbc.connect(
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        fr"DBQ={db_file};"
    )

def fetch_workflows():
    with get_conn() as conn:
        return pd.read_sql(
            "SELECT workflow_id, object_name as workflow_name FROM Workflows", conn
        )

def fetch_rules(workflow_id):
    with get_conn() as conn:
        return pd.read_sql(
            "SELECT A.workflow_id,A.objecT_name as job_name , C.rule_name,C.description ,  B.rule_param, B.is_active as enabled FROM (jobs  A inner join job_rules B on A.job_id = B.job_id) inner join rules C on C.rule_id = B.rule_id  WHERE A.workflow_id=?",
            conn, params=[workflow_id]
        )

def fetch_default_rules(workflow_id):
    print("workflow id = {workflow_id}")
    db_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_crl_modern.accdb"
    wf_details_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\code_base\jhp_prod_crl_modernization\all_workflow_details.csv"
    workflow_name ="JOBP.DAILY_CHIP_834_MEMBERSHIP_NEW_REDESIGN"
    configur_jobs_n_rules(db_file, wf_details_file, workflow_name, workflow_id)
    # return pd.DataFrame([
    #     {
    #         "rule_id": None,
    #         "workflow_id": workflow_id,
    #         "job_name": "",
    #         "rule_name": "",
    #         "rule_value": "",
    #         "enabled": True
    #     }
    # ])

    return fetch_rules(workflow_id)
layout = dbc.Card(
    dbc.CardBody([
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(
                    id="workflow-dd",
                    placeholder="Select Workflow"
                ),
                width=4
            ),
            dbc.Col(dbc.Button("Fetch Rules", id="btn-fetch", color="primary"), width="auto"),
            dbc.Col(dbc.Button("Configure Rules", id="btn-configure", color="secondary"), width="auto")
        ], className="mb-3"),

        dash_table.DataTable(
            id="rules-table",
            editable=True,
            row_deletable=True,
            columns=[
                {"name": "Job Name", "id": "job_name"},
                {"name": "Rule Name", "id": "rule_name"},
               # {"name": "Description", "id": "description"},
                {"name": "Rule Param", "id": "rule_param"},
                {
                    "name": "Enabled",
                    "id": "enabled",
                    "presentation": "dropdown"
                }
            ],
            dropdown={
                "enabled": {
                    "options": [
                        {"label": "True", "value": True},
                        {"label": "False", "value": False}
                    ]
                }
            },
            style_table={"overflowX": "auto"},
        ),

        dbc.Row([
            dbc.Button("Add Rule", id="btn-add-row", className="me-2"),
            dbc.Button("Update Rules", id="btn-update", color="success")
        ], className="mt-3")
    ]),
    className="mt-4"
)


@dash.callback(
    Output("workflow-dd", "options"),
    Input("workflow-dd", "id")
)
def load_workflows(_):
    df = fetch_workflows()
    return [{"label": r.workflow_name, "value": r.workflow_id}
            for _, r in df.iterrows()]


# @dash.callback(
#     Output("rules-table", "data"),
#     Input("btn-fetch", "n_clicks"),
#     Input("btn-configure", "n_clicks"),
#     State("workflow-dd", "value"),
#     prevent_initial_call=True
# )
# def load_rules(fetch_click, config_click, workflow_id):
#     if not workflow_id:
#         return []

#     triggered = ctx.triggered_id
#     if triggered == "btn-fetch":
#         df = fetch_rules(workflow_id)
#     else:
#         df = fetch_default_rules(workflow_id)

#     return df.to_dict("records")



# @dash.callback(
#     Output("rules-table", "data"),
#     Input("btn-add-row", "n_clicks"),
#     State("rules-table", "data"),
#     State("workflow-dd", "value"),
#     prevent_initial_call=True
# )
# def add_rule(_, rows, workflow_id):
#     rows.append({
#         "rule_id": None,
#         "workflow_id": workflow_id,
#         "job_name": "",
#         "rule_name": "",
#         "rule_value": "",
#         "enabled": True
#     })
#     return rows


@dash.callback(
    Output("btn-update", "children"),
    Input("btn-update", "n_clicks"),
    State("rules-table", "data"),
    prevent_initial_call=True
)
def save_rules(_, rows):
    df = pd.DataFrame(rows)
    with get_conn() as conn:
        cur = conn.cursor()

        for _, r in df.iterrows():
            if pd.isna(r.rule_id):
                cur.execute("""
                    INSERT INTO WorkflowRules
                    (workflow_id, job_name, rule_name, rule_value, enabled)
                    VALUES (?,?,?,?,?)
                """, r.workflow_id, r.job_name, r.rule_name, r.rule_value, r.enabled)
            else:
                cur.execute("""
                    UPDATE WorkflowRules
                    SET rule_value=?, enabled=?
                    WHERE rule_id=?
                """, r.rule_value, r.enabled, r.rule_id)
        conn.commit()

    return "Updated ✅"




@dash.callback(
    Output("rules-table", "data"),
    Input("btn-fetch", "n_clicks"),
    Input("btn-configure", "n_clicks"),
    Input("btn-add-row", "n_clicks"),
    State("workflow-dd", "value"),
    State("workflow-dd", "value"),
    State("rules-table", "data"),
    prevent_initial_call=True
)
def manage_rules(fetch_click, configure_click, add_click, workflow_id, existing_rows):

    trigger = ctx.triggered_id

    if not workflow_id:
        return []

    # ---------- FETCH EXISTING RULES ----------
    if trigger == "btn-fetch":
        df = fetch_rules(workflow_id)
        return df.to_dict("records")

    # ---------- LOAD DEFAULT RULES ----------
    if trigger == "btn-configure":
        df = fetch_default_rules(workflow_id)
        return df.to_dict("records")

    # ---------- ADD NEW ROW ----------
    if trigger == "btn-add-row":
        if not existing_rows:
            existing_rows = []

        existing_rows.append({
            "rule_id": None,
            "workflow_id": workflow_id,
            "job_name": "",
            "rule_name": "",
            "rule_value": "",
            "enabled": True
        })
        return existing_rows

    return existing_rows