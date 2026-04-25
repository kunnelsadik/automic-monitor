import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc

dash.register_page(__name__, path="/incident", title="Incident Management")

layout = dbc.Container([

    html.H2("🛠 Incident Management", className="mb-3"),

    dbc.Alert(
        "Use this form to manually create an incident or review an alert‑based incident.",
        color="info"
    ),

    dbc.Form([

        dbc.Row([
            dbc.Col(dbc.Input(id="inc-job", placeholder="Job Name"), md=6),
            dbc.Col(dbc.Input(id="inc-run", placeholder="Run ID (optional)"), md=6),
        ], className="mb-2"),

        dbc.Row([
            dbc.Col(
                dbc.Select(
                    id="inc-severity",
                    options=[
                        {"label": "CRITICAL", "value": "CRITICAL"},
                        {"label": "HIGH", "value": "HIGH"},
                        {"label": "MEDIUM", "value": "MEDIUM"},
                        {"label": "LOW", "value": "LOW"},
                    ],
                    placeholder="Select Severity"
                ),
                md=6
            ),
            dbc.Col(
                dbc.Input(id="inc-reason", placeholder="Failure Reason"),
                md=6
            ),
        ], className="mb-2"),

        dbc.Textarea(
            id="inc-notes",
            placeholder="Incident Notes / Description",
            style={"height": "120px"},
            className="mb-3"
        ),

        dbc.Button(
            "Submit Incident",
            id="submit-incident",
            color="danger",
            className="me-2"
        ),

        dbc.Button(
            "Clear Form",
            id="clear-incident",
            color="secondary"
        ),
    ]),

    html.Div(id="incident-output", className="mt-3")

], fluid=True)



@dash.callback(
    Output("inc-job", "value"),
    Output("inc-run", "value"),
    Output("inc-severity", "value"),
    Output("inc-reason", "value"),
    Input("url", "pathname"),          # ✅ Detect page load
    State("selected-alert", "data")    # ✅ Read stored alert
)
def populate_from_alert(pathname, alert):
    # Only act when on incident page
    if pathname != "/incident":
        raise dash.exceptions.PreventUpdate

    # Manual navigation → empty form
    if not alert:
        return "", "", None, ""

    # Auto-populate from alert
    return (
        alert.get("job_name", ""),
        alert.get("run_id", ""),
        alert.get("severity", ""),
        alert.get("failure_reason", "")
    )

@dash.callback(
    Output("incident-output", "children"),
    Input("submit-incident", "n_clicks"),
    State("inc-job", "value"),
    State("inc-run", "value"),
    State("inc-severity", "value"),
    State("inc-reason", "value"),
    State("inc-notes", "value"),
)
def submit_incident(n, job, run, severity, reason, notes):
    if not n:
        return ""

    if not job or not severity:
        return dbc.Alert(
            "❌ Job Name and Severity are required.",
            color="warning"
        )

    # Mock persistence (replace later with ServiceNow / DB)
    incident_payload = {
        "job": job,
        "run_id": run,
        "severity": severity,
        "reason": reason,
        "notes": notes
    }

    print("Incident Created:", incident_payload)

    return dbc.Alert(
        f"✅ Incident created successfully for job `{job}` (Severity: {severity})",
        color="success"
    )


# @dash.callback(
#     Output("inc-job", "value"),
#     Output("inc-run", "value"),
#     Output("inc-severity", "value"),
#     Output("inc-reason", "value"),
#     Output("inc-notes", "value"),
#     Input("clear-incident", "n_clicks"),
# )
# def clear_form(n):
#     if not n:
#         raise dash.exceptions.PreventUpdate

#     return "", "", None, "", ""


# import dash
# from dash import html, dcc, Input, Output, State
# import dash_bootstrap_components as dbc

# dash.register_page(__name__, path="/incident", title="Incident Management")

# layout = dbc.Container([
#     html.H2("🛠 Incident Management"),

#     dbc.Input(id="inc-job", placeholder="Job Name"),
#     dbc.Input(id="inc-run", placeholder="Run ID", className="mt-2"),
#     dbc.Input(id="inc-severity", placeholder="Severity", className="mt-2"),
#     dbc.Input(id="inc-reason", placeholder="Failure Reason", className="mt-2"),
#     dbc.Textarea(id="inc-notes", placeholder="Incident Notes", className="mt-2"),

#     dbc.Button("Submit Incident", id="submit-inc", color="success", className="mt-3"),
#     html.Div(id="incident-output", className="mt-3")
# ])



# @dash.callback(
#     Output("inc-job", "value"),
#     Output("inc-run", "value"),
#     Output("inc-severity", "value"),
#     Output("inc-reason", "value"),
#     Input("selected-alert", "data")
# )
# def populate_from_alert(alert):
#     if not alert:
#         return "", "", "", ""

#     return (
#         alert["job_name"],
#         alert["run_id"],
#         alert["severity"],
#         alert["failure_reason"]
#     )



# @dash.callback(
#     Output("incident-output", "children"),
#     Input("submit-inc", "n_clicks"),
#     State("inc-job", "value"),
#     State("inc-reason", "value")
# )
# def submit_incident(n, job, reason):
#     if not n:
#         return ""

#     return dbc.Alert(
#         f"✅ Incident created for {job} | Reason: {reason}",
#         color="success"
#     )
