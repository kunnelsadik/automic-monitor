import json
import dash
from dash import html, dcc, Input, Output, State
from dash.exceptions import PreventUpdate

from log_parser import load_external_log, parse_job_log, read_log_from_shared_drive
from main import get_job_logs, load_config

 
# -------------------------------------------------------
# Dash App Setup
# -------------------------------------------------------

app = dash.Dash(__name__)
app.title = "Automic File Transfer Analysis"

# -------------------------------------------------------
# Layout
# -------------------------------------------------------

app.layout = html.Div(
    style={"maxWidth": "1200px", "margin": "auto", "fontFamily": "Arial"},
    children=[

        html.H2("Automic File Transfer Log Analyzer"),

        # ---- Run ID input ----
        html.Div([
            html.Label("Run ID:", style={"fontWeight": "bold"}),
            dcc.Input(
                id="run-id-input",
                type="text",
                placeholder="Enter run_id",
                style={"width": "300px", "marginRight": "10px"}
            ),
            html.Button("Fetch Logs", id="fetch-logs-btn"),
            html.Button("Fetch Remote Logs", id="fetch-remote-logs-btn"),
            html.Button(
                "Analyse Logs",
                id="analyse-logs-btn",
                style={"marginLeft": "10px"}
            )
        ], style={"marginBottom": "20px"}),

        # ---- Logs output with spinner ----
        html.Div([
            html.H4("Raw Logs"),

            dcc.Loading(
                id="log-loading",
                type="circle",
                children=dcc.Textarea(
                    id="log-output",
                    style={
                        "width": "100%",
                        "height": "300px",
                        "whiteSpace": "pre",
                        "fontFamily": "monospace"
                    }
                )
            ),
             html.H4("External Logs"),

            dcc.Loading(
                id="ext-log-loading",
                type="circle",
                children=dcc.Textarea(
                    id="ext-log-output",
                    style={
                        "width": "100%",
                        "height": "300px",
                        "whiteSpace": "pre",
                        "fontFamily": "monospace"
                    }
                )
            )
        ], style={"marginBottom": "20px"}),

        # ---- Collapsible JSON Viewer ----
        html.Details(
            open=True,
            children=[
                html.Summary(
                    "Analysis Output (JSON)",
                    style={
                        "fontWeight": "bold",
                        "cursor": "pointer",
                        "marginBottom": "10px"
                    }
                ),
                # dcc.Textarea(
                #     id="analysis-output",
                #     style={
                #         "width": "100%",
                #         "height": "300px",
                #         "whiteSpace": "pre",
                #         "fontFamily": "monospace"
                #     }
                # )

                html.Pre(
                id="analysis-output",
                style={
                "maxHeight": "400px",
                "overflow": "auto",
                "backgroundColor": "#f6f8fa",
                "padding": "10px",
                "border": "1px solid #ccc",
                "fontFamily": "monospace",
                "whiteSpace": "pre-wrap"
            }
        )

            ]
        )
    ]
)

# -------------------------------------------------------
# Callbacks
# -------------------------------------------------------

# ✅ Fetch logs (spinner triggers automatically)
@app.callback(
    Output("log-output", "value"),
    Input("fetch-logs-btn", "n_clicks"),
    State("run-id-input", "value"),
    prevent_initial_call=True
)
def fetch_logs(n_clicks, run_id):
    if not run_id:
        return "ERROR: run_id is required"

    try:
        # db_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_crl_modern.accdb"
        config =  load_config()
        resp = get_job_logs(base_url=config.base_url,config=config,run_id=run_id)
        print(resp)
        return resp
    except Exception as e:
        return f"ERROR fetching logs: {str(e)}"

@app.callback(
    Output("ext-log-output", "value"),
    Input("fetch-remote-logs-btn", "n_clicks"),
    State("run-id-input", "value"),
    State("log-output", "value"),
    prevent_initial_call=True
)
def fetch_external_logs(n_clicks, run_id,log_text):
    if not run_id and not log_text :
        return "ERROR: run_id is required"

    try:
        # db_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_crl_modern.accdb"
        config =  load_config()
        if log_text:
            resp  = load_external_log(log_text)
            return resp
        elif run_id:
            job_log = get_job_logs(base_url=config.base_url,config=config,run_id=run_id)
            resp  = load_external_log(job_log)
        
            return resp
    except Exception as e:
        return f"ERROR fetching logs: {str(e)}"


# ✅ Analyse logs
@app.callback(
    Output("analysis-output", "children"),
    Input("analyse-logs-btn", "n_clicks"),
    State("log-output", "value"),
    prevent_initial_call=True
)
def analyse(n_clicks, log_text):
    if not log_text:
        raise PreventUpdate

    try:
        analysis = parse_job_log(log_text, external_log_loader=read_log_from_shared_drive)
        return json.dumps(analysis, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)

# -------------------------------------------------------
# Run server
# -------------------------------------------------------

if __name__ == "__main__":
    
    app.run(debug=True)