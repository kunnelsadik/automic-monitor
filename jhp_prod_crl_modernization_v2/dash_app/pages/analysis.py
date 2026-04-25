import dash
from dash import html, dcc, Input, Output
from dash import dash_table
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import numpy as np
from datetime import datetime, timedelta
from data.data_loader import get_data

# ------------------------------------------------------------
# Page Registration
# ------------------------------------------------------------
dash.register_page(__name__, path="/analysis", title="Job Analytics & Intelligence")

np.random.seed(42)

# ------------------------------------------------------------
# MOCK DATA — JOB EXECUTION & DATA VOLUME
# ------------------------------------------------------------
job_exec_stats = pd.DataFrame([
    {
        "job_name": "JOB_A",
        "run_date": datetime.now().date() - timedelta(days=i),
        "estimated_runtime_sec": 300,
        "actual_runtime_sec": np.random.normal(320, 40),
        "file_count": int(np.random.normal(12, 3)),
        "total_file_size_mb": np.random.normal(550, 120),
        "record_count": int(np.random.normal(4_500_000, 900_000)),
    }
    for i in range(30)
])

df = job_exec_stats.copy()

# ------------------------------------------------------------
# DERIVED METRICS & Z‑SCORES
# ------------------------------------------------------------
def zscore(series):
    std = series.std(ddof=0)
    if std == 0:
        return pd.Series([0] * len(series), index=series.index)
    return (series - series.mean()) / std

df["runtime_z"] = zscore(df["actual_runtime_sec"])
df["filesize_z"] = zscore(df["total_file_size_mb"])
df["filecount_z"] = zscore(df["file_count"])
df["recordcount_z"] = zscore(df["record_count"])

df["is_anomaly"] = (
        (df["runtime_z"].abs() >= 3)
        | (df["filesize_z"] >= 3)
        | (df["filecount_z"] >= 3)
        | (df["recordcount_z"] >= 3)
)



# ------------------------------------------------------------
# MOCK REAL‑TIME HEALTH TELEMETRY
# ------------------------------------------------------------
realtime_health = pd.DataFrame([
    {
        "job_name": "JOB_A",
        "timestamp": datetime.now() - timedelta(minutes=i * 5),
        "cpu_pct": np.random.normal(65, 8),
        "memory_pct": np.random.normal(70, 10),
        "io_wait_ms": np.random.normal(18, 6),
    }
    for i in range(12)
])

# ------------------------------------------------------------
# MOCK PREDICTIVE SIGNALS
# ------------------------------------------------------------
predictive_signals = pd.DataFrame([
    {
        "job_name": "JOB_A",
        "run_date": datetime.now().date() - timedelta(days=i),
        "avg_runtime_sec": 170 + i * 4,
        "queue_delay_sec": 20 + i * 2,
    }
    for i in range(15)
])

# ------------------------------------------------------------
# PAGE LAYOUT
# ------------------------------------------------------------
layout = dbc.Container(fluid=True, children=[

    html.H2("📊 Job Analytics, Performance & Risk Intelligence", className="mb-4"),

    # ---------------- EXECUTION EFFECTIVENESS ----------------
    dbc.Card([
        dbc.CardHeader("Execution vs Estimated Performance"),
        dbc.CardBody([
            dcc.Graph(id="runtime-estimate-chart"),
            html.Div(id="execution-summary")
        ])
    ], className="mb-4"),

    # ---------------- DATA VOLUME INTELLIGENCE ----------------
    dbc.Card([
        dbc.CardHeader("Data Volume & Processing Intelligence"),
        dbc.CardBody([
            dcc.Graph(id="data-volume-chart"),
            html.Div(id="volume-summary")
        ])
    ], className="mb-4"),

    # ---------------- ANOMALY DETECTION ----------------
    dbc.Card([
        dbc.CardHeader("Detected Anomalies"),
        dbc.CardBody([
            dash_table.DataTable(
                id="anomaly-table",
                page_size=6,
                style_table={"overflowX": "auto"}
            )
        ])
    ], className="mb-4"),

    # ---------------- REAL‑TIME HEALTH ----------------
    dbc.Card([
        dbc.CardHeader("Near Real‑Time Health Signals"),
        dbc.CardBody([
            dcc.Graph(id="realtime-health-chart"),
            html.Div(id="realtime-alerts")
        ])
    ], className="mb-4"),

    # ---------------- PREDICTIVE RE‑ACCOMMODATION ----------------
    dbc.Card([
        dbc.CardHeader("Predictive Re‑accommodation"),
        dbc.CardBody([
            dcc.Graph(id="predictive-runtime-chart"),
            html.Div(id="predictive-risk")
        ])
    ], className="mb-4"),

    # ---------------- KNOWLEDGE ENGINE ----------------
    dbc.Card([
        dbc.CardHeader("Knowledge & Recommendations"),
        dbc.CardBody(
            html.Div(id="knowledge-insights")
        )
    ])
])

# ------------------------------------------------------------
# CALLBACK — EXECUTION EFFECTIVENESS
# ------------------------------------------------------------
@dash.callback(
    Output("runtime-estimate-chart", "figure"),
    Output("execution-summary", "children"),
    Input("url", "pathname")
)
def execution_effectiveness(_):
    df1 = get_data()
    df1  = df1[df1["ert_analysis_result"].isin(["Above Threshold", "Below Threshold"])]
    # df_data  = df_data.get("ert_analysis_result")
    df1["runtime_delta_sec"] = df1["runtime"] - df1["estimated_runtime"]
    df1["runtime_delta_pct"] = (df1["runtime_delta_sec"] / df1["estimated_runtime"]) * 100

#     df1["runtime_z"] = zscore(df["runtime"])
#     df1["filesize_z"] = zscore(df["total_file_size_mb"])
#     df1["filecount_z"] = zscore(df["file_count"])
#     df1["recordcount_z"] = zscore(df["record_count"])

#     df["is_anomaly"] = (
#         (df["runtime_z"].abs() >= 3)
#         | (df["filesize_z"] >= 3)
#         | (df["filecount_z"] >= 3)
#         | (df["recordcount_z"] >= 3)
# )

    # fig = px.line(
    #     df1,
    #     x="start_time",
    #     y=["estimated_runtime", "runtime"],
    #     title="Estimated vs Actual Runtime Trend",
    #     labels={"value": "Seconds"}
    # )
    # fig.update_yaxes(type="log")
    
   

    df_long = df1.melt(
    id_vars=["start_time"],
    value_vars=["estimated_runtime", "runtime"],
    var_name="type",
    value_name="seconds"
)

    fig = px.scatter(
    df_long,
    x="start_time",
    y="seconds",
    color="type",
    title="Estimated vs Actual Runtime (Scatter View)"
)





    summary = html.Ul([
        html.Li(f"Runs longer than estimate: {(df1['runtime_delta_sec'] > 0).sum()}"),
        html.Li(f"Runs shorter than estimate: {(df1['runtime_delta_sec'] < 0).sum()}"),
        html.Li(f"Max deviation observed: {df1['runtime_delta_pct'].abs().max():.2f}%"),
    ])

    return fig, summary

# ------------------------------------------------------------
# CALLBACK — DATA VOLUME INTELLIGENCE
# ------------------------------------------------------------
@dash.callback(
    Output("data-volume-chart", "figure"),
    Output("volume-summary", "children"),
    Input("url", "pathname")
)
def data_volume_intelligence(_):
    fig = px.bar(
        df,
        x="run_date",
        y="record_count",
        title="Records Processed per Run"
    )

    summary = html.Ul([
        html.Li(f"Large file anomalies: {(df['filesize_z'] >= 3).sum()}"),
        html.Li(f"High file‑count anomalies: {(df['filecount_z'] >= 3).sum()}"),
        html.Li(f"High record‑count anomalies: {(df['recordcount_z'] >= 3).sum()}"),
    ])

    return fig, summary

# ------------------------------------------------------------
# CALLBACK — ANOMALY TABLE
# ------------------------------------------------------------
@dash.callback(
    Output("anomaly-table", "data"),
    Output("anomaly-table", "columns"),
    Input("url", "pathname")
)
def anomaly_table_data(_):
    anomaly_df = df[df["is_anomaly"]][[
        "run_date", "actual_runtime_sec", "file_count",
        "total_file_size_mb", "record_count"
    ]]

    return (
        anomaly_df.to_dict("records"),
        [{"name": c, "id": c} for c in anomaly_df.columns],
    )

# ------------------------------------------------------------
# CALLBACK — REAL‑TIME HEALTH
# ------------------------------------------------------------
@dash.callback(
    Output("realtime-health-chart", "figure"),
    Output("realtime-alerts", "children"),
    Input("url", "pathname")
)
def realtime_health_view(_):
    fig = px.line(
        realtime_health,
        x="timestamp",
        y=["cpu_pct", "memory_pct"],
        title="CPU & Memory Utilization (Near Real‑Time)"
    )

    latest = realtime_health.iloc[-1]
    alerts = []
    if latest["cpu_pct"] > 80:
        alerts.append("High CPU utilization")
    if latest["memory_pct"] > 85:
        alerts.append("High memory pressure")
    if latest["io_wait_ms"] > 30:
        alerts.append("I/O wait escalation")

    if alerts:
        return fig, dbc.Alert("⚠️ " + "; ".join(alerts), color="danger")

    return fig, dbc.Alert("✅ Real‑time system health within normal limits.", color="success")

# ------------------------------------------------------------
# CALLBACK — PREDICTIVE RE‑ACCOMMODATION
# ------------------------------------------------------------
@dash.callback(
    Output("predictive-runtime-chart", "figure"),
    Output("predictive-risk", "children"),
    Input("url", "pathname")
)
def predictive_analysis(_):
    dfp = predictive_signals.copy()
    dfp["ewma"] = dfp["avg_runtime_sec"].ewm(span=4).mean()

    fig = px.line(
        dfp, x="run_date",
        y=["avg_runtime_sec", "ewma"],
        title="Runtime Trend & Predictive EWMA"
    )

    drift = (dfp["ewma"].iloc[-1] - dfp["ewma"].iloc[0]) / dfp["ewma"].iloc[0]
    backlog_growth = dfp["queue_delay_sec"].iloc[-1] - dfp["queue_delay_sec"].iloc[0]

    if drift > 0.25 or backlog_growth > 20:
        msg = "🔮 Predictive risk identified — recommend workload re‑accommodation."
        color = "warning"
    else:
        msg = "🔮 Predictive outlook stable."
        color = "success"

    return fig, dbc.Alert(msg, color=color)

# ------------------------------------------------------------
# CALLBACK — KNOWLEDGE ENGINE
# ------------------------------------------------------------
@dash.callback(
    Output("knowledge-insights", "children"),
    Input("url", "pathname")
)
def knowledge_engine(_):
    return dbc.Alert(
        """
        📘 Operational Insight:
        • Runtime growth correlates with rising data volumes
        • Large record spikes often precede runtime anomalies

        ✅ Recommendations:
        - Validate upstream extract logic
        - Monitor next two runs closely
        - Auto‑scale or stagger retries if trend persists
        """,
        color="info"
    )