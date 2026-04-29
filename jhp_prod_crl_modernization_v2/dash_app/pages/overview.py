import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output
import plotly.express as px
import pandas as pd
from data.data_loader import get_data
from dash import dash_table

dash.register_page(__name__, path="/", name="Overview")

layout = dbc.Container([

    dcc.Store(id='cached-data'),
    dcc.Interval(id='interval-component', interval=60_000),

    html.H2("Job Overview", className="my-3"),

    # ---------- FILTERS ----------
    dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col(dcc.Dropdown(id='workflow-name-filter', multi=True, placeholder="Workflow Name"), md=3),
                dbc.Col(dcc.Dropdown(id='job-name-filter', multi=True, placeholder="Job Name"), md=3),
                dbc.Col(dcc.Dropdown(id='status-filter', multi=True, placeholder="Status"), md=3),
                dbc.Col(dcc.Dropdown(id='ert-analysis-filter', multi=True, placeholder="ERT Analysis"), md=3),
                dbc.Col(dcc.Dropdown(id='activator-filter', multi=True, placeholder="Activator"), md=3),
                dbc.Col(dcc.DatePickerRange(id='time-range-filter'), md=3),
            ])
        ])
    ], className="mb-3"),

    # ---------- KPIs ----------
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Total Jobs"), html.H3(id="total-jobs")
        ])), md=4),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Success Rate"), html.H3(id="success-rate",className="text-success")
        ])), md=4),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Failed Jobs"), html.H3(id="failed-jobs", className="text-danger")
        ])), md=4),
    ], className="mb-3"),

    dbc.Row([
        dbc.Col(dcc.Graph(id='status-pie-chart'), md=4),
        dbc.Col(dcc.Graph(id='time-series-chart'), md=8),
    ]),

    dbc.Row([
        dbc.Col(dash_table.DataTable(
            id='job-table',
            page_size=10,
            style_table={'overflowX': 'auto'}
        ))
    ])
], fluid=True)


@dash.callback(
    Output('cached-data', 'data'),
    Input('interval-component', 'n_intervals')
)
def load_data(n):
    df = get_data()
    return df.to_dict('records')



@dash.callback(
    [
        Output('workflow-name-filter', 'options'),
        Output('job-name-filter', 'options'),
        Output('status-filter', 'options'),
        Output('ert-analysis-filter', 'options'),
        Output('activator-filter', 'options')
    ],
    Input('cached-data', 'data')
)
def populate_filters(data):
    df = pd.DataFrame(data)
    return (
        [{'label': i, 'value': i} for i in sorted(df.workflow_name.unique())],
        [{'label': i, 'value': i} for i in sorted(df.object_name.unique())],
        [{'label': i, 'value': i} for i in sorted(df.status_text.unique())],
        [{'label': i, 'value': i} for i in sorted(df['ert_analysis_result'].dropna().unique()) ],
        [{'label': i, 'value': i} for i in sorted(df['activator'].dropna().unique())]
    )


@dash.callback(
    [
        Output('total-jobs', 'children'),
        Output('success-rate', 'children'),
        Output('failed-jobs', 'children'),
        Output('status-pie-chart', 'figure'),
        Output('time-series-chart', 'figure'),
        Output('job-table', 'data'),
        Output('job-table', 'columns')
    ],
    [
        Input('cached-data', 'data'),
        Input('workflow-name-filter', 'value'),
        Input('job-name-filter', 'value'),
        Input('status-filter', 'value'),
        Input('ert-analysis-filter', 'value'),
        Input('activator-filter', 'value'),
        Input('time-range-filter', 'start_date'),
        Input('time-range-filter', 'end_date')
    ]
)
def update_overview(data, workflows, jobs, status,ert, activators, start, end):
    df = pd.DataFrame(data)
    
    
    #df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    
    # Ensure datetime (required after dcc.Store)
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')

    # Drop rows with invalid timestamps (optional but recommended)
    df = df.dropna(subset=['start_time'])

    if workflows:
        df = df[df.workflow_name.isin(workflows)]
    if jobs:
        df = df[df.object_name.isin(jobs)]
    if status:
        df = df[df.status_text.isin(status)]
    if ert:
        df = df[df.ert_analysis_result.isin(ert)]
    if activators:
        df = df[df.activator.isin(activators)]
    if start and end:
        df = df[(df.start_time >= start) & (df.start_time <= end)]

    total = len(df)
    success = len(df[df.status == '1900'])
    failed = total - success
    rate = f"{(success/total)*100:.1f}%" if total else "0%"

    pie = px.pie(df, names='status_text', hole=.3)
    trend = df.set_index('start_time').resample('h').size().reset_index(name='count')
    line = px.line(trend, x='start_time', y='count')

    cols = [{"name": c.replace('_',' ').title(), "id": c}
            for c in ['run_id','workflow_name','object_name','status_text','activator','start_time','end_time','ert_analysis_result','estimated_runtime','runtime']]

    return total, rate, failed, pie, line, df.to_dict('records'), cols