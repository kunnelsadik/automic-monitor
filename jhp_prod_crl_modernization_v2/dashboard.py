import dash
from dash import dcc, html, dash_table, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import pyodbc
import plotly.express as px
from datetime import datetime, timedelta, timezone
import re


def ordinal_days_nanos_to_datetime(days, nanoseconds):
    base = datetime(1, 1, 1)
    return base + timedelta(days=days, seconds=nanoseconds / 1_000_000_000)
 

def decode_access_extended_bytes(byte_data):
    """
    Decodes MS Access Date/Time Extended bytes specifically for:
    b'0000000000000739694:0000000258140000000:7\x00'
    Target: 2026-03-20 07:10:14
    """
    try:
        if byte_data:
            # 1. Clean and Split
            clean_str = byte_data.decode('utf-8').strip('\x00')
            parts = clean_str.split(':')
            
            # Part 0 is the "Tick" count
            days = int(parts[0]) 
            # Part 1 is the high-precision nanoseconds into the day
            nanoseconds_today = int(parts[1])  *100
            
            # 2. Define the MS Access Base Epoch (1899-12-30)
            base_epoch = datetime(1, 1, 1)

            # 3. Calculation:                        
           
            ts =  ordinal_days_nanos_to_datetime(days,nanoseconds_today)
            return ts
        else:
            return None
 
    except Exception as e:
        return f"Error: {e}"

# --- 1. Database Connection ---
def get_data():
    
    db_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_support_modern.accdb"
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file};"
    with pyodbc.connect(conn_str) as conn:
        # Use CStr to handle the 'Extended' timestamp issue we discussed
        query = "SELECT * FROM job_stats"
        df = pd.read_sql(query, conn)
    
    # Clean data
    #df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df['start_time'] = df['start_time'].apply(decode_access_extended_bytes)
    df['end_time'] = df['end_time'].apply(decode_access_extended_bytes)
    #df = df.dropna(subset=['start_time']) # Remove invalid dates
    return df

# --- 2. Initialize Dash App with a Clean Theme ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

# --- 3. App Layout (Dashboard Structure) ---
# app.layout = dbc.Container([
#     dbc.Row([
#         dbc.Col(html.H1("Prod Job Monitoring Dashboard", className="text-center my-4"), width=12)
#     ]),

#     # KPI Cards Row
#     dbc.Row([
#         dbc.Col(dbc.Card([
#             dbc.CardBody([
#                 html.H4("Total Jobs", className="card-title"),
#                 html.H2(id="total-jobs", className="text-primary")
#             ])
#         ]), width=4),
#         dbc.Col(dbc.Card([
#             dbc.CardBody([
#                 html.H4("Success Rate", className="card-title"),
#                 html.H2(id="success-rate", className="text-success")
#             ])
#         ]), width=4),
#         dbc.Col(dbc.Card([
#             dbc.CardBody([
#                 html.H4("Failed Jobs", className="card-title"),
#                 html.H2(id="failed-jobs", className="text-danger")
#             ])
#         ]), width=4),
#     ], className="mb-4"),

#     # Filters and Charts
#     dbc.Row([
#         dbc.Col([
#             html.Label("Filter by Status:"),
#             dcc.Dropdown(
#                 id='status-filter',
#                 options=[{'label': i, 'value': i} for i in ['ENDED_OK - ended normally','ENDED_NOT_OK - aborted' ,'ENDED_CANCEL - manually canceled' ,'ENDED_INACTIVE - Task was manually set inactive.' ,'ENDED_INACTIVE_OBJECT - Object is inactive due to definition.' ,'ENDED_JP_CANCEL - Workflow canceled manually.'  ,'Sleeping' ,'Waiting for predecessor']],
#                 multi=True,
#                 placeholder="Select Status..."
#             ),
#             dcc.Graph(id='status-pie-chart')
#         ], width=4),
#         dbc.Col([
#             dcc.Graph(id='time-series-chart')
#         ], width=8),
#     ]),

#     # Data Table
#     dbc.Row([
#         dbc.Col([
#             html.H3("Recent Job Details", className="mt-4"),
#             dash_table.DataTable(
#                 id='job-table',
#                 page_size=10,
#                 style_table={'overflowX': 'auto'},
#                 style_cell={'textAlign': 'left', 'padding': '10px'},
#                 style_header={'backgroundColor': '#2c3e50', 'color': 'white', 'fontWeight': 'bold'}
#             )
#         ], width=12)
#     ]),
    
#     # # --- TAB NAVIGATION ---
#     # dcc.Tabs(id="tabs-navigation", value='tab-overview', children=[
#     #     dcc.Tab(label='General Overview', value='tab-overview', className="custom-tab"),
#     #     dcc.Tab(label='Stats by Activator', value='tab-activator', className="custom-tab"),
#     # ]),

#     # # This div will be populated dynamically by the callback
#     # html.Div(id='tabs-content'),

    
#     dcc.Interval(id='interval-component', interval=60*1000, n_intervals=0) # Auto-refresh every minute
# ], fluid=True)

#new code
app.layout = dbc.Container([

    # ------------------ TITLE ------------------
    dbc.Row([
        dbc.Col(
            html.H1("Prod Job Monitoring Dashboard", className="text-center my-4"),
            width=12
        )
    ]),

    # ------------------ KPI CARDS ------------------
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4("Total Jobs", className="card-title"),
            html.H2(id="total-jobs", className="text-primary")
        ])), width=4),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4("Success Rate", className="card-title"),
            html.H2(id="success-rate", className="text-success")
        ])), width=4),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4("Failed Jobs", className="card-title"),
            html.H2(id="failed-jobs", className="text-danger")
        ])), width=4),
    ], className="mb-4"),

    # ------------------ FILTERS ------------------
    dbc.Card([
        dbc.CardBody([
            dbc.Row([

                # Job Name Filter
                dbc.Col([
                    html.Label("Job Name"),
                    dcc.Dropdown(
                        id='job-name-filter',
                        multi=True,
                        placeholder="Select Job Name(s)"
                    )
                ], md=3),

                # Status Filter
                dbc.Col([
                    html.Label("Status"),
                    dcc.Dropdown(
                        id='status-filter',
                        options=[
                            {'label': i, 'value': i} for i in [
                                'ENDED_OK - ended normally',
                                'ENDED_NOT_OK - aborted',
                                'ENDED_CANCEL - manually canceled',
                                'ENDED_INACTIVE - Task was manually set inactive.',
                                'ENDED_INACTIVE_OBJECT - Object is inactive due to definition.',
                                'ENDED_JP_CANCEL - Workflow canceled manually.',
                                'Sleeping',
                                'Waiting for predecessor'
                            ]
                        ],
                        multi=True,
                        placeholder="Select Status"
                    )
                ], md=3),

                # Activator Filter
                dbc.Col([
                    html.Label("Activator"),
                    dcc.Dropdown(
                        id='activator-filter',
                        multi=True,
                        placeholder="Select Activator(s)"
                    )
                ], md=3),

                # Time Range Filter
                dbc.Col([
                    html.Label("Start / End Time"),
                    dcc.DatePickerRange(
                        id='time-range-filter',
                        display_format='YYYY-MM-DD',
                        start_date_placeholder_text="Start Date",
                        end_date_placeholder_text="End Date",
                        minimum_nights=0
                    )
                ], md=3),

            ])
        ])
    ], className="mb-4"),

    # ------------------ CHARTS ------------------
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='status-pie-chart')
        ], width=4),

        dbc.Col([
            dcc.Graph(id='time-series-chart')
        ], width=8),
    ]),

    # ------------------ DATA TABLE ------------------
    dbc.Row([
        dbc.Col([
            html.H3("Recent Job Details", className="mt-4"),
            dash_table.DataTable(
                id='job-table',
                page_size=10,
                style_table={'overflowX': 'auto'},
                style_cell={
                    'textAlign': 'left',
                    'padding': '10px',
                    'whiteSpace': 'normal'
                },
                style_header={
                    'backgroundColor': '#2c3e50',
                    'color': 'white',
                    'fontWeight': 'bold'
                }
            )
        ], width=12)
    ]),

    # ------------------ AUTO REFRESH ------------------
    dcc.Interval(
        id='interval-component',
        interval=60 * 1000,  # 1 minute
        n_intervals=0
    )

], fluid=True)

# @app.callback(
#     Output('tabs-content', 'children'),
#     [Input('tabs-navigation', 'value'),
#      Input('interval-component', 'n_intervals')]
# )
# def render_content(tab, n):
#     df = get_data() # Using your existing data fetch logic

#     if tab == 'tab-overview':
#         # --- (Existing Overview Content: Cards + Pie Chart + Time Series) ---
#         return html.Div([
#             dbc.Row([
#                 dbc.Col(dcc.Graph(id='status-pie-chart', figure=create_status_pie(df)), width=4),
#                 dbc.Col(dcc.Graph(id='time-series-chart', figure=create_time_series(df)), width=8),
#             ])
#         ])

#     elif tab == 'tab-activator':
#         # --- NEW ACTIVATOR STATS TAB ---
#         # 1. Group by Activator (Who started the job)
#         activator_counts = df['activator'].value_counts().reset_index()
#         activator_counts.columns = ['activator', 'count']

#         # 2. Bar Chart for Activators
#         fig_bar = px.bar(
#             activator_counts, 
#             x='activator', 
#             y='count', 
#             title='Jobs Triggered per Activator',
#             color='count',
#             color_continuous_scale='Blues'
#         )
        
#         # 3. Success Rate by Activator
#         success_by_act = df.groupby('activator')['status'].apply(
#             lambda x: (x == 'ENDED_OK').mean() * 100
#         ).reset_index(name='success_rate')

#         fig_success = px.bar(
#             success_by_act, 
#             x='activator', 
#             y='success_rate', 
#             title='Success % by Activator',
#             labels={'success_rate': 'Success %'}
#         )
#         fig_success.update_traces(marker_color='mediumseagreen')

#         return html.Div([
#             dbc.Row([
#                 dbc.Col(dcc.Graph(figure=fig_bar), width=6),
#                 dbc.Col(dcc.Graph(figure=fig_success), width=6),
#             ], className="mt-4"),
            
#             # Detailed Table for Activator runs
#             dbc.Row([
#                 dbc.Col([
#                     html.H4("Detailed Activator Logs", className="mt-4"),
#                     dash_table.DataTable(
#                         data=df[['activator', 'job_name', 'status', 'start_time_str']].to_dict('records'),
#                         columns=[{"name": i, "id": i} for i in ['activator', 'job_name', 'status', 'start_time_str']],
#                         page_size=10,
#                         style_table={'overflowX': 'auto'}
#                     )
#                 ])
#             ])
#         ])

# @app.callback(
#     Output('tabs-content', 'children'), # This now controls the WHOLE page body
#     [Input('tabs-navigation', 'value'),
#      Input('status-filter', 'value'),   # Keep your existing filters
#      Input('interval-component', 'n_intervals')]
# )
def update_dashboard(active_tab, selected_status, n):
    # 1. Get and Clean Data (Same as before)
    df = get_data() 
    
    # 2. Apply your existing status filter logic
    if selected_status:
        df = df[df['status'].isin(selected_status)]

    # --- 3. SWITCH CONTENT BASED ON TAB ---
    if active_tab == 'tab-overview':
        # Move your existing Overview Logic here
        return html.Div([
            dbc.Row([
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.H4("Total Jobs", className="card-title"),
                        html.H2(len(df), className="text-primary")
                    ])
                ]), width=4),
                # ... Add your other KPI cards here ...
            ], className="mb-4"),

            dbc.Row([
                dbc.Col(dcc.Graph(figure=create_status_pie(df)), width=4),
                dbc.Col(dcc.Graph(figure=create_time_series(df)), width=8),
            ]),
            
            # Your existing Data Table
            dbc.Row([
                dbc.Col(dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[{"name": i, "id": i} for i in df.columns],
                    page_size=10
                ), width=12)
            ])
        ])

    elif active_tab == 'tab-activator':
        # Add the NEW Activator Logic here
        activator_counts = df['activator'].value_counts().reset_index()
        activator_counts.columns = ['activator', 'count']
        
        fig_bar = px.bar(activator_counts, x='activator', y='count', title='Jobs by Activator')
        
        return html.Div([
            dbc.Row([
                dbc.Col(dcc.Graph(figure=fig_bar), width=12),
            ]),
            # Add any activator-specific tables here
        ])
    
# --- Helper Functions for existing charts ---
def create_status_pie(df):
    fig = px.pie(df, names='status_text', hole=0.3)
    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, font=dict(size=10)),
        margin=dict(l=20, r=20, t=50, b=100) # Prevents overlap
    )
    return fig

def create_time_series(df):
    # (Same resample logic as before)
    df_trend = df.set_index('start_time').resample('h').size().reset_index(name='counts')
    return px.line(df_trend, x='start_time', y='counts', title='Job Volume')


# --- 4. Callbacks (Interactivity) ---
@app.callback(
    [Output('total-jobs', 'children'),
     Output('success-rate', 'children'),
     Output('failed-jobs', 'children'),
     Output('status-pie-chart', 'figure'),
     Output('time-series-chart', 'figure'),
     Output('job-table', 'data'),
     Output('job-table', 'columns')],
    [Input('status-filter', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_dashboard_ol(selected_status, n):
    df = get_data()
    print(df)
    print(selected_status)
    # --- 3. SWITCH CONTENT BASED ON TAB ---
    #if active_tab == 'tab-overview':
    # Apply Filter
    if selected_status:
        filtered_df = df[df['status_text'].isin(selected_status)]
    else:
        filtered_df = df

    # Calculate KPIs
    total = len(df)
    success = len(df[df['status'] == '1900'])
    failed = len(df[df['status'] != '1900'])
    rate = f"{(success/total)*100:.1f}%" if total > 0 else "0%"

    # Pie Chart
    pie_fig = px.pie(df, names='status_text', title='Job Status Distribution', hole=.3)
    # 1. Reduce Legend Font and Move it to the Bottom
    pie_fig.update_layout(
        legend=dict(
            orientation="h",       # Horizontal legend
            yanchor="top",
            y=-0.2,                # Push legend below the chart
            xanchor="center",
            x=0.5,
            font=dict(size=10)     # Smaller font size
        ),
        # 2. Reduce Margins to increase Chart Area
        margin=dict(l=20, r=20, t=50, b=20),
        height=500                 # Set a fixed height
    )

    # 3. Optional: Move text labels inside the slices to save even more space
    # pie_fig.update_traces(
    #     textposition='inside', 
    #     textinfo='percent+label'
    # )
    # Time Series (Trend)
    # Grouping by hour to see job volume
    df_trend = filtered_df.set_index('start_time').resample('h').size().reset_index(name='counts')
    line_fig = px.line(df_trend, x='start_time', y='counts', title='Job Volume Over Time (Hourly)')

    # Table Data
    table_cols = [{"name": i, "id": i} for i in ['run_id', 'object_name', 'object_type','status_text', 'activator']]
    table_data = filtered_df.sort_values('start_time', ascending=False).to_dict('records')
    print(f"total job -{total}")
    print(f"total job -{success}")
    return total, rate, failed, pie_fig, line_fig, table_data, table_cols

if __name__ == '__main__':
    app.run(debug=True)