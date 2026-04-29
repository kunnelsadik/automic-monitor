
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent 
sys.path.insert(0, str(PROJECT_ROOT))

import dash
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc

app = Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP]
)

server = app.server  # for deployment

# -------- NAVBAR --------
navbar = dbc.NavbarSimple(
    brand="Job Performance Intelligence & Risk Analytics",
    brand_href="/",
    color="dark",
    dark=True,
    children=[
        dbc.NavItem(dbc.NavLink("Overview", href="/")),
        #dbc.NavItem(dbc.NavLink("By Activator", href="/activator")),
        dbc.NavItem(dbc.NavLink("Alerts", href="/alerts")),
        dbc.NavItem(dbc.NavLink("Scientific Analysis", href="/analysis")),
        dbc.NavItem(dbc.NavLink("Incident Management", href="/incident")),
        dbc.NavItem(dbc.NavLink("Analyse Log", href="/log_analysis_dashboard")),
        dbc.NavItem(dbc.NavLink("Analyse Job", href="/job-analysis")),
        dbc.NavItem(dbc.NavLink("Config Rules", href="/rules_config")),

    ],
)

# -------- APP LAYOUT --------
# app.layout = dbc.Container([
#     dcc.Location(id="url"),
#     navbar,
#     dash.page_container
# ], fluid=True)

app.layout = html.Div([
    dcc.Location(id="url"),
    dcc.Store(id="selected-alert", storage_type="session"),  # ✅ shared
    navbar,
    dash.page_container
])
if __name__ == "__main__":
    app.run(debug=True)
