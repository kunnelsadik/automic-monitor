import dash
from dash import html, dcc
import plotly.express as px
import pandas as pd
from data.data_loader import get_data

dash.register_page(__name__, path="/activator", name="By Activator")

df = get_data()
fig = px.bar(
    df.groupby('activator').size().reset_index(name='jobs'),
    x='activator',
    y='jobs',
    title='Jobs by Activator'
)

layout = html.Div([
    html.H2("Jobs by Activator"),
    dcc.Graph(figure=fig)
])