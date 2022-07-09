import sqlite3 as sql
from app import dash_app
from dash import Dash,Input,Output,html,dcc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import pdb



#Querying all tables from db before app runs
conn = sql.connect('./data/fx_pair.db')
spot_df = pd.read_sql(con=conn,sql='SELECT * FROM spot;',index_col='index')
stats_spot_df = pd.read_sql(con=conn,sql='SELECT \"index\",mean,median,std,\"max\",\"min\",skew,kurtosis,qtyNA,mode,\
\"jarque_bera\",\"durbin_watson\",adf FROM \"spot_eda\";',index_col='index')
fwd_df = pd.read_sql(con=conn,sql='SELECT * FROM fwd;',index_col='index')
stats_fwd_df = pd.read_sql(con=conn,sql='SELECT \"index\",mean,median,std,\"max\",\"min\",skew,kurtosis,qtyNA,mode,\
\"jarque_bera\",\"durbin_watson\",adf FROM \"fwd_eda\";',index_col='index')
container_dd = {'spot':spot_df,'fwd':fwd_df}
container_stats_dd = {'spot':stats_spot_df,'fwd':stats_fwd_df}

dash_app.title = 'Dashboard | EDA'

dash_app.layout = html.Div([html.H1(children=[html.B('Dashboard')],className='header',id='page'),\
                            html.Div([html.P('Exploratory Data Analysis'),\
                                      html.Br(),\
                                      dcc.RadioItems(['spot', 'fwd'],'spot',inline=True,id='instrument_type'),\
                                      dcc.Graph(id='stats')])])

@dash_app.callback(
    Output(component_id='stats',component_property='figure'),
    Input(component_id='instrument_type',component_property='value')
)
def stats_table(value):
    df = container_stats_dd[value]
    df[['mean','median','std','max','min','skew','kurtosis','qtyNA','mode','jarque_bera',\
        'durbin_watson','adf']] = df[['mean','median','std','max','min','skew','kurtosis','qtyNA','mode','jarque_bera',\
        'durbin_watson','adf']].astype(float).round(4)
    df.index.name = None
    df = df.transpose().reset_index().rename(columns={'index':'metrics'})
    table_stats = go.Figure(data=[go.Table(header=dict(values=list(df.columns),align='left'),\
                                           cells=dict(values=df.transpose().values,align='left',height=20),\
                                           columnwidth = [150]+[80]*(df.shape[1]-1))])
    table_stats.update_layout(margin={'t':10,'b':10},height=250)
    return table_stats