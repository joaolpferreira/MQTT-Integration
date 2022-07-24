# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash, dcc, html
import plotly
import os
import psycopg2
from collections import deque
from dash.dependencies import Output, Input, State
from requests import put, get
import paho.mqtt.client as mqtt
import time

app = Dash(__name__)
server = app.server


def on_connect(client, data, flags, rc):
    client.subscribe("ISCF/accel_x", 1)
    client.subscribe("ISCF/accel_y", 1)
    client.subscribe("ISCF/accel_z", 1)

def on_message(client, data, msg):
    if(msg.topic =="ISCF/accel_x"):
        X.append(msg.payload)
        TS.append(time.time())
    if(msg.topic =="ISCF/accel_y"):
        Y.append(msg.payload)
    if(msg.topic =="ISCF/accel_z"):
        Z.append(msg.payload)
    
client = mqtt.Client()
client.username_pw_set("uAw2wbUS5tJP3uSGJmIV8T9znhdYaB3Z") # Secret Key is in https://beebotte.com/account#credentials
client.on_connect = on_connect
client.on_message = on_message
client.connect("mqtt.beebotte.com", 1883, 60)

# read database connection url from the environment variable 
DATABASE_URL = os.environ.get('DATABASE_URL')
# create a new database connection by calling the connect() function
con = psycopg2.connect(DATABASE_URL)
# create a new cursor
cur = con.cursor()


X = deque(maxlen = 20)
Y = deque(maxlen = 20)
Z = deque(maxlen = 20)
TS = deque(maxlen = 20)
  
app.layout = html.Div([

    html.H1(
        children='Monitorização de um acelómetro',
        style={
            'textAlign': 'center'
        }
    ),


    html.Div(children=[
        dcc.Graph(id = 'live-graph', animate = True, style={'width': '100%'}),
        dcc.Interval(
            id = 'graph-update',
            interval = 1000,
            n_intervals = 0
        ),
    ]),   

    html.Div(children=[
        html.Div(id='live-update-text'),
        dcc.Interval(
            id='interval-component',
            interval=1*1000, # in milliseconds
            n_intervals=0
        ),
        dcc.Input(id='current-rate', value=10, type='number', min=1, step=1),
        html.Button(id='submit-button', type='submit', children='Submit')
    ])   
])

@app.callback(
    Output('live-update-text', 'children'),
    [Input('submit-button', 'n_clicks')],
    [State('current-rate', 'value')],
)

def update_output(clicks, input_value):           
    if clicks is not None: 
       put('http://127.0.0.1:5000/currentRate', data={'rate': input_value}).json()
    return [
        'Dados atualizados em ' + str(input_value) + ' segundos'
    ]    
    
  

@app.callback(
    Output('live-graph', 'figure'),
    [ Input('graph-update', 'n_intervals') ]
)
  
def update_graph_scatter(n): 
    
    fig = plotly.tools.make_subplots(rows=1, cols=3, vertical_spacing=0.1)

    fig.append_trace({
        'x': list(TS),
        'y': list(X),
        'name': 'Aceleração x',
        'mode': 'lines+markers',
        'type': 'scatter'
    }, 1, 1)
    fig.append_trace({
        'x': list(TS),
        'y': list(Y),
        'name': 'Aceleração y',
        'mode': 'lines+markers',
        'type': 'scatter'
    }, 1, 2)
    fig.append_trace({
        'x': list(TS),
        'y': list(Z),
        'name': 'Aceleração z',
        'mode': 'lines+markers',
        'type': 'scatter'
    }, 1, 3)

    fig.update_xaxes(range=[min(TS),max(TS)])

    return fig
  

if __name__ == '__main__':
    app.run_server(debug=True)
    # close communication with the database            
    cur.close()
    if con is not None:
        con.close()