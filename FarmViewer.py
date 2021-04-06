import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import socket, json
import datetime
import plotly.graph_objs as go

pollPeriod = 10
WD_ip = "192.168.2.22"
WD_port = 6116
ClayMon_ip = "192.168.2.22"
ClayMon_port = 6111
maxSamples = 400

app = dash.Dash(__name__)
app.title='Watchdog web runner'

app.css.config.serve_locally = True
app.scripts.config.serve_locally = True

#app.config.supress_callback_exceptions = True

def GimmeRigFrame(rigNum):
    rigN = str(rigNum)
    rigName = "M%02d"%rigNum
    frame = html.Div([
        html.H1(rigName, id='miner caption '+rigN),
        html.Div([
        html.Button('Reboot', id='reboot button '+rigN, style=dict(margin=10, width=100)),
        html.H1('199.99 Mh/sec', id='hashrate '+rigN, style=dict(margin=10, width=300)),
        html.Button('Reset', id='reset button '+rigN, style=dict(margin=10, width=100)),
        ],
        className="row"),],
    className='col',
    style={'border': '1px solid', 'border-radius': 4, 'margin': 10},
    )
    return frame, rigName
rigs = {}
rigNums = range(10)
ch2 = []
for i in rigNums:
    frame, rigName = GimmeRigFrame(i)
    rigs[rigName] = {"Num": i, "HashrateOutputId": 'hashrate '+str(i)}
    ch2.append(frame)

ch1 = [
    html.Div(ch2[:5], className='col-md-5'),
    html.Div(ch2[5:], className='col-md-5'),]

layout = html.Div([
                html.H1('H1', id='h1-1-id'),
                html.H1('H1', id='h1-2-id'),
                html.Div(dcc.Graph(id='graph-field'), style={'margin-top': 20}),
                html.Div(ch1, className='row'),
                dcc.Interval('interval-id', interval=pollPeriod*1000)],
            className='col',)
app.layout = layout

rebootInputs = []
resetInputs = []
for i in rigNums:
    btn_id = 'reboot button '+str(i)
    rebootInputs.append(Input(btn_id, 'n_clicks'))
    btn_id = 'reset button '+str(i)
    resetInputs.append(Input(btn_id, 'n_clicks'))


rebootclicks, resetclicks = None, None

def GimmeDiff(clicks1, clicks2):
    for i, (c1, c2) in enumerate(zip(clicks1, clicks2)):
        if (c1 is None) and (c2 is None):
            continue
        if (c1 is None) != (c2 is None):
            return i
        if c1-c2 in [-1, 1]:
            return i

def sendReset(rig):
    rigs = [rig]
    buff = json.dumps({"Command": "Rig", "Data": rigs})
    sock = socket.socket()
    sock.settimeout(1)
    try:
        sock.connect((WD_ip, WD_port))
        sock.send(buff.encode('utf-8'))
    except:
        print("Watchdog server script is offline")

def sendReboot(rig):
    ip = '192.168.2.1%02d'%rig
    port = 3333
    sock = socket.socket()
    sock.settimeout(1)
    try:
        sock.connect((ip, port))
        buff = json.dumps({"id": 0, "jsonrpc": "2.0", "method": "miner_reboot"})
        sock.send(buff.encode('utf-8'))
    except:
        print("Watchdog server script is offline")

rez = dict(lastUpdate=datetime.datetime.now())
def TakeStateJsonAndUpdateRigs():
    global rez
    if (datetime.datetime.now() - rez['lastUpdate']).total_seconds() > 0.5*pollPeriod:
        # f = open('state.json', 'r')
        # strs = f.readlines()
        # data = json.loads(strs[0])
        rez['lastUpdate'] = datetime.datetime.now()
        ip = ClayMon_ip
        port = ClayMon_port
        buff = json.dumps({"Command": "State", "Data" : []})
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            print('Try to connect')
            sock.connect((ip, port))
            print('Connected to ', ip, port, 'trying to send', buff)
            sock.send(buff.encode('utf-8'))
            print('Command sended, try to recieve')
            dta = sock.recv(100000)
            print('Recieved', len(dta), 'bytes')
        except:
            print("Watchdog server script is offline")
            return rez
        udata = dta.decode("utf-8")
        try:
            data = json.loads(udata)
        except:
            print("Recieved packet is broken")
            return rez

        if data is None:
            return rez
        for list1 in data:
            for list2 in list1:
                # rez[list2[0]] = list2[1]
                print(list2)
#                return rez
#                print(list2[1])
                rez[list2[0]] = 0
                if list2[1] and 'result' in list2[1]:
                    answer = list2[1]['result']
                    list3 = answer[2].split(';')
                    hr = int(list3[0]) / 1000.0
                    rez[list2[0]] = hr
    return rez

@app.callback(
    output=Output('h1-1-id', 'children'),
    inputs=rebootInputs)
def process_reboot_clicks(*args, **kwargs):
    global rebootclicks
    if rebootclicks is None:
        rebootclicks = args
        return
    N = GimmeDiff(rebootclicks, args)
    rebootclicks = args
    sendReboot(N)
    return 'reboot '+str(N)


@app.callback(
    output=Output('h1-2-id', 'children'),
    inputs=resetInputs)
def process_reset_clicks(*args, **kwargs):
    global resetclicks
    if resetclicks is None:
        resetclicks = args
        return
    N = GimmeDiff(resetclicks, args)
    resetclicks = args
    sendReset(N)
    return 'reset '+str(N)

datapool = None

@app.callback(
    Output('graph-field', 'figure'),
    inputs = [Input('interval-id', 'n_intervals')],
)
def _graph_field(nint):
    global datapool, rez
    if datapool == None:
        datapool = {}
        for rigName in rigs:
            datapool[rigName] = [0]*maxSamples

    data = []
    Xs = [datetime.datetime.now() - datetime.timedelta(seconds=10*x) for x in range(0, maxSamples)]
    names = list(datapool.keys())
    names.sort()
    for rigName in names:
        Ys = datapool[rigName]
        Ys.pop()
        hr = 0
        if rigName in rez:
            hr = rez[rigName]
        Ys.insert(0, hr)
        trace = dict(
            x=Xs,
            y=Ys,
            name=rigName,
            mode='lines',
            # line=dict(width=0.5,
            #           color='rgb(184, 247, 212)'),
            stackgroup='one',
            # groupnorm='percent'
        )
        data += [trace]
    fig = go.Figure(data=data)
    return fig

def generate_interval_update_rig(rig, name):
    def process_interval_tick_for_one_rig(nint):
        data = TakeStateJsonAndUpdateRigs()
        if data and name in data:
            return '%.1f Mh/s'%data[name]
        else:
            return 'Offline'
    return process_interval_tick_for_one_rig

for name in rigs.keys():
    rig = rigs[name]
    app.callback(
        Output(rig['HashrateOutputId'], 'children'), [Input('interval-id', 'n_intervals')]
    )(generate_interval_update_rig(rig, name))

if __name__ == '__main__':
    # app.run_server()
    app.run_server(debug=True)
