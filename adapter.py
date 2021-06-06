import urllib.request
import json
import threading
import socket
import os

# miner_url = "http://192.168.0.20:3333/stat"
miner_url = "http://127.0.0.1:4444/stat"
pollPeriod = 10

example_input = ''' 
            {
            "miner": "GMiner 2.33",
            "uptime": 19566,
            "server": "eu.btgpool.pro:1445",
            "user": "GZdx44gPVFX7GfeWXA3kyiuXecym3CWGHi.rig0",
            "algorithm": "Equihash 144,5 \"BgoldPoW\"",
            "electricity": 0.000,
            "total_accepted_shares": 0,
            "total_rejected_shares": 0,
            "devices": [{
                "gpu_id": 0,
                "bus_id": "0000:01:00.0",
                "name": "MSI GeForce GTX 1050 Ti 4GB",
                "speed": 0.1,
                "accepted_shares": 0,
                "rejected_shares": 0,
                "temperature": 55,
                "temperature_limit": 90,
                "power_usage": 0
            }],
            "speed_rate_precision": 1,
            "speed_unit": "Sol/s",
            "power_unit": "Sol/W"
            }    
        '''

example_output = '''
        REQUEST:
        {"id":0,"jsonrpc":"2.0","method":"miner_getstat1"}
    
        RESPONSE:
        {"result": ["9.3 - ETH", "21", "182724;51;0", "30502;30457;30297;30481;30479;30505", "0;0;0", "off;off;off;off;off;off", "53;71;57;67;61;72;55;70;59;71;61;70", "eth-eu1.nanopool.org:9999", "0;0;0;0"]}
        "9.3 - ETH"             - miner version.
        "21"                    - running time, in minutes.
        "182724"                - total ETH hashrate in MH/s, number of ETH shares, number of ETH rejected shares.
        "30502;30457;30297;30481;30479;30505"   - detailed ETH hashrate for all GPUs.
        "0;0;0"                 - total DCR hashrate in MH/s, number of DCR shares, number of DCR rejected shares.
        "off;off;off;off;off;off"       - detailed DCR hashrate for all GPUs.
        "53;71;57;67;61;72;55;70;59;71;61;70"   - Temperature and Fan speed(%) pairs for all GPUs.
        "eth-eu1.nanopool.org:9999"     - current mining pool. For dual mode, there will be two pools here.
        "0;0;0;0"               - number of ETH invalid shares, number of ETH pool switches, number of DCR invalid shares, number of DCR pool switches.
        
        REQUEST:
        {"id":0,"jsonrpc":"2.0","method":"miner_getstat2"}
        
        RESPONSE:
        Same as "miner_getstat1" but also appends additional information:
        
        - ETH accepted shares for every GPU.
        - ETH rejected shares for every GPU.
        - ETH invalid shares for every GPU.
        - DCR accepted shares for every GPU.
        - DCR rejected shares for every GPU.
        - DCR invalid shares for every GPU.
        - PCI bus index for every GPU.
        - min/max/average time (in ms) of accepting shares for current pool (1 hour).
        - total power consumption of GPUs.
    '''


def make_stat1(input_json):
    pass

def make_stat2(input_json):
    inp = input_json
    if inp is None:
        return ''
    try:
        hrs, bns, ts = [], [], []
        gpus_acptd, gpus_rejctd = [], []
        total_hr = 0
        total_power = 0
        power = []
        dev_cnt = len(inp['devices'])
        for d in inp['devices']:
            hrs += [d['speed']//1000]
            bns += [d['bus_id'].split(':')[1]]
            ts += [d['temperature'], 0]
            total_hr += d['speed']//1000
            gpus_acptd += [d['accepted_shares']]
            gpus_rejctd += [d['rejected_shares']]
            power += [d['power_usage']]
            total_power += d['power_usage']

        l = []
        l += [inp['miner']]
        l += [f"{inp['uptime'] // 60}"]
        l += [';'.join([str(total_hr), str(inp['total_accepted_shares']), str(inp['total_rejected_shares'])])]
        l += [';'.join(map(str, hrs))]
        l += ['0;0;0']
        l += [';'.join(['off']*dev_cnt)]
        l += [';'.join(map(str, ts))]
        l += [inp['server']]
        l += ["0;0;0;0"]
        l += [';'.join(map(str, gpus_acptd))]
        l += [';'.join(map(str, gpus_rejctd))]
        l += [';'.join(['0']*dev_cnt)]
        l += [';'.join(['0']*dev_cnt)]
        l += [';'.join(['0']*dev_cnt)]
        l += [';'.join(['0']*dev_cnt)]
        l += [';'.join(bns)]
        l += ['0;0;0']
        # l += [';'.join(map(str, power))]
        l += [str(total_power)]

        rez = dict(result=l)
        return rez
    except:
        return ''



def poll_stat(event_for_wait, event_for_set):
    global last_stat, Have_to_exit
    while not Have_to_exit:
        try:
            contents = urllib.request.urlopen(miner_url).read()
            udata = contents.decode("utf-8")
            last_stat = json.loads(udata)
        except:
            last_stat = None

        event_for_wait.wait()
        event_for_wait.clear()
        event_for_set.set()



def give_stat(event_for_wait, event_for_set):
    global last_stat, Have_to_exit

    try:
        sock = socket.socket()
        sock.bind(("", 3333))
        sock.listen(1)
    except:
        Have_to_exit = False

    while not Have_to_exit:
        event_for_wait.wait()
        event_for_wait.clear()
        sock.settimeout(pollPeriod)
        data = None
        conn, addr = sock.accept()
        try:
            request = conn.recv(1000)
            if request:
                udata = request.decode("utf-8")
                packet = json.loads(udata)
                if packet["method"] == "miner_getstat1":
                    responce = make_stat1(last_stat)
                elif packet["method"] == "miner_getstat2":
                    responce = make_stat2(last_stat)
                elif packet["method"] in ("miner_restart", "miner_reboot"):
                    os.system("shutdown /r /f /t 1")
                else:
                    responce = {'{EQ': -1}
                buff = json.dumps(responce)
                conn.sendall(buff.encode('utf-8'))
        except socket.error as msg:
            pass
        finally:
            conn.close()

        event_for_set.set()

Have_to_exit = False
thrCnt = 2
threads = []
events = []
statspool = []
laststat = None
for i in range(thrCnt):
    events.append(threading.Event())
e2 = events[thrCnt-1]
for i in range(thrCnt):
    e1 = e2
    e2 = events[i]
    if i == 0:
        threads.append(threading.Thread(target=poll_stat, args=(e1, e2)))
    if i == 1:
        threads.append(threading.Thread(target=give_stat, args=(e1, e2)))

for i in range(thrCnt):
    threads[i].start()

events[thrCnt-1].set()
