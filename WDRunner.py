import socket, json, sqlite3
WD_ip = "192.168.2.22"
WD_port = 6116
ClayMon_ip = "192.168.2.22"
ClayMon_port = 6111

def CheckConfigs(wrks):
    ws = []
    if wrks[0] == "All" or wrks[0] == "all":
        connection = sqlite3.connect("Claymon.sqlite")
        cursor = connection.cursor()
        sql = "SELECT DISTINCT Worker FROM Workers WHERE Rig >= 0"
        cursor.execute(sql)
        for row in cursor.fetchall():
            ws.append(row[0])
    else:
        for w in wrks:
            ws.append(w)

    dbWs = {}
    for wname in ws:
        widx = 'ABCDEFGH'.find(wname[-1])
        wnum = wname[1:]
        port = 3333
        if widx >= 0:
            wnum = wname[1:-1]
            port += widx
        ip = '192.168.2.1' + wnum
        wrk = {}
        wrk["Port"] = port
        wrk["ip"] = ip
        wrk["letter"] = ""
        if widx >= 0:
            wrk["letter"] = 'ABCDEFGH'[widx]
        dbWs[wname] = wrk
    for wname in ws:
        wrk = dbWs[wname]
        port = wrk["Port"]
        ip = wrk["ip"]
        # {"id": 0, "jsonrpc": "2.0", "method": "miner_getfile", "params": ["config.txt"]}
        buff = json.dumps({"id": 0, "jsonrpc" : "2.0", "method" : "miner_getfile", "params" : ["config%s.txt"%wrk["letter"]]})
        sock = socket.socket()
        sock.settimeout(2)
        try:
            sock.connect((ip, port))
            sock.send(buff.encode('utf-8'))
            data = sock.recv(5000)
        except socket.error as msg:
            sock.close()
            sock = None
            continue
        udata = data.decode("utf-8")
        try:
            resp = json.loads(udata)
            rez = resp["result"]
            conf = bytearray.fromhex(rez[1]).decode().split()
            i1 = conf.index("-ewal")
            i2 = conf.index("-epool")
        except:
            print("udata = ", udata)
            continue
        try:
            i3 = conf.index("-eworker")
        except:
            i3 = -1
            print(i3)
        if i3 >= 0:
            print(wname, "ewal =", conf[i1+1], "epool =", conf[i2+1], "eworker =", conf[i3+1])
        else:
            print(wname, "ewal =", conf[i1+1], "epool =", conf[i2+1])

    return None

def UpdateGPU(data):
    gpu = data[0]
    rig = data[1]
    BN = data[2]
    if not BN in (1, 2, 3, 5, 6, 7, 8, 10):
        print("Wrong bus number = ", BN)
        return

    conn = sqlite3.connect('Claymon.sqlite')
    cursor = conn.cursor()
    sql = "Select * From Rigs Where RigNum = {0}".format(rig)
    cursor.execute(sql)
    ttt = cursor.fetchall()
    if len(ttt) < 1:
        print("The database does not contain rig number ", rig)
        return

    cursor = conn.cursor()
    sql = "Select * From GPUs Where Num = {0}".format(gpu)
    cursor.execute(sql)
    ttt = cursor.fetchall()
    if len(ttt) < 1:
        print("The database does not contain GPU number ", gpu)
        return

    sql = "UPDATE GPUs SET Rig = -1 WHERE Rig = {0} And BusNum = {1}".format(rig, BN)
    conn.execute(sql)
    conn.commit()

    sql = "UPDATE GPUs SET Rig = {0}, BusNum = {1} WHERE Num = {2}".format(rig, BN, gpu)
    conn.execute(sql)
    conn.commit()

    print("Update complete")
    return

while 1==1:
    print("Watchdog API commands: Start, Stop, Exit, Rig [numbers], Pin [numbers], Test [number], Switch")
    print("Additional commands : Configs [numbers|all], CheckPins, TempPin [pin, rig], UpdateGPU [gpu, rig, BN], UploadGPU [filename], Report [hours]")
    print("Enter command")
    inp = input().split()
    command = inp[0].lower()

    ip = WD_ip
    port = WD_port

    if command == "start":
        buff = json.dumps({"Command": "Start"})
    elif command == "stop":
        buff = json.dumps({"Command": "Stop"})
    elif command == "exit":
        buff = json.dumps({"Command": "Exit"})
    elif command == "checkpins":
        buff = json.dumps({"Command": "CheckPins"})
    elif command == "rig":
        rigs = inp[1:]
        if len(inp) < 2:
            print("Enter rig numbers for reset:")
            rigs = input().split()
        buff = json.dumps({"Command": "Rig", "Data" : rigs})
        print(buff)
    elif command == "pin":
        pins = inp[1:]
        if len(inp) < 2:
            print("Enter pin number for reset:")
            pins = input().split()
        buff = json.dumps({"Command": "Pin", "Data" : pins})
    elif command == "test":
        if len(inp) < 2:
            print("Enter arduino port number for test:")
            testport = input()
        else:
            testport = inp[1]
        buff = json.dumps({"Command": "TestPort", "Data" : testport})
    elif command == "switch":
        buff = json.dumps({"Command": "Switch"})
    elif command == "configs":
        wrks = inp[1:]
        if len(inp) < 2:
            print("Enter workers for check congigs:")
            wrks = input().split()
        CheckConfigs(wrks)
        continue
    elif command == "temppin":
        if len(inp) < 3:
            print("Enter pin number and rig number:")
            nums = input().split()
        else:
            nums = inp[1:]
        buff = json.dumps({"Command": "TempPin", "Data" : nums})
    elif command == "updategpu":
        ip = ClayMon_ip
        port = ClayMon_port
        if len(inp) < 4:
            print("Enter GPU number, new rig number and new BN (bus number):")
            inp = input().split()
        try:
            g = int(inp[1])
            r = int(inp[2])
            bn = int(inp[3])
            UpdateGPU([g, r, bn])
        except:
            print("Wrong input")
            continue
    elif command == "uploadgpus":
        fname = inp[1]
    elif command == "report":
        ip = ClayMon_ip
        port = ClayMon_port
        period = 24
        if len(inp) >= 2:
            period = int(inp[1])
        buff = json.dumps({"Command": "Report", "Data" : period})
    elif command == "state":
        ip = ClayMon_ip
        port = ClayMon_port
        buff = json.dumps({"Command": "State", "Data" : []})
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        try:
            sock.connect((ip, port))
            sock.send(buff.encode('utf-8'))
            data = sock.recv(100000)
            udata = data.decode("utf-8")
            print(udata)
#            f4 = open('state.json', 'w')
#            f4.write(udata)
#            f4.close()
            continue
        except:
            print("Watchdog server script is offline")
            continue

    else:
        print("Unknown command")
        continue

    sock = socket.socket()
    sock.settimeout(1)
    try:
        sock.connect((ip, port))
        sock.send(buff.encode('utf-8'))
    except:
        print("Watchdog server script is offline")

