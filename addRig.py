import socket, json, sqlite3, threading, time
from datetime import datetime

pollPeriod = 10
RegCount = 12
WaitingForBooyUpTimeout = 400

def watchDog(event_for_wait, event_for_set):
    i = 0
    WaitingForBootUp = 0
    Watching = 1
    rigsToReset = []
    State = WaitingForBootUp
    WaitingStart = i
    while 0 == 0:
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 1")
        event_for_wait.wait()
        event_for_wait.clear()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 2")
        i += 1
        if State == WaitingForBootUp:
            print('.......................watch dog waiting for bootup', rigsToReset, ' ,',WaitingForBooyUpTimeout - (i-WaitingStart-1)*pollPeriod, 'seconds left')
            if (i-WaitingStart)*pollPeriod >= WaitingForBooyUpTimeout:
                State = Watching
        elif State == Watching:
            conn = sqlite3.connect('Claymon.sqlite')
            cursor = conn.cursor()

            sql = """
                Select rig, resetPin from
                (Select rig From
                (Select rig, c.worker worker, IfNull(c.secs, 0) LastHang, IfNull(d.secs,-1) LastWork From
                (Select a.worker, a.rig, b.secs, b.EthHashrate From
                (Select Distinct worker, rig from Workers) as a
                Left Join 
                (Select worker, Max(Seconds) secs, EthHashrate from LastDayWorkerEthStats
                Where IfNull(EthHashrate, 0) = 0
                Group by worker) as b
                on a.worker = b.worker) as c
                Left Join 
                (Select worker, Max(Seconds) secs, EthHashrate from LastDayWorkerEthStats
                Where EthHashrate > 0
                Group by worker) as d
                On c.worker = d.worker) as e
                Group by Rig
                Having Max(LastHang - LastWork) > 100) as f
                Left Join 
                (Select r.RigNum, ResetPin from
                Rigs r Inner Join WatchDogPins w on
                r.Address = w.Address) as g
                On f.rig = g.rigNum
                Where IfNull(resetPin, -1) >= 0
            """

            cur = cursor.execute(sql)
            rigsToReset = cur.fetchall()
#            rigsToReset = [(65, 0)]
            conn.close()
            print('...........................watchdog: reset ', rigsToReset)

            if len(rigsToReset) > 0:
                buf = bytearray(3+RegCount)
                buf[0] = 0x5A
                buf[1] = RegCount
                buf2 = bytearray(3+RegCount)
                buf2[0] = 0x5A
                buf2[1] = RegCount

                sss = ''.rjust(8*RegCount, '1')
                sss2 = ''.rjust(8*RegCount, '1')
                for rig in rigsToReset:
                    pin = rig[1]
                    pin = (pin//8)*8 + (7 - pin%8)
                    sss = sss[:pin]+'0'+sss[pin+1:]
                sum = buf[1]
                sum2 = buf2[1]
                for i in range(RegCount):
                    p1 = 8*i
                    p2 = p1+8
                    a = sss[p1:p2]
                    b = int(a,2)
                    buf[i+2] = b
                    sum += buf[i+2]
                    a = sss2[p1:p2]
                    b = int(a,2)
                    buf2[i+2] = b
                    sum2 += buf2[i+2]
                print(sss)
                buf[-1] = sum % 256
                buf2[-1] = sum2 % 256

                sock = socket.socket()
                sock.settimeout(1)
                ip = '192.168.2.44'
                port = 7850
                try:
                    sock.connect((ip, port))
                    sock.send(buf)
                    time.sleep(1)
                    sock.send(buf2)
                    State = WaitingForBootUp
                    WaitingStart = i
                except socket.error as msg:
                    print('watchdog is offline!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                sock.close()
                sock = None
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 3")
        event_for_set.set()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 4")

def purgeLastDayStat(event_for_wait, event_for_set):
    i = 0
    while 0 == 0:
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Purge 1")
        event_for_wait.wait()
        event_for_wait.clear()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Purge 2")
        i += 1
        if i%10 == 0:
            print("purge old stat in DB")
            conn = sqlite3.connect('Claymon.sqlite')
            conn.execute('Delete From LastDayWorkerEthStats Where (Select Max(seconds) from LastDayWorkerEthStats) - seconds > 86400', [])
            conn.execute('Delete From LastDayGPUStats Where (Select Max(seconds) from LastDayGPUStats) - seconds > 86400', [])
            conn.commit()
            conn.close()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Purge 3")
        event_for_set.set()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Purge 4")

def pollStat (wrks, name, statspool, event_for_wait, event_for_set):
    while 0 == 0:
        stats = []
        online = []
        offline = []
        for wname in wrks.keys():
            wrk = wrks[wname]
            sock = socket.socket()
            sock.settimeout(1.5)
            ip = wrk["ip"]
            port = wrk["Port"]
            try:
                sock.connect((ip, port))
                buff = json.dumps({"id": 0, "jsonrpc": "2.0", "method": "miner_getstat2"})
                sock.send(buff.encode('utf-8'))
                data = sock.recv(1024)
            except socket.error as msg:
                sock.close()
                sock = None
                stttt = [wrk["Name"], None]
                stats.append(stttt)
                offline.append(wrk["Name"])
                continue
            udata = data.decode("utf-8")
            stat = [wrk["Name"], json.loads(udata)]
            online.append(wrk["Name"])
            sock.close()
            stats.append(stat)

        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Poll 1", name)
        event_for_wait.wait()
        event_for_wait.clear()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Poll 2", name)
        print('Poll thread #', name, len(stats), 'total workers. Online:', online, 'Offline:', offline)
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Poll 3", name)
        statspool.append(stats)
        event_for_set.set()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Poll 4", name)

def pushStatToDB(workers, statspool, event_for_wait, event_for_set):
    while 0 == 0:
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Push 1")
        event_for_wait.wait()
        event_for_wait.clear()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Push 2")
        rez1, rez2 = [], []
        wrksOnline, wrksOffline = 0, 0
        gpuOnline = 0
        while statspool:
            stats = statspool.pop()
            for stat in stats:
                tst = stat[1]
                trz = None
                if tst != None:
                    trz = tst['result']
                tm = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
                tm2 = time.time()
                wrk = stat[0]
                worker = workers[wrk]
                ip = worker["ip"]
                port = worker["Port"]
                if trz != None:
                    wrksOnline += 1
                    ver = trz[0]
                    uptime = trz[1]
                    lst1 = trz[2].split(";")
                    erate = lst1[0]
                    shares = lst1[1]
                    rej = lst1[2]
                    lst2 = trz[8].split(";")
                    inv = lst2[0]
                    sw = lst2[1]
                    bns = trz[15].split(";")
                    gpuethhr = trz[3].split(";")
                    tfans = trz[6].split(";")
                    fans = tfans[0::2]
                    ts = tfans[1::2]
                    gpushares = trz[9].split(";")
                    gpurej = trz[10].split(";")
                    gpuinv = trz[11].split(";")
                    st1 = (tm, tm2, wrk, ip, port, ver, uptime, erate, shares, rej, inv, sw)
                    for i in range(len(bns)):
                        hr = gpuethhr[i]
                        dbhr = 0
                        if hr == "stopped":
                            gpustate = "S"
                        elif hr == "off":
                            gpustate = "O"
                        elif (hr.isdecimal()) and (int(hr) == 0):
                            gpustate = "H"
                        elif (hr.isdecimal()) and (int(hr) > 0):
                            gpustate = "W"
                            dbhr = int(hr)
                            gpuOnline += 1

                        stgpu = (tm, tm2, wrk, bns[i], gpustate, dbhr, ts[i], fans[i], gpushares[i], gpurej[i], gpuinv[i])
                        rez2.append(stgpu)
                else:
                    st1 = (tm, tm2, wrk, ip, port, None, None, None, None, None, None, None)
                    wrksOffline += 1
                rez1.append(st1)

        conn = sqlite3.connect('Claymon.sqlite')
        conn.executemany('INSERT INTO LastDayWorkerEthStats (time, seconds, worker, ip, port, ClaymoreVer, Uptime, EthHashrate, Shares, Rejected, Invalid, PoolSwitches) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', rez1)
        conn.executemany('INSERT INTO LastDayGPUStats (time, seconds, worker, BusNum, State, EthHashrate, T, Fan, Accepted, Rejected, Invalid) VALUES (?,?,?,?,?,?,?,?,?,?,?)', rez2)
        conn.commit()
        conn.close()
        print(tm)
        print("Push stat to DB", len(rez1), 'workers', len(rez2), 'gpu')
        print("Workers online", wrksOnline, 'offline', wrksOffline, 'gpu works', gpuOnline)
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Push 3")
        event_for_set.set()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Push 4")

def delayThr( event_for_wait, event_for_set):
    while 0 == 0:
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 1")
        event_for_wait.wait()
        event_for_wait.clear()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 2")
        time.sleep(pollPeriod)
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 3")
        event_for_set.set()
        print(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 4")

def LoadWorkersFromJSON():
    global ws
    f2 = open('wrks.json', 'r')
    st2 = f2.read()
    ws= json.loads(st2)["Workers"]

def LoadWorkersFromDB():
    global ws
    conn = sqlite3.connect('Claymon.sqlite')
    cursor = conn.cursor()
    for row in cursor.execute('SELECT ActualHashrate, Worker FROM Workers'):
        tt = {}
        tt['Hashrate'] = row[0]
        tt['Name'] = row[1]
        ws.append(tt)
    conn.close()
    print('Workers loaded from db', len(ws))
    print(ws)

ws = []
LoadWorkersFromDB()

thrCnt = len(ws) // pollPeriod
thrWs = []
dbWs = {}
for i in range(thrCnt):
    thrWs.append({})
i = 0
for wrk in ws:
    wname = wrk["Name"]
    widx = 'ABCDEFGH'.find(wname[-1])
    wnum = wname[1:]
    port = 3333
    if widx >= 0:
        wnum = wname[1:-1]
        port += widx
    ip = '192.168.2.1' + wnum
    wrk["Port"] = port
    wrk["ip"] = ip
    idx = i%thrCnt
    thrW = thrWs[idx]
    thrW[wname] = wrk
    dbWs[wname] = wrk
    i += 1

thrCnt += 4
threads = []
events = []
statspool = []
for i in range(thrCnt):
    events.append(threading.Event())
e2 = events[thrCnt-1]
for i in range(thrCnt):
    e1 = e2
    e2 = events[i]
    if i == thrCnt-4:
        threads.append(threading.Thread(target=watchDog, args=(e1, e2)))
    if i == thrCnt-3:
        threads.append(threading.Thread(target=purgeLastDayStat, args=(e1, e2)))
    if i == thrCnt-2:
        threads.append(threading.Thread(target=pushStatToDB, args=(dbWs, statspool, e1, e2)))
    if i == thrCnt-1:
        threads.append(threading.Thread(target=delayThr, args=(e1, e2)))
    if i < thrCnt-4:
        threads.append(threading.Thread(target=pollStat, args=(thrWs[i], i, statspool, e1, e2)))
for i in range(thrCnt):
    threads[i].start()
events[thrCnt-1].set()
