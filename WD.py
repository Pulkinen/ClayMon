import socket, json, sqlite3, threading, time, sys, shutil, os
from datetime import datetime
from os import listdir
from os.path import isfile, join

pollPeriod = 10
RegCount = 12
WaitingForBootUpTimeout = 400
vrbDbg = 10
vrbDbg2 = 8
vrbPrintTimeStamps = 9
vrbMustPrint = 0
#maxVerbosity = vrbDbg
maxVerbosity = vrbPrintTimeStamps
HaveToExit = False
Stopped = False
wd_ip = '192.168.2.55'
wd_port = 7850
dbRigs = {}
GlobalUptime = 0

log_suffix = datetime.strftime(datetime.now(), "%Y-%m-%d %H-%M-%S")
wdlog_fname = 'wdlog\wdlog\watchdoglog %s.txt' % log_suffix

def printDbg(verbosity = vrbDbg, *args):
    global wdlog_fname
    if verbosity <= maxVerbosity:
        tstamp = ""
        if verbosity < vrbPrintTimeStamps:
            tstamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
            print(tstamp, end=" ")
        for arg in args:
            print(arg, end=" ")
        print()
        try:
            f = open(wdlog_fname, 'a')
            print(tstamp, sep = " ", end =  " ", file = f, flush = False)
            for arg in args:
                print(arg, sep = " ", end = " ", file = f, flush = False)
            print("", file = f, flush = True)
            f.close()
        except Exception as e:
            print("Ooops", e)

def API(data):
    global Stopped
    udata = data.decode("utf-8")
    try:
        packet = json.loads(udata)
        printDbg(vrbMustPrint, "API command recieved ", packet)
        command = packet["Command"]
        if command == "Exit":
            global HaveToExit
            HaveToExit = True
            return
        elif command == "Stop":
            Stopped = True
            return
        elif command == "Start":
            Stopped = False
            return
        elif command == "Rig":
            printDbg(vrbDbg, 'Rig')
            rigs = packet["Data"]
            ResetRigs(rigs)
            return
        elif command == "Pin":
            pins = packet["Data"]
            ResetPins(pins)
            return
        elif command == "CheckPins":
            DoGlobalPinsCheck()
            return
        elif command == "TestPort":
            printDbg(vrbMustPrint, "Command is disabled")
            return
            # port = int(packet["Data"])
            # TestPort(port)
            # return
        elif command == "Switch":
            printDbg(vrbMustPrint, "Command is disabled")
            return
            # printDbg(vrbMustPrint, "Api switch")
            # SwitchHashrate()
            # return
        elif command == "TempPin":
            printDbg(vrbMustPrint, "Command is disabled")
            return
            # nums = packet["Data"]
            # TempAddress(nums)
            # return
        else:
            printDbg(vrbMustPrint, "Unknown command")
            return
    except:
        printDbg(vrbMustPrint, "Wrong udata = ", udata)

def TempAddress(data):
    # conn = sqlite3.connect('Claymon.sqlite')
    conn = sqlite3.connect('Claymon.sqlite')

    sql = """
        CREATE TABLE IF NOT EXISTS TempAddresses (
            Rig INTEGER UNIQUE,
            Pin INTEGER UNIQUE);
        """
    conn.execute(sql)
    conn.commit()
    pin = int(data[0])
    rig = int(data[1])
    sql = "DELETE From TempAddresses Where Rig = {0} OR Pin = {1}".format(rig, pin)
    conn.execute(sql)
    conn.commit()

    sql = "INSERT INTO TempAddresses (Rig, Pin) VALUES ({0}, {1})".format(rig, pin)
    conn.execute(sql)
    conn.commit()

    return

def CheckResetSuccess(rig):
    sock = socket.socket()
    sock.settimeout(1.5)
    if not rig in dbRigs:
        printDbg(vrbMustPrint, "Cannot check, rig not found", rig)
        return True
    tuple = dbRigs[rig]
    ip, port = tuple
    try:
        sock.connect((ip, port))
        buff = json.dumps({"id": 0, "jsonrpc": "2.0", "method": "miner_getstat2"})
        sock.send(buff.encode('utf-8'))
        data = sock.recv(1024)
        return False
    except socket.error as msg:
        sock.close()
        sock = None
        return True

def ResetRigs(rigs):
    printDbg(vrbDbg, 'ResetRigs(rigs):', rigs)
    for r in rigs:
        rig = int(r)
        ResetRig(rig)

def ResetPins(pins):
    for p in pins:
        pin = int(p)
        ResetPin(pin)

def ResetRig(rig):
    try:
        conn = sqlite3.connect('Claymon.sqlite')
        cursor = conn.cursor()

        sql = """
            Select rigNum, r.Address, ResetPin
            From Rigs r Inner Join WatchDogPins p
            On r.Address = p.Address
            Where RigNum = {0}
            UNION ALL
            Select Rig, "", Pin
            From TempAddresses
            Where Rig = {0}
            """.format(rig)

        cur = cursor.execute(sql)
        rigsToReset = cur.fetchall()
        conn.close()
        printDbg(vrbDbg, sql)
        printDbg(vrbDbg, 'pins from db', rigsToReset)
        cfgpin = GetPinFromConfig("U:\\AU1\\Autoupdate\\Config\\", rig)
        if cfgpin != None:
            rigsToReset[0:0] = [(rig, 'from config', cfgpin)]
        printDbg(vrbDbg, rigsToReset)
        for r in rigsToReset:
            pin = r[2]
            ResetPin(pin)
            rig = r[0]
            if r[1] == 'from config':
                return
            if not CheckResetSuccess(rig):
                printDbg(vrbMustPrint, "Probably reset did not works on rig ", r, "!!!!!!!!!!!!!!")
            else:
                printDbg(vrbDbg, "Rig ", r, "does not respond, reset ok")

    except:
        printDbg(vrbMustPrint, "Bad rig number: ", rig)
        return


def ResetPin(pin, verbosity = vrbMustPrint):
    printDbg(vrbDbg, 'reset pin', pin)
    if pin < 0 or pin >= RegCount*8:
        printDbg(vrbMustPrint, "incorrect pin: ", pin)
        return -1

    buf = bytearray(3 + RegCount)
    buf[0] = 0x5A
    buf[1] = RegCount
    buf2 = bytearray(3 + RegCount)
    buf2[0] = 0x5A
    buf2[1] = RegCount

    sss = ''.rjust(8 * RegCount, '1')
    sss2 = ''.rjust(8 * RegCount, '1')
    pin = (pin // 8) * 8 + (7 - pin % 8)
    sss = sss[:pin] + '0' + sss[pin + 1:]
    sum = buf[1]
    sum2 = buf2[1]
    for i in range(RegCount):
        p1 = 8 * i
        p2 = p1 + 8
        a = sss[p1:p2]
        b = int(a, 2)
        buf[i + 2] = b
        sum += buf[i + 2]
        a = sss2[p1:p2]
        b = int(a, 2)
        buf2[i + 2] = b
        sum2 += buf2[i + 2]
    printDbg(verbosity, sss)
    buf[-1] = sum % 256
    buf2[-1] = sum2 % 256

    sock = socket.socket()
    sock.settimeout(1)
    global wd_ip, wd_port
    ip = wd_ip
    port = wd_port
    try:
        sock.connect((ip, port))
        sock.send(buf)
        time.sleep(1)
        sock.send(buf2)
        printDbg(verbosity, 'watchdog ok')
    except socket.error as msg:
        print('watchdog is offline!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        sock.close()
        return -2
    sock.close()
    sock = None
    return 42

def TestPort(prt):
    global RegCount
    AllOnes = [255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]
    fr1 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    def SendBytes(bts):
        bytesToSend = bts[:]
        buf = bytearray(3 + RegCount)
        buf[0] = 0x5A
        buf[1] = RegCount
        sum = buf[1]
        sss = ''.rjust(8 * RegCount, '1')
        for i in range(RegCount):
            p1 = 8 * i
            p2 = p1 + 8
            a = sss[p1:p2]
            b = int(a, 2)
            buf[i + 2] = b
            buf[i + 2] = bytesToSend[i]
            sum += buf[i + 2]
            buf[-1] = sum % 256

        sock = socket.socket()
        sock.settimeout(1)
        ip = wd_ip
        port = wd_port
        try:
            sock.connect((ip, port))
            sock.send(buf)
        except socket.error as msg:
            printDbg(vrbMustPrint,'watchdog is offline')
        sock.close()
        sock = None
        print('Ok')

    i = 0
    idx = prt
    fr1 = AllOnes[:]
    while i < 8:
        fr1[idx] = 2 ** i
        print(fr1)
        SendBytes(fr1)
        i += 1
        time.sleep(0.4)
        SendBytes(AllOnes)
    return

def SwitchHashrate():
    printDbg(vrbMustPrint, "Switch hashrate")
    conn = sqlite3.connect('Claymon.sqlite')
    cursor = conn.cursor()
    sql = """
        Select Distinct Rig from Workers 
        Where Not (ActualUser = "pulk" or ActualUser = "Ipp")    
    """
    cur = cursor.execute(sql)
    rigsToReset = cur.fetchall()
    print(rigsToReset)
    for rig in rigsToReset:
        ResetRig(rig[0])

def watchDog(event_for_wait, event_for_set):
    i = 0
    WaitingForBootUp = 0
    Watching = 1
    rigsToReset = []
    State = WaitingForBootUp
    WaitingStart = i
    global HaveToExit, Stopped
    while not HaveToExit:
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 1")
        event_for_wait.wait()
        event_for_wait.clear()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 2")
        i += 1
        if Stopped:
            State = WaitingForBootUp
            printDbg(vrbMustPrint, "Watch dog is stopped")
            event_for_set.set()
            printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 5")
            continue
        if State == WaitingForBootUp:
            printDbg(vrbMustPrint, '.......................watch dog waiting for bootup', rigsToReset, ' ,', WaitingForBootUpTimeout - (i - WaitingStart - 1) * pollPeriod, 'seconds left')
            if (i-WaitingStart)*pollPeriod >= WaitingForBootUpTimeout:
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
            conn.close()
            print(vrbMustPrint, '.........................watchdog: reset ', rigsToReset)
            for rig in rigsToReset:
                ResetRig(rig[0])
            State = WaitingForBootUp
            WaitingStart = i

        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 3")
        event_for_set.set()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 4")

def delayThr( event_for_wait, event_for_set):
    sock = socket.socket()
    sock.bind(("", 6116))
    sock.listen(1)

    global HaveToExit, GlobalUptime, wdlog_fname
    while not HaveToExit:
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 1")
        event_for_wait.wait()
        event_for_wait.clear()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 2")

        GlobalUptime += pollPeriod
        days = GlobalUptime // 86400
        prev_days = (GlobalUptime - pollPeriod) // 86400
        if days != prev_days and days > 0:
            log_suffix = datetime.strftime(datetime.now(), "%Y-%m-%d %H-%M-%S")
            wdlog_fname = 'wdlog\wdlog\watchdoglog %s.txt' % log_suffix

        sock.settimeout(pollPeriod)
        data = None
        try:
            conn, addr = sock.accept()
            data = conn.recv(1000)
            conn.close()
        except socket.error as msg:
            printDbg(vrbDbg, 'Nothing was recieved')
        if data:
            API(data)

        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 3")
        event_for_set.set()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 4")

def GetPinFromConfig(base_folder, rig, verbosity=vrbMustPrint):
    mnfldr = base_folder + "Miner%02d"%rig
    printDbg(vrbDbg, "config folder:", mnfldr)
    onlyfiles = [f for f in listdir(mnfldr) if isfile(join(mnfldr, f))]
    printDbg(vrbDbg, "config files:", onlyfiles)
    for fname in onlyfiles:
        f = open(mnfldr+'\\'+fname, 'r')
        st = f.readlines()
        f.close()

        for s in st:
            st2 = s.split()
            printDbg(vrbDbg, st2)
            if len(st2) < 2:
                printDbg(vrbDbg, 'len(st2)=', len(st2))
                continue
            if st2[0].upper() != "#PIN":
                printDbg(vrbDbg, 'st2[0].upper()=', st2[0].upper())
                continue
            if not st2[1].isnumeric():
                printDbg(vrbDbg, 'st2[1].isnumeric()=', st2[1].isnumeric())
                continue
            pin = int(st2[1])
            printDbg(verbosity, 'Pin found! M%d, Pin%d'%(rig, pin))
            return(pin)
        printDbg(vrbMustPrint, f'Pin not found in config M{rig}')
    return 99

def LoadWorkersFromJSON():
    global ws
    f2 = open('wrks.json', 'r')
    st2 = f2.read()
    ws= json.loads(st2)["Workers"]
    f2.close()

def LoadWorkersFromDB():
    global ws
    conn = sqlite3.connect('Claymon.sqlite')
    cursor = conn.cursor()
    for row in cursor.execute('SELECT ActualHashrate, Worker FROM Workers WHERE Rig >= 0'):
        tt = {}
        tt['Hashrate'] = row[0]
        tt['Name'] = row[1]
        ws.append(tt)
    conn.close()
    printDbg(vrbMustPrint, 'Workers loaded from db', len(ws))
    printDbg(vrbMustPrint, ws)

def testPorts(ip, ports):
    printDbg(vrbDbg, f'Test {ip}:{ports}')
    for port in ports:
        try:
            sock = socket.socket()
            sock.settimeout(2)
            printDbg(vrbDbg, f'    check {ip}:{port}')
            sock.connect((ip, port))
            buff = json.dumps({"id": 0, "jsonrpc": "2.0","method": ""})
            sock.send(buff.encode('utf-8'))
            data = sock.recv(1024)
            sock.close()
            printDbg(vrbDbg, f'    {ip}:{port} is alive')
            return True
        except:
            printDbg(vrbDbg, f'    {ip}:{port} doesnt answer')
    return False

def DoGlobalPinsCheck():
    global Stopped
    global testRigs
    Stopped = True
    printDbg(vrbMustPrint, '\n\n\n----------------------CHECK PINS STARTED-----------------------')
    try:
        rez = {}
        rigs = list(testRigs.keys())
        pins = []
        for rig in rigs:
            pins += [GetPinFromConfig("U:\\AU1\\Autoupdate\\Config\\", rig, vrbDbg)]
        rig_pins = list(zip(rigs, pins))
        rig_pins = sorted(rig_pins, key = lambda x: x[1])


        for rig, pin in rig_pins:
            rigname = f'M{rig:02d}'
            ip, __, ports = testRigs[rig]
            if pin is None:
                continue
            alive = testPorts(ip, ports)
            if not alive:
                rez[rig] = 'UNKNOWN. Cannot check, rig doesnt work'
                printDbg(vrbMustPrint, rigname, f'Pin {pin:02d}      ', rez[rig])
                time.sleep(2)
                continue
            ttt = ResetPin(pin, vrbDbg)
            if ttt == -2:
                rez[rig] = 'UNKNOWN. Arduino is offline'
                printDbg(vrbMustPrint,rez[rig])
                time.sleep(2)
                continue
            alive = testPorts(ip, ports)
            if alive:
                rez[rig] = f'!!!!!!!!!!!!!!Check pin {pin} on rig {rigname}, reset doesnt work!!!!!!!!!!!!!!!!!!'
                printDbg(vrbMustPrint, rigname, f'Pin {pin:02d}      ',rez[rig])
            else:
                rez[rig] = 'OK. Seems pin is correct, reset works.'
                printDbg(vrbMustPrint, rigname, f'Pin {pin:02d}      ', rez[rig])
            time.sleep(15)

        report = open('PinsCheckReport.txt', 'w')
        for k, v in rez.items():
            print(k, v, file=report)
        report.close()
    except Exception as e:
        print("Ooops", e)

    printDbg(vrbMustPrint, '\n----------------------CHECK PINS FINISHED-----------------------\n\n\n')
    Stopped = False
    return rez

printDbg(vrbMustPrint, "===================================================", datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Claymore monitor started. Version 0.01 ===================================================" )
ws = []
LoadWorkersFromDB()

thrCnt = 2
printDbg(vrbMustPrint, "pollPeriod = ", pollPeriod, "Threads count = ", thrCnt)
dbWs = {}
testRigs = {}
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
    dbWs[wname] = wrk
    if wnum.isnumeric():
        rig = int(wnum)
        if rig in testRigs:
            aaa = testRigs[rig]
            lst = aaa[2] + [port]
            testRigs[rig] = (aaa[0], aaa[1], lst)
        else:
            testRigs[rig] = (ip, None, [port])
        if rig in dbRigs:
            tuple = dbRigs[rig]
            if port < tuple[1]:
                dbRigs[rig] = (ip, port)
        else:
            dbRigs[rig] = (ip, port)

threads = []
events = []
statspool = []
for i in range(thrCnt):
    events.append(threading.Event())
e2 = events[thrCnt-1]
for i in range(thrCnt):
    e1 = e2
    e2 = events[i]
    if i == thrCnt-2:
        threads.append(threading.Thread(target=watchDog, args=(e1, e2)))
    if i == thrCnt-1:
        threads.append(threading.Thread(target=delayThr, args=(e1, e2)))
for i in range(thrCnt):
    threads[i].start()
events[thrCnt-1].set()