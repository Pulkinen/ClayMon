import json
import socket
import sqlite3
import threading
import time
from datetime import datetime

pollPeriod = 10
RegCount = 12
WaitingForBootUpTimeout = 400
vrbDbg = 10
vrbDbg2 = 8
vrbPrintTimeStamps = 9
vrbMustPrint = 0
maxVerbosity = vrbPrintTimeStamps
HaveToExit = False
Stopped = False
wd_ip = '192.168.2.55'
wd_port = 7850
dbRigs = {}

def printDbg(verbosity, *args):
    if verbosity <= maxVerbosity:
        tstamp = ""
        if verbosity < vrbPrintTimeStamps:
            tstamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
            print(tstamp, end=" ")
        for arg in args:
            print(arg, end=" ")
        print()
        try:
            f = open('watchdoglog.txt', 'a')
            print(tstamp, sep = " ", end =  " ", file = f, flush = False)
            for arg in args:
                print(arg, sep = " ", end = " ", file = f, flush = False)
            print("", file = f, flush = True)
            f.close()
        except:
            print("Ooops")

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
            rigs = packet["Data"]
            ResetRigs(rigs)
            return
        elif command == "Pin":
            pins = packet["Data"]
            ResetPins(pins)
            return
        elif command == "TestPort":
            port = int(packet["Data"])
            TestPort(port)
            return
        elif command == "Switch":
            printDbg(vrbMustPrint, "Api switch")
            SwitchHashrate()
            return
        elif command == "TempPin":
            nums = packet["Data"]
            TempAddress(nums)
            return
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
        print(rigsToReset)
        for r in rigsToReset:
            pin = r[2]
            ResetPin(pin)
            rig = r[0]
            if not CheckResetSuccess(rig):
                printDbg(vrbMustPrint, "Probably reset did not works on rig ", r, "!!!!!!!!!!!!!!")
            else:
                printDbg(vrbDbg, "Rig ", r, "does not respond, reset ok")

    except:
        printDbg(vrbMustPrint, "Bad rig number: ", rig)
    return

def ResetPin(pin):
    if pin < 0 or pin >= RegCount*8:
        print("incorrect pin: ", pin)
        return

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
    print(sss)
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
        print('watchdog ok')
    except socket.error as msg:
        print('watchdog is offline!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    sock.close()
    sock = None

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
            print('watchdog is offline')
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

def SwitchHashrate2():
    # Сперва загружаем из файлика или БД табличку - воркер, юзер, айпи, порт
    # Потом опрашиваем все машинки, спрашиваем конфиг у каждой. Узнаем на какого юзера она работает
    # Потом пихаем в тасклист задачина перезагрузку машинок, которые работают не туда
    # Получаем отчеты о выполнении тасков.
    # По каждому успешному отчету снова проверяем конфиг. Если юзер не тот - пишем в консоль сообщение об ошибке, ждем сколько-нибудь, задачу вотчдогу повторяем, до бесконечности
    # По неуспешным отчетам, то есть машинку перезагрузить не удалось - пишем в консоль сообщение об ошибке, ждем сколько-нибудь, задачу вотчдогу повторяем, до бесконечности
    pass


def getIncomingTaskList():
    return []

def prepareRigsList():
    return {}

def addToRebootQueue():
    pass

def updateRigsOnlineStatus(rigList):
    pass

def reportTask(rigNum, success):
    pass

def watchDog2(event_for_wait, event_for_set):
    # Контролировать успешность перезагрузки, правильность конфига надо в SwitchAll, не здесь
    # Нужно у каждой машинки вести свои тайминги на перезагрузку итд.
    # И как-то сделать чтоб сперва пробовать перезагруить командой reboot.cmd, а если не получилось - ресетом на пин
    rigList = prepareRigsList()
    pocessingTasks = {}
    i = 0
    global HaveToExit, Stopped
    while not HaveToExit:
        event_for_wait.wait()
        event_for_wait.clear()
        i += 1
        if Stopped:
            printDbg(vrbMustPrint, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Watch dog is stopped")
            event_for_set.set()
            continue

        # Освежаем текущее состояние ригов
        updateRigsOnlineStatus(rigList)

        # Получаем и обрабатываем новые задания на перезагрузку
        incomingTaskList = getIncomingTaskList()
        while incomingTaskList:
            task = incomingTaskList.pop()
            rigNum = task["Rig"]
            if not rigNum in rigList:
                continue
            rig = rigList[rigNum]
            if rig["Online"]:
                rig["Waiting"] = "Offline"
                rig["Timers"] = rig["ToOfflineTimeouts"][:]
            else:
                rig["Waiting"] = "Online"
                rig["Timers"] = rig["ToOnlineTimeouts"][:]
            addToRebootQueue(rigNum, rig)

        # Уменьшаем таймеры и обрабатываем их истечение
        for rigNum in rigList:
            rig = rigList[rigNum]
            # У ригов без цели таймеров нет, нам от них ничего не нужно
            if not rig["Waiting"] in ("Online", "Offline"):
                continue

            # Мы ждали что он заработает и дождались? Ура, задача выполнена, рапортуем
            if (rig["Waiting"] == "Online") and rig["Online"]:
                rig["Waiting"] = "Nothing"
                success = True
                reportTask(rigNum, success)
                continue

            # Мы ждали чтобы он ушел в перезагрузку и дождались? Отлично, теперь надо пождать пока он запустится
            if (rig["Waiting"] == "Offline") and not rig["Online"]:
                rig["Waiting"] = "Offline"
                rig["Timers"] = rig["ToOnlineTimeouts"][:]
                continue

            # Это сокращенная запись условия "Текущее состояние" != "Ожидаемое состояние"
            assert(rig["Online"] != (rig["Waiting"] == "Online"))

            # Мы чего-то ждали, но не дождались? Уменьшим таймер
            rig["Timers"][0] -= pollPeriod

            # Ой, мы ждали, ждали, и вышел таймаут?
            if rig["Timers"][0] < 0:
                rig["Timers"].pop(0)
                # Если таймеры кончились, то увы, задачу выполнить не удалось, завершаем и рапортуем
                if rig["Timers"] == []:
                    rig["Waiting"] = "Nothing"
                    fail = False
                    reportTask(rigNum, fail)
                # А если таймеры еще остались, то очередной пинок делаем, и ждем дальше (для этого уже все готово)
                addToRebootQueue(rigNum, rig)

        event_for_set.set()

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
            print(vrbMustPrint, '...........................watchdog: reset ', rigsToReset)
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

    global HaveToExit
    while not HaveToExit:
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 1")
        event_for_wait.wait()
        event_for_wait.clear()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 2")
        sock.settimeout(pollPeriod)
        try:
            conn, addr = sock.accept()
            data = conn.recv(1000)
            conn.close()
            if data:
                API(data)
        except socket.error as msg:
            printDbg(vrbDbg, "Nothing was recieved")

        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 3")
        event_for_set.set()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 4")

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

printDbg(vrbMustPrint, "===================================================", datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Claymore monitor started. Version 0.01 ===================================================" )
ws = []
LoadWorkersFromDB()

thrCnt = 2
printDbg(vrbMustPrint, "pollPeriod = ", pollPeriod, "Threads count = ", thrCnt)
dbWs = {}
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
    dbWs[wname] = wrk
    if wnum.isnumeric():
        rig = int(wnum)
        if rig in dbRigs:
            tuple = dbRigs[rig]
            if port < tuple[1]:
                dbRigs[rig] = (ip, port)
        else:
            dbRigs[rig] = (ip, port)
    i += 1

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