import json
import socket
import sqlite3
import threading
import time
from datetime import datetime
import sys

pollPeriod = 10
socketTimeout = 2
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
SwitchQueue = []
incomingTasksQueue = []
Users = []

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
    sock.settimeout( socketTimeout )
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

def SwitchHashrate2(filename = 'wrks.json'):
    # Сперва загружаем из файлика или БД табличку - воркер, юзер, айпи, порт
    # Потом опрашиваем все машинки, спрашиваем конфиг у каждой. Узнаем на какого юзера она работает
    # Потом пихаем в тасклист задачи на перезагрузку машинок, которые работают не туда
    # Получаем отчеты о выполнении тасков.
    # По каждому успешному отчету снова проверяем конфиг. Если юзер не тот - пишем в консоль сообщение об ошибке, ждем сколько-нибудь, задачу вотчдогу повторяем, до бесконечности
    # По неуспешным отчетам, то есть машинку перезагрузить не удалось - пишем в консоль сообщение об ошибке, ждем сколько-нибудь, задачу вотчдогу повторяем, до бесконечности
    global SwitchQueue
    global Users
    f2 = open(filename, 'r')
    st = f2.read()
    ws = json.loads(st)
    f2.close()
    wrks = ws["Workers"]
    Users = ws["Users"]
    for wrk in wrks:
        SwitchQueue.append(wrk)

def checkWorkerConfig(udata, wrk):
    if not udata:
        return
    i1, i2, i3 = None, None, None
    try:
        resp = json.loads(udata)
        rez = resp["result"]
        conf = bytearray.fromhex(rez[1]).decode().split()
        i1 = conf.index("-ewal")
        i2 = conf.index("-epool")
        i3 = conf.index("-eworker")
    except:
        if not (i1 or i2):
            return
    remote_wal = conf[i1+1].upper()
    remote_pool = conf[i2+1].upper()
    remote_worker = None
    if i3:
        remote_worker = conf[i3+1].upper()
    global Users
    Users_dict = {u["Nick"] : u for u in Users}
    uname = wrk["User"]
    user = Users_dict[uname]
    if remote_worker:
        A = wrk["User"].upper()
        B = remote_wal.upper()
        if A in B:
            return True
    else:
        try:
            pool = user["pool"].upper()
            wal = user["wallet"].upper()
            if pool in remote_pool and wal in remote_wal:
                return True
        except:
            return

def retrieveWorkerConfig(wrk):
    port = wrk["Port"]
    ip = wrk["ip"]
    buff = json.dumps(
        {"id": 0, "jsonrpc": "2.0", "method": "miner_getfile", "params": ["config%s.txt" % wrk["letter"]]})
    sock = socket.socket()
    sock.settimeout(socketTimeout)
    try:
        sock.connect((ip, port))
        sock.send(buff.encode('utf-8'))
        data = sock.recv(5000)
    except socket.error as msg:
        sock.close()
        return
    udata = data.decode("utf-8")
    return udata


def SwitchHashrateThread(event_for_wait, event_for_set):
    global HaveToExit, Stopped
    global SwitchQueue
    while not HaveToExit:
        event_for_wait.wait()
        event_for_wait.clear()
        if Stopped:
            event_for_set.set()
            continue

        switchCnt = 0
        while switchCnt < (pollPeriod // socketTimeout) and SwitchQueue:
            switchCnt += 1
            wrk = SwitchQueue.pop()
            udata = retrieveWorkerConfig(wrk)
            if udata and checkWorkerConfig(udata, wrk): # Если воркер ответил, и конфиг в порядке - ничего не делаем, все отлично
                continue
            addToIncomingTask(wrk["RigNum"])
        event_for_set.set()

def addToIncomingTask(rigNum):
    global incomingTasksQueue
    task = {"Rig" : rigNum}
    incomingTasksQueue.append(task)


def getIncomingTaskList():
    global incomingTasksQueue
    return incomingTasksQueue

def prepareRigsList():
    # rig["ToOfflineTimeouts"][:]
    rig = {"RigNum": 0, "ToOfflineTimeouts": [1,1,1], "ToOnlineTimeouts": [1,1,1], "OnlinePort": 3333, "ip": "192.168.2.100"}
    return [rig]

def updateRigsOnlineStatus(rigList):
    pass

def reportTask(rigNum, success):
    pass

def RebootResetManagingThread(event_for_wait, event_for_set):
    # Контролировать успешность перезагрузки, правильность конфига надо в SwitchAll, не здесь
    rigList = prepareRigsList()
    i = 0
    rebootQueue = []
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
                rig["Action"] = "Reboot"
            else:
                rig["Waiting"] = "Online"
                rig["Timers"] = rig["ToOnlineTimeouts"][:]
                rig["Action"] = "Reset"
            rebootQueue.append(rig)

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
                rig["Waiting"] = "Online"
                rig["Timers"] = rig["ToOnlineTimeouts"][:]
                continue

            # Это сокращенная запись условия "Текущее состояние" != "Ожидаемое состояние"
            assert(rig["Online"] != (rig["Waiting"] == "Online"))

            # Мы чего-то ждали, но не дождались? Уменьшим таймер
            rig["Timers"][0] -= pollPeriod

            # Ой, мы ждали, ждали, и вышел таймаут?
            if rig["Timers"][0] < 0:
                rig["Timers"].pop(0)
                if rig["Timers"]:
                    # А если таймеры еще остались, то очередной пинок делаем, и ждем дальше (для этого уже все готово)
                    rebootQueue.append(rig)
                # Если таймеры кончились, мы хотели чтоб машинка ушла в оффлайн, но она так и осталась онлайн, то пробуем ее ресетить
                if rig["Waiting"] == "Offline":
                    rig["Timers"] = rig["ToOfflineTimeouts"][:]
                    rig["Action"] = "Reset"
                    rebootQueue.append(rig)
                rig["Waiting"] = "Nothing"
                fail = False
                reportTask(rigNum, fail)

        rebootsCnt = 0
        while rebootsCnt < (pollPeriod // socketTimeout) and rebootQueue:
            rig = rebootQueue.pop()
            action = rig["Action"]
            if action == "Reboot":
                RebootRig(rig)
            elif action == "Reset":
                pin = rig["Pin"]
                ResetPin(pin)
            rebootsCnt += 1

        event_for_set.set()

def RebootRig(rig):
    sock = socket.socket()
    sock.settimeout( socketTimeout )
    ip = rig["ip"]
    port = rig["OnlinePort"]
    try:
        sock.connect((ip, port))
        buff = json.dumps({"id": 0, "jsonrpc": "2.0", "method": "miner_reboot"})
        sock.send(buff.encode('utf-8'))
    finally:
        sock.close()

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

# rez = checkWorkerConfig('{"id": 0, "result": ["config.txt", "2d6d6f646520310d0a2d6d706f727420333333330d0a2d6d696e73706565642031300d0a232d6469203031323334350d0a0d0a232d6d636c6f636b20323030302c20323030302c20323030302c20313930302c20323030302c20323030302c20323030302c20313835300d0a2d6d636c6f636b20323030302c20313935302c20313930302c20323035302c20323130302c20323130302c20202020302c20202020300d0a2d706f776c696d20202032352c20202032352c20202032352c20202032352c20202032352c20202032352c20202032352c20202020300d0a2d63766464632020313030302c20313035302c20203935302c20203935302c20203935302c20313030302c20202020302c20202020300d0a0d0a2d74742036350d0a2d7474646372203735200d0a2d74746c692038300d0a2d7473746172742035350d0a2d7473746f70203835200d0a2d65746869203136200d0a2d65706f6f6c206575726f7065312e657468657265756d2e6d696e696e67706f6f6c6875622e636f6d3a3230353335200d0a2d6577616c2070756c6b322e25434f4d50555445524e414d45250d0a2d65776f726b65722070756c6b322e25434f4d50555445524e414d45250d0a2d65736d20320d0a2d657073772078200d0a2d616c6c706f6f6c732031200d0a2d616c6c636f696e732031200d0a0d0a2d706c6174666f726d20310d0a2d6c6f67736d617873697a65203130"], "error": null}', wrk)
# rez = checkWorkerConfig('{"id": 0, "error": null, "result": ["config.txt", "2d6d6f646520310d0a2d6d706f727420333333330d0a2d6d696e73706565642031300d0a232d6463726920390d0a0d0a232d6d636c6f636b20323135302c20323135302c20323135302c20323135302c20323135302c20323135302c20323135302c20323130300d0a2d706f776c696d20202020352c20202020352c20202020352c20202020352c20202020352c20202020352c20202020352c20202020350d0a2d63766464632020203930302c20203930302c20203930302c20203930302c20203930302c20203930302c20203930302c20203930300d0a0d0a2d74742036350d0a2d7474646372203730200d0a2d74746c692038300d0a2d7473746172742035350d0a2d7473746f70203835200d0a2d65746869203136200d0a2d65706f6f6c206574682d6575312e6e616e6f706f6f6c2e6f72673a393939390d0a2d6577616c203078396266343863353236323830653636353765356232626534616134383063663065306337373762342f25434f4d50555445524e414d45252f4d6f6f6e3537394079616e6465782e72750d0a2d657073772078200d0a2d616c6c706f6f6c732031200d0a2d616c6c636f696e732031200d0a0d0a2d64706f6f6c207374726174756d2b7463703a2f2f65752e7369616d696e696e672e636f6d3a373737370d0a2d6477616c20393936366531366331633966313736373437316463396438336564653938636562366433306139353135383461653565653561393065643139333363343539383861376637343132383637610d0a2d64636f696e2073630d0a2d6470737720780d0a0d0a2d706c6174666f726d20310d0a2d6c6f67736d617873697a65203130"]}', wrk)

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