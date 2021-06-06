import socket, json, sqlite3, threading, time, sys, shutil, os
from datetime import datetime
import copy
import random

pollPeriod = 10
GlobalUptime = 0
RegCount = 12
WaitingForBooyUpTimeout = 400
vrbDbg = 10
vrbDbg2 = 8
vrbPrintTimeStamps = 9
vrbMustPrint = 0
maxVerbosity = vrbDbg2
HaveToExit = False

uptime_id = random.randint(0, 10000000)
server_start_time_str = datetime.strftime(datetime.now(), '%Y-%m-%d %H-%M-%S')
log_filename = f'claymonlog/claymonlog/claymonlog {server_start_time_str[:-3]}.txt'

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
            f = open(log_filename, 'a')
            print(tstamp, sep = " ", end =  " ", file = f, flush = False)
            for arg in args:
                print(arg, sep = " ", end = " ", file = f, flush = False)
            print("", file = f, flush = True)
            f.close()
        except:
            print("Ooops")

def API(data):
    udata = data.decode("utf-8")
    try:
        packet = json.loads(udata)
        printDbg(vrbMustPrint, "API command recieved ", packet)
        command = packet["Command"]
        if command == "Exit":
            global HaveToExit
            HaveToExit = True
            return
        elif command == "Replicate":
            UpdateLocalHistory()
            dbname = packet["Name"]
            FillAndCopyHistoryDB(dbname)
            return
        elif command == "Report":
            period = packet["Data"]
            UpdateLocalHistory()
            BuildMonitoringReport(period)
        elif command == "State":
            responce_buff = GetState()
            printDbg(vrbDbg,'API, responce_buff: ', responce_buff)
            return responce_buff
        else:
            printDbg(vrbMustPrint, "Unknown command = ", packet)
    except:
        printDbg(vrbMustPrint, "Wrong udata = ", udata)

def GetIdleGPUsList():
    # Задача - получить все видяхи, которые по таблице GPUs установлены, но по таблице LastDayGPUStats - не работают
    sql = """
        With
        t1 as (Select Num, Rig, BusNum From GPUs g Where g.Rig >= 0),
        t2 as (Select Max(Seconds) s from LastDayGPUStats),
        t3 as (Select * From LastDayGPUStats Where Seconds >= (Select s from t2) - 1200),
        t4 as (Select Worker, BusNum From t3 Group by Worker, BusNum),
        t5 as (Select Rig, BusNum From t4 inner join Workers w On t4.worker = w.worker),
        t6 as (Select Num, g.Rig, g.BusNum from t5 inner join GPUs g on t5.Rig = g.Rig and t5.BusNum = g.BusNum)
        Select * from t1 except Select * from t6    
    """
    conn = sqlite3.connect('Claymon.sqlite')
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows

def GetProductivityComedownGPUsList(lastHours):
    sql = """
        With 
        t1 as (Select Num GPUNum, targetHr maxHr, Rig, BusNum BN From GPUs where Rig >= 0),
        t2 as (Select Sum(Hr) from t1),
        t3 as (Select * From GPUsHistory where EndSecs > strftime('%s', 'now', 'localtime') - 5 * 3600 - {0} * 3600),
        t4 as (Select Sum(Invalid) inv, Sum(SharesProduced) shares, Max(AvgT) T, Sum(AvgHashrate * (EndSecs - StartSecs)) accHr, Min(StartSecs) st, strftime('%s', 'now', 'localtime') - 5 * 3600 nowSecs, GPUNum From t3 Group by GPUNum),
        t5 as (Select 1.0*inv/shares invrate, accHr / (nowSecs - st) effHr, T, GPUNum From t4),
        t6 as (Select t1.GPUNum, Rig, BN, round((1-invrate)*effHr/ 10)/100 effHR, round((maxHr - (1-invrate)*effHr)/10)/100 losses, maxHr/1000 maxHr, 100*invrate, T From t1 inner join t5 on t1.GPUNum = t5.GPUNum order by losses DESC)
        Select * from t6    
    """.format(lastHours)
    conn = sqlite3.connect('Claymon.sqlite')
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows

def GetProductivityComedownWorkersList(lastHours):
    sql = """
        With 
        t3 as (Select * From WorkersHistory where EndSecs > strftime('%s', 'now', 'localtime') - 5 * 3600 - {0} * 3600),
        t4 as (Select Sum(AvgHashrate * (EndSecs - StartSecs)) accHr, Avg(AvgHAshrate) ahr, Min(StartSecs) st, strftime('%s', 'now', 'localtime') - 5 * 3600 nowSecs, Worker From t3 Group by Worker),
        t5 as (Select accHr / (nowSecs - st) effHr, aHr, Worker From t4),
        t7 as (Select (effHr)/1000 hr, aHr/1000 aHr, Worker from t5 Where worker <> "Total")
        Select Worker, (1-hr/aHr)*{0} idleTime, hr, ahr from t7 Where idletime > 0.5 order by idleTime Desc    
    """.format(lastHours)
    conn = sqlite3.connect('Claymon.sqlite')
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows


def BuildMonitoringReport(period):
    tstamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H-%M-%S")
    fname = "MonitoringReport " + tstamp + ".txt"

    f = open(fname, 'w')
    try:
        l1 = GetIdleGPUsList()
        d1, d2 = [], []
        print("Monitoring report, build", tstamp, "for a period of", period, "hours", file=f)
        print("Idle GPUs, that did not even work a second:", file=f)
        for r in l1:
            print("GPU%03d, M%02d, BN%02d" % r, file=f)
            d1.append(r[0])

        l2 = GetProductivityComedownWorkersList(period)
        print("", file=f)
        print("Workers, that hangs on or reboot too often:", file=f)
        print("Worker, Wasted hours, Effective Hrt, Normal hashrate", file=f)
        for r in l2:
            print("%6s, %12.2f, %8.2f Mh/s, %8.2f Mh/s" % r, file=f)
            d2.append(r[0])

        l3 = GetProductivityComedownGPUsList(period)
        print("", file=f)
        print("Not idle GPUs, that has losses of hashrate:", file=f)
        print("   GPU, Rig, BusNum, Effctv Hrt, Lost Hrate,  Max Hrate, Invalids,   T °C", file=f)

        for r in l3:
            if (not ((r[0] in d1) or ("M" + str(r[1]) in d2))) and ((r[4] > 1.0) or (r[5] >= 80)):
                print("GPU%03d, M%02d,   BN%02d, %5.2f Mh/s, %5.2f Mh/s, %5.2f Mh/s, %6.2f %%, %6.2f °C" % r, file=f)

        print("", file=f)
        print("There is possibly wrong GPUs in these rigs and BNs, please check it:", file=f)

        for r in l3:
            if r[4] < -1.0:
                print("GPU%03d, M%02d, BN%02d" % (r[0], r[1], r[2]), file=f)

    except:
        print("Shit hapened")
    f.close()

def UpdateLocalHistory():
    printDbg(vrbDbg2, 'Local history: ready to fetch workers history from main DB')

    conn = sqlite3.connect('Claymon.sqlite')

    sql = """
        CREATE TABLE IF NOT EXISTS Settings (
            Name     STRING (50) UNIQUE,
            ValueInt INTEGER);
        """
    conn.execute(sql)
    conn.commit()
    printDbg(vrbDbg2, "Settings table has recreated if was not existed")

    sql = """
        INSERT INTO Settings (Name, ValueInt) VALUES ("AlsoReplicatedTill", NULL)
        """
    try:
        conn.execute(sql)
        conn.commit()
    except:
        printDbg(vrbDbg2, "Settings table also exists and populated")

# 1 создаем временную таблицу
    sql = """
        DROP TABLE StatsTmp 
    """
    try:
        conn.execute(sql)
        conn.commit()
    except:
        printDbg(vrbDbg2, "There is not StatsTmp table in DB")

    printDbg(vrbDbg2, "Trying to create StatsTmp table")
    sql = """
        CREATE TABLE StatsTmp (
            s        INTEGER,
            worker   STRING (5),
            Uptime   INTEGER,
            Hashrate INTEGER,
            Shares   INTEGER,
            Invalid  INTEGER);
    """
    conn.execute(sql)
    conn.commit()
    printDbg(vrbDbg2, "StatsTmp table has created")

#2 подтягиваем значение секунд, до которых стата уже перегружена в хистори
#3 Перегоняем во временную таблицу отфильтрованную стату
    sql = """
        WITH 
            t2 AS (Select Max(IfNull(ValueInt, 0)) lastSeconds FROM Settings Where Name = "AlsoReplicatedTill"
            UNION ALL Select round(Min(seconds)) s FROM LastDayWorkerEthStats),
            t1 AS (Select Max(lastseconds) lastseconds FROM t2),
            ws AS (Select round(seconds) s, Worker, Uptime, IfNull(Ethhashrate, 0) hashrate, Shares, Invalid from LastDayWorkerEthStats, t1 where hashrate > 0 And seconds > lastSeconds Limit 10000)
            Insert Into StatsTmp Select * FROM ws
    """
    conn.execute(sql)
    conn.commit()
    printDbg(vrbDbg2, "StatsTmp table has populated")


#4 Поднимаем только более позднюю стату, для перегрузки в хистори
    sql = """
        WITH
        ws AS (Select * From StatsTmp),

        starts AS (SELECT w1.s, w1.worker FROM ws w1 inner join ws w2 on w1.worker = w2.worker and w1.s - w2.s between 0 and 30
        group by w1.s, w1.worker Having Count(*) = 1),
        
        ends AS (SELECT w1.s, w1.worker FROM ws w1 inner join ws w2 on w1.worker = w2.worker and w1.s - w2.s between -30 and 0
        group by w1.s, w1.worker Having Count(*) = 1),
        
        tmp1 AS (Select st.s s1, e.s s2, st.worker From starts st Inner Join Ends e
        On st.worker = e.worker AND st.s <= e.s),
        
        periods AS (Select s1, Min(s2) s2, worker from tmp1 group by s1, worker),
        
        tmp2 AS (Select s1, s2, s, IfNull(Uptime, 0) uptime, periods.worker, Hashrate, IfNull(Shares, 0) shares, IfNull(Invalid, 0) inv from periods inner join ws ON periods.worker = ws.worker AND ws.s Between periods.s1 And periods.s2),
        
        PsHrShr AS (Select worker, s1, s2, IfNull(AVG(Hashrate), 0) Hashrate, Max(shares)-Min(shares) shares, Max(inv)-Min(inv) inv, Max(uptime) UpTime from tmp2
        Group by worker, s1, s2)
        
        Select * from PsHrShr
    """

#5 Заносим переработанную стату в хистори
    cursor = conn.cursor()
    cur = cursor.execute(sql)
    historyW = cur.fetchall()
    printDbg(vrbDbg2, 'Workers history fetched ok, total rows: ', len(historyW))

    printDbg(vrbDbg2, 'Now insert history to WorkersHistory table')
    conn.executemany('INSERT INTO WorkersHistory (Worker, StartSecs, EndSecs, AvgHashrate, SharesProduced, Invalid, UpTime) VALUES (?,?,?,?,?,?,?)', historyW)
    conn.commit()
    printDbg(vrbDbg2, 'WorkersHistory table has populated')

#6 Повторяем ##2-5 для гпустатс и гпухистори

#6-1 создаем временную таблицу
    sql = """
        DROP TABLE StatsTmpGPU 
    """
    try:
        conn.execute(sql)
        conn.commit()
    except:
        printDbg(vrbDbg2, "There is not StatsTmpGPU table in DB")

    printDbg(vrbDbg2, "Trying to create StatsTmpGPU table")
    sql = """
        CREATE TABLE StatsTmpGPU (
            Secs           INTEGER,
            Worker         STRING(5),
            BusNum         INTEGER                      NOT NULL,
            EthHashrate    INTEGER,
            T              INTEGER,
            Fan            INTEGER,
            Accepted       INTEGER,
            Invalid        INTEGER);    
        """
    conn.execute(sql)
    conn.commit()
    printDbg(vrbDbg2, "StatsTmpGPU table has created")

#6-2 подтягиваем значение секунд, до которых стата уже перегружена в хистори
#6-3 Перегоняем во временную таблицу отфильтрованную стату
    sql = """
        WITH 
        t2 AS (Select Min(s) s1, Max(s) s2 From StatsTmp),
        raw1 AS (Select round(Seconds) secs, Worker, BusNum, EthHashrate, T, Fan, Accepted, Invalid from LastDayGPUStats, t2 where EthHashrate > 0 And seconds BETWEEN s1 AND s2)
        Insert Into StatsTmpGPU Select * FROM raw1
    """
    conn.execute(sql)
    conn.commit()
    printDbg(vrbDbg2, "StatsTmpGPU table has populated")


#6-4 Поднимаем только более позднюю стату, для перегрузки в хистори
    sql = """
        WITH
        raw1 AS (Select * FROM StatsTmpGPU),
        
        starts AS (SELECT t1.secs, t1.worker, t1.busnum FROM raw1 t1 inner join raw1 t2 on t1.Worker = t2.worker and t1.BusNum = t2.BusNum and t1.secs - t2.secs between 0 and 30
        group by t1.secs, t1.worker, t1.BusNum Having Count(*) = 1),
        
        ends AS (SELECT t1.secs, t1.worker, t1.busnum FROM raw1 t1 inner join raw1 t2 on t1.Worker = t2.worker and t1.BusNum = t2.BusNum and t1.secs - t2.secs between -30 and 0
        group by t1.secs, t1.worker, t1.BusNum Having Count(*) = 1),
        
        tmp2 AS (Select st.secs s1, e.secs s2, st.worker, st.busnum From starts st Inner Join Ends e
        On st.worker = e.worker AND st.BusNum = e.BusNum AND st.secs <= e.secs),
        
        periods AS (Select s1, Min(s2) s2, worker, busNum from tmp2 group by s1, worker, BusNum),
        
        tmp3 AS (Select s1, s2, secs, periods.worker, periods.busNum, EthHashrate hashrate, T, Fan, IfNull(Accepted, 0) shares, IfNull(Invalid, 0) inv from periods inner join raw1 ON periods.worker = raw1.worker AND periods.busNum = raw1.busNum AND raw1.secs Between periods.s1 And periods.s2),
        
        tmp4 AS (Select worker, busNum, s1, s2, AVG(Hashrate) hashrate, AVG(T) T, AVG(Fan) Fan, Max(shares) - Min(shares) shares, Max(inv)-Min(inv) inv from tmp3
        Group by worker, busnum, s1, s2),
        
        tmp5 AS (Select s1, s2, Rig, BusNum, hashrate, T, Fan, shares, inv From tmp4 Inner Join Workers ON tmp4.worker = Workers.Worker),
        
        gpuHist AS (Select s1, s2, tmp5.Rig, tmp5.BusNum, Num GpuNum, hashrate, T, Fan, shares, Inv From tmp5 Inner Join GPUs ON tmp5.Rig = GPUs.Rig AND tmp5.BusNum = GPUs.BusNum)
        
        Select * FROM gpuHist    
    """

#6-5 Заносим переработанную стату в хистори
    cur = cursor.execute(sql)
    historyGPU = cur.fetchall()
    printDbg(vrbDbg2, 'GPUs history fetched ok, total rows: ', len(historyGPU))

    printDbg(vrbDbg2, 'Now insert history to GPUsHistory table')
    conn.executemany('INSERT INTO GPUsHistory (StartSecs, EndSecs, Rig, BusNum, GPUNum, AvgHashrate, AvgT, AvgFan, SharesProduced, Invalid) VALUES (?,?,?,?,?,?,?,?,?,?)', historyGPU)
    conn.commit()

#7 Обновляем в БД значение секунд, до которых стата уже перегружена в хистори
    sql = """
        UPDATE Settings
        SET ValueInt = (Select Max(s) s2 From StatsTmp)
        WHERE Name = "AlsoReplicatedTill";
    """
    conn.execute(sql)
    conn.commit()

#8 Дропаем временные таблицы
    sql = """DROP TABLE StatsTmpGPU"""
    try:
        conn.execute(sql)
        conn.commit()
    except:
        printDbg(vrbDbg2, "There is no StatsTmpGPU table in DB")

    sql = """DROP TABLE StatsTmp"""
    try:
        conn.execute(sql)
        conn.commit()
    except:
        printDbg(vrbDbg2, "There is no StatsTmp table in DB")

    conn.close()
#9 Не забыть напечать в лог время работы всех запросов


def FillAndCopyHistoryDB(newDBName):
# 1. делаем копию файла с пустой дб
    try:
        dbName = newDBName
        if newDBName == "":
            dbName = 'DenseHistory '+datetime.strftime(datetime.now(), "%Y-%m-%d %H-%M-%S") + '.sqlite'
        shutil.copyfile('DenseHistoryEmpty.sqlite', dbName)
        printDbg(vrbDbg2, 'empty DB file copied ok')
    except:
        printDbg(vrbMustPrint, 'Cannot copy DB file')
        return

# 2. Подготавливаем историю

# 2-1. Подтягиваем секунды, раньше которых историю копировать не нужно, она уже была скопирована
    printDbg(vrbDbg2, 'Ready to connect to main DB')
    conn = sqlite3.connect('Claymon.sqlite')
    sql = """
        INSERT INTO Settings (Name, ValueInt) VALUES ("AlsoHistoryCopiedTill", NULL)
        """
    try:
        conn.execute(sql)
        conn.commit()
    except:
        printDbg(vrbDbg2, "Settings table also exists and populated")

    cursor = conn.cursor()

    printDbg(vrbDbg2, 'Ready to fetch workers history from main DB')
    sql = """
        Select * from WorkersHistory 
        Where StartSecs >= (Select Max(IfNull(ValueInt, 0)) lastSeconds FROM Settings Where Name = "AlsoHistoryCopiedTill")
    """

    cur = cursor.execute(sql)
    historyW = cur.fetchall()
    printDbg(vrbDbg2, 'Workers history fetched ok, total rows: ', len(historyW))

    cursor2 = conn.cursor()
    printDbg(vrbDbg2, 'Ready to fetch GPU history from main DB')
    sql2 = """
        Select * from GPUsHistory 
        Where StartSecs >= (Select Max(IfNull(ValueInt, 0)) lastSeconds FROM Settings Where Name = "AlsoHistoryCopiedTill")
    """

    cur2 = cursor2.execute(sql2)
    historyGPU = cur2.fetchall()
    printDbg(vrbDbg2, 'GPU history fetched ok, total rows: ', len(historyGPU))
    conn.close()

# 3. Переливаем историю в пустую дб
    printDbg(vrbDbg2, 'Ready to connect to empty DB')
    conn = sqlite3.connect(dbName)
    printDbg(vrbDbg2, 'Now insert history to empty DB')
    conn.executemany('INSERT INTO WorkersHistory (Worker, StartSecs, EndSecs, AvgHashrate, SharesProduced, Invalid, Uptime) VALUES (?,?,?,?,?,?,?)', historyW)
    conn.commit()

    conn.executemany('INSERT INTO GPUsHistory (StartSecs, EndSecs, Rig, BusNum, GPUNum, AvgHashrate, AvgT, AvgFan, SharesProduced, Invalid) VALUES (?,?,?,?,?,?,?,?,?,?)', historyGPU)
    conn.commit()

    printDbg(vrbDbg2, 'History inserted ok')
    conn.close()

# 4. копируем заполненую дб в место назначения
    try:
        printDbg(vrbDbg2, 'Ready to copy DB slice to cloud')
        shutil.copyfile(dbName, 'R:' + '\\' + dbName)
        printDbg(vrbDbg2, 'DB slice copied to clowd ok')
        os.remove(dbName)
        printDbg(vrbDbg2, 'DB slice src removed')
    except:
        printDbg(vrbMustPrint, 'Cannot copy DB file')
        return
# 5. Пишем в БД секунды до которых была скопирована история
    conn = sqlite3.connect('Claymon.sqlite')
    sql = """
        UPDATE Settings
        SET ValueInt = (Select Max(EndSecs) s2 From WorkersHistory)
        WHERE Name = "AlsoHistoryCopiedTill";
        """
    conn.execute(sql)
    conn.commit()
    conn.close()
    printDbg(vrbDbg2, "AlsoHistoryCopiedTill field has updated")


def watchDog(event_for_wait, event_for_set):
    global HaveToExit
    while not HaveToExit:
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 1")
        event_for_wait.wait()
        event_for_wait.clear()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 2")
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 3")
        event_for_set.set()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "WD 4")

def purgeLastDayStat(event_for_wait, event_for_set):
    i = 0
    global HaveToExit
    while not HaveToExit:
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Purge 1")
        event_for_wait.wait()
        event_for_wait.clear()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Purge 2")
        i += 1
        if i%90 == 1:
            UpdateLocalHistory()

        # if i%1000 == 2:
        #     FillAndCopyHistoryDB("")

        if i % 1000 == 0:
            printDbg(vrbMustPrint, "purge old stat in DB")
            conn = sqlite3.connect('Claymon.sqlite')
            conn.execute('Delete From LastDayWorkerEthStats Where (Select Max(seconds) from LastDayWorkerEthStats) - seconds > 86400', [])
            conn.execute('Delete From LastDayGPUStats Where (Select Max(seconds) from LastDayGPUStats) - seconds > 86400', [])
            conn.commit()
            conn.close()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Purge 3")
        event_for_set.set()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Purge 4")

def pollStat (wrks, name, statspool, event_for_wait, event_for_set):
    global HaveToExit
    while not HaveToExit:
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
            try:
                stat = [wrk["Name"], json.loads(udata)]
            except:
                printDbg(vrbMustPrint, "udata = ", udata)
                stttt = [wrk["Name"], None]
                stats.append(stttt)
                offline.append(wrk["Name"])
            online.append(wrk["Name"])
            sock.close()
            stats.append(stat)

        nowstr = datetime.strftime(datetime.now(), '%Y-%m-%d %H-%M-%S')
        meta = ["Meta", dict(uptime_id = uptime_id, ts = nowstr, start_time = server_start_time_str)]
        stats.append(meta)

        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Poll 1", name)
        event_for_wait.wait()
        event_for_wait.clear()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Poll 2", name)
        printDbg(vrbMustPrint, 'Poll thread #', name, len(stats), 'total workers. Online:', online, 'Offline:', offline)
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Poll 3", name)
        statspool.append(stats)
        printDbg(vrbDbg, 'pollStat', statspool)
        event_for_set.set()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Poll 4", name)

def pushStatToDB(workers, statspool, event_for_wait, event_for_set):
    global HaveToExit, GlobalUptime, laststats
    local_stats = {}
    while not HaveToExit:
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Push 1")
        event_for_wait.wait()
        event_for_wait.clear()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Push 2")
        rez1, rez2 = [], []
        wrksOnline, gpuOnline = 0, 0
        wrksOffline = []
        TotalHashrate = 0
        printDbg(vrbDbg, "----------------------------------------------------------------------")
        printDbg(vrbDbg, 'pushStatToDB', statspool)
        laststats = copy.deepcopy(statspool)
        nowstr = datetime.strftime(datetime.now(), '%Y-%m-%d %H-%M-%S')
        local_stats[nowstr] = (copy.deepcopy(statspool))
        if len(local_stats) >= 10:
            fname = r"U:\logs\stat\%s.json" % nowstr
            try:
                buff = json.dumps(local_stats)
                # rez = buff.encode('utf-8')
                stat_file = open(fname, 'w')
                stat_file.write(buff)
                stat_file.close()
                printDbg(vrbMustPrint, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), 'push stat to file %s successfully'%fname)
            except Exception as e:
                printDbg(vrbMustPrint, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), str(e))
            local_stats = {}

        while statspool:
            stats = statspool.pop()
            for stat in stats:
                if stat[0] == 'Meta':
                    continue
                tst = stat[1]
                trz = None
                if tst != None:
                    try:
                        trz = tst['result']
                    except:
                        printDbg(vrbMustPrint, '!!!!!!!!!!!!!!!!!!'+tst+'!!!!!!!!!!!!!!!!!!')
                        trz = None
                        
                tm = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
                tm2 = round(time.time())
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
                    TotalHashrate += float(erate)
                    shares = lst1[1]
                    rej = lst1[2]
                    lst2 = trz[8].split(";")
                    inv = lst2[0]
                    sw = lst2[1]
                    bns = trz[15].split(";")
                    gpuethhr = trz[3].split(";")
                    tfans = trz[6].split(";")
                    ts = tfans[0::2]
                    fans = tfans[1::2]
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

                        try:
                            stgpu = (tm, tm2, wrk, bns[i], gpustate, dbhr, ts[i], fans[i], gpushares[i], gpurej[i], gpuinv[i])
                            rez2.append(stgpu)
                        except:
                            printDbg(vrbMustPrint, bns)
                            printDbg(vrbMustPrint, ts)
                            printDbg(vrbMustPrint, fans)
                            printDbg(vrbMustPrint, gpushares)
                            printDbg(vrbMustPrint, gpurej)
                            printDbg(vrbMustPrint, gpuinv)
                else:
                    st1 = (tm, tm2, wrk, ip, port, None, None, None, None, None, None, None)
                    wrksOffline.append(wrk)
                rez1.append(st1)
        st1 = (tm, tm2, 'Total', '', 0, 0, GlobalUptime, TotalHashrate, 0, 0, 0, 0)
        rez1.append(st1)

        conn = sqlite3.connect('Claymon.sqlite', timeout=30)
        conn.executemany('INSERT INTO LastDayWorkerEthStats (time, seconds, worker, ip, port, ClaymoreVer, Uptime, EthHashrate, Shares, Rejected, Invalid, PoolSwitches) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', rez1)
        conn.executemany('INSERT INTO LastDayGPUStats (time, seconds, worker, BusNum, State, EthHashrate, T, Fan, Accepted, Rejected, Invalid) VALUES (?,?,?,?,?,?,?,?,?,?,?)', rez2)
        conn.commit()
        conn.close()
        printDbg(vrbMustPrint, tm)
        printDbg(vrbMustPrint, "Push stat to DB", len(rez1), 'workers', len(rez2), 'gpu')
        printDbg(vrbMustPrint, "Workers online", wrksOnline, 'gpu works', gpuOnline)
        printDbg(vrbMustPrint, "Offline workers", wrksOffline)
        printDbg(vrbMustPrint, "TotalHashrate", TotalHashrate/1000)
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Push 3")
        event_for_set.set()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Push 4")

def delayThr( event_for_wait, event_for_set):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("", 6111))
    sock.listen(1)

    global HaveToExit, GlobalUptime, log_filename

    while not HaveToExit:
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 1")
        event_for_wait.wait()
        event_for_wait.clear()
        printDbg(vrbDbg, datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Delay 2")
        GlobalUptime += pollPeriod
        days = GlobalUptime // 86400
        prev_days = (GlobalUptime - pollPeriod) // 86400
        if days != prev_days and days > 0:
            time_str = datetime.strftime(datetime.now(), '%Y-%m-%d %H-%M')
            log_filename = f'claymonlog/claymonlog/claymonlog {time_str}.txt'

        sock.settimeout(pollPeriod)
        try:
            conn, addr = sock.accept()
            data = conn.recv(1000)
            if data:
                responce_buff = API(data)
            if data and responce_buff is not None:
                printDbg(vrbDbg2, "Try to send response")
                conn.send(responce_buff)
                printDbg(vrbDbg2, "Response has sended", len(responce_buff))
            conn.close()
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

def GetState():
    printDbg(vrbDbg, "GetState")
    global laststats
    printDbg(vrbMustPrint, "GetState", laststats)
    buff = json.dumps(laststats)
    rez = buff.encode('utf-8')
    return rez

printDbg(vrbMustPrint, "===================================================", datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "Claymore monitor started. Version 0.01 ===================================================" )
ws = []
LoadWorkersFromDB()

thrCnt = len(ws) // pollPeriod
printDbg(vrbMustPrint, "pollPeriod = ", pollPeriod, "Threads count = ", thrCnt)
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
laststats = []
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
