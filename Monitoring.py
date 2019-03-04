import socket, json, sqlite3, threading, time, sys, shutil, os, csv
from datetime import datetime

vrbDbg = 10
vrbDbg2 = 8
vrbPrintTimeStamps = 9
vrbMustPrint = 0
maxVerbosity = vrbDbg2

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
            f = open('claymonlog.txt', 'a')
            print(tstamp, sep = " ", end =  " ", file = f, flush = False)
            for arg in args:
                print(arg, sep = " ", end = " ", file = f, flush = False)
            print("", file = f, flush = True)
            f.close()
        except:
            print("Ooops")

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

tstamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H-%M-%S")
fname = "MonitoringReport " + tstamp + ".txt"
period = 24

f = open(fname, 'w')
try:
    l1 = GetIdleGPUsList()
    d1, d2 = [], []
    print("Monitoring report, build", tstamp, "for a period of", period, "hours", file=f)
    print("Idle GPUs, that did not even work a second:", file=f)
    for r in l1:
        print("GPU%03d, M%02d, BN%02d" % r, file = f)
        d1.append(r[0])

    l2 = GetProductivityComedownWorkersList(period)
    print("", file=f)
    print("Workers, that hangs on or reboot too often:", file=f)
    print("Worker, Wasted hours, Effective Hrt, Normal hashrate", file=f)
    for r in l2:
        print("%6s, %12.2f, %8.2f Mh/s, %8.2f Mh/s" % r, file = f)
        d2.append(r[0])

    l3 = GetProductivityComedownGPUsList(period)
    print("", file=f)
    print("Not idle GPUs, that has losses of hashrate:", file=f)
    print("   GPU, Rig, BusNum, Effctv Hrt, Lost Hrate,  Max Hrate, Invalids,   T °C", file=f)

    for r in l3:
        if (not ((r[0] in d1) or ("M"+str(r[1]) in d2))) and ((r[4] > 1.0) or (r[5] >= 80)):
            print("GPU%03d, M%02d,   BN%02d, %5.2f Mh/s, %5.2f Mh/s, %5.2f Mh/s, %6.2f %%, %6.2f °C" % r, file = f)

    print("", file=f)
    print("There is possibly wrong GPUs in these rigs and BNs, please check it:", file=f)

    for r in l3:
        if r[4] < -1.0:
            print("GPU%03d, M%02d, BN%02d" % (r[0], r[1], r[2]), file=f)

except:
    print("Shit hapened")
f.close()