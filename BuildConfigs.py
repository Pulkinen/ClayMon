import socket, json, sqlite3, threading, time, sys, shutil, os
from datetime import datetime

GlobalUptime = 0
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

# Общий алгоритм формирования конфигов:
# Находим средний хешрейт каждого воркера за последние сутки
# По сэту хешрейтов раскидываем хешрейты пользователей
# Строим конфиги

def GetLastDayWorkersHashrate():
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
