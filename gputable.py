import socket, json, sqlite3, threading, time, sys, shutil, os, random
from datetime import datetime

def GetGPUsJSONFromDB():
    sql = """
        With
        t1 as (Select DISTINCT g.Rig From GPUs g Where g.Rig >= 0 Order by 1 ASC)
        Select * from t1
    """
    conn = sqlite3.connect('Claymon.sqlite')
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    rez = {}
    for row in rows:
        rig = row[0]
        rez[rig] = {}
    sql = """
        With
        t1 as (Select Rig, BusNum, Num From GPUs Where Rig >= 0 Order by 1, 2 ASC)
        Select * from t1
    """
    conn = sqlite3.connect('Claymon.sqlite')
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        rigNum, BN, gpu = row
        rig = rez[rigNum]
        rig[BN] = gpu

    sql = """
        With
        t1 as (Select Rig, Num From GPUs Where Rig < 0 Order by 2 ASC)
        Select * from t1
    """
    conn = sqlite3.connect('Claymon.sqlite')
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    for row in rows:
        rigNum, gpu = row
        if not rigNum in rez:
            rez[rigNum] = []
        rez[rigNum].append(gpu)

    return rez


def SaveGPUsTableToFile(rigs, filename):
    f = open(filename, 'w')
    for r in rigs:
        rig = rigs[r]

        if r < 0:
            s3 = ('Shell', 'Returned', 'Home', 'Unknown')[-1-r]
            l = ['GPU%03d'%(g) for g in rig]
            s4 = ', '.join(l)
            print(s3, ':', s4, file = f)
            continue

        l =  ['BN%02d: GPU%03d'%(bn, rig[bn]) for bn in rig]
        s = ', '.join(l)
        s2 = 'M%02d : {%s}'%(r, s)

        print(s2, file = f)
    f.close()
    return None

def LoadGPUsFromFile(filename):
    rezs = {}
    f = open(filename, 'r')
    for s in f:
        l = s[:-1].split(' : ')
        if l[0].isalpha():
            rig = -1 - ('Shell', 'Returned', 'Home', 'Unknown').index(l[0])
            s6 = l[1].replace('GPU', '')
            l2 = s6.split(', ')
            l3 = [int(sg) for sg in l2]
            rezs[rig] = l3
            continue
        rig = int(l[0][1:])
        s2 = l[1].replace('BN', '')
        s9 = s2.replace('GPU', '')
        s3 = s9[1:-1]
        l3 = s3.split(', ')
        rez = {}
        for s4 in l3:
            l4 = s4.split(': ')
            bn = int(l4[0])
            gpu = int(l4[1])
            rez[bn] = gpu
#        print(rez)
        rezs[rig] = rez
    return rezs

def CheckGPUs(rigs):
    rez = 0
    gpus = {}
    for r in rigs:
        if r < 0:
            addr = ('Shell', 'Returned', 'Home', 'Unknown')[-1-r]
            tmpg = {}
            l = rigs[r]
            for g in l:
                tmpg[g] = addr
        else:
            rig = rigs[r]
            tmpg = {}
            for bn in rig:
                g = rig[bn]
                tmpg[g] = 'M%02d BN%02d'%(r, bn)

        for gpu in tmpg:
            addr = tmpg[gpu]
            if gpu in gpus:
                rez = 1                 
                print('Duplicate!, GPU%03d at %s'%(gpu, addr))
            else:
                gpus[gpu] = addr
    return rez

def PushGPUsToDB(rigs):
    backupFN = 'gpus_backup_%s.txt'%(datetime.strftime(datetime.now(), "%Y-%m-%d %H-%M-%S"))
    backupRigs = GetGPUsJSONFromDB()
    SaveGPUsTableToFile(backupRigs, backupFN)

    rows1 = []
    rows2 = []
    for r in rigs:
        rig = rigs[r]
        if r >= 0:
            for bn in rig:
                rows1.append((rig[bn],))
                rows2.append((r, bn, rig[bn]))
        else:
            rows1.extend([(g,) for g in rig])
            rows2.extend([(r, None, g) for g in rig])

    sql = """
        UPDATE GPUs
        SET Rig = Null, BusNum = Null
        WHERE Num = ?;
    """
    conn = sqlite3.connect('Claymon_test.sqlite')
    conn.executemany(sql, rows1)
    conn.commit()

    sql = """
        UPDATE GPUs
        SET Rig = ?, BusNum = ?
        WHERE Num = ?;
    """
    print(rows2)
    conn.executemany(sql, rows2)
    conn.commit()

def Test1():
    rigs = GetGPUsJSONFromDB()
    SaveGPUsTableToFile(rigs, 'test.txt')
    rigs = LoadGPUsFromFile('test.txt')
    error = CheckGPUs(rigs)
    if error == 0:
        print('GPU table is correct, duplicates not found')
        PushGPUsToDB(rigs)
    rigs2 = GetGPUsJSONFromDB()
    SaveGPUsTableToFile(rigs, 'test2.txt')

def Test2():
    rigs = LoadGPUsFromFile('test.txt')
    addrL = []
    gpuL = []
    for r in rigs:
        rig = rigs[r]
        if r >= 0:
            for bn in rig:
                gpuL.append(rig[bn])
                addrL.append((r, bn))
        else:
            gpuL.extend(rig)
            addrL.extend([(r, None) for g in rig])
    random.shuffle(addrL)
    rigs2 = {}
    z = zip(gpuL, addrL)
    for it in z:
        print(it)
        r = it[1]
        if r < 0:
            rigs2[r] = []
        else:
            rigs2[r] = {}

    for it in z:
        g, r, b = it
        rig = rigs2[r]
        if r >= 0:
            rig[b] = g
        else:
            rig.append(g)
            
    print(rigs2)
        

    

Test2()
input()