import socket, json, sqlite3, threading, time, shutil, os, random
from datetime import datetime

def downloadAndReplicate():
#    try:
        folder = 'Y:\Replication'
        files = os.listdir(folder)
        print(files)
        for db in files:
            filename, ext = os.path.splitext(db)
            if ext != ".sqlite":
                continue
            dbname = folder + '\\' + db
            print(dbname)
            conn = sqlite3.connect(dbname)

            cursor = conn.cursor()
            sql = """
                Select Worker, StartSecs, EndSecs, AvgHashrate, SharesProduced, Invalid, UpTime From WorkersHistory
            """
            cur = cursor.execute(sql)
            historyW = cur.fetchall()
            print('Workers history ', len(historyW), 'rows')

            cursor = conn.cursor()
            sql = """
                Select StartSecs, EndSecs, Rig, BusNum, GPUNum, AvgHashrate, AvgT, AvgFan, SharesProduced, Invalid From GPUsHistory
            """
            cur = cursor.execute(sql)
            historyG = cur.fetchall()
            print('GPUs history ', len(historyG), 'rows')
            conn.close()

            conn = sqlite3.connect('ClayMon.sqlite')
            conn.executemany('INSERT INTO WorkersHistoryTmp (Worker, StartSecs, EndSecs, AvgHashrate, SharesProduced, Invalid, Uptime) VALUES (?,?,?,?,?,?,?)', historyW)
            conn.commit()

            sql = """
                INSERT INTO WorkersHistory
                SELECT * FROM WorkersHistoryTmp
                EXCEPT SELECT * FROM WorkersHistory
            """
            conn.execute(sql)
            conn.commit()

            conn.executemany('INSERT INTO GPUsHistoryTmp (StartSecs, EndSecs, Rig, BusNum, GPUNum, AvgHashrate, AvgT, AvgFan, SharesProduced, Invalid) VALUES (?,?,?,?,?,?,?,?,?,?)', historyG)
            conn.commit()

            sql = """
                INSERT INTO GPUsHistory
                SELECT * FROM GPUsHistoryTmp
                EXCEPT SELECT * FROM GPUsHistory
            """
            conn.execute(sql)
            conn.commit()

            sql = """
                DELETE FROM WorkersHistoryTmp
            """
            conn.execute(sql)
            conn.commit()

            sql = """
                DELETE FROM GPUsHistoryTmp
            """
            conn.execute(sql)
            conn.commit()

            conn.close()
            print('Done')

            os.remove(dbname)

#    except:
#        print('Ooops')


downloadAndReplicate()