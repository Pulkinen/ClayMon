import socket, json, sqlite3, threading, time
from datetime import datetime

def Reset():
            conn = sqlite3.connect('Claymon.sqlite')
            cursor = conn.cursor()

            sql = """
Select rigNum, r.Address, ResetPin
From Rigs r Inner Join WatchDogPins p
On r.Address = p.Address
		""" 

            cur = cursor.execute(sql)
            rigsToReset = cur.fetchall()
            conn.close()
            f = open('rigs.txt', 'w')
            for rig in rigsToReset:
	            print(rig)
	            print(rig, file = f)
            f.close()


Reset()