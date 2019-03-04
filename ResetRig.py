import socket, json, sqlite3, threading, time
from datetime import datetime

Rig = 37
RegCount = 12

def Reset(rig):
            conn = sqlite3.connect('Claymon.sqlite')
            cursor = conn.cursor()

            sql = """
Select rigNum, r.Address, ResetPin
From Rigs r Inner Join WatchDogPins p
On r.Address = p.Address
Where RigNum = """ + str(rig)

            cur = cursor.execute(sql)
            rigsToReset = cur.fetchall()
            conn.close()
            print(rigsToReset)

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
                    pin = rig[2]
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
                ip = '192.168.2.77'
                port = 7850
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

while 1==1:
                print("Input rig number to reset")
                rig = int(input())
                Reset(rig)