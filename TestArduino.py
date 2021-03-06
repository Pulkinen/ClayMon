import socket, json, sqlite3, threading, time
from datetime import datetime

RegCount = 12
AllZeroes = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
AllOnes = [255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]
#bytesToSend = (170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170)
#bytesToSend = (255, 255, 255, 255, 255, 255,   0,   0,   0,   0,   0,   0)

fr1 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


def SendBytes(bts):
    bytesToSend = bts[:]
    buf = bytearray(3+RegCount)
    buf[0] = 0x5A
    buf[1] = RegCount
    sum = buf[1]
    sss = ''.rjust(8*RegCount, '1')
    for i in range(RegCount):
        p1 = 8*i
        p2 = p1+8
        a = sss[p1:p2]
        b = int(a,2)
        buf[i+2] = b
        buf[i+2] = bytesToSend[i]
        sum += buf[i+2]
        buf[-1] = sum % 256

    sock = socket.socket()
    sock.settimeout(1)
    #ip = '192.168.2.44'
    ip = '192.168.2.77'
    port = 7850
    #ip = 'localhost'
    #port = 6116
    try:
        sock.connect((ip, port))
        sock.send(buf)
    except socket.error as msg:
        print('watchdog is offline')
    sock.close()
    sock = None
    print('Ok')

while 1==1:
    i = 0
    idx = int(input())
    fr1 = AllOnes[:]
    while i < 8:
        fr1[idx] = 2 ** i
        print(fr1)
        SendBytes(fr1)
        i += 1
        time.sleep(0.4)
        SendBytes(AllOnes)
