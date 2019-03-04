import socket, json, sqlite3, threading, time
from datetime import datetime

serverIP = '192.168.2.22'
serverAPIport = 6116

sock = socket.socket()
sock.settimeout(1)
ip = 'localhost'
#ip = serverIP
port = serverAPIport
sock.connect((ip, port))

try:
    buff = json.dumps({"Command": "Exit"})
    sock.send(buff.encode('utf-8'))

    sock.connect((ip, port))
    sock.send(buf)
except socket.error as msg:
    print('Command does not sended')
sock.close()
sock = None
print('Ok')