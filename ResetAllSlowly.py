import socket, json, sqlite3, time
WD_ip = "192.168.2.22"
#WD_ip = "127.0.0.1"
WD_port = 6116

for i in range(75):
    buff = json.dumps({"Command": "Rig", "Data" : [i]})
    print(buff)

    sock = socket.socket()
    sock.settimeout(1)
    ip = WD_ip
    port = WD_port
    try:
        sock.connect((ip, port))
        sock.send(buff.encode('utf-8'))
        print("sended ok")
    except:
        print("Watchdog server script is offline")
    time.sleep(30)

