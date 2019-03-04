import socket, json

sock = socket.socket()
sock.settimeout(5)
ip = "192.168.2.100"
port = 3333
try:
                sock.connect((ip, port))
                buff = json.dumps({"id": 0, "jsonrpc": "2.0", "method": "rpc_functions_list"})
                sock.send(buff.encode('utf-8'))
                data = sock.recv(1024)
except socket.error as msg:
                print("exception socket")
                sock.close()
                sock = None
udata = data.decode("utf-8")
print("udata = ", udata)
try:
                stat = json.loads(udata)
except:
                print("exception loads json")
sock.close()
