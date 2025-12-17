import socket
HOST='127.0.0.1'
PORT=5002
s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((HOST,PORT))
s.listen(1)
print('Listening on', HOST, PORT)
try:
    conn, addr = s.accept()
    print('Accepted', addr)
    data = conn.recv(1024)
    print('Received', data)
    conn.sendall(b'OK')
    conn.close()
finally:
    s.close()
