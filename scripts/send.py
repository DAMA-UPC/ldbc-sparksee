import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = "127.0.0.1"
port = 9998

s.connect((host,port))
s.send('{"query1" : [{"id":"1", "name":"A", "limit":"10"}]}') 
data = ''
data = s.recv(1024).decode("UTF8");
print data
s.close ()
