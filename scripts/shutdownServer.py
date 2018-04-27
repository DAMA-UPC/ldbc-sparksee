import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = "127.0.0.1"
port = 9999

s.connect((host,port))
s.close ()
