import socket
import sys

print "Shutting down server"
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = sys.argv[1] 
port = 9999

s.connect((host,port))
s.close ()

print "Server shutdown"
