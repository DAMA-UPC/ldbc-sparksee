import socket
import time

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = "127.0.0.1"
port = 9990
connected = False

while not connected:
    time.sleep(5)
    try:
        print("Testing server connectivity")
        s.connect((host,port))
        connected = True
    except socket.error as msg:
        print(msg)
    finally:
        s.close()

print "Server Up!"
