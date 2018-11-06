import socket
import time
import sys

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = sys.argv[1]
port = 9990
connected = False

while not connected:
    print("Testing server connectivity")
    try:
        s.connect((host,port))
        connected = True
    except socket.error as msg:
        print(msg)

    sys.stdout.flush()
    time.sleep(5)

s.close()
print "Server Up!"
