import re
import glob
import sys

inputFileName = sys.argv[1]
outputBaseFileName = sys.argv[2]
numOutputFiles = int(sys.argv[3])
numPartitions = int(sys.argv[4])

inputFile = open(inputFileName,'r')
header = inputFile.readline()
totalNumFiles = numOutputFiles*numPartitions
outputFiles = []
for i in range(0,numOutputFiles):
    for j in range(0,numPartitions):
        outputFiles.append(open(outputBaseFileName+"_"+str(i)+"_"+str(j)+".csv","w"))
        outputFiles[-1].write(header)

count = 0
column = 1
lastValue = -1
currentOutputFile = 0
for line in inputFile.readlines():
    #if count != 0:
    fields = line.split("|")
    if int(fields[column]) != lastValue:
        currentOutputFile = (currentOutputFile + 1) % totalNumFiles   
        lastValue = int(fields[column])
    outputFiles[currentOutputFile].write(line)
    #count=1

inputFile.close()
for file in outputFiles:
    file.close();
