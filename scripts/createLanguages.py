import re
import glob
from sets import Set

languages = Set()
speaks_filenames = glob.glob("person_speaks_language_?_?.csv")
for filename in speaks_filenames:
    with open(filename) as file:
      count = 0
      print("LOADING "+filename)
      for line in file.readlines():
        if count > 0:
            fields = line[0:-1].split("|")
            languages.add(fields[1])
        count += 1

max_partition = -1
max_thread = -1
for filename in speaks_filenames:
    fields = filename.split('_')
    thread = int(fields[3])
    partition = int(fields[4].split('.')[0])
    if thread > max_thread:
        max_thread = thread 
    if partition > max_partition:
        max_partition = partition 

num_threads = max_thread+1
num_partitions = max_partition+1


outFiles = []

for i in range(0,num_threads):
    for j in range(0,num_partitions):
        outFiles.append(open("language_"+str(i)+"_"+str(j)+".csv","w"));

num_files = num_threads*num_partitions

current_file=0
for language in languages:
    outFiles[current_file].write(language+"\n")
    current_file = (current_file + 1) % num_files

for i in range(0,num_threads):
    for j in range(0,num_partitions):
        outFiles[i*num_partitions+j].close()
