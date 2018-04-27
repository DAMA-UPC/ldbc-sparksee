import re
import glob

personComments = {}
comment_hasCreator_filenames = glob.glob("comment_hasCreator_person_?_?.csv")
for filename in comment_hasCreator_filenames:
    with open(filename) as file:
      index = 0
      for line in file.readlines():
        fields = line.split("|")
        if index != 0:
          if fields[1] in personComments:
            personComments[fields[1]].append(fields[0])
          else:
            personComments[fields[1]] = [fields[0]]
        index+=1

comments = {}
comment_filenames = glob.glob("comment_?_?.csv")
for filename in comment_filenames:
    with open(filename) as file:
      index = 0
      for line in file.readlines():
        if index != 0:
          fields = line.split("|")
          comments[fields[0]] = line
        index+=1

max_partition = -1
max_thread = -1
for filename in comment_filenames:
    fields = filename.split('_')
    thread = int(fields[1])
    partition = int(fields[2].split('.')[0])
    if thread > max_thread:
        max_thread = thread 
    if partition > max_partition:
        max_partition = partition 

num_threads = max_thread+1
num_partitions = max_partition+1


outFiles = []

for i in range(0,num_threads):
    for j in range(0,num_partitions):
        outFiles.append(open("comment_"+str(i)+"_"+str(j)+".csv.sorted","w"));
        outFiles[len(outFiles)-1].write("id|creationDate|locationIP|browserUsed|content|length|\n")

num_files = num_threads*num_partitions

current_file=0
for person in personComments:
  for comment in personComments[person]:
    outFiles[current_file].write(comments[comment])
  current_file = (current_file + 1) % num_files

for i in range(0,num_threads):
    for j in range(0,num_partitions):
        outFiles[i*num_partitions+j].close()
