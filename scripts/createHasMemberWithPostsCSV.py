import re
import glob

forumPosts = {}
forum_container_filenames = glob.glob("forum_containerOf_post_?_?.csv")
for filename in forum_container_filenames:
    with open(filename) as file:
      count = 0
      print("LOADING "+filename)
      for line in file.readlines():
        if count > 0:
            fields = line[0:-1].split("|")
            if int(fields[0]) in forumPosts:
                forumPosts[int(fields[0])].append(int(fields[1]))
            else:
                forumPosts[int(fields[0])] = [int(fields[1])]
        count += 1

postCreator = {}
post_creator_filenames = glob.glob("post_hasCreator_person_?_?.csv")
for filename in post_creator_filenames:
    with open(filename) as file:
      count = 0
      print("LOADING "+filename)
      for line in file.readlines():
        if count > 0:
            fields = line[0:-1].split("|")
            postCreator[int(fields[0])] = int(fields[1])
        count +=1

max_partition = -1
max_thread = -1
for filename in post_creator_filenames:
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
        outFiles.append(open("forum_hasMemberWithPosts_person_"+str(i)+"_"+str(j)+".csv","w"));

num_files = num_threads*num_partitions

current_file=0
forum_member_filenames = glob.glob("forum_hasMember_person_?_?.csv")
for filename in forum_member_filenames:
    with open(filename) as file:
      print("LOADING "+filename)
      count = 0;
      for line in file.readlines():
        if count == 0:
          outFiles[current_file].write(line)
          count = 1
          continue
        count = count + 1
        fields = line[0:-1].split("|")
        #if count % 1000 == 0:
        #  print ("LINE: "+str(count))
        if int(fields[0]) in forumPosts:
          for post in forumPosts[int(fields[0])]:
            if postCreator[post] == int(fields[1]):
                outFiles[current_file].write(line)
                break
    current_file = (current_file + 1) % num_files

for i in range(0,num_threads):
    for j in range(0,num_partitions):
        outFiles[i*num_partitions+j].close()
