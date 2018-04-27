

import sys, os, math 
import json 
def parse_query_1(instance):
    query = {} 
    member = instance.split("|") 
    query["id"] = member[0] 
    query["name"] = member[1]
    query["limit"] = "20" 
    return query 

def parse_query_2(instance):
    query = {} 
    member = instance.split("|") 
    query["id"] = member[0] 
    query["date"] = member[1]
    query["limit"] = "20" 
    return query 

def parse_query_3(instance): 
    query = {}
    member = instance.split("|") 
    query["id"] = member[0]
    query["date"] = member[1]
    query["duration"] = member[2]
    query["country1"] = member[3]
    query["country2"] = member[4] 
    query["limit"] ="20" 
    return query 

def parse_query_4(instance):
    query = {} 
    member = instance.split("|") 
    query["id"] = member[0]
    query["date"] = member[1]
    query["days"] = member[2]
    query["limit"] = "10" 
    return query 

def parse_query_5(instance):
    query = {}
    member = instance.split("|") 
    query["id"] = member[0]
    query["date"] = member[1]
    query["limit"] = "20" 
    return query 

def parse_query_6(instance): 
    query = {} 
    member = instance.split("|") 
    query["id"] = member[0]
    query["tag"] = member[1]
    query["limit"] ="10"
    return query

def parse_query_7(instance):
    query = {} 
    member = instance.split("|") 
    query["id"] = member[0]
    query["limit"] = "20" 
    return query 

def parse_query_8(instance): 
    query = {} 
    member = instance.split("|") 
    query["id"] = member[0] 
    query["limit"] = "20" 
    return query

def parse_query_9(instance): 
    query = {} 
    member = instance.split("|") 
    query["id"] = member[0]
    query["date"] = member[1]
    query["limit"] = "20" 
    return query 

def parse_query_10(instance): 
    query = {} 
    member = instance.split("|") 
    query["id"] = member[0]
    query["month"] = member[1] 
    query["limit"] = "10" 
    return query 

def parse_query_11(instance):
    query = {} 
    member = instance.split("|") 
    query["id"] = member[0]
    query["country"] = member[1]
    query["year"] = member[2] 
    query["limit"] = "10" 
    return query 

def parse_query_12(instance): 
    query = {} 
    member = instance.split("|") 
    query["id"] = member[0] 
    query["tagclass"] = member[1]
    query["limit"] = "20" 
    return query 

def parse_query_13(instance):
    query = {} 
    member = instance.split("|") 
    query["id1"] = member[0]
    query["id2"] = member[1] 
    return query 

def parse_query_14(instance): 
    query = {} 
    member = instance.split("|") 
    query["id1"] = member[0] 
    query["id2"] = member[1] 
    return query 

if len(sys.argv) < 3: 
    print("Usage: \n" + "datagen_params_format_converter.py "
        "<parameter_folder> <output_file_name>")
    exit() 

folder_path = sys.argv[1] 
output_file_name = sys.argv[2] 
num_queries = 14;
selected_queries = [];
if len(sys.argv) == 3:
    for i in range(1, 15):
        selected_queries.append(i)
else:
    for i in range(3, len(sys.argv)):
        selected_queries.append(int(sys.argv[i]))

parse_query = [ parse_query_1, parse_query_2, parse_query_3, parse_query_4, parse_query_5, parse_query_6, parse_query_7, parse_query_8, parse_query_9, parse_query_10, parse_query_11, parse_query_12, parse_query_13, parse_query_14]


output_file = open(output_file_name, 'w')
queries = {} 
for query in range(1,num_queries+1):
    if query in selected_queries: 
        query_instance_list = []
        query_file = open(folder_path+"/query_"+str(query)+"_param.txt")
        count = 0
        for instance in query_file.readlines():
            if count != 0:
                query_instance_list.append(parse_query[query-1](instance[0:-1]))
            count = count + 1
        query_file.close();
        queries["query" + str(query)] =  query_instance_list 
output_file.write(json.dumps( queries, indent = 4, ensure_ascii=False)) 
output_file.close()
