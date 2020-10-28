#!/bin/python2
import os
import sys
import math
#import counter
import pandas as pd 
import matplotlib as mpl 
from optparse import OptionParser
from matplotlib import mlab
mpl.use("Cairo") 
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
import numpy as np
#from sets import Set

import time
import datetime

import pylatex
from pylatex import Document, Subsection, Section, Figure, NoEscape, Table, Tabular, Package
from pylatex.position import Center

def timestamp_to_minutes(timestamp):
    return timestamp / (60.0*1000.0)

def create_property_value_table(filename, document, sep, caption):
    tablespec = "|l|l|"
    tableheader = [pylatex.utils.bold("property"),pylatex.utils.bold("value")]
    
    with document.create(Table(position="H")) as table:
        table.add_caption(caption)
        with document.create(Center()):
            with document.create(Tabular(tablespec)) as tabular:
                tabular.add_hline()
                tabular.add_row(tableheader)
                tabular.add_hline()

                with open(filename) as config_file:
                    for line in config_file.readlines():
                        fields = line.split(sep)
                        key = fields[0]
                        value = fields[1]
                        if "license" not in key:
                            tabular.add_row([key,value])
                            tabular.add_hline();

###### SCRIPT STARTS HERE

parser = OptionParser()
parser.add_option("-i", "--input-prefix", 
                  dest="input_prefix",
                  help="The input prefix of all iput files", 
                  action="append")

parser.add_option("-l", "--labels", 
                  dest="labels",
                  help="The input prefix of all iput files", 
                  action="append")

(options, args) = parser.parse_args()

if not options.input_prefix:
    parser.error("Input prefix is missing")

if not options.labels:
    parser.error("Input prefix is missing")

# Initializing Latex Document
geometry_options = {
		    "margin" : "1.5cm",
		    "head" : "1.5cm",
     		    "includeheadfoot": True
  		   }

document = Document("ldbc-report", geometry_options=geometry_options)

document.packages.append(Package("float"))

#### CREATING HEADER AND FOOTER

header = pylatex.PageStyle("header",
			   header_thickness=1.0,
			   footer_thickness=1.0)
with header.create(pylatex.Head("L")) as header_left:
    with header_left.create(pylatex.MiniPage(width=NoEscape(r"0.49\textwidth"),pos='c')) as logo_wrapper:
        logo_file = os.path.join(os.path.dirname(__file__),'ldbc_logo.eps')
        logo_wrapper.append(pylatex.StandAloneGraphic(image_options="width=80px",filename=logo_file))

with header.create(pylatex.Head("C")) as header_center:
    title = "LDBC SNB Execution Report - "+str(datetime.datetime.now())
    header_center.append(title)

with header.create(pylatex.Head("R")) as header_right:
    with header_right.create(pylatex.MiniPage(width=NoEscape(r"0.49\textwidth"),pos='c', align='r')) as logo_wrapper:
        logo_file = os.path.join(os.path.dirname(__file__),'sparsity_logo.png')
        logo_wrapper.append(pylatex.StandAloneGraphic(image_options="width=40px",filename=logo_file))


with header.create(pylatex.Foot("R")) as footer_right:
    footer_right.append(pylatex.simple_page_number())


document.preamble.append(header)
document.change_document_style("header")


execution_index = 1
for prefix in options.input_prefix:
    #### CREATING BENCHMARK CONFIG TABLE
    with document.create(pylatex.Section("Execution "+str(execution_index))):
        with document.create(pylatex.Subsection("Configuration")):
            create_property_value_table(prefix+".config",
                                        document,
                                        "=",
                                        "Benchmark Configuration")
        	
        	#### CREATING SPARKSEE CONFIG TABLE
        	
            create_property_value_table(prefix+".sparksee.cfg",
                                        document,
                                        "=",
                                        "Sparksee Configuration")
        
        document.append(pylatex.NewPage())
        # Reading log data, with the following columns
        #operation_type|scheduled_start_time|actual_start_time|execution_duration_MILLISECONDS|result_code|original_start_time
        with document.create(Subsection("Execution Results")):
            log = pd.read_csv(prefix+".log", sep='|')
        
            ##### CREATING EXECUTION SUMMARY TABLE
        	
            labels = []
            mins = []
            maxs = []
            avgs = []
            num = []
            num_late = []
            for i, query in enumerate(log.groupby("operation_type")):
                labels.append(query[0])
                entries = []
                mins.append(min(query[1]["execution_duration_MILLISECONDS"]))
                maxs.append(max(query[1]["execution_duration_MILLISECONDS"]))
                count = len(query[1]["execution_duration_MILLISECONDS"])
                num.append(count)
                avg = sum(query[1]["execution_duration_MILLISECONDS"])/count
                avgs.append(avg)
                count = 0
                for row in query[1].iterrows():
                    start_time = int(row[1]["scheduled_start_time"])
                    actual_time = int(row[1]["actual_start_time"])
                    if(actual_time - start_time > 1000):
                        count +=1
                num_late.append(count)
        
                tablespec = "|l|l|l|l|l|l|"
                tableheader = [pylatex.utils.bold("query"),
                	       pylatex.utils.bold("min"),
                	       pylatex.utils.bold("avg."),
                	       pylatex.utils.bold("max"),
                	       pylatex.utils.bold("count"),
                	       pylatex.utils.bold("num_late")]
                
            with document.create(Table(position="H")) as table:
                table.add_caption("Execution Summary - Times in Milliseconds") 
                with document.create(Center()):
                    with document.create(Tabular(tablespec)) as tabular:
                        tabular.add_hline()
                        tabular.add_row(tableheader)
                        tabular.add_hline()
                        for i in range(0,len(labels)):
                            row = [labels[i],
                                   round(mins[i],2),
                                   round(avgs[i],2),
                                   round(maxs[i],2),
                                   num[i],
                                   num_late[i]]
                            tabular.add_row(row)
                            tabular.add_hline();
        
            
            
            reads = log[~log["operation_type"].str.contains("Update")]
            reads = reads[~reads["operation_type"].str.contains("Short")]
            updates = log[log["operation_type"].str.contains("Update")]
            shorts = log[log["operation_type"].str.contains("Short")]
            
            reads_color = "xkcd:ocean blue"
            updates_color = "xkcd:coral"
            shorts_color = "xkcd:light teal"
            
            ##### PLOTTING THROUGHPUT AND DIFFERENCE
            plt.clf()
            fig = plt.figure()
            host = fig.add_subplot(111)
            legend_handles = []
            
            # Plotting difference between actual and scheduled start times
            mintime = log.loc[0,"scheduled_start_time"]
            #mintime = log.loc[0,"actual_start_time"]
            minutes = timestamp_to_minutes((np.array(log["actual_start_time"]) - mintime)) 
            
            reads_minutes = timestamp_to_minutes((np.array(reads["actual_start_time"]) - mintime))
            reads_difference = np.array(reads["actual_start_time"]) - np.array(reads["scheduled_start_time"])
            reads_handle = host.plot(reads_minutes, 
                                     reads_difference,
                                     marker = 'x',
                                     markersize = 2.0,
                                     linestyle="",
                                     color = reads_color,
                                     label = "reads",
                                     zorder = 1.0)
            legend_handles.append(reads_handle[0])
            updates_minutes = timestamp_to_minutes((np.array(updates["actual_start_time"]) - mintime))
            updates_difference = np.array(updates["actual_start_time"]) - np.array(updates["scheduled_start_time"])
            updates_handle = host.plot(updates_minutes, 
                                       updates_difference,
                                       marker = 'x',
                                       markersize = 2.0,
                                       linestyle="",
                                       color = updates_color,
                                       label = "updates",
                                       zorder = 1.0)
            legend_handles.append(updates_handle[0])
            shorts_minutes = timestamp_to_minutes((np.array(shorts["actual_start_time"]) - mintime))
            shorts_difference = np.array(shorts["actual_start_time"]) - np.array(shorts["scheduled_start_time"])
            shorts_handle = host.plot(shorts_minutes, 
                                      shorts_difference,
                                      marker = 'x',
                                      markersize = 2.0,
                                      linestyle="",
                                      color = shorts_color,
                                      label = "shorts",
                                      zorder = 1.0)
            legend_handles.append(shorts_handle[0])
            
            
            # Plotting horizontal line at 1 second
            threshold_handle = host.plot(minutes,
                                         [1000 for i in iter(range(len(minutes)))], 
                                         label="threshold",
                                         zorder = 1.0)
            legend_handles.append(threshold_handle[0])
            
            
            host.set_xlabel("minutes")
            host.set_ylabel("actual - scheduled (ms)")
            
            # Plotting expected throughput
            window_size = 6 # in seconds
            par1 = host.twinx()
            actual_seconds = (np.array(log["actual_start_time"]) - mintime +
                              np.array(log["execution_duration_MILLISECONDS"])) / (1000.0)
            num_buckets = int(max(actual_seconds)-min(actual_seconds))/window_size+1
            actual_buckets = np.zeros(int(num_buckets))
            index = 0
            for row in log.iterrows():
                actual_start_time = int(row[1]["actual_start_time"])
                exec_duration = int(row[1]["execution_duration_MILLISECONDS"])
                actual_bucket = int(( actual_start_time +
                                  exec_duration - mintime)/(window_size * 1000))
                if actual_bucket < 0:
                    print("Possible error at line "+str(index)+". scheduled start time is after actual start time")
                else:
                    actual_buckets[actual_bucket] += 1
                index = index + 1
            
            xdata = np.arange(0,max(actual_seconds)/60.0,window_size/60.0)
            throughput_handle = par1.plot(xdata,
                                          actual_buckets / float(window_size),
                                          color = "olive",
                                          linestyle = "-",
                                          label = "throughput",
                                          zorder = 1.0)
            legend_handles.append(throughput_handle[0])
        
            avg_throughput = np.average(actual_buckets / float(window_size))
        
            avg_throughput_handle = par1.plot(minutes,
                                              [avg_throughput for i in iter(range(len(minutes)))], 
                                              label="avg. throughput",
                                              color = "darkolivegreen",
                                              linestyle = "--",
                                              zorder = 1.0)
            legend_handles.append(avg_throughput_handle[0])
        
            par1.text(minutes[-1], 
                      avg_throughput, 
                      str(round(avg_throughput,2)), 
                      fontsize = 8, 
                      zorder = 1.0)
        
            
            par1.set_ylabel("Throughput (op/s)")
            
            # Plotting checkpoint events
            
            checkpoint_ranges = []
            with open(prefix+".sparksee.log") as log_file:
                count = 0
                nextrange = [0,0]
                for line in log_file.readlines():
                    fields = line.split("|")
                    if "Checkpoint" in fields[1]:
                        date = fields[0].split(" ")[0]
                        year = int(date.split("-")[0])
                        month = int(date.split("-")[1])
                        day = int(date.split("-")[2])
                        daytime = fields[0].split(" ")[1]
                        hour = int(daytime.split(":")[0])
                        minute = int(daytime.split(":")[1])
                        second = int(daytime.split(":")[2].split(".")[0])
                        millisecond = int(daytime.split(":")[2].split(".")[1])
                        dt = datetime.datetime(year,
                                               month,
                                               day,
                                               hour,
                                               minute,
                                               second,
                                               millisecond*1000)
                        timestamp = int(time.mktime(dt.timetuple())*1000)
                        index = count%2
                        nextrange[count%2] = timestamp_to_minutes((timestamp - mintime))
                        if count % 2 == 1:
                            checkpoint_ranges.append((nextrange[0],nextrange[1] - nextrange[0]))
                        count+=1
            
            checkpoint_ranges = list(filter(lambda x: x[0] > 0, checkpoint_ranges))
            yticks = host.get_yticks()
            checkpoint_handle = host.broken_barh(checkpoint_ranges,
                                                  (0.0, max(yticks)),
                                                  color = "orange",
                                                  linewidth = 1.0,
                                                  zorder=0.0,
                                                  label = "checkpoint")
            legend_handles.append(checkpoint_handle)
            
                    
            legend = host.legend(handles = legend_handles)
            plt.grid(True, axis='both')
            plt.xticks(np.arange(min(actual_seconds)/60, max(actual_seconds)/60+1, 1))
            fig.set_size_inches(11.86, 4)
            plt.title("Execution Overview")
            plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
            #plt.savefig(options.output_file+".eps",format='eps')
            
            # adding plot to latex document
            with document.create(Figure(position="H")) as plot:
                plot.add_plot()
            
            #### PLOTTING GANTT DIAGRAM OF QUERY EXECUTION
            
            plt.clf()
            fig,ax = plt.subplots()
            
            minstart = 1000000000
            maxend = 0 
            labels = []
            for i, query in enumerate(log.groupby("operation_type")):
                labels.append(query[0])
                entries = []
                for row in query[1].iterrows():
                    start = int(row[1]["actual_start_time"]) - mintime
                    diff = int(row[1]["execution_duration_MILLISECONDS"]) 
                    start /= (60.0*1000.0)
                    diff /= (60.0*1000.0)
                    minstart = min(minstart,start)
                    maxend = max(maxend,start+diff)
                    entries.append((start,diff))
                    color = reads_color
                if "Update" in query[0]:
                    color = updates_color
                if "Short" in query[0]:
                    color = shorts_color
                ax.broken_barh(entries,
                               (i-0.4, 0.8),
                              color = color,
                              linewidth = 0.0)
            
            ax.set_yticks(range(len(labels)))
            ax.set_yticklabels(labels) 
            ax.set_yticklabels(labels, fontsize=6, rotation=45)
            ax.set_xlabel("minutes")
            ax.set_xticks(np.arange(minstart, maxend, 1))
            fig.set_size_inches(11.86, 4)
            plt.title("Query Gantt")
            plt.grid(axis="x")
            plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
            #plt.tight_layout() 
            
            with document.create(Figure(position="H")) as plot:
                plot.add_plot()
            
            
            #### PLOTTING UPDATES DENSITY
            
            plt.clf()
            window_size = 6 # in seconds
            update_seconds = (np.array(log["scheduled_start_time"]) - mintime) / (1000.0)
            num_buckets = int(int(max(update_seconds)-min(update_seconds))/window_size+1)
            update_buckets = np.zeros(num_buckets)
            for row in log.iterrows():
                if "Update" in row[1]["operation_type"]:
                    update_bucket = int((int(row[1]["scheduled_start_time"]) - mintime)/(window_size * 1000))
                    update_buckets[update_bucket] += 1
            
            
            plt.plot(np.arange(0,max(update_seconds)/60.0,window_size/60.0),
                      update_buckets / float(window_size),
                      color = updates_color,
                      linestyle = "-")
            
            plt.xlabel("minutes")
            plt.ylabel("Updates (op/s)")
            plt.title("Workload Updates Start Density")
            plt.grid(True, axis='both')
            plt.xticks(np.arange(min(update_seconds)/60, max(update_seconds)/60+1, 1))
            
            with document.create(Figure(position="H")) as plot:
                plot.add_plot()
            
            #### PLOTTING AVERAGE LATENCY PER QUERY
            plt.clf()
            execution_index = execution_index + 1
    
fig, ax = plt.subplots()
execution_index = 1
width = 0.35
with document.create(pylatex.Section("Latencies Summary")):
    for prefix in options.input_prefix:
        class Data:
            query_type = ""
            avg_latency = 0.0
        
            def __init__(self, query_type, avg_latency):
                self.query_type = query_type
                self.avg_latency = avg_latency
        
            def __lt__(self, other):
                return self.query_type < other.query_type
        
        
        log = pd.read_csv(prefix+".log", sep='|')
        query_counts = {}
        query_latencies = {}
        for row in log.iterrows():
            query_type = row[1]["operation_type"]
            query_latency = row[1]["execution_duration_MILLISECONDS"]
            if query_type not in query_counts:
                query_counts[query_type] = 0
            query_counts[query_type] += 1
            if query_type not in query_latencies:
                query_latencies[query_type] = 0
            query_latencies[query_type] += int(query_latency)
        
        plot_data = []
        for key in query_counts:
            avg_latency = query_latencies[key] / float(query_counts[key])
            plot_data.append(Data(key, avg_latency))
        
        plot_data.sort()
        
        labels = [x.query_type for x in plot_data]
        avg_latencies = [x.avg_latency for x in plot_data]
        width_offset = (-len(options.input_prefix)*(width/2.0) + (len(options.input_prefix) - execution_index)*width)
        ax.barh(np.arange(len(plot_data)) + width_offset, 
                avg_latencies, 
                width, 
                label=options.labels[execution_index - 1])
        
        execution_index = execution_index+1

    ax.set_yticks(np.arange(len(plot_data)))
    ax.set_yticklabels(labels, fontsize=6, rotation=45)
    ax.set_xlabel("average execution time (ms)")
    ax.legend()
    fig.set_size_inches(11.86, 4)
    plt.title("Avg. Lagencies per Query Type")
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
    plt.grid(axis="x")
    
    with document.create(Figure(position="h")) as plot:
        plot.add_plot()

# generating latex document
document.generate_pdf(clean_tex=False)
