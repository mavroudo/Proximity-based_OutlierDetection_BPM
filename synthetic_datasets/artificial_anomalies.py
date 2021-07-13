#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 11:31:21 2021

@author: mavroudo
"""
from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.objects.log.exporter.xes import factory as xes_exporter
import random
from statistics import mean, stdev
from datetime import timedelta

def add_reg_date(log):  
    for trace in log:
        secs=random.random()*1440+2880
        trace.attributes["REG_DATE"]=trace[0]["time:timestamp"]-timedelta(seconds=secs)


def mean_value_per_Activity(log):
    data=dict()
    data_durations=[[] for i in log]
    for index_t,trace in enumerate(log):
        previous_time=0
        for index,event in enumerate(trace):
            if index==0:
                previous_time=trace.attributes["REG_DATE"]
            event_name=event["concept:name"]
            if event_name not in data:
                data[event_name]=[[],0]
            time=event["time:timestamp"]
            duration=time-previous_time
            data[event_name][0].append(duration.total_seconds())
            data_durations[index_t].append(duration.total_seconds())
            data[event_name][1]+=1
            previous_time=time
    return data, data_durations
            
def add_randomness_to_executions_times(log,mean_values,log_durations):
    #to previous time delta na mpainei arxika sto epomeno kai meta na upologizetai
    #+-10% ston xrono duration
    means={i:mean(mean_values[i][0]) for i in mean_values}
    for i_trace,trace in enumerate(log):
        previous_diff=timedelta(weeks=52)
        trace.attributes["REG_DATE"]+=timedelta(weeks=52)
        for i_event, event in enumerate(trace):
            m=means[event["concept:name"]]
            if log_durations[i_trace][i_event]==0:
                previous_diff+=timedelta(seconds=m)
            min_ten_per=m-0.1*m
            plus_Ten_pre=m+0/1*m
            secs=random.random()*(plus_Ten_pre-min_ten_per)+min_ten_per
            previous_diff+=timedelta(seconds=secs)
            event["time:timestamp"]+=previous_diff

    

def delay_an_activity(log,trace_id,activity_stats):
    activity_id=random.randint(0,len(log[trace_id])-1)
    activity_name=log[trace_id][activity_id]["concept:name"]
    mean_value=mean(activity_stats[activity_name][0])
    std = stdev(activity_stats[activity_name][0])
    diff=timedelta(seconds=(3+random.random()/2)*std+mean_value) #increase the time by 3 times the standard deviation
    for event in log[trace_id][activity_id:]:
        event["time:timestamp"]+=diff
    return activity_id

def create_measurement_error(log,trace_id,activity_stats):
    activity_id=random.randint(0,len(log[trace_id])-2)
    activity_name=log[trace_id][activity_id]["concept:name"]
    mean_value=mean(activity_stats[activity_name][0])
    std = stdev(activity_stats[activity_name][0])
    diff=timedelta(seconds=(3+random.random()/2)*std+mean_value)
    log[trace_id][activity_id]["time:timestamp"]+=diff
    log[trace_id][activity_id+1]["time:timestamp"]-=diff
    return activity_id
    
def end_faster_activity(log,trace_id,activity_stats):
    activity_id=random.randint(0,len(log[trace_id])-1)
    activity_name=log[trace_id][activity_id]["concept:name"]
    mean_value=mean(activity_stats[activity_name][0])
    std = stdev(activity_stats[activity_name][0])
    diff=timedelta(seconds=mean_value-(3+random.random()/2)*std)
    for event in log[trace_id][activity_id:]:
        event["time:timestamp"]-=diff
    return activity_id


from pm4py.algo.filtering.log.attributes import attributes_filter

def stats(log):
    activities = list([i for  i in attributes_filter.get_attribute_values(log, "concept:name")])
    times=[[0 for _ in range(len(activities))] for _ in range(len(log))] 
    for index_t,trace in enumerate(log):
        previous_time=0
        for index,event in enumerate(trace):
            if index==0:
                previous_time=trace.attributes["REG_DATE"]
            time=event["time:timestamp"]
            duration=time-previous_time
            times[index_t][activities.index(event["concept:name"])]+=duration.total_seconds()

logfile="30_activities_10k.xes"
#logfile = "test.xes"
#logfile="outliers_30_activities_3k_0.1.xes"
logfile2="datasets/"+logfile
log=xes_import_factory.apply(logfile2)
add_reg_date(log)
activities_stats,log_durations=mean_value_per_Activity(log)
add_randomness_to_executions_times(log,activities_stats,log_durations)
activities_stats,log_durations=mean_value_per_Activity(log)
percentage=0.005
file="datasets/results_"+logfile.split(".")[0]+"_"+str(percentage)+"_description"
with open(file,"w") as f:
    for i in range(int(percentage*len(log))):
        trace_id=random.randint(0,len(log)-1)
        mode=random.random()
        outlier=""
        if mode<0.45:
            act_id=delay_an_activity(log,trace_id,activities_stats)
            outlier="delay"
        elif mode<0.9:
            act_id=end_faster_activity(log,trace_id,activities_stats)
            outlier="faster"
        else:
            act_id=create_measurement_error(log,trace_id,activities_stats)
            outlier="measurement"
        f.write(",".join([outlier,str(trace_id),str(act_id)])+"\n")
filename="datasets/outliers_"+logfile.split(".")[0]+"_"+str(percentage)
xes_exporter.export_log(log,filename+".xes")
