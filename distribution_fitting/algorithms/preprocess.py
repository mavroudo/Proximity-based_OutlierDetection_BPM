#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 10:52:39 2020

@author: mavroudo
"""
from pm4py.algo.filtering.log.attributes import attributes_filter as log_attributes_filter
from pm4py.objects.log.importer.xes import factory as xes_factory
def dataPreprocess2012(log):
    """
        Takes the log file and transform every trace in a way, that we will keep 
        the information for the time per event and also the original sequence
        for every event in the same trace
    """
    activities_all = log_attributes_filter.get_attribute_values(log, "concept:name")
    activities = list(activities_all.keys())
    times = [[] for i in range(len(activities))]
    sequence = []
    for indexTrace, trace in enumerate(log):
        previousTime = trace.attributes["REG_DATE"]
        sequence.append([])
        for index, event in enumerate(trace):
            indexActivity = activities.index(event["concept:name"])
            time = event["time:timestamp"] - previousTime
            times[indexActivity].append([indexTrace, index, time.total_seconds()])
            previousTime = event["time:timestamp"]
            sequence[-1].append([indexActivity, time.total_seconds()])
    return times, sequence

def dataPreprocess2017(log):
    """
        Takes the log file and transform every trace in a way, that we will keep 
        the information for the time per event and also the original sequence
        for every event in the same trace
    """
    activities_all = log_attributes_filter.get_attribute_values(log, "concept:name")
    activities = list(activities_all.keys())
    times = [[] for i in range(len(activities))]
    sequence = []
    for indexTrace, trace in enumerate(log):
        previousTime = trace[0]['time:timestamp']
        sequence.append([])
        for index, event in enumerate(trace):
            indexActivity = activities.index(event["concept:name"])
            time = event["time:timestamp"] - previousTime
            times[indexActivity].append([indexTrace, index, time.total_seconds()])
            previousTime = event["time:timestamp"]
            sequence[-1].append([indexActivity, time.total_seconds()])
    return times, sequence




