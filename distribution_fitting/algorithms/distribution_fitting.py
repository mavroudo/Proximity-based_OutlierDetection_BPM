#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from algorithms.outlierPairsCurveFitting import getDistributionsFitting
from pm4py.algo.filtering.log.attributes import attributes_filter as log_attributes_filter
from pm4py.objects.log.importer.xes import factory as xes_factory
from algorithms import preprocess
from statistics import mean
import numpy as np
import warnings, scipy
from sklearn.preprocessing import StandardScaler
#time: per activity [30] => [trace index, activity index, time in seconds]
#sequence: per trace [10.000]=> [[activity index, time]]
logFile="../BPM Temporal Anomalies/scala_trace_outlier/input/outliers_30_activities_10k_0.005.xes"
logFileResults="../BPM Temporal Anomalies/scala_trace_outlier/input/results_30_activities_10k_0.005_description"
log=xes_factory.apply(logFile)
time,sequence = preprocess.dataPreprocess2012(log) 


times=[[i[2] for i in x]for x in time]
means=[mean(i) for i in times]
distributionsDF=getDistributionsFitting(times,log) 

#get the distributions in a array
thresholds=[0.020,0.01,0.0075,0.005,0.0025,0.001]
for threshold in thresholds:
    warnings.filterwarnings("ignore")
    distributions=[]
    for index in range(len(distributionsDF)):
        if distributionsDF.iloc[index]["R2"]>=0.9:
            dist = getattr(scipy.stats, distributionsDF.iloc[index]["Distribution"])
            param = dist.fit(times[index])
            distribution=dist(*param[:-2], loc=param[-2],scale=param[-1])
            distributions.append([distribution])
        else: 
            size=len(times[index])
            down=int(size*threshold)
            up=int(size-size*threshold)
            distributions.append([float(sorted(times[index])[down]),float(sorted(times[index])[up])])
            
    outliers=[]
    for traceIndex,dataVector in enumerate(sequence):
        for activityIndex,activity in enumerate(dataVector): # looping through the time events           
            if len(distributions[activity[0]])==1: #fit distribution
                dist=distributions[activity[0]][0] 
                predict=float(dist.pdf(activity[1]))    
                if predict<threshold:
                        outliers.append([traceIndex,activityIndex])   
            else:
                minValue,maxValue=distributions[activity[0]]
                if activity[1]<minValue or activity[1]>maxValue:
                    outliers.append([traceIndex,activityIndex])                     
    
    
    
    #define precision and recall
    results=[]
    with open(logFileResults,"r") as f:
        for line in f:
            l=line.split(",")
            results.append([int(l[1]),int(l[2])])
    found=0
    for outlier in outliers:
        if outlier in results:
            found+=1
    print(",".join([str(threshold),str(found/len(outliers)),str(found/50)]))
    
                   


