#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 09:10:00 2020

@author: mavroudo
"""

#6 different activities, with 6 different distributions Know the outliers with pdf
from scipy.stats import norm,alpha,expon,powerlognorm
import matplotlib.pyplot as plt
import numpy as np
import random
import algorithms.outlierDistanceActivities as activities
import algorithms.outlierPairsDistribution as distribution


activitiesTimes=[]
#2 normal distributions
rv = norm(scale=3,loc=-5)
rv2=norm(loc=10,scale=2)
x = rv.rvs(size=1000)
x2=rv2.rvs(size=1000)
isOutlier=[ True if norm.pdf(data,scale=3,loc=-5)<0.01 else False for data in x ]
data=[[xi,isOutlieri] for xi,isOutlieri in zip (x,isOutlier)]
isOutlier2=[ True if norm.pdf(data,loc=10,scale=2)<0.01 else False for data in x2 ]
data=data+[[xi,isOutlieri] for xi,isOutlieri in zip (x2,isOutlier2)]
fig, ax = plt.subplots(1, 1)
ax.hist([i[0] for i in data], density=True, histtype='stepfilled', alpha=0.2)
plt.savefig("distribution.eps",format="eps")
plt.show()
activitiesTimes.append(data)

#2 alpha distributions
a1,a2=3.2,3.2
scale1,scale2=10,10
loc1,loc2=0,15
rv = alpha(a1,loc=loc1,scale=scale1)
rv2=alpha(a2,loc=loc2,scale=scale2)
x = rv.rvs(size=1000)
x2 = rv2.rvs(size=1000)
isOutlier=[ True if alpha.pdf(data,a1,loc=loc1,scale=scale1)<0.01 else False for data in x ]
data=[[xi,isOutlieri] for xi,isOutlieri in zip (x,isOutlier)]
isOutlier2=[ True if alpha.pdf(data,a2,loc=loc2,scale=scale2)<0.01 else False for data in x2 ]
data=data+[[xi,isOutlieri] for xi,isOutlieri in zip (x2,isOutlier2)]
fig, ax = plt.subplots(1, 1)
ax.hist([i[0] for i in data], density=True, histtype='stepfilled', alpha=0.2)
plt.show()
activitiesTimes.append(data)

#2 exponentials
scale1,scale2=2,5
loc1,loc2=0,15
rv = expon(loc=loc1,scale=scale1)
rv2=expon(loc=loc2,scale=scale2)
x = rv.rvs(size=1000)
x2 = rv2.rvs(size=1000)
isOutlier=[ True if expon.pdf(data,loc=loc1,scale=scale1)<0.01 else False for data in x ]
data=[[xi,isOutlieri] for xi,isOutlieri in zip (x,isOutlier)]
isOutlier2=[ True if expon.pdf(data,loc=loc2,scale=scale2)<0.01 else False for data in x2 ]
data=data+[[xi,isOutlieri] for xi,isOutlieri in zip (x2,isOutlier2)]
fig, ax = plt.subplots(1, 1)
ax.hist([i[0] for i in data], density=True, histtype='stepfilled', alpha=0.2)
plt.show()
activitiesTimes.append(data)

#powerlognorm
c, s = 2.14, 0.446
rv=powerlognorm(c,s,scale=3,loc=5)
x=rv.rvs(size=2000)
isOutlier=[ True if powerlognorm.pdf(data,c=c,s=s,loc=5,scale=3)<0.01 else False for data in x ]
data=[[xi,isOutlieri] for xi,isOutlieri in zip (x,isOutlier)]
fig, ax = plt.subplots(1, 1)
ax.hist([i[0] for i in data], density=True, histtype='stepfilled', alpha=0.2)
plt.show()
activitiesTimes.append(data)

#we have 4 types of events and now create traces: untill all are used from data
minTrace,maxTrace=5,25
numberOfEvents=sum([len(i) for i in activitiesTimes])
traces=[]
dataVectors=[[] for _ in range(len(activitiesTimes))]
while numberOfEvents >0:  
    eventsInTrace=random.randint(minTrace,maxTrace)
    if numberOfEvents >= eventsInTrace:
        numberOfEvents-=eventsInTrace
    else:
        eventsInTrace=numberOfEvents
        numberOfEvents=0
    trace=[]
    for index in range(eventsInTrace):
        #pick a random activity
        activity=random.randint(0,len(activitiesTimes)-1)
        event=activitiesTimes[activity].pop(random.randint(0,len(activitiesTimes[activity])-1))
        trace.append([activity,event[0],event[1]])
        dataVectors[activity].append([len(traces),index,event[0]])
        if len(activitiesTimes[activity])==0:
            activitiesTimes.pop(activity)
    traces.append(trace)

totalOutlierEvents=sum([1 for x in traces for i in x  if i[2]])

with open("synthetic_dataset_4.txt","w") as fTraces:
    for trace in traces:
        string=[]
        for event in trace:
            string.append(str(event[0])+"_"+str(event[1]))
        fTraces.write(",".join(string)+"\n")
        
with open("resuts_dataset_4.txt","w") as fResults:
    for tindex,trace in enumerate(traces):
        for eindex,event in enumerate(trace):
            if event[2]:
                fResults.write(str(tindex)+","+str(eindex)+"\n")
            
    
    

def percisionRecall(traces,outliersFound,totalOutliers):
    count=0
    for outlier in outliersFound:
        if traces[outlier[0]][outlier[1]][2]:
           count+=1
    print(count)
    return count/len(outliersFound), count/totalOutliers
    
   
    
data3=[]
#Try the methods below :P
kOptions=[5,10,20,50,75,100,250,500]
for k in kOptions:
    myOutliers = activities.findOutlierEvents(dataVectors, k, stdDeviationTImes=3,threshold=None)
    p,r=percisionRecall(traces,myOutliers,totalOutlierEvents)
    print(p,r)
    #data3.append([p,r])
    
data=[]
thresholds=[0.020,0.01,0.0075,0.005,0.0025,0.001]
for t in thresholds:
    myOutliers2,distributions,means= distribution.outlierDetectionWithDistribution(None,dataVectors,t,activityNames=["a1","a2","a3","a4"])
    p,r=percisionRecall(traces,myOutliers2,totalOutlierEvents)
    print(",".join([str(t),str(p),str(r)])+"\n")
    data.append([p,r])
    
data2=[]   
for t in thresholds:
    myOutliers = activities.findOutlierEvents(dataVectors, 50, stdDeviationTImes=3,threshold=t)
    p,r=percisionRecall(traces,myOutliers,totalOutlierEvents)
    print(p,r)
    data2.append([p,r])

import matplotlib.pyplot as plt
fig=plt.figure()
#plt.title("Percision/Recall")
ax=fig.add_subplot(121, label="precision")
ax2=fig.add_subplot(121, label="recall", frame_on=False)

minimum=min(min([i[1] for i in data2]),min([i[0] for i in data2]))
maximum=max(max([i[1] for i in data2]),max([i[0] for i in data2]))

ax.plot([int(i*8000) for i in thresholds], [i[0] for i in data2], color="C0",label="Percision")
ax.set_xlabel("Top outliers")
ax.set_ylabel("Percent (%)")
ax.set_ylim([0,1])
ax2.plot([int(i*8000) for i in thresholds],[i[1] for i in data2], color="C1",label="Recall")
ax2.set_ylim([0,1])
plt.grid(True)

ax=fig.add_subplot(122, label="precision")
ax2=fig.add_subplot(122, label="recall", frame_on=False)

minimum=min(min([i[1] for i in data]),min([i[0] for i in data]))
maximum=max(max([i[1] for i in data]),max([i[0] for i in data]))

ax.plot(thresholds, [i[0] for i in data], color="C0",label="Percision")
ax.set_xlabel("Thresholds")
ax.set_ylim([0,1])
ax2.plot(thresholds,[i[1] for i in data], color="C1",label="Recall")
ax2.set_ylim([0,1])
plt.grid(True)
plt.savefig("tests/graphs/percisionRecall.png")

