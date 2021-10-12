#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 26 19:37:18 2021

@author: mavroudo
"""
from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.algo.filtering.log.attributes import attributes_filter
from pm4py.objects.log.log import EventLog
from statistics import mean, stdev
import torch
import torch.nn.functional as F
import numpy as np
import matplotlib.pyplot as plt

def mean_value_per_Activity(log):
    data = dict()
    data_durations = [[] for i in log]
    for index_t, trace in enumerate(log):
        previous_time = 0
        for index, event in enumerate(trace):
            if index == 0:
                previous_time = trace.attributes["REG_DATE"]
            event_name = event["concept:name"]
            if event_name not in data:
                data[event_name] = [[], 0]
            time = event["time:timestamp"]
            duration = time - previous_time
            data[event_name][0].append(duration.total_seconds())
            data_durations[index_t].append(duration.total_seconds())
            data[event_name][1] += 1
            previous_time = time
    return data, data_durations

def meanAndstdev(data,activity_names)->list:
    mean_values=[]
    std_values=[]
    for name in activity_names:
        mean_values.append(mean(data[name][0]))
        std_values.append(stdev(data[name][0]))
    return mean_values,std_values

def transformTraces(log:EventLog) -> list:
    activities = attributes_filter.get_attribute_values(log, "concept:name")
    activity_names = [i for i in activities]
    data,data_durations=mean_value_per_Activity(log)
    log_list=[]
    for n_trace,trace in enumerate(log):
        l_trace=[0 for i in range(len(activity_names))]
        times=[0 for i in range(len(activity_names))]
        for n_event,event in enumerate(trace):
            index = activity_names.index(event["concept:name"])
            l_trace[index]+=data_durations[n_trace][n_event]
            times[index]+=1
        l_trace=[x/y if y!=0 else 0 for x,y in zip(l_trace,times)]
        log_list.append(l_trace)
    means,stdevs = meanAndstdev(data,activity_names)
    log_list= [[(x-y)/z if z!=0 else 0 for x,y,z in zip(l,means,stdevs)]for l in log_list]
    return log_list

def addGaussianNoise(traces:list,mean:float=0,stddev:float=0.1)->list:
    noisyTraces=[]
    for trace in traces:
        noise = np.random.normal(loc=mean,scale=stddev,size=len(trace))
        noisyTraces.append([i+x for i,x in zip(trace,noise)])
    return noisyTraces
    
    
    
#read xes
log=xes_import_factory.apply("input/outliers_30_activities_10k_0.005.xes")

#transform it to traces as explained in our paper
transformed = transformTraces(log)

#Create training set byt adding noise
tracesWithNoise = addGaussianNoise(transformed)

#create autoencoder model as in Denoising autoencoders Nolle
class AE(torch.nn.Module):
    def __init__(self,length):
        super().__init__()
        self.encoder = torch.nn.Linear(in_features=length,out_features=length*2)
        self.decoder = torch.nn.Linear(in_features=length*2,out_features=length)
    def forward(self,x):
        x=F.relu(self.encoder(x))
        x=F.relu(self.decoder(x))
        return x

model = AE(len(transformed[0]))
loss_function=torch.nn.MSELoss()
optimizer = torch.optim.SGD(model.parameters(),lr=0.01,momentum=0.9,nesterov=True, weight_decay=1e-5)

#train on the training set
trainSet= [torch.FloatTensor(i) for i in tracesWithNoise ]
testSet = [torch.FloatTensor(i) for i in transformed ]

epochs = 500
train_loss=[]
for epoch in range(epochs):
    running_loss = 0.0
    for trace in [torch.FloatTensor(i) for i in addGaussianNoise(transformed) ]:
        reconstructed = model(trace)
        loss = loss_function(reconstructed, trace)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        running_loss += loss.item()
    loss = running_loss / len(trainSet)
    train_loss.append(loss)
    print('Epoch {} of {}, Train Loss: {:.3f}'.format(epoch+1, epochs, loss))  

#save model
torch.save(model,"model")



#read xes
log=xes_import_factory.apply("input/outliers_30_activities_10k_0.005.xes")

#transform it to traces as explained in our paper
transformed = transformTraces(log)
testSet = [torch.FloatTensor(i) for i in transformed ]

model=torch.load("model_0.005")
model.eval()

#get results
losses=[]
for trace in testSet:
    reconstructed=model(trace)
    loss = loss_function(reconstructed, trace)
    losses.append(loss.item())
m=mean(losses)
std=stdev(losses)
r=[]
with open("input/results_30_activities_10k_0.005_description","r") as f:
    for line in f:
        r.append(int(line.split(",")[1]))
outliers=[]
threshold = sorted(losses)[-len(r)]
for i,x in enumerate(losses):
    if x>=threshold:
        outliers.append(i)

tp=sum([1 for i in outliers if i in r ])
fp=len(outliers)-tp
precision = tp/len(outliers)
recall =tp/len(r) 
f1=2/((1/precision)*(1/recall))

plt.plot(sorted(losses))