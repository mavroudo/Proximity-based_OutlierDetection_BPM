#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  4 13:23:45 2021

@author: mavroudo
"""

from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.algo.filtering.log.attributes import attributes_filter
from pm4py.objects.log.log import EventLog
from statistics import mean, stdev
import torch
import torch.nn as nn
class DaGMM(nn.Module):
    """Residual Block."""
    def __init__(self, n_gmm = 2, latent_dim=3):
        super(DaGMM, self).__init__()
        
        layers = []
        layers += [nn.Linear(30,16)]
        layers += [nn.Tanh()]        
        layers += [nn.Linear(16,8)]
        layers += [nn.Tanh()]        
        layers += [nn.Linear(8,4)]

        self.encoder = nn.Sequential(*layers)        
        layers = []
        layers += [nn.Linear(4,8)]
        layers += [nn.Tanh()]        
        layers += [nn.Linear(8,16)]
        layers += [nn.Tanh()]        
        layers += [nn.Linear(16,30)]
        self.decoder = nn.Sequential(*layers)

    def forward(self,x):
        enc=self.encoder(x)
        dec=self.decoder(enc)
        return dec
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

if __name__ == "__main__":
    #read xes
    log=xes_import_factory.apply("input/outliers_30_activities_10k_0.1.xes")
    description_file="input/results_30_activities_10k_0.1_description"
    outlier_ids=[]
    with open(description_file,"r") as f:
        for line in f:
            outlier_ids.append(int(line.split(",")[1]))
            
    #transform it to traces as explained in our paper
    transformed = transformTraces(log)
    trainSet = [torch.FloatTensor(i) for index,i in enumerate(transformed) if index not in outlier_ids ]
    testSet = [torch.FloatTensor(i) for i in transformed]
    
    model = DaGMM()
    loss_function=torch.nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(),lr=0.01,momentum=0.9,nesterov=True, weight_decay=1e-5)
    epochs = 100
    train_loss=[]
    for epoch in range(epochs):
        running_loss = 0.0
        for trace in trainSet:
            reconstructed = model(trace)
            loss = loss_function(reconstructed, trace)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
        loss = running_loss / len(trainSet)
        train_loss.append(loss)
        print('Epoch {} of {}, Train Loss: {:.3f}'.format(epoch+1, epochs, loss))  
    torch.save(model,"model_deep_0.1")
    
    model=torch.load("model_deep_0.05")
    model.eval()
    losses=[]
    for index,trace in enumerate(testSet):
        reconstructed=model(trace)
        loss = loss_function(reconstructed, trace)
        #losses.append([index,loss.item()])
        losses.append(loss.item())
        
    #create the roc_curve
    from sklearn.metrics import RocCurveDisplay,auc,roc_curve
    losses_normalized = [(float(i)-min(losses))/(max(losses)-min(losses)) for i in losses]
    true_outliers=[1 if i in outlier_ids else 0 for i in range(len(losses))]
    fpr, tpr, thresholds = roc_curve(true_outliers, losses_normalized)
    roc_auc = auc(fpr, tpr)
    display = RocCurveDisplay(fpr=fpr, tpr=tpr, roc_auc=roc_auc,estimator_name='Deeplearning autoencoder')
    display.plot()
    
    
    import matplotlib.pyplot as plt
    fig = plt.Figure()
    plt.plot(displayS.fpr,displayS.tpr,label="Denoising autoencoders")
    plt.plot(display.fpr,display.tpr,label="Deep-Learning autoencoders")
    plt.legend()
    plt.show()
    
    
    losses.sort(key=lambda x: x[1])
    accuracy=sum([1 if i[0] in outlier_ids else 0 for i in losses[len(testSet)-len(outlier_ids):]])