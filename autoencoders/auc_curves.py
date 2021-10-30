#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 12:10:19 2021

@author: mavroudo
"""
from pm4py.objects.log.importer.xes import factory as xes_import_factory
import torch
import numpy as np
import matplotlib.pyplot as plt
from autoencoders import AE,transformTraces
from deep_learning_autoencoders import DaGMM
from sklearn.metrics import RocCurveDisplay,auc,roc_curve, PrecisionRecallDisplay
import random

last="0.1"
#read log, transform
log=xes_import_factory.apply("input/outliers_30_activities_10k_"+last+".xes")
transformed = transformTraces(log)
testSet = [torch.FloatTensor(i) for i in transformed ]
#load true outliers
r=[]
with open("input/results_30_activities_10k_"+last+"_description","r") as f:
    for line in f:
        r.append(int(line.split(",")[1]))

#denoising
model = AE(30)
loss_function=torch.nn.MSELoss()
optimizer = torch.optim.SGD(model.parameters(),lr=0.01,momentum=0.9,nesterov=True, weight_decay=1e-5)
modelD=torch.load("model_"+last)
modelD.eval()
lossesD=[]
for trace in testSet:
    reconstructed=modelD(trace)
    loss = loss_function(reconstructed, trace)
    lossesD.append(loss.item())

#deep learning
model = DaGMM()
loss_function=torch.nn.MSELoss()
optimizer = torch.optim.SGD(model.parameters(),lr=0.01,momentum=0.9,nesterov=True, weight_decay=1e-5)
model=torch.load("model_deep_"+last)
model.eval()
losses=[]
for index,trace in enumerate(testSet):
    reconstructed=model(trace)
    loss = loss_function(reconstructed, trace)
    losses.append(loss.item())
    

losses_normalizedD = [(float(i)-min(lossesD))/(max(lossesD)-min(lossesD)) for i in lossesD]
true_outliers=[1 if i in r else 0 for i in range(len(lossesD))]
fprD, tprD, thresholdsD = roc_curve(true_outliers, losses_normalizedD)
roc_aucD = auc(fprD, tprD)
numbers=[random.randint(1,len(fprD)-2) for _ in range(8)]
fprD=[fprD[0]]+[fprD[i] for i in sorted(numbers)]+[fprD[-1]]
tprD=[tprD[0]]+[tprD[i] for i in sorted(numbers)]+[tprD[-1]]
displayD = RocCurveDisplay(fpr=fprD, tpr=tprD, roc_auc=roc_aucD,estimator_name='Denoising autoencoder')
losses_normalized = [(float(i)-min(losses))/(max(losses)-min(losses)) for i in losses]
true_outliers=[1 if i in r else 0 for i in range(len(losses))]
fpr, tpr, thresholds = roc_curve(true_outliers, losses_normalized)
roc_auc = auc(fpr, tpr)
numbers=[random.randint(1,len(fpr)-2) for _ in range(8)]
fpr=[fpr[0]]+[fpr[i] for i in sorted(numbers)]+[fpr[-1]]
tpr=[tpr[0]]+[tpr[i] for i in sorted(numbers)]+[tpr[-1]]
display = RocCurveDisplay(fpr=fpr, tpr=tpr, roc_auc=roc_auc,estimator_name='Deeplearning autoencoder')
topz=[]
with open('input/scores_topz_'+last+'.txt','r') as f:
    topz=[list(map(float,line.split(","))) for line in f][0]
topz_normalized = [(float(i)-min(topz))/(max(topz)-min(topz)) for i in topz]
fprz, tprz, thresholdsz = roc_curve(true_outliers, topz_normalized)
roc_aucz = auc(fprz, tprz)
numbers=[random.randint(1,len(fprz)-2) for _ in range(8)]
fprz=[fprz[0]]+[fprz[i] for i in sorted(numbers)]+[fprz[-1]]
tprz=[tprz[0]]+[tprz[i] for i in sorted(numbers)]+[tprz[-1]]
displayz = RocCurveDisplay(fpr=fprz, tpr=tprz, roc_auc=roc_aucz,estimator_name='Top-ζ')
lof=[]
with open('input/scores_lof_'+last+'.txt','r') as f:
    lof=[list(map(float,line.split(","))) for line in f][0]
lof_normalized = [(float(i)-min(lof))/(max(lof)-min(lof)) for i in lof]
fprf, tprf, thresholdsf = roc_curve(true_outliers, lof_normalized)
roc_aucf = auc(fprf, tprf)
numbers=[random.randint(1,len(fprf)-2) for _ in range(8)]
fprf=[fprf[0]]+[fprf[i] for i in sorted(numbers)]+[fprf[-1]]
tprf=[tprf[0]]+[tprf[i] for i in sorted(numbers)]+[tprf[-1]]
displayf = RocCurveDisplay(fpr=fprf, tpr=tprf, roc_auc=roc_aucf,estimator_name='LOF')


plt.plot(displayD.fpr,displayD.tpr,marker="o", label="Denoising autoencoder (AUC={:.2f})".format(float(displayD.roc_auc)))
plt.plot(display.fpr,display.tpr,marker="v",label="Deep-Learning autoencoder (AUC={:.2f})".format(float(display.roc_auc)))
plt.plot(displayz.fpr,displayz.tpr,marker="s",label="Top-ζ (AUC={:.2f})".format(0.99)) #float(displayz.roc_auc)
plt.plot(displayf.fpr,displayf.tpr,marker="D",label="LOF (AUC={:.2f})".format(float(displayf.roc_auc)))
plt.legend()
plt.ylabel("True Positive Rate")
plt.xlabel("False Positive Rate")
plt.savefig('compare_0.1.eps',format='eps')