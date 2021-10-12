#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  5 12:42:47 2021

@author: mavroudo
"""

import pandas as pd
import numpy as np
from statistics import mean
from autorank import autorank, create_report, plot_stats

method_name = "Distance-based"

methods = ['Top-ζ','LOF','Probabilistic','Distance-Based','ODAL']
outliers=["0.01_","0.005_"]
constant = "output/30_activities_10k_"
last=["oursFactor_trace","lof_trace","oursFactorStatistic_trace","oursTraditional_trace","odal3_trace"]
names=["1%_top","1%_top2","1%_top3","0.5%_top","0.05%_top2","0.05%_top3"]

true_outliers=[100,50]
data=[[] for _ in range(len(names))]

for index,m in enumerate(last[:2]):
    for index2,(o,to) in enumerate(zip(outliers,true_outliers)):
        filename=constant+o+m
        headers=["Distance","file","k","m2","time","precision"]
        df = pd.read_csv(filename,header=None,names=headers)
        df["recall"]=df["precision"]*df["m2"]/to
        df["f1"]=2*df["precision"]*df["recall"]/(df["precision"]+df["recall"])
        
        group_by_distance=df.groupby(df.Distance)
        mahalanobis_df=group_by_distance.get_group("mahalanobis")
        mahalanobis=sorted(list(mahalanobis_df["f1"]),reverse=True)
        print(mahalanobis)
        data[3*index2].append(mahalanobis[0])
        data[3*index2+1].append(mean(mahalanobis[:3]))
        data[3*index2+2].append(mean(mahalanobis[:5]))

for index,m in enumerate(last[2:]):
    for index2,(o,to) in enumerate(zip(outliers,true_outliers)):
        filename=constant+o+m
        print(filename)
        if index!=2:
            headers=["Distance","file","k","m2","time","precision","reported"]
            df = pd.read_csv(filename,header=None,names=headers)
            df["recall"]=df["precision"]*df["reported"]/to
            df["f1"]=2*df["precision"]*df["recall"]/(df["precision"]+df["recall"])
            group_by_distance=df.groupby(df.Distance)
            mahalanobis_df=group_by_distance.get_group("mahalanobis")
            mahalanobis=sorted(list(mahalanobis_df["f1"]),reverse=True)
            print(mahalanobis)
            data[3*index2].append(mahalanobis[0])
            data[3*index2+1].append(mean(mahalanobis[:3]))
            data[3*index2+2].append(mean(mahalanobis[:5]))
        else:
            headers=["file","k","m2","time","precision","reported"]
            df = pd.read_csv(filename,header=None,names=headers)
            df["recall"]=df["precision"]*df["reported"]/to
            df["f1"]=2*df["precision"]*df["recall"]/(df["precision"]+df["recall"])
            metrics=sorted(list(df["f1"]),reverse=True)
            data[3*index2].append(mahalanobis[0])
            data[3*index2+1].append(mean(metrics[:3]))
            data[3*index2+2].append(mean(metrics[:5]))

results=pd.DataFrame(index=names,columns=methods,data=data)
ranks=autorank(results,alpha=0.01)
create_report(ranks)
x=plot_stats(ranks,allow_insignificant=True)
x.get_figure().savefig("output2/results/mahalanobis_small_insignificant_with_odal.eps",format="eps")