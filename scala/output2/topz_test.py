#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  5 10:40:09 2021

@author: mavroudo
"""
import pandas as pd
import numpy as np
from statistics import mean
from autorank import autorank, create_report, plot_stats
#one diagram per method for every metric

method_name = "Top-ζ"

metrics = ['RMSE','Euclidean','Chebyshev','Mahalanobis']
headers=["Distance","file","k","m2","time","f1"]
constant = "output/30_activities_10k_"
outliers=["0.1_","0.05_","0.01_","0.005_"]
last="oursFactor_trace"
true_outliers=[1000,500,100,50]
names=["10%_top","10%_top2","10%_top3","5%_top","5%_top2","5%_top3","1%_top","11%_top2","1%_top3","0.5%_top","0.05%_top2","0.5%_top3"]


data=[[] for i in range(len(names))]

for index,outlier in enumerate(outliers):
    filename=constant+outlier+last
    
    
    
    #load f1 scores per metric
    df = pd.read_csv(filename,header=None,names=headers)
    group_by_distance=df.groupby(df.Distance)
    
    
    #for rmse
    rmse_df=group_by_distance.get_group("rmse")
    rmse_df=rmse_df[rmse_df["m2"]==true_outliers[index]]
    rmse=sorted(list(rmse_df["f1"]),reverse=True)
    data[3*index].append(rmse[0])
    data[3*index+1].append(mean(rmse[:2]))
    data[3*index+2].append(mean(rmse[:3]))
    
    #for euclidean
    euclidean_df=group_by_distance.get_group("euclidean")
    euclidean_df=euclidean_df[euclidean_df["m2"]==true_outliers[index]]
    euclidean=sorted(list(euclidean_df["f1"]),reverse=True)
    data[3*index].append(euclidean[0])
    data[3*index+1].append(mean(euclidean[:2]))
    data[3*index+2].append(mean(euclidean[:3]))
    
    #for minkowski
    minkowski_df=group_by_distance.get_group("minkowski")
    minkowski_df=minkowski_df[minkowski_df["m2"]==true_outliers[index]]
    minkowski=sorted(list(minkowski_df["f1"]),reverse=True)
    data[3*index].append(minkowski[0])
    data[3*index+1].append(mean(minkowski[:2]))
    data[3*index+2].append(mean(minkowski[:3]))
    
    #for mahalanobis
    mahalanobis_df=group_by_distance.get_group("mahalanobis")
    mahalanobis_df=mahalanobis_df[mahalanobis_df["m2"]==true_outliers[index]]
    mahalanobis=sorted(list(mahalanobis_df["f1"]),reverse=True)
    data[3*index].append(mahalanobis[0])
    data[3*index+1].append(mean(mahalanobis[:2]))
    data[3*index+2].append(mean(mahalanobis[:3]))



import matplotlib.pyplot as plt
results=pd.DataFrame(index=names,columns=metrics,data=data)
ranks=autorank(results,alpha=0.01)
create_report(ranks)
x=plot_stats(ranks)
plt.tight_layout()
x.get_figure().savefig("output2/results/topz_distances.eps",format="eps")