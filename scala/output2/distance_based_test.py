#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  5 12:19:54 2021

@author: mavroudo
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  5 12:09:26 2021

@author: mavroudo
"""


import pandas as pd
import numpy as np
from statistics import mean
from autorank import autorank, create_report, plot_stats
#one diagram per method for every metric

method_name = "Distance-based"

metrics = ['RMSE','Euclidean','Chebyshev','Mahalanobis']
headers=["Distance","file","k","m2","time","precision","reported"]
constant = "output/30_activities_10k_"
outliers=["0.1_","0.05_","0.01_","0.005_"]
last="oursTraditional_trace"
true_outliers=[1000,500,100,50]
names=["10%_top","10%_top2","10%_top3","5%_top","5%_top2","5%_top3","1%_top","11%_top2","1%_top3","0.5%_top","0.05%_top2","0.5%_top3"]
names2=[str(i) for i in range(12)]

data=[[] for i in range(len(names))]

for index_i,outlier in enumerate(outliers):
    filename=constant+outlier+last
    
    
    
    #load f1 scores per metric
    df = pd.read_csv(filename,header=None,names=headers)
    df["recall"]=df["precision"]*df["reported"]/true_outliers[index_i]
    df["f1"]=2*df["precision"]*df["recall"]/(df["precision"]+df["recall"])
    
    
    group_by_distance=df.groupby(df.Distance)
    
    
    #for rmse
    rmse_df=group_by_distance.get_group("rmse")
    rmse=sorted(list(rmse_df["f1"]),reverse=True)
    data[3*index_i].append(rmse[0])
    data[3*index_i+1].append(mean(rmse[:2]))
    data[3*index_i+2].append(mean(rmse[:3]))
    
    #for euclidean
    euclidean_df=group_by_distance.get_group("euclidean")
    euclidean=sorted(list(euclidean_df["f1"]),reverse=True)
    data[3*index_i].append(euclidean[0])
    data[3*index_i+1].append(mean(euclidean[:2]))
    data[3*index_i+2].append(mean(euclidean[:3]))
    
    #for minkowski
    minkowski_df=group_by_distance.get_group("minkowski")
    minkowski=sorted(list(minkowski_df["f1"]),reverse=True)
    data[3*index_i].append(minkowski[0])
    data[3*index_i+1].append(mean(minkowski[:2]))
    data[3*index_i+2].append(mean(minkowski[:3]))
    
    #for mahalanobis
    mahalanobis_df=group_by_distance.get_group("mahalanobis")
    mahalanobis=sorted(list(mahalanobis_df["f1"]),reverse=True)
    data[3*index_i].append(mahalanobis[0])
    data[3*index_i+1].append(mean(mahalanobis[:2]))
    data[3*index_i+2].append(mean(mahalanobis[:3]))



import matplotlib.pyplot as plt

results=pd.DataFrame(index=names,columns=metrics,data=data)
ranks=autorank(results,alpha=0.01)
create_report(ranks)
x=plot_stats(ranks)

fig=x.get_figure()
plt.tight_layout()
fig.savefig("output2/results/traditional_distances.eps",format="eps",bbox_inches="tight")