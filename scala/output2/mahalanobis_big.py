#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  5 14:38:16 2021

@author: mavroudo
"""

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

methods = ['Top-ζ','LOF','Probabilistic','Distance-Based']
outliers=["0.1_","0.05_"]
#outliers=["0.1_"]
constant = "output/30_activities_10k_"
last=["oursFactor_trace","lof_trace","oursFactorStatistic_trace","oursTraditional_trace"]
names=["10%_top1","10%_top2","10%_top3","10%_top4","10%_top5","10%_top6","10%_top1","10%_top2","10%_top3","10%_top4","10%_top5","10%_top6","10%_top3","10%_top4","10%_top5","10%_top6"]

true_outliers=[1000,500]
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
        data[8*index2].append(mahalanobis[0])
        data[8*index2+1].append(mean(mahalanobis[:2]))
        data[8*index2+2].append(mean(mahalanobis[:3]))
        data[8*index2+3].append(mean(mahalanobis[:4]))
        data[8*index2+4].append(mean(mahalanobis[:5]))
        data[8*index2+5].append(mean(mahalanobis[:6]))
        data[8*index2+6].append(mean(mahalanobis[:7]))
        data[8*index2+7].append(mean(mahalanobis[:8]))


for index,m in enumerate(last[2:]):
    for index2,(o,to) in enumerate(zip(outliers,true_outliers)):
        filename=constant+o+m
        print(filename)
        headers=["Distance","file","k","m2","time","precision","reported"]
        df = pd.read_csv(filename,header=None,names=headers)
        df["recall"]=df["precision"]*df["reported"]/to
        df["f1"]=2*df["precision"]*df["recall"]/(df["precision"]+df["recall"])
        
        group_by_distance=df.groupby(df.Distance)
        mahalanobis_df=group_by_distance.get_group("mahalanobis")
        mahalanobis=sorted(list(mahalanobis_df["f1"]),reverse=True)
        print(mahalanobis)
        data[8*index2].append(mahalanobis[0])
        data[8*index2+1].append(mean(mahalanobis[:2]))
        data[8*index2+2].append(mean(mahalanobis[:3]))
        data[8*index2+3].append(mean(mahalanobis[:4]))
        data[8*index2+4].append(mean(mahalanobis[:5]))
        data[8*index2+5].append(mean(mahalanobis[:6]))
        data[8*index2+6].append(mean(mahalanobis[:7]))
        data[8*index2+7].append(mean(mahalanobis[:8]))
        
results=pd.DataFrame(index=names,columns=methods,data=data)
ranks=autorank(results,alpha=0.01)
create_report(ranks)
x=plot_stats(ranks,allow_insignificant=True)
x.get_figure().savefig("output2/results/mahalanobis_big_10.eps",format="eps",bbox_inches="tight")