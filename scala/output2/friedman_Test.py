#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  5 09:16:37 2021

@author: mavroudo
"""

from autorank import autorank, create_report, plot_stats
import pandas as pd

data_names=['0.5% outliers','1% outleirs','5% outliers','10% outliers','nonsense']
methods=['Τοp-ζ','LOF','Probabilistic','Distance-Based']
metrics = ['RMSE','Eucl','Mink','Mahal']
names=[i+"-"+j for i in methods for j in metrics]


x05=[0.677,0.69,0.84	,1,0.762,0.816,0.802,1,0.71,0.73,0.58,0.967,0.645,0.557,0.706,0.878]
x1=[0.656	,0.639,0.716,1,0.707,0.706,0.753,0.996,0.618,0.658,0.493,0.909,0.725,0.676,0.692,1]
x5=[0.635	,0.623,0.64,0.9,0.655,0.666,0.674,0.927,0.51,0.524,0.469,0.703,0.679,0.709,0.743,0.945]
x10=[0.644,0.634,0.64,0.822,0.716,0.715,0.67,0.907,0.374,0.378,0.365,0.574,0.668,0.678,0.728,0.928]
x_last=[1]*16
data=[x05,x1,x5,x10,x_last]

results = pd.DataFrame(data=data,index=data_names,columns=names)

ranks=autorank(results)
create_report(ranks)
plot_stats(ranks)