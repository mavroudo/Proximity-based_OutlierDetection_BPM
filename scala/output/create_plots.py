#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 19 11:47:21 2024

@author: mavroudo
"""
import os

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.path import Path
plt.rcParams['hatch.linewidth'] = 1
plt.rcParams['xtick.labelsize'] = 14
plt.rcParams['ytick.labelsize'] = 12
plt.rcParams['font.size'] = '12'

distances = ["RMSE", "Euclidean", "Chebyshev", "Mahalanobis"]
outliers = ["0.5", "1", "5", "10"]
colors= ['#1E90FF','#FF4500','#FFA500','#800080']

verts = [
    (-0.5, -0.5), (0.5, 0.5),  # Diagonal line \
    (0.5, -0.5), (-0.5, 0.5),  # Diagonal line /
    (-0.5, 0), (0.5, 0),           # Horizontal line -
    (0, -0.5), (0, 0.5)            # Vertical line |
]
codes = [
    Path.MOVETO, Path.LINETO,  # First diagonal line
    Path.MOVETO, Path.LINETO,  # Second diagonal line
    Path.MOVETO, Path.LINETO,  # Horizontal line
    Path.MOVETO, Path.LINETO   # Vertical line
]
custom_marker = Path(verts, codes)

marker= ['o','x','s',custom_marker]

def distance_names(x: str):
    if x == 'rmse':
        return 'RMSE'
    elif x == 'minkowski':
        return "Chebyshev"
    else:
        return x.capitalize()


true_outliers = {}
for file in os.listdir('../input'):
    if "_description" in file:
        with open(f'../input/{file}', 'r') as f:
            counter = 0
            for line in f:
                counter += 1
        outliers = file.split("_")[-2]
        true_outliers[float(outliers)] = counter

# file name characteristic
file_characteristic = "oursFactor_trace"
data = []
for f in os.listdir('../output/'):
    if file_characteristic in f:
        with open(f'../output/{f}', 'r') as file:
            for line in file:
                data.append(line.split(','))

df = pd.DataFrame(data)
df.columns = ['distance', 'outliers', 'k', 'z', 'duration', 'precision']
df['outliers'] = df['outliers'].str.split('_').apply(lambda x: float(x[-1]))
df["k"] = df["k"].apply(lambda x: int(x))
df["z"] = df["z"].apply(lambda x: int(x))
df["duration"] = df["duration"].apply(lambda x: float(x))
df["precision"] = df["precision"].apply(lambda x: float(x))
df["recall"] = df.apply(lambda row: (row["precision"] * row["z"]) / true_outliers[row['outliers']], axis=1)
df['distance'] = df['distance'].apply(distance_names)

fig, ax = plt.subplots(1, 4, figsize=(12, 10))
# plot for precision
for index, i in enumerate(zip([0.005, 0.01, 0.05, 0.1], ["0.5", "1", "5", "10"])):
    d = df[(df["outliers"] == i[0]) & (df["k"] == 50)]
    for d_index,distance in enumerate(distances):
        d_in = d[d["distance"] == distance].sort_values(by='z', ascending=True)
        ax[index].plot(d_in['z'],d_in['precision'],linestyle='-', color=colors[d_index], marker=marker[d_index], markersize=20, markerfacecolor='none',label=distances[d_index])
        ax[index].set_title(f"{i[1]}% outliers",fontsize=18)
ax[0].set_ylabel('Precision',fontsize=18)
plt.legend()
plt.tight_layout()
plt.savefig(f'../graphs/precision_oursFactor_k_constant_new.eps',format='eps')

plt.show()

