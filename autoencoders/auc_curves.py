#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 12:10:19 2021

@author: mavroudo
"""
from pm4py.objects.log.importer.xes import importer as xes_import_factory
import torch
import matplotlib.pyplot as plt
from autoencoders import Autoencoder, transform_traces
from deep_learning_autoencoders import DaGMM
from sklearn.metrics import RocCurveDisplay, auc, roc_curve
import random


if __name__ == "__main__":
    last = "0.005"
    log_file = f"../scala/input/outliers_30_activities_10k_{last}.xes"
    description_file = f"../scala/input/results_30_activities_10k_{last}_description"
    denoising_model_file = f"models/model_{last}"
    deep_learning_model_file = f"models/model_deep_{last}"
    scores_topz_file = f'../scala/output2/scores_topz_{last}.txt'
    scores_lof_file = f'../scala/output2/scores_lof_{last}.txt'
    output_plot_file = f'compare_{last}.eps'

    # Read the log and transform it
    log = xes_import_factory.apply(log_file)
    transformed = transform_traces(log)
    test_set = [torch.FloatTensor(i) for i in transformed]

    # Load true outliers
    outlier_ids = []
    with open(description_file, "r") as f:
        for line in f:
            outlier_ids.append(int(line.split(",")[1]))

    # Evaluate Denoising Autoencoder
    model_denoising = torch.load(denoising_model_file)
    model_denoising.eval()
    loss_function = torch.nn.MSELoss()
    losses_denoising = []

    for trace in test_set:
        with torch.no_grad():
            reconstructed = model_denoising(trace)
            loss = loss_function(reconstructed, trace)
            losses_denoising.append(loss.item())

    # Evaluate Deep Learning Autoencoder
    model_deep = torch.load(deep_learning_model_file)
    model_deep.eval()
    losses_deep = []

    for trace in test_set:
        with torch.no_grad():
            reconstructed = model_deep(trace)
            loss = loss_function(reconstructed, trace)
            losses_deep.append(loss.item())

    # Normalize losses
    losses_normalized_denoising = [(i - min(losses_denoising)) / (max(losses_denoising) - min(losses_denoising)) for i
                                   in losses_denoising]
    true_outliers = [1 if i in outlier_ids else 0 for i in range(len(losses_denoising))]
    fpr_denoising, tpr_denoising, _ = roc_curve(true_outliers, losses_normalized_denoising)
    roc_auc_denoising = auc(fpr_denoising, tpr_denoising)
    display_denoising = RocCurveDisplay(fpr=fpr_denoising, tpr=tpr_denoising, roc_auc=roc_auc_denoising,
                                        estimator_name='Denoising autoencoder')

    losses_normalized_deep = [(i - min(losses_deep)) / (max(losses_deep) - min(losses_deep)) for i in losses_deep]
    fpr_deep, tpr_deep, _ = roc_curve(true_outliers, losses_normalized_deep)
    roc_auc_deep = auc(fpr_deep, tpr_deep)
    display_deep = RocCurveDisplay(fpr=fpr_deep, tpr=tpr_deep, roc_auc=roc_auc_deep,
                                   estimator_name='Deep-Learning autoencoder')

    # Load and evaluate Top-ζ method
    with open(scores_topz_file, 'r') as f:
        topz_scores = list(map(float, f.readline().split(',')))
    topz_normalized = [(i - min(topz_scores)) / (max(topz_scores) - min(topz_scores)) for i in topz_scores]
    fpr_topz, tpr_topz, _ = roc_curve(true_outliers, topz_normalized)
    roc_auc_topz = auc(fpr_topz, tpr_topz)
    display_topz = RocCurveDisplay(fpr=fpr_topz, tpr=tpr_topz, roc_auc=roc_auc_topz, estimator_name='Top-ζ')

    # Load and evaluate LOF method
    with open(scores_lof_file, 'r') as f:
        lof_scores = list(map(float, f.readline().split(',')))
    lof_normalized = [(i - min(lof_scores)) / (max(lof_scores) - min(lof_scores)) for i in lof_scores]
    fpr_lof, tpr_lof, _ = roc_curve(true_outliers, lof_normalized)
    roc_auc_lof = auc(fpr_lof, tpr_lof)
    display_lof = RocCurveDisplay(fpr=fpr_lof, tpr=tpr_lof, roc_auc=roc_auc_lof, estimator_name='LOF')

    # Plot ROC Curves
    plt.figure(figsize=(10, 8))
    plt.plot(display_denoising.fpr, display_denoising.tpr, marker="o",
             label=f"Denoising autoencoder (AUC={roc_auc_denoising:.2f})")
    plt.plot(display_deep.fpr, display_deep.tpr, marker="v",
             label=f"Deep-Learning autoencoder (AUC={roc_auc_deep:.2f})")
    plt.plot(display_topz.fpr, display_topz.tpr, marker="s", label=f"Top-ζ (AUC={roc_auc_topz:.2f})")
    plt.plot(display_lof.fpr, display_lof.tpr, marker="D", label=f"LOF (AUC={roc_auc_lof:.2f})")

    plt.legend()
    plt.ylabel("True Positive Rate")
    plt.xlabel("False Positive Rate")
    plt.title("ROC Curve Comparison of Outlier Detection Methods")
    plt.savefig(output_plot_file, format='eps')
    plt.show()