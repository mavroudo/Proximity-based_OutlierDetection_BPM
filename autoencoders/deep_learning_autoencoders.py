#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  4 13:23:45 2021

@author: mavroudo
"""

import torch
import torch.nn as nn
from pm4py.objects.log.importer.xes import importer as xes_import_factory
from autoencoders import transform_traces, normalize_losses
from sklearn.metrics import RocCurveDisplay, auc, roc_curve
import matplotlib.pyplot as plt


class DaGMM(nn.Module):
    """
    Deep Autoencoding Gaussian Mixture Model (DaGMM) for anomaly detection.
    """
    def __init__(self, input_dim=30, latent_dim=3):
        super(DaGMM, self).__init__()

        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 16),
            nn.Tanh(),
            nn.Linear(16, 8),
            nn.Tanh(),
            nn.Linear(8, 4)
        )

        self.decoder = nn.Sequential(
            nn.Linear(4, 8),
            nn.Tanh(),
            nn.Linear(8, 16),
            nn.Tanh(),
            nn.Linear(16, input_dim)
        )

    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded


if __name__ == "__main__":

    filename = "../scala/input/outliers_30_activities_10k_0.005.xes"
    description_file = "../scala/input/results_30_activities_10k_0.005_description"
    model_name = "models/model_deep_0.005"
    epoches = 500

    # Read XES file
    log = xes_import_factory.apply(filename)

    # Load outlier IDs
    outlier_ids = []
    with open(description_file, "r") as file:
        for line in file:
            outlier_ids.append(int(line.split(",")[1]))

    # Transform log into traces
    transformed_traces = transform_traces(log)

    # Prepare training and test sets
    train_set = [torch.FloatTensor(trace) for idx, trace in enumerate(transformed_traces) if idx not in outlier_ids]
    test_set = [torch.FloatTensor(trace) for trace in transformed_traces]

    # Initialize model, loss function, and optimizer
    model = DaGMM().cuda() if torch.cuda.is_available() else DaGMM()
    loss_function = torch.nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01, momentum=0.9, nesterov=True, weight_decay=1e-5)

    # Train model
    train_losses = []
    for epoch in range(epoches):
        model.train()
        running_loss = 0.0
        for trace in train_set:
            trace = trace.cuda() if torch.cuda.is_available() else trace
            optimizer.zero_grad()
            reconstructed = model(trace)
            loss = loss_function(reconstructed, trace)
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
        average_loss = running_loss / len(train_set)
        train_losses.append(average_loss)
        print(f'Epoch {epoch + 1}/{epoches}, Train Loss: {average_loss:.3f}')

    # Save the trained model
    torch.save(model, f"models/{model_name}")

    # Load the model for evaluation
    model = torch.load(f"models/{model_name}")
    model.eval()

    # Evaluate the model
    losses = []
    for trace in test_set:
        trace = trace.cuda() if torch.cuda.is_available() else trace
        with torch.no_grad():
            reconstructed = model(trace)
            loss = loss_function(reconstructed, trace)
            losses.append(loss.item())

    # Normalize losses for ROC curve
    normalized_losses = normalize_losses(losses)
    true_outliers = [1 if i in outlier_ids else 0 for i in range(len(losses))]

    # Plot ROC curve
    fpr, tpr, thresholds = roc_curve(true_outliers, normalized_losses)
    roc_auc = auc(fpr, tpr)
    display = RocCurveDisplay(fpr=fpr, tpr=tpr, roc_auc=roc_auc, estimator_name='Deep-Learning autoencoder')
    display.plot()
    plt.title('ROC Curve for Deep-Learning Autoencoder')
    plt.show()

    # Sort and calculate accuracy
    sorted_losses = sorted(enumerate(losses), key=lambda x: x[1])
    accuracy = sum(
        1 for i in range(len(test_set) - len(outlier_ids), len(test_set)) if sorted_losses[i][0] in outlier_ids)
    print(f"Accuracy: {accuracy}/{len(outlier_ids)}")
