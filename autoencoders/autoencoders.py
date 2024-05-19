#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 26 19:37:18 2021

@author: mavroudo
"""
from statistics import mean, stdev

import matplotlib.pyplot as plt
import numpy as np
import torch
from pm4py.algo.filtering.log.attributes import attributes_filter
from pm4py.objects.log.importer.xes import importer as xes_import_factory
from pm4py.objects.log.obj import EventLog
from sklearn.metrics import RocCurveDisplay, auc, roc_curve


def mean_value_per_activity(log: EventLog):
    """
    Calculate the mean value of durations per activity.

    Args:
    log (EventLog): The event log object.

    Returns:
    dict: A dictionary with activity names as keys and lists of durations as values.
    list: A list of lists containing durations for each trace.
    """
    data = dict()
    data_durations = [[] for _ in log]
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


def mean_and_stdev(data, activity_names):
    """
    Calculate the mean and standard deviation of activity durations.

    Args:
    data (dict): Dictionary with activity names and their durations.
    activity_names (list): List of activity names.

    Returns:
    list: List of mean values for each activity.
    list: List of standard deviation values for each activity.
    """
    mean_values = []
    std_values = []
    for name in activity_names:
        mean_values.append(mean(data[name][0]))
        std_values.append(stdev(data[name][0]))
    return mean_values, std_values


def transform_traces(log: EventLog) -> list:
    """
    Transform traces into a format suitable for training the autoencoder.

    Args:
    log (EventLog): The event log object.

    Returns:
    list: A list of transformed traces.
    """
    activities = attributes_filter.get_attribute_values(log, "concept:name")
    activity_names = [i for i in activities]
    data, data_durations = mean_value_per_activity(log)
    log_list = []
    for n_trace, t in enumerate(log):
        l_trace = [0 for _ in range(len(activity_names))]
        times = [0 for _ in range(len(activity_names))]
        for n_event, event in enumerate(t):
            index = activity_names.index(event["concept:name"])
            l_trace[index] += data_durations[n_trace][n_event]
            times[index] += 1
        log_list.append([l_trace[i] / times[i] if times[i] != 0 else 0 for i in range(len(l_trace))])
    return log_list


def add_gaussian_noise(data, mean_value=0, stddev=0.1):
    """
    Add Gaussian noise to the data.

    Args:
    data (list): The input data.
    mean (float): Mean of the Gaussian noise.
    stddev (float): Standard deviation of the Gaussian noise.

    Returns:
    list: Data with added Gaussian noise.
    """
    noisy_data = []
    for d in data:
        noise = np.random.normal(mean_value, stddev, len(d))
        noisy_d = d + noise
        noisy_data.append(noisy_d)
    return noisy_data


# create autoencoder model as in Denoising autoencoders Nolle
class Autoencoder(torch.nn.Module):
    """
    Autoencoder neural network definition.
    """

    def __init__(self, input_size):
        super(Autoencoder, self).__init__()
        self.encoder = torch.nn.Sequential(
            torch.nn.Linear(input_size, 64),
            torch.nn.ReLU(),
            torch.nn.Linear(64, 32),
            torch.nn.ReLU(),
            torch.nn.Linear(32, 16),
            torch.nn.ReLU())
        self.decoder = torch.nn.Sequential(
            torch.nn.Linear(16, 32),
            torch.nn.ReLU(),
            torch.nn.Linear(32, 64),
            torch.nn.ReLU(),
            torch.nn.Linear(64, input_size),
            torch.nn.ReLU())

    def forward(self, x):
        x = self.encoder(x)
        x = self.decoder(x)
        return x


def normalize_losses(losses):
    """
    Normalize losses to the range [0, 1].

    Args:
    losses (list): List of loss values.

    Returns:
    list: Normalized loss values.
    """
    min_loss = min(losses)
    max_loss = max(losses)
    return [(loss - min_loss) / (max_loss - min_loss) for loss in losses]


if __name__ == "__main__":
    filename = "input/outliers_30_activities_10k_0.005.xes"
    outlier_description = "input/results_30_activities_10k_0.005_description"
    model_name = "model_0.005"
    epoches = 500

    # Check for GPU availability
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # Read XES file
    log = xes_import_factory.apply(filename)

    # Transform traces
    transformed = transform_traces(log)

    # Create autoencoder model
    input_size = len(transformed[0])
    model = Autoencoder(input_size).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    loss_function = torch.nn.MSELoss()

    # Add Gaussian noise to traces
    tracesWithNoise = add_gaussian_noise(transformed)

    # Train on the training set
    trainSet = [torch.FloatTensor(i) for i in tracesWithNoise]
    testSet = [torch.FloatTensor(i) for i in transformed]

    train_loss = []
    for epoch in range(epoches):
        running_loss = 0.0
        for trace in [torch.FloatTensor(i) for i in add_gaussian_noise(transformed)]:
            reconstructed = model(trace)
            loss = loss_function(reconstructed, trace)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
        loss = running_loss / len(trainSet)
        train_loss.append(loss)
        print('Epoch {} of {}, Train Loss: {:.3f}'.format(epoch + 1, epoches, loss))

    # Process can start from here by importing a previously created model
    # Save model
    torch.save(model, f"models/{model_name}")

    # Read XES file again
    log = xes_import_factory.apply(filename)

    # Transform it to traces
    transformed = transform_traces(log)
    testSet = [torch.FloatTensor(i) for i in transformed]

    # Load model
    modelS = torch.load(f"models/{model_name}")
    modelS.eval()

    # Get results
    losses = []
    for trace in testSet:
        reconstructed = modelS(trace)
        loss = loss_function(reconstructed, trace)
        losses.append(loss.item())
    m = mean(losses)
    std = stdev(losses)
    r = []
    with open(outlier_description, "r") as f:
        for line in f:
            r.append(int(line.split(",")[1]))
    outliers = []
    threshold = sorted(losses)[-len(r)]
    for i, x in enumerate(losses):
        if x >= threshold:
            outliers.append(i)

    losses_normalized = normalize_losses(losses)
    true_outliers = [1 if i in r else 0 for i in range(len(losses))]
    fprS, tprS, thresholds = roc_curve(true_outliers, losses_normalized)
    roc_auc = auc(fprS, tprS)
    displayS = RocCurveDisplay(fpr=fprS, tpr=tprS, roc_auc=roc_auc, estimator_name='Denoising autoencoder')
    displayS.plot()
    plt.show()

    plt.plot(sorted(losses))
    plt.show()
