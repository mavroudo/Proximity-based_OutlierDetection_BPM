
# Outlier Detection with Autoencoders

This repository contains scripts for detecting outliers in event logs using autoencoders. The repository includes three Python scripts, each serving a different purpose in the outlier detection process.

## Files Overview

1. **autoencoders.py**
   - **Description:** This script reads an XES file using the pm4py library, creates and trains a denoising autoencoder with Gaussian noise, and uses this model to predict outliers. The script includes data transformation, model training, and evaluation.
   - **Key Features:**
     - Adds Gaussian noise to the data to create a robust denoising autoencoder.
     - Trains the autoencoder and evaluates its performance in detecting outliers.

2. **deep_learning_autoencoders.py**
   - **Description:** This script implements a more complex autoencoder using a deep learning approach. It reads an XES file, transforms the event log data, and trains a Deep Autoencoding Gaussian Mixture Model (DaGMM) to detect outliers.
   - **Key Features:**
     - Utilizes a deeper neural network structure for encoding and decoding.
     - Implements the DaGMM for enhanced outlier detection capabilities.

3. **auc_curves.py**
   - **Description:** This script creates a plot comparing the performance of different autoencoder methods against other outlier detection approaches using ROC curves. It aggregates results from the denoising autoencoder, deep learning autoencoder, and other methods such as Top-ζ and LOF.
   - **Key Features:**
     - Reads previously computed outlier scores and true outlier labels.
     - Generates and plots ROC curves to compare the performance of various outlier detection methods.

## Key Differences Between Autoencoder Approaches

- **Denoising Autoencoder (autoencoders.py):**
  - Adds Gaussian noise to the input data during training to make the model robust to small perturbations.
  - Trains on the noisy data and aims to reconstruct the original, clean data.
  - Suitable for scenarios where the data is expected to contain noise.

- **Deep Autoencoder (deep_learning_autoencoders.py):**
  - Utilizes a deeper and more complex neural network architecture.
  - Implements a Deep Autoencoding Gaussian Mixture Model (DaGMM) which combines autoencoding with Gaussian Mixture Models for improved anomaly detection.
  - Capable of capturing more complex patterns in the data, making it effective for larger and more complex datasets.

## Usage

1. **Training and Evaluation:**
   - Run `autoencoders.py` or `deep_learning_autoencoders.py` to train the respective autoencoder models on your event log data.

2. **Comparison of Methods:**
   - After training the models, use `auc_curves.py` to generate ROC curves and compare the performance of the different outlier detection methods.

## Prerequisites

- Python 3.x
- Required Python libraries (complete list can be found in `requirements.txt`):
  - pm4py
  - torch
  - sklearn
  - matplotlib

