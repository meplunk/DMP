# Eviction Moratoria and Homelessness

This repository contains the replication code and data preparation
pipeline for the project examining the relationship between state
eviction moratoria and homelessness outcomes.

------------------------------------------------------------------------

## Replication Instructions

Follow the steps below to reproduce the cleaned analysis dataset.

### 1. Clone the repository

Clone the repository and navigate to the project directory.

### 2. Download Raw Data

All required raw data files are available in the following Dropbox
folder:

https://www.dropbox.com/scl/fo/hfw7m0peeqsy96gpegyvf/AGIL-TykGfvSOa3WyQ2JP-M?rlkey=dllspdj7lli17w875jnhau26i&st=947n21vd&dl=0

Download all files and place them in:

data/01_raw/

Alternatively, download the datasets directly from their original
sources by following the instructions in:

data/01_raw/README.md

------------------------------------------------------------------------

### 3. Run the Cleaning Pipeline

From the project root directory, run:

conda env create -f environment.yml
conda activate dmp_env
python cleaning_code/master_clean.py

This script executes the full cleaning and merging pipeline in the
correct order and produces the final analysis dataset.

------------------------------------------------------------------------

## Citation

When using this replication package:

-   Please cite all original data sources as indicated in the
    documentation.
-   Acknowledge the data providers listed in data/01_raw/README.md.

------------------------------------------------------------------------

## Requirements

Install dependencies with:

pip install -r requirements.txt

