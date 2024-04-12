#!/usr/bin/env bash

#ROOT_PATH_PATHOLOGY_REPO=/gpfs/mindphidata/fongc2/github/pathology_report_segmentation/
# Define which variables to use from msk_cdm.data_classes.<class> library
PATH_PATHOLOGY_CLEAN=pipeline/pipeline_clean_pathology.py

set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

SCRIPT_FULL_PATH="$ROOT_PATH_PATHOLOGY_REPO/$PATH_PATHOLOGY_CLEAN"
echo $SCRIPT_FULL_PATH

# Run script
python $SCRIPT_FULL_PATH

