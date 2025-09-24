#!/usr/bin/env bash

set -e

ROOT_PATH_PATHOLOGY_REPO=/gpfs/mindphidata/cdm_repos_dev/dev/github/pathology_report_segmentation
CONDA_INSTALL_PATH=/gpfs/mindphidata/fongc2/miniconda3
CONDA_ENV_NAME=conda-env-cdm-fongc2
MINIO_ENV=/gpfs/mindphidata/fongc2/minio_env_dev.txt
PATH_SCRIPT=$1
DBX_ENV=/gpfs/mindphidata/fongc2/databricks_env_prod.txt

test -n "$ROOT_PATH_PATHOLOGY_REPO"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$MINIO_ENV"
test -n "$PATH_SCRIPT"
test -n "$DBX_ENV"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate $CONDA_ENV_NAME

SCRIPT_FULL_PATH="$ROOT_PATH_PATHOLOGY_REPO/$PATH_SCRIPT"
echo $SCRIPT_FULL_PATH

# Run script
python $SCRIPT_FULL_PATH --minio_env=$MINIO_ENV --databricks_env=$DBX_ENV
