#!/usr/bin/env bash

set -e

PATH_SCRIPT=$1
ROOT_PATH_PATHOLOGY_REPO=$2
CONDA_INSTALL_PATH=$3
CONDA_ENV_NAME=$4
MINIO_ENV=$5
DATABRICKS_ENV=$6

test -n "$PATH_SCRIPT"
test -n "$ROOT_PATH_PATHOLOGY_REPO"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$MINIO_ENV"
test -n "$DATABRICKS_ENV"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate $CONDA_ENV_NAME

SCRIPT_FULL_PATH="$ROOT_PATH_PATHOLOGY_REPO/$PATH_SCRIPT"
echo $SCRIPT_FULL_PATH

# Run script
python $SCRIPT_FULL_PATH --minio_env=$MINIO_ENV --databricks_env=$DATABRICKS_ENV