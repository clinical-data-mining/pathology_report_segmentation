#!/usr/bin/env bash

set -e

ROOT_PATH_PATHOLOGY_REPO=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
MINIO_ENV=$4
PATH_SCRIPT=$5
DBX_ENV=$6

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
