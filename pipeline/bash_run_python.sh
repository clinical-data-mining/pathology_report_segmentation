#!/usr/bin/env bash

set -e

ROOT_PATH_PATHOLOGY_REPO=$1
PATH_SCRIPT=$2

test -n "$ROOT_PATH_PATHOLOGY_REPO"
test -n "$PATH_SCRIPT"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate $CONDA_ENV

SCRIPT_FULL_PATH="$ROOT_PATH_PATHOLOGY_REPO/$PATH_SCRIPT"
echo $SCRIPT_FULL_PATH

# Run script
python $SCRIPT_FULL_PATH