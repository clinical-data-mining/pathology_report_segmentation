#!/usr/bin/env bash

set -e

ROOT_PATH_PATHOLOGY_REPO=$1
PATH_SCRIPT=$2

test -n "$ROOT_PATH_PATHOLOGY_REPO"
test -n "$PATH_SCRIPT"

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

SCRIPT_FULL_PATH="$ROOT_PATH_PATHOLOGY_REPO/$PATH_SCRIPT"
echo $SCRIPT_FULL_PATH

# Run script
python $SCRIPT_FULL_PATH