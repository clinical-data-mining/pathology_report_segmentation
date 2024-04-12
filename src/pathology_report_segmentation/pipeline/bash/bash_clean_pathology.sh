#!/usr/bin/env bash

# Define which variables to use from msk_cdm.data_classes.<class> library
# TODO make these inputs instead of hardcoded variables
VAR_SCRIPT="config_cdm.script_query_to_minio"
VAR_IDB_CONFIG="config_cdm.idb_config"


set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

# Get creds
#. /gpfs/mindphidata/fongc2/.env_cf

# Get variables
SCRIPT=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm; print (${VAR_SCRIPT})")
IDB_CONFIG=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm; print (${VAR_IDB_CONFIG})")
MINIO_ENV=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm; print (${VAR_MINIO_ENV})")
SQL=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm; print (${VAR_SQL})")
FNAME_SAVE=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm; print (${VAR_FNAME_SAVE})")

# Run script
python $SCRIPT \
  --user=$USER \
  --pw=$PW \
  --db2c=$IDB_CONFIG \
  --minio=$MINIO_ENV \
  --sql=$SQL \
  --fsave=$FNAME_SAVE

