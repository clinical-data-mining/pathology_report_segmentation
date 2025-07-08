import argparse
import pandas as pd

from pathology_report_segmentation.annotations import extractMMR
from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar


## Constants
FNAME_SAVE = c_dar.fname_path_mmr
FNAME_PATH = c_dar.fname_pathology

parser = argparse.ArgumentParser(description="pipeline_mmr_extraction.py")
parser.add_argument(
    "--minio_env",
    dest="minio_env",
    required=True,
    help="location of Minio environment file",
)
args = parser.parse_args()

obj_minio = MinioAPI(fname_minio_env=args.minio_env)
obj = obj_minio.load_obj(path_object=FNAME_PATH)
df_path = pd.read_csv(obj, sep='\t', low_memory=False)

df_save = extractMMR(df=df_path)

# Save to Minio
obj_minio.save_obj(
    df=df_save,
    path_object=FNAME_SAVE,
    sep='\t'
)
