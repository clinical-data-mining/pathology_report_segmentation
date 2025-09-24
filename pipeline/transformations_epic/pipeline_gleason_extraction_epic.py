import argparse
import numpy as np
import pandas as pd

from msk_cdm.data_processing import convert_to_int
from msk_cdm.minio import MinioAPI
from msk_cdm.databricks import DatabricksAPI
from pathology_report_segmentation.annotations import extractGleason

## Constants
FNAME_SAVE = 'epic_ddp_concat/pathology/pathology_gleason_calls_epic.tsv'
FNAME_PATH = 'cdsi_prod.cdm_epic_impact_pipeline_prod.t14_epic_impact_pathology_reports'
COL_TEXT = 'path_prpt_p1'
COLS_SAVE = ['MRN','Accession Number','Path Procedure Date','Gleason']

def main():
    parser = argparse.ArgumentParser(description="pipeline_gleason_extraction_epic.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    # Instantiate I/O objects
    obj_dbk = DatabricksAPI(fname_databricks_env=args.databricks_env)
    obj_minio = MinioAPI(fname_minio_env=args.minio_env)

    # Query data from dbx
    print(f"Loading pathology reports from {FNAME_PATH}")
    sql = f"""
        select * FROM {FNAME_PATH}
        """

    df_path = obj_dbk.query_from_sql(sql=sql)

    filter_gleason = df_path[COL_TEXT].fillna('').str.contains('Gleason',case=False)
    df_path_gleason = df_path[filter_gleason].copy()
    print('Abstracting Gleason scores')
    df_path_gleason['Gleason'] = df_path_gleason[COL_TEXT].apply(extractGleason)
    df_path_gleason = df_path_gleason.rename(
        columns={
            'ACCESSION_NUMBER': 'Accession Number',
            'DTE_PATH_PROCEDURE':'Path Procedure Date'
        }
    )
    df_save = df_path_gleason[COLS_SAVE]

    # Do last cleaning -- Gleason scores should not be under 6. Convert 1's to 10
    # TODO: Fix regex to grab 10s
    df_save.loc[df_save['Gleason'] == 1] = 10
    df_save.loc[df_save['Gleason'] < 6] = np.NaN
    df_save = df_save[df_save['Gleason'].notnull() & df_save['MRN'].notnull()]
    df_save = convert_to_int(df=df_save, list_cols=['MRN', 'Gleason'])

    print('Saving %s' % FNAME_SAVE)
    obj_minio.save_obj(
        df=df_save,
        path_object=FNAME_SAVE,
        sep='\t'
    )

if __name__ == '__main__':
    main()

