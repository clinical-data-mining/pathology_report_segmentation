import argparse
import numpy as np
import pandas as pd

from msk_cdm.data_processing import convert_to_int
from msk_cdm.databricks import DatabricksAPI
from annotations import extractGleason

## Constants
FNAME_SAVE = 'epic_ddp_concat/pathology/pathology_gleason_calls_epic.tsv'
FNAME_PATH = 'cdsi_prod.cdm_epic_impact_pipeline_prod.t14_epic_impact_pathology_reports'
COL_TEXT = 'path_prpt_p1'
COLS_SAVE = ['MRN','Accession Number','Path Procedure Date','Gleason']

# Table configuration (dummy variables for now)
TABLE_CATALOG = 'cdsi_prod'
TABLE_SCHEMA = 'cdm_epic_impact_pipeline_prod'
TABLE_NAME = 'pathology_gleason_calls_epic'

def main():
    parser = argparse.ArgumentParser(description="pipeline_gleason_extraction_epic.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    # Instantiate I/O object
    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Query data from Databricks
    print(f"Loading pathology reports from {FNAME_PATH}")
    sql = f"""
        select * FROM {FNAME_PATH}
        """

    df_path = obj_db.query_from_sql(sql=sql)

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
    # Save to both volume file and create table
    obj_db.write_db_obj(
        df=df_save,
        volume_path=FNAME_SAVE,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': TABLE_CATALOG,
            'schema': TABLE_SCHEMA,
            'table': TABLE_NAME,
            'volume_path': FNAME_SAVE,
            'sep': '\t'
        }
    )

if __name__ == '__main__':
    main()

