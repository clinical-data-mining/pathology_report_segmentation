import pandas as pd

from pathology_report_segmentation.annotations import _extractMMR_from_str
from msk_cdm.minio import MinioAPI
from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar


## Constants
fname_databricks_env = '/gpfs/mindphidata/fongc2/databricks_env_prod.txt'
FNAME_SAVE = 'epic_ddp_concat/pathology/pathology_mmr_calls_epic.tsv'
FNAME_PATH = 'cdsi_prod.cdm_epic_impact_pipeline_prod.t14_epic_impact_pathology_reports'
user = 'fongc2'
FNAME_MINIO_ENV = f"/gpfs/mindphidata/{user}/minio_env.txt"
COL_TEXT = 'path_prpt_p1'
COLS_SAVE = ['MRN','Accession Number','Path Procedure Date','MMR_ABSENT']

def extractMMR(df):
    filter_mmr = df[COL_TEXT].fillna('').str.contains('MLH1|PMS2|MSH2|MSH6',regex=True,case=False)
    filter_mnumber = ~df['ACCESSION_NUMBER'].str.contains('M')
    df_mmr = df[filter_mmr & filter_mnumber].copy()
    df_mmr['MMR_ABSENT'] = df_mmr[COL_TEXT].apply(_extractMMR_from_str)
    df_mmr = df_mmr.rename(
        columns={
            'ACCESSION_NUMBER': 'Accession Number',
            'DTE_PATH_PROCEDURE':'Path Procedure Date'
        }
    )
    df_save = df_mmr[COLS_SAVE]

    return df_save


def main():

    # Instantiate I/O objects
    obj_dbk = DatabricksAPI(fname_databricks_env=fname_databricks_env)
    obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)

    # Query data from dbx
    print(f"Loading pathology reports from {FNAME_PATH}")
    sql = f"""
    select * FROM {FNAME_PATH}
    """

    df_path = obj_dbk.query_from_sql(sql=sql)
    df_save = extractMMR(df=df_path)

    # Save to Minio
    obj_minio.save_obj(
        df=df_save,
        path_object=FNAME_SAVE,
        sep='\t'
    )

    return 0

if __name__ == '__main__':
    main()