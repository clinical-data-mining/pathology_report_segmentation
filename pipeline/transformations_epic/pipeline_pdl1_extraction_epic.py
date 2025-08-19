from msk_cdm.databricks import DatabricksAPI
from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations_epic import PathologyExtractPDL1Epic


fname_pathology_reports = 'cdsi_prod.cdm_epic_impact_pipeline_prod.t14_epic_impact_pathology_reports'
fname_path_pdl1_save = 'epic_ddp_concat/pathology/pathology_pdl1_calls_epic.tsv'
fname_databricks_env = '/gpfs/mindphidata/fongc2/databricks_env_prod.txt'
user = 'fongc2'
fname_minio = f"/gpfs/mindphidata/{user}/minio_env.txt"
COL_TEXT = 'path_prpt_p1'

def main():
    ## Constants
    col_text = COL_TEXT
    fname_save = fname_path_pdl1_save
    fname_path = fname_pathology_reports
    fname_minio_env = fname_minio
    fname_dbx_env = fname_databricks_env

    # Instantiate I/O objects
    obj_dbk = DatabricksAPI(fname_databricks_env=fname_dbx_env)
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    # Query data from dbx
    print(f"Loading pathology reports from {fname_path}")
    sql = f"""
    select * FROM {fname_pathology_reports}
    """

    df_pathology_reports_epic = obj_dbk.query_from_sql(sql=sql)

    print("Extracting PD-L1 fro reports")
    # Extract PD-L1
    obj_p = PathologyExtractPDL1Epic(
        df_pathology_reports=df_pathology_reports_epic,
        col_text=col_text
    )

    df_pdl1 = obj_p.return_extraction()
    print(df_pdl1.sample())

    # Save data
    print(f"Saving PD-L1 annotations to {fname_save}")
    obj_minio.save_obj(
        df=df_pdl1,
        path_object=fname_path_pdl1_save,
        sep='\t'
    )

    tmp = 0

if __name__ == '__main__':
    main()