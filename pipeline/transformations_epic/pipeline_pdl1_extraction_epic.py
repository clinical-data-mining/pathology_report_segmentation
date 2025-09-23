import argparse
from msk_cdm.databricks import DatabricksAPI
from msk_cdm.minio import MinioAPI
from pathology_report_segmentation.annotations_epic import PathologyExtractPDL1Epic


fname_pathology_reports = 'cdsi_prod.cdm_epic_impact_pipeline_prod.t14_epic_impact_pathology_reports'
fname_path_pdl1_save = 'epic_ddp_concat/pathology/pathology_pdl1_calls_epic.tsv'
COL_TEXT = 'path_prpt_p1'

def main():
    parser = argparse.ArgumentParser(description="pipeline_pdl1_extraction_epic.py")
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

    ## Constants
    col_text = COL_TEXT
    fname_save = fname_path_pdl1_save
    fname_path = fname_pathology_reports

    # Instantiate I/O objects
    obj_dbk = DatabricksAPI(fname_databricks_env=args.databricks_env)
    obj_minio = MinioAPI(fname_minio_env=args.minio_env)

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