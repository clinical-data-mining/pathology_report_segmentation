import argparse
from msk_cdm.databricks import DatabricksAPI
from annotations import PathologyExtractPDL1Epic


fname_pathology_reports = 'cdsi_prod.cdm_epic_impact_pipeline_prod.t14_epic_impact_pathology_reports'
COL_TEXT = 'path_prpt_p1'

# Table configuration (dummy variables for now)
fname_path_pdl1_save = '/Volumes/cdsi_eng_phi/cdm_eng_pathology_report_segmentation/cdm_eng_pathology_report_segmentation_volume/pathology/pathology_pdl1_calls_epic.tsv'
TABLE_CATALOG = 'cdsi_eng_phi'
TABLE_SCHEMA = 'cdm_eng_pathology_report_segmentation'
TABLE_NAME = 'pathology_pdl1_calls_epic'

def main():
    parser = argparse.ArgumentParser(description="pipeline_pdl1_extraction_epic.py")
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

    # Instantiate I/O object
    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Query data from Databricks
    print(f"Loading pathology reports from {fname_path}")
    sql = f"""
    select * FROM {fname_pathology_reports}
    """

    df_pathology_reports_epic = obj_db.query_from_sql(sql=sql)

    print("Extracting PD-L1 from reports")
    # Extract PD-L1
    obj_p = PathologyExtractPDL1Epic(
        df_pathology_reports=df_pathology_reports_epic,
        col_text=col_text
    )

    df_pdl1 = obj_p.return_extraction()
    print(df_pdl1.sample())

    # Save to both volume file and create table
    print(f"Saving PD-L1 annotations to {fname_save}")
    obj_db.write_db_obj(
        df=df_pdl1,
        volume_path=fname_path_pdl1_save,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': TABLE_CATALOG,
            'schema': TABLE_SCHEMA,
            'table': TABLE_NAME,
            'volume_path': fname_path_pdl1_save,
            'sep': '\t'
        }
    )

    tmp = 0

if __name__ == '__main__':
    main()