import argparse
from annotations import PathologyExtractDOPEpic
from msk_cdm.databricks import DatabricksAPI


## Constants
col_label_1 = 'SAMPLE_ID'
col_label_2 = 'PDRX_ACCESSION_NO'
col_spec_sub = 'PART_DESCRIPTION'
table_map = 'cdsi_prod.cdm_impact_pipeline_prod.t03_id_mapping_pathology_sample_xml_parsed'

# Table configuration (dummy variables for now)
FNAME_DOP_SAVE = '/Volumes/cdsi_eng_phi/cdm_eng_pathology_report_segmentation/cdm_eng_pathology_report_segmentation_volume/pathology/pathology_spec_part_dop.tsv'
TABLE_CATALOG = 'cdsi_eng_phi'
TABLE_SCHEMA = 'cdm_eng_pathology_report_segmentation'
TABLE_NAME = 'pathology_spec_part_dop'


def main():
    parser = argparse.ArgumentParser(description="pipeline_dop_extraction_epic.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    # Extract DOP
    obj_p = PathologyExtractDOPEpic(
            fname_databricks_env=args.databricks_env,
            table=table_map,
            list_col_index=[col_label_1, col_label_2],
            col_spec_sub=col_spec_sub
        )

    df_dop = obj_p.return_df()
    df_orig = obj_p.return_df_original()

    df_f = df_orig.merge(right=df_dop, how='left', on=[col_label_1, col_label_2])
    df_f = df_f.drop(columns=['DMP_ID'])
    df_f = df_f.rename(columns={'PDRX_ACCESSION_NO': 'ACCESSION_NUMBER'})

    print(f"Saving {FNAME_DOP_SAVE}")
    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Save to both volume file and create table
    obj_db.write_db_obj(
        df=df_f,
        volume_path=FNAME_DOP_SAVE,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': TABLE_CATALOG,
            'schema': TABLE_SCHEMA,
            'table': TABLE_NAME,
            'volume_path': FNAME_DOP_SAVE,
            'sep': '\t'
        }
    )

    print("Saved!")
    print(df_f.head())

    tmp = 0

if __name__ == '__main__':
    main()