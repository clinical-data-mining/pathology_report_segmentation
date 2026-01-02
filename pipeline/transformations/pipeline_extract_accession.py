import argparse
from msk_cdm.databricks import DatabricksAPI
from annotations import PathologyExtractAccessionEpic

## Constants
col_label_1 = 'SAMPLE_ID'
col_label_2 = 'PDRX_ACCESSION_NO'
col_spec_sub = 'PART_DESCRIPTION'

fname_path = 'epic_ddp_concat/id-mapping/epic_ddp_id_mapping_pathology.tsv'
FNAME_ACCESSION_NUMBER_SAVE = 'epic_ddp_concat/pathology/path_accessions.tsv'

# Table configuration (dummy variables for now)
TABLE_CATALOG = 'cdsi_prod'
TABLE_SCHEMA = 'cdm_epic_impact_pipeline_prod'
TABLE_NAME = 'path_accessions'


def main():
    parser = argparse.ArgumentParser(description="pipeline_extract_accession.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    # Extract DOP
    obj_path = PathologyExtractAccessionEpic(
        fname_databricks_env=args.databricks_env,
        fname=fname_path,
        list_col_index=[col_label_1, col_label_2],
        col_spec_sub=col_spec_sub
    )

    df_dop = obj_path.return_df()
    df_orig = obj_path.return_df_original()
    df_f = df_orig.merge(right=df_dop, how='left', on=['SAMPLE_ID', 'PDRX_ACCESSION_NO'])
    df_f = df_f.drop(columns=['DMP_ID'])

    df_f = df_f.rename(
        columns={
            'SOURCE_ACCESSION_NUMBER': 'SOURCE_ACCESSION_NUMBER_0',
            'SPECIMEN_NUMBER': 'SOURCE_SPEC_NUM_0'
        })

    # Save data
    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    print(f"Saving {FNAME_ACCESSION_NUMBER_SAVE}")

    # Save to both volume file and create table
    obj_db.write_db_obj(
        df=df_f,
        volume_path=FNAME_ACCESSION_NUMBER_SAVE,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': TABLE_CATALOG,
            'schema': TABLE_SCHEMA,
            'table': TABLE_NAME,
            'volume_path': FNAME_ACCESSION_NUMBER_SAVE,
            'sep': '\t'
        }
    )

    print(df_f.head())

    tmp = 0

if __name__ == '__main__':
    main()