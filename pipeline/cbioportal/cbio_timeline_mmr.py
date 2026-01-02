""""
cbio_timeline_mmr.py



"""
#Import the requisite library
import argparse
import pandas as pd

from msk_cdm.databricks import DatabricksAPI


# Table configuration
TABLE_MMR = 'cdsi_prod.cdm_epic_impact_pipeline_prod.pathology_mmr_calls_epic_idb_combined'
fname_timeline_mmr = "epic_ddp_concat/pathology/table_timeline_mmr_calls.tsv"

OUTPUT_TABLE_CATALOG = 'cdsi_prod'
OUTPUT_TABLE_SCHEMA = 'cdm_epic_impact_pipeline_prod'
OUTPUT_TABLE_NAME = 'table_timeline_mmr_calls'
_col_order_mmr = [
    'MRN',
    'START_DATE',
    'STOP_DATE',
    'EVENT_TYPE',
    'SUBTYPE',
    'SOURCE',
    'MMR'
]


def main():
    parser = argparse.ArgumentParser(description="cbio_timeline_mmr.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Load data from table
    print('Loading %s' % TABLE_MMR)
    sql = f"SELECT * FROM {TABLE_MMR}"
    df_mmr = obj_db.query_from_sql(sql=sql)

    cols_rename = {
        'Path Procedure Date': 'START_DATE',
        'MMR_ABSENT': 'MMR'
    }
    df_mmr = df_mmr.rename(columns=cols_rename)
    df_mmr = df_mmr.assign(STOP_DATE='')
    df_mmr = df_mmr.assign(EVENT_TYPE='PATHOLOGY')
    df_mmr = df_mmr.assign(SUBTYPE='MMR Deficiency')
    df_mmr = df_mmr.assign(SOURCE='Pathology Reports (NLP)')

    df_mmr = df_mmr.replace({'MMR': {True: 'deficient', False: 'proficient'}})

    # TODO add color for positive and negative

    df_mmr = df_mmr[df_mmr['MMR'].notnull() & (df_mmr['MMR'] != '')].copy()
    df_mmr = df_mmr[_col_order_mmr]

    # Save to both volume file and create table
    print('Saving %s' % fname_timeline_mmr)
    obj_db.write_db_obj(
        df=df_mmr,
        volume_path=fname_timeline_mmr,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': OUTPUT_TABLE_CATALOG,
            'schema': OUTPUT_TABLE_SCHEMA,
            'table': OUTPUT_TABLE_NAME,
            'volume_path': fname_timeline_mmr,
            'sep': '\t'
        }
    )


if __name__ == '__main__':
    main()
