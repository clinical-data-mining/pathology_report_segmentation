""""
cbio_timeline_pdl1.py
"""
#Import the requisite library
import argparse
import pandas as pd

from msk_cdm.databricks import DatabricksAPI


# Table configuration
TABLE_PDL1 = 'cdsi_prod.cdm_epic_impact_pipeline_prod.pathology_pdl1_calls_epic_idb_combined'
fname_timeline_pdl1 = 'epic_ddp_concat/pathology/table_timeline_pdl1_calls.tsv'

OUTPUT_TABLE_CATALOG = 'cdsi_prod'
OUTPUT_TABLE_SCHEMA = 'cdm_epic_impact_pipeline_prod'
OUTPUT_TABLE_NAME = 'table_timeline_pdl1_calls'
_col_order_pdl1 = [
    'MRN', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'SUBTYPE', 
    'SOURCE', 
    'PDL1_POSITIVE'
]

    
def main():
    parser = argparse.ArgumentParser(description="cbio_timeline_pdl1.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Load data from table
    print('Loading %s' % TABLE_PDL1)
    sql = f"SELECT * FROM {TABLE_PDL1}"
    df_pdl1 = obj_db.query_from_sql(sql=sql)

    df_pdl1 = df_pdl1.rename(columns={'DTE_PATH_PROCEDURE': 'START_DATE'})
    df_pdl1 = df_pdl1.assign(STOP_DATE='')
    df_pdl1 = df_pdl1.assign(EVENT_TYPE='PATHOLOGY')
    df_pdl1 = df_pdl1.assign(SUBTYPE='PD-L1 Positive')
    df_pdl1 = df_pdl1.assign(SOURCE='CDM')

    # TODO add color for positive and negative

    df_pdl1 = df_pdl1[df_pdl1['PDL1_POSITIVE'].notnull() & (df_pdl1['PDL1_POSITIVE'] != '')].copy()
    df_pdl1 = df_pdl1[_col_order_pdl1]

    # Save to both volume file and create table
    print('Saving %s' % fname_timeline_pdl1)
    obj_db.write_db_obj(
        df=df_pdl1,
        volume_path=fname_timeline_pdl1,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': OUTPUT_TABLE_CATALOG,
            'schema': OUTPUT_TABLE_SCHEMA,
            'table': OUTPUT_TABLE_NAME,
            'volume_path': fname_timeline_pdl1,
            'sep': '\t'
        }
    )


if __name__ == '__main__':
    main()
