""""
cbio_timeline_gleason.py

"""
#Import the requisite library
import argparse
import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import convert_to_int


# Table configuration
TABLE_GLEASON = 'cdsi_eng_phi.cdm_eng_pathology_report_segmentation.pathology_gleason_calls_epic_idb_combined'
fname_timeline_gleason = '/Volumes/cdsi_eng_phi/cdm_eng_pathology_report_segmentation/cdm_eng_pathology_report_segmentation_volume/cbioportal/table_timeline_gleason_scores.tsv'

OUTPUT_TABLE_CATALOG = 'cdsi_eng_phi'
OUTPUT_TABLE_SCHEMA = 'cdm_eng_pathology_report_segmentation'
OUTPUT_TABLE_NAME = 'table_timeline_gleason_scores'
_col_order_gleason = [
    'MRN', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'SUBTYPE', 
    'SOURCE', 
    'GLEASON_SCORE'
]

    
def main():
    parser = argparse.ArgumentParser(description="cbio_timeline_gleason.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Load data from table
    print('Loading %s' % TABLE_GLEASON)
    sql = f"SELECT * FROM {TABLE_GLEASON}"
    df_gleason = obj_db.query_from_sql(sql=sql)
    df_gleason = convert_to_int(df=df_gleason, list_cols=['Gleason'])
    df_gleason = df_gleason.drop(columns=['Accession Number'])

    df_gleason = df_gleason.rename(columns={'Path Procedure Date': 'START_DATE', 'Gleason':'GLEASON_SCORE'})
    df_gleason = df_gleason.assign(STOP_DATE='')
    df_gleason = df_gleason.assign(EVENT_TYPE='PATHOLOGY')
    df_gleason = df_gleason.assign(SUBTYPE='Gleason Score')
    df_gleason = df_gleason.assign(SOURCE='Pathology Reports (NLP)')

    df_gleason = df_gleason[df_gleason['GLEASON_SCORE'].notnull() & (df_gleason['GLEASON_SCORE'] != '')].copy()
    df_gleason = df_gleason[_col_order_gleason].copy()

    # Save to both volume file and create table
    print('Saving %s' % fname_timeline_gleason)
    obj_db.write_db_obj(
        df=df_gleason,
        volume_path=fname_timeline_gleason,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': OUTPUT_TABLE_CATALOG,
            'schema': OUTPUT_TABLE_SCHEMA,
            'table': OUTPUT_TABLE_NAME,
            'volume_path': fname_timeline_gleason,
            'sep': '\t'
        }
    )


if __name__ == '__main__':
    main()
