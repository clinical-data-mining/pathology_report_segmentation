""""
cbioportal_timeline_specimen.py

Generates cBioPortal timeline files for date of surgery for corresponding sequenced samples
"""
import argparse
import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import convert_to_int


# Table configuration
TABLE_IMPACT_SUMMARY = 'cdsi_prod.cdm_epic_impact_pipeline_prod.table_pathology_impact_sample_summary_dop_anno_epic_idb_combined'
FNAME_SAVE_TIMELINE_SPEC = 'epic_ddp_concat/pathology/table_timeline_specimen_surgery.tsv'

OUTPUT_TABLE_CATALOG = 'cdsi_prod'
OUTPUT_TABLE_SCHEMA = 'cdm_epic_impact_pipeline_prod'
OUTPUT_TABLE_NAME = 'table_timeline_specimen_surgery'
COL_ORDER_SEQ = [
    'MRN', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE',
    'SUBTYPE',
    'SAMPLE_ID'
]


def sample_acquisition_timeline(fname_databricks_env):
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)
    ### Load Dx timeline data
    col_use = [
        'MRN',
        'SAMPLE_ID',
        'DATE_OF_PROCEDURE_SURGICAL_EST'
    ]
    print('Loading %s' % TABLE_IMPACT_SUMMARY)
    cols_str = ', '.join(col_use)
    sql = f"SELECT {cols_str} FROM {TABLE_IMPACT_SUMMARY}"
    df_samples_seq = obj_db.query_from_sql(sql=sql)
    df_samples_seq['DATE_OF_PROCEDURE_SURGICAL_EST'] = pd.to_datetime(df_samples_seq['DATE_OF_PROCEDURE_SURGICAL_EST'])
    df_samples_seq = df_samples_seq.rename(columns={'DATE_OF_PROCEDURE_SURGICAL_EST': 'START_DATE'})
    # Convert MRN column from float or str to int
    df_samples_seq['MRN_numeric'] = pd.to_numeric(df_samples_seq['MRN'], errors='coerce')
    df_samples_seq = df_samples_seq.dropna(subset=['MRN_numeric'])
    df_samples_seq = convert_to_int(df=df_samples_seq, list_cols=['MRN'])


    df_samples_seq = df_samples_seq[df_samples_seq['SAMPLE_ID'].notnull() & df_samples_seq['SAMPLE_ID'].str.contains('T')]

    df_samples_seq = df_samples_seq.assign(STOP_DATE='')
    df_samples_seq = df_samples_seq.assign(EVENT_TYPE='Sample acquisition')
    df_samples_seq = df_samples_seq.assign(SUBTYPE='')

    # Reorder columns
    df_samples_seq_f = df_samples_seq[COL_ORDER_SEQ]

    # Save timeline to both volume file and create table
    print('Saving: %s' % FNAME_SAVE_TIMELINE_SPEC)
    obj_db.write_db_obj(
        df=df_samples_seq_f,
        volume_path=FNAME_SAVE_TIMELINE_SPEC,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': OUTPUT_TABLE_CATALOG,
            'schema': OUTPUT_TABLE_SCHEMA,
            'table': OUTPUT_TABLE_NAME,
            'volume_path': FNAME_SAVE_TIMELINE_SPEC,
            'sep': '\t'
        }
    )

    return df_samples_seq_f

def main():
    parser = argparse.ArgumentParser(description="cbioportal_timeline_specimen.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    df_seq_timeline = sample_acquisition_timeline(fname_databricks_env=args.databricks_env)
    print(df_seq_timeline.sample())
    

if __name__ == '__main__':
    main()



    
