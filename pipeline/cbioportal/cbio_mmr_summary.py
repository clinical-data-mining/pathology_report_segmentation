#Import the requisite library
import argparse
import pandas as pd

from msk_cdm.databricks import DatabricksAPI


# Table configuration
TABLE_MMR = 'cdsi_prod.cdm_epic_impact_pipeline_prod.table_timeline_mmr_calls'
FNAME_SAVE_PATIENT = 'epic_ddp_concat/pathology/table_summary_mmr_patient.tsv'

OUTPUT_TABLE_CATALOG = 'cdsi_prod'
OUTPUT_TABLE_SCHEMA = 'cdm_epic_impact_pipeline_prod'
OUTPUT_TABLE_PATIENT = 'table_summary_mmr_patient'


def _load_data(obj_db):
    print('Loading %s' % TABLE_MMR)
    sql = f"SELECT * FROM {TABLE_MMR}"
    df_mmr = obj_db.query_from_sql(sql=sql)
    df_mmr['START_DATE'] = pd.to_datetime(df_mmr['START_DATE'], errors='coerce')

    return df_mmr


def _clean_data_patient(df_mmr):
    df_mmr = df_mmr.sort_values(by=['MRN', 'START_DATE'])
    reps = {True:'deficient', False:'proficient'}
    list_mrns_mmr = df_mmr.loc[df_mmr['MMR'] == 'deficient', 'MRN']
    df_mmr_summary = df_mmr[['MRN']].drop_duplicates()
    df_mmr_summary['HISTORY_OF_D_MMR'] = df_mmr_summary['MRN'].isin(list_mrns_mmr).replace(reps)

    return df_mmr_summary



def create_dmmr_summary(fname_databricks_env):
    # Create Databricks object
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

    # Load data
    df_mmr = _load_data(obj_db=obj_db)

    # Create summaries
    ## Patient summary
    df_mmr_p = _clean_data_patient(df_mmr=df_mmr)

    # Save data
    ## Patient summary
    print('Saving %s' % FNAME_SAVE_PATIENT)
    obj_db.write_db_obj(
        df=df_mmr_p,
        volume_path=FNAME_SAVE_PATIENT,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': OUTPUT_TABLE_CATALOG,
            'schema': OUTPUT_TABLE_SCHEMA,
            'table': OUTPUT_TABLE_PATIENT,
            'volume_path': FNAME_SAVE_PATIENT,
            'sep': '\t'
        }
    )

    return df_mmr_p

def main():
    parser = argparse.ArgumentParser(description="cbio_mmr_summary.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    print('Creating dMMR Summaries')
    create_dmmr_summary(fname_databricks_env=args.databricks_env)

if __name__ == '__main__':
    main()

