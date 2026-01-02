#Import the requisite library
import argparse
import pandas as pd

from msk_cdm.databricks import DatabricksAPI


# Table configuration
TABLE_PDL1 = 'cdsi_eng_phi.cdm_epic_impact_pipeline_prod.pathology_pdl1_calls_epic_idb_combined'
TABLE_MAP = 'cdsi_eng_phi.cdm_eng_pathology_report_segmentation.table_pathology_impact_sample_summary_dop_anno_epic_idb_combined'
FNAME_SAVE_PATIENT = '/Volumes/cdsi_eng_phi/cdm_eng_pathology_report_segmentation/cdm_eng_pathology_report_segmentation_volume/cbioportal/table_summary_pdl1_patient.tsv'
FNAME_SAVE_SAMPLE = '/Volumes/cdsi_eng_phi/cdm_eng_pathology_report_segmentation/cdm_eng_pathology_report_segmentation_volume/cbioportal/table_summary_pdl1_sample.tsv'

OUTPUT_TABLE_CATALOG = 'cdsi_eng_phi'
OUTPUT_TABLE_SCHEMA = 'cdm_epic_impact_pipeline_prod'
OUTPUT_TABLE_PATIENT = 'table_summary_pdl1_patient'
OUTPUT_TABLE_SAMPLE = 'table_summary_pdl1_sample'


def _load_data(obj_db):
    print('Loading %s' % TABLE_PDL1)
    sql_pdl1 = f"SELECT * FROM {TABLE_PDL1}"
    df_pdl1 = obj_db.query_from_sql(sql=sql_pdl1)
    df_pdl1['DTE_PATH_PROCEDURE'] = pd.to_datetime(df_pdl1['DTE_PATH_PROCEDURE'], errors='coerce')

    print('Loading %s' % TABLE_MAP)
    sql_map = f"SELECT SAMPLE_ID, SOURCE_ACCESSION_NUMBER_0 FROM {TABLE_MAP}"
    df_map = obj_db.query_from_sql(sql=sql_map)

    return df_pdl1, df_map


def _clean_data_patient(df_pdl1):
    df_pdl1 = df_pdl1.sort_values(by=['MRN', 'DTE_PATH_PROCEDURE'])
    reps = {True:'Yes', False:'No'}
    list_mrns_pdl1 = df_pdl1.loc[df_pdl1['PDL1_POSITIVE'] == 'Yes', 'MRN']
    df_pdl1_summary = df_pdl1[['MRN']].drop_duplicates()
    df_pdl1_summary['HISTORY_OF_PDL1'] = df_pdl1_summary['MRN'].isin(list_mrns_pdl1).replace(reps)
    
    return df_pdl1_summary


def _clean_data_sample(
    df_pdl1,
    df_map
):
    df_pdl1_s1 = df_pdl1.merge(
        right=df_map[['SAMPLE_ID', 'SOURCE_ACCESSION_NUMBER_0']], 
        how='inner', 
        left_on='ACCESSION_NUMBER', 
        right_on='SOURCE_ACCESSION_NUMBER_0'
    )
    
    df_pdl1_s = df_pdl1_s1[['SAMPLE_ID', 'PDL1_POSITIVE']].copy()
    df_pdl1_s['DMP_ID'] = df_pdl1_s['SAMPLE_ID'].apply(lambda x: x[:9])
    
    return df_pdl1_s
    
    
def create_pdl1_summaries(fname_databricks_env):
    # Create Databricks object
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

    # Load data
    df_pdl1, df_map = _load_data(obj_db=obj_db)

    # Create summaries
    ## Patient summary
    df_pdl1_p = _clean_data_patient(df_pdl1=df_pdl1)

    ## Sample summary
    df_pdl1_s = _clean_data_sample(
        df_pdl1=df_pdl1,
        df_map=df_map
    )

    # Save data
    ## Patient summary
    print('Saving %s' % FNAME_SAVE_PATIENT)
    obj_db.write_db_obj(
        df=df_pdl1_p,
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

    ## Sample summary
    print('Saving %s' % FNAME_SAVE_SAMPLE)
    obj_db.write_db_obj(
        df=df_pdl1_s,
        volume_path=FNAME_SAVE_SAMPLE,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': OUTPUT_TABLE_CATALOG,
            'schema': OUTPUT_TABLE_SCHEMA,
            'table': OUTPUT_TABLE_SAMPLE,
            'volume_path': FNAME_SAVE_SAMPLE,
            'sep': '\t'
        }
    )

    return True

def main():
    parser = argparse.ArgumentParser(description="cbio_pdl1_summary.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    print('Creating PD-L1 Summaries')
    create_pdl1_summaries(fname_databricks_env=args.databricks_env)
    
if __name__ == '__main__':
    main()
    
