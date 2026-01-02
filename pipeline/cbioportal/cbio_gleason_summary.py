#Import the requisite library
import argparse
import pandas as pd

from msk_cdm.databricks import DatabricksAPI


# Table configuration
TABLE_GLEASON = 'cdsi_eng_phi.cdm_eng_pathology_report_segmentation.pathology_gleason_calls_epic_idb_combined'
TABLE_MAP = 'cdsi_eng_phi.cdm_eng_pathology_report_segmentation.table_pathology_impact_sample_summary_dop_anno_epic_idb_combined'
FNAME_SAVE_PATIENT = '/Volumes/cdsi_eng_phi/cdm_eng_pathology_report_segmentation/cdm_eng_pathology_report_segmentation_volume/cbioportal/table_summary_gleason_patient.tsv'
FNAME_SAVE_SAMPLE = '/Volumes/cdsi_eng_phi/cdm_eng_pathology_report_segmentation/cdm_eng_pathology_report_segmentation_volume/cbioportal/table_summary_gleason_sample.tsv'
RENAME_SAMPLE = {'Gleason': 'GLEASON_SAMPLE_LEVEL'}

OUTPUT_TABLE_CATALOG = 'cdsi_eng_phi'
OUTPUT_TABLE_SCHEMA = 'cdm_eng_pathology_report_segmentation'
OUTPUT_TABLE_PATIENT = 'table_summary_gleason_patient'
OUTPUT_TABLE_SAMPLE = 'table_summary_gleason_sample'


def _load_data(obj_db):
    print('Loading %s' % TABLE_GLEASON)
    sql_gleason = f"SELECT * FROM {TABLE_GLEASON}"
    df_gleason = obj_db.query_from_sql(sql=sql_gleason)
    df_gleason['Path Procedure Date'] = pd.to_datetime(df_gleason['Path Procedure Date'], errors='coerce')

    print('Loading %s' % TABLE_MAP)
    sql_map = f"SELECT SAMPLE_ID, SOURCE_ACCESSION_NUMBER_0 FROM {TABLE_MAP}"
    df_map = obj_db.query_from_sql(sql=sql_map)

    return df_gleason, df_map


def _clean_data_patient(df_gleason):
    df_gleason = df_gleason.sort_values(by=['MRN', 'Path Procedure Date'])
    gleason_highest = df_gleason.groupby(['MRN'])['Gleason'].max().rename('GLEASON_HIGHEST_REPORTED').reset_index()
    gleason_first = df_gleason.groupby(['MRN']).first().reset_index()
    gleason_first = gleason_first.rename(columns={'Gleason': 'GLEASON_FIRST_REPORTED'})
    gleason_first = gleason_first[['MRN', 'GLEASON_FIRST_REPORTED']]

    df_gleason_patient = gleason_first.merge(right=gleason_highest, how='inner', on='MRN')
    
    return df_gleason_patient


def _clean_data_sample(
    df_gleason,
    df_map
):
    df_gleason_s1 = df_gleason.merge(
        right=df_map[['SAMPLE_ID', 'SOURCE_ACCESSION_NUMBER_0']], 
        how='inner', 
        left_on='Accession Number', 
        right_on='SOURCE_ACCESSION_NUMBER_0'
    )
    
    df_gleason_s = df_gleason_s1[['SAMPLE_ID', 'Gleason']].rename(columns=RENAME_SAMPLE)
    df_gleason_s['DMP_ID'] = df_gleason_s['SAMPLE_ID'].apply(lambda x: x[:9])
    
    return df_gleason_s
    
    
def create_gleason_summaries(fname_databricks_env):
    # Create Databricks object
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

    # Load data
    df_gleason, df_map = _load_data(obj_db=obj_db)

    # Create summaries
    ## Patient summary
    df_gleason_p = _clean_data_patient(df_gleason=df_gleason)

    ## Sample summary
    df_gleason_s = _clean_data_sample(
        df_gleason=df_gleason,
        df_map=df_map
    )

    # Save data
    ## Patient summary
    print('Saving %s' % FNAME_SAVE_PATIENT)
    obj_db.write_db_obj(
        df=df_gleason_p,
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
        df=df_gleason_s,
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
    parser = argparse.ArgumentParser(description="cbio_gleason_summary.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    print('Creating Gleason Score Summaries')
    create_gleason_summaries(fname_databricks_env=args.databricks_env)
    
if __name__ == '__main__':
    main()
    
