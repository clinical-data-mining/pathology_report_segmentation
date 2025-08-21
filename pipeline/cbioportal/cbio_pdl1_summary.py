#Import the requisite library
import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as var


FNAME_PDL1 = 'epic_ddp_concat/pathology/pathology_pdl1_calls_epic_idb_combined.tsv'
FNAME_MAP = 'epic_ddp_concat/pathology/table_pathology_impact_sample_summary_dop_anno_epic_idb_combined.tsv'
user = 'fongc2'
FNAME_MINIO_ENV = f"/gpfs/mindphidata/{user}/minio_env.txt"
FNAME_SAVE_PATIENT = 'epic_ddp_concat/pathology/table_summary_pdl1_patient.tsv'
FNAME_SAVE_SAMPLE = 'epic_ddp_concat/pathology/table_summary_pdl1_sample.tsv'


def _load_data(
    obj_minio,
    fname_pdl1,
    fname_map
):
    print('Loading %s' % fname_pdl1)
    obj = obj_minio.load_obj(path_object=fname_pdl1)
    df_pdl1 = pd.read_csv(obj, sep='\t')
    df_pdl1['DTE_PATH_PROCEDURE'] = pd.to_datetime(df_pdl1['DTE_PATH_PROCEDURE'], errors='coerce')

    
    print('Loading %s' % fname_map)
    obj = obj_minio.load_obj(path_object=fname_map)
    df_map = pd.read_csv(obj, sep='\t')
    
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
    
    
def create_pdl1_summaries(
    fname_minio_env,
    fname_pdl1,
    fname_map,
    fname_save_patient,
    fname_save_sample
):
    # Create minio object
    obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
    
    # Load data
    df_pdl1, df_map = _load_data(
        obj_minio=obj_minio,
        fname_pdl1=fname_pdl1,
        fname_map=fname_map
    )
    
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
    print('Saving %s' % fname_save_patient)
    obj_minio.save_obj(
        df=df_pdl1_p, 
        path_object=fname_save_patient, 
        sep='\t'
    )
    
    ## Sample summary
    print('Saving %s' % fname_save_sample)
    obj_minio.save_obj(
        df=df_pdl1_s, 
        path_object=fname_save_sample, 
        sep='\t'
    )
    
    return True

def main():
    fname_minio_env = FNAME_MINIO_ENV
    fname_pdl1 = FNAME_PDL1
    fname_map = FNAME_MAP
    fname_save_patient = FNAME_SAVE_PATIENT
    fname_save_sample = FNAME_SAVE_SAMPLE
    
    print('Creating PD-L1 Summaries')
    create_pdl1_summaries(
        fname_minio_env=fname_minio_env,
        fname_pdl1=fname_pdl1,
        fname_map=fname_map,
        fname_save_patient=fname_save_patient,
        fname_save_sample=fname_save_sample
    )
    
if __name__ == '__main__':
    main()
    
