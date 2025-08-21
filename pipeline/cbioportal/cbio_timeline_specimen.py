""""
cbioportal_timeline_specimen.py

Generates cBioPortal timeline files for date of surgery for corresponding sequenced samples
"""
import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
from msk_cdm.data_processing import convert_to_int


user = 'fongc2'
FNAME_MINIO_ENV = f"/gpfs/mindphidata/{user}/minio_env.txt"
FNAME_IMPACT_SUMMARY_SAMPLE = 'epic_ddp_concat/pathology/table_pathology_impact_sample_summary_dop_anno_epic_idb_combined.tsv'
FNAME_SAVE_TIMELINE_SPEC = 'epic_ddp_concat/pathology/table_timeline_specimen_surgery.tsv'
COL_ORDER_SEQ = [
    'MRN', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE',
    'SUBTYPE',
    'SAMPLE_ID'
]


def sample_acquisition_timeline():
    obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
    ### Load Dx timeline data
    col_use = [
        'MRN', 
        'SAMPLE_ID', 
        'DATE_OF_PROCEDURE_SURGICAL_EST'
    ]
    print('Loading %s' % FNAME_IMPACT_SUMMARY_SAMPLE)
    obj = obj_minio.load_obj(path_object=FNAME_IMPACT_SUMMARY_SAMPLE)
    df_samples_seq = pd.read_csv(
        obj, 
        header=0, 
        low_memory=False, 
        sep='\t', 
        usecols=col_use
    )
    df_samples_seq['DATE_OF_PROCEDURE_SURGICAL_EST'] = pd.to_datetime(df_samples_seq['DATE_OF_PROCEDURE_SURGICAL_EST'])
    df_samples_seq = convert_to_int(df=df_samples_seq, list_cols=['MRN'])
    df_samples_seq =df_samples_seq.rename(columns={'DATE_OF_PROCEDURE_SURGICAL_EST': 'START_DATE'})
    df_samples_seq = df_samples_seq[df_samples_seq['SAMPLE_ID'].notnull() & df_samples_seq['SAMPLE_ID'].str.contains('T')]

    df_samples_seq = df_samples_seq.assign(STOP_DATE='')
    df_samples_seq = df_samples_seq.assign(EVENT_TYPE='Sample acquisition')
    df_samples_seq = df_samples_seq.assign(SUBTYPE='')

    # Reorder columns
    df_samples_seq_f = df_samples_seq[COL_ORDER_SEQ]
    
    # Save timeline
    print('Saving: %s' % FNAME_SAVE_TIMELINE_SPEC)
    obj_minio.save_obj(
        df=df_samples_seq_f,
        path_object=FNAME_SAVE_TIMELINE_SPEC,
        sep='\t'
    )
        

    return df_samples_seq_f

def main():

    df_seq_timeline = sample_acquisition_timeline()
    print(df_seq_timeline.sample())
    

if __name__ == '__main__':
    main()



    
