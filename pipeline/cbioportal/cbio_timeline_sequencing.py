""""
cbio_timeline_sequencing.py

Generates cBioPortal timeline files for sequencing dates
"""
import argparse
import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm


FNAME_PATHOLOGY = config_cdm.fname_path_clean
FNAME_SAVE_TIMELINE_SEQ = config_cdm.fname_path_sequencing_cbio_timeline
COL_DTE_SEQ = 'DTE_TUMOR_SEQUENCING'
COLS_PATHOLOGY = [
    COL_DTE_SEQ,
    'MRN',
    'SAMPLE_ID'
]
COL_ORDER_SEQ = [
    'MRN', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'SUBTYPE',
    'SAMPLE_ID'
]


def sequencing_timeline(fname_minio_env):
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
    
    print('Loading %s' % FNAME_PATHOLOGY)
    obj = obj_minio.load_obj(path_object=FNAME_PATHOLOGY)
    df_path = pd.read_csv(
        obj, 
        header=0, 
        low_memory=False, 
        sep='\t', 
        usecols=COLS_PATHOLOGY
    )
    
    df_path = df_path.dropna().copy()
    df_path[COL_DTE_SEQ] = pd.to_datetime(
        df_path[COL_DTE_SEQ],
        errors='coerce'
    )
    df_path_filt = df_path[df_path['SAMPLE_ID'].notnull() & df_path['SAMPLE_ID'].str.contains('T')]
    df_path_filt = df_path_filt.rename(columns={COL_DTE_SEQ: 'START_DATE'})
    
    # Drop samples without sequencing date
    df_path_filt = df_path_filt[df_path_filt['START_DATE'].notnull()]
    
    df_path_filt = df_path_filt.assign(STOP_DATE='')
    df_path_filt = df_path_filt.assign(EVENT_TYPE='Sequencing')
    df_path_filt = df_path_filt.assign(SUBTYPE='')
    
    # Reorder columns
    df_samples_seq_f = df_path_filt[COL_ORDER_SEQ]
    
    df_samples_seq_f = df_samples_seq_f.dropna()
    
    # Save timeline
    print('Saving: %s' % FNAME_SAVE_TIMELINE_SEQ)
    obj_minio.save_obj(
        df=df_samples_seq_f,
        path_object=FNAME_SAVE_TIMELINE_SEQ,
        sep='\t'
    )

    return df_samples_seq_f

def main():
    parser = argparse.ArgumentParser(description="cbio_timeline_sequencing.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    df_seq_timeline = sequencing_timeline(fname_minio_env=args.minio_env)
    print(df_seq_timeline.sample())
    

if __name__ == '__main__':
    main()