""""
cbio_timeline_sequencing.py

Generates cBioPortal timeline files for sequencing dates
"""
import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'utils')))
import pandas as pd
import numpy as np
from minio_api import MinioAPI
from utils import drop_cols, mrn_zero_pad, convert_to_int, read_minio_api_config, save_appended_df
from data_classes_cdm import CDMProcessingVariables as config_cdm
from data_classes_cdm import CDMProcessingVariablesCbioportal as config_cbio_etl


FNAME_MINIO_ENV = config_cdm.minio_env
FNAME_PATHOLOGY = config_cdm.fname_path_clean
FNAME_SAVE_TIMELINE_SEQ = config_cdm.fname_path_sequencing_cbio_timeline
COLS_PATHOLOGY = ['DTE_PATH_PROCEDURE', 'MRN', 'SAMPLE_ID']
COL_ORDER_SEQ = [
    'MRN', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'SUBTYPE',
    'SAMPLE_ID'
]


def sequencing_timeline():
    obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
    
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
    df_path['DTE_PATH_PROCEDURE'] = pd.to_datetime(
        df_path['DTE_PATH_PROCEDURE'],
        errors='coerce'
    )
    df_path_filt = df_path[df_path['SAMPLE_ID'].notnull() & df_path['SAMPLE_ID'].str.contains('T')]
    df_path_filt = df_path_filt.rename(columns={'DTE_PATH_PROCEDURE': 'START_DATE'})
    
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

    df_seq_timeline = sequencing_timeline()
    print(df_seq_timeline.sample())
    

if __name__ == '__main__':
    main()
    
    
