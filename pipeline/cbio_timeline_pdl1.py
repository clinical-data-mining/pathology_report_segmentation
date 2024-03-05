""""
cbio_timeline_pdl1.py



"""
#Import the requisite library
import sys
sys.path.insert(0, '/mind_data/cdm_repos/cdm-utilities/')
sys.path.insert(0, '/mind_data/cdm_repos/cdm-utilities/minio_api')
import re

from data_classes_cdm import CDMProcessingVariables as config_cdm
import pandas as pd
from minio_api import MinioAPI
from utils import set_debug_console, mrn_zero_pad, print_df_without_index, convert_to_int


fname_pdl1 = config_cdm.fname_path_pdl1
fname_timeline_pdl1 = config_cdm.fname_path_pdl1_cbio_timeline
fname_minio_env = config_cdm.minio_env
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
    
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    obj = obj_minio.load_obj(path_object=fname_pdl1)
    df_pdl1 = pd.read_csv(obj, sep='\t')

    df_pdl1 = df_pdl1.rename(columns={'DTE_PATH_PROCEDURE': 'START_DATE'})
    df_pdl1 = df_pdl1.assign(STOP_DATE='')
    df_pdl1 = df_pdl1.assign(EVENT_TYPE='PATHOLOGY')
    df_pdl1 = df_pdl1.assign(SUBTYPE='PD-L1 Positive')
    df_pdl1 = df_pdl1.assign(SOURCE='CDM')
    
    # TODO add color for positive and negative
    
    df_pdl1 = df_pdl1[df_pdl1['PDL1_POSITIVE'].notnull() & (df_pdl1['PDL1_POSITIVE'] != '')].copy()
    df_pdl1 = df_pdl1[_col_order_pdl1]
    
    # Save to MinIO
    print('Saving %s' % fname_timeline_pdl1)
    obj_minio.save_obj(
        df=df_pdl1, 
        path_object=fname_timeline_pdl1, 
        sep='\t'
    )


if __name__ == '__main__':
    main()
