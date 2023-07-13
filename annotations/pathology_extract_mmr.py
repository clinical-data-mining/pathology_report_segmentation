import os
import sys

import pandas as pd
import numpy as np
from pandas import Series
import re

sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
from minio_api import MinioAPI
from utils import read_minio_api_config
from data_classes_cdm import CDMProcessingVariables as c_dar


## Constants
FNAME_SAVE = c_dar.fname_path_mmr
FNAME_PATH = c_dar.fname_pathology
FNAME_MINIO_ENV = c_dar.minio_env
COLS_SAVE = ['MRN','Accession Number','Path Procedure Date','MMR_ABSENT']

obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
obj = obj_minio.load_obj(path_object=FNAME_PATH)
df_path = pd.read_csv(obj, sep='\t', low_memory=False)
'''
     MLH1:     staining absent in tumor
     PMS2:     staining absent in tumor
     MSH2:     staining present in tumor
     MSH6:     staining present in tumor'''


def extractMMR(s):
    factors = ['MLH1','PMS2','MSH2','MSH6']
    for f in factors:
        if f in s:
            statement = s.split(f)[1][:50]
            absentLoc = statement.lower().find('absent')
            presentLoc= statement.lower().find('present')
            if absentLoc>=0 and presentLoc>=0:
                if absentLoc<presentLoc:
                    return True
            if absentLoc>=0 and presentLoc<0:
                    return True
    return False

filter_mmr = df_path['path_prpt_p1'].fillna('').str.contains('MLH1|PMS2|MSH2|MSH6',regex=True,case=False)
filter_mnumber = ~df_path['Accession Number'].str.contains('M') 
df_mmr = df_path[filter_mmr & filter_mnumber].copy()
df_mmr['MMR_ABSENT'] = df_mmr['path_prpt_p1'].apply(extractMMR)
df_save = df_mmr[COLS_SAVE]

# Save to Minio
obj_minio.save_obj(
    df=df_save, 
    path_object=FNAME_SAVE, 
    sep='\t'
)

# df_mmr[['MRN','Accession Number','Path Procedure Date','MMR_ABSENT']].to_csv('MMR.csv',index=False)

# bytedata=df_mmr[COLS_SAVE].to_csv(index=False).encode('utf-8')
# bufferdata=BytesIO(bytedata)
# client.put_object('cdm-data', 'pathology/pathology_mmr_calls.csv',
#                   data=bufferdata,
#                   length=len(bytedata),content_type='application/csv')
