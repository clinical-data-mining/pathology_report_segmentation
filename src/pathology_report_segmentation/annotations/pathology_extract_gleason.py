import pandas as pd
import numpy as np
import re

from msk_cdm.minio import MinioAPI
from msk_cdm.data_processing import convert_to_int
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar

## Constants
FNAME_SAVE = c_dar.fname_path_gleason
FNAME_PATH = c_dar.fname_pathology
FNAME_MINIO_ENV = c_dar.minio_env
COLS_SAVE = ['MRN','Accession Number','Path Procedure Date','Gleason']

print('Loading %s' % FNAME_PATH)
obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
obj = obj_minio.load_obj(path_object=FNAME_PATH)
df_path = pd.read_csv(obj, sep='\t', low_memory=False)


def extractGleason(s):
    splitstr = 'Gleason'
    if 'Gleason score' in s:
        splitstr = 'Gleason score'
    tokens = re.split(splitstr,s,flags=re.IGNORECASE)
    if len(tokens)>1:
        maxGleason = []
        for t in tokens[1:]:
            ppg = parsePostGleasonStr(t[:min(len(t),20)])
            if ppg==ppg:
                maxGleason+=[ppg]
        if len(maxGleason)>0:
            return max(maxGleason)
    return np.nan

def parsePostGleasonStr(s):
    if '+' in s:
        s = ''.join(s.split())
        tokens = s.split('+')
        if len(tokens)>1 and len(tokens[0])>0 and len(tokens[1])>0 and tokens[0][-1].isnumeric() and tokens[1][0].isnumeric():
            return int(tokens[0][-1])+int(tokens[1][0])
        #else:
        #    print(tokens[0])
        #    print(tokens[1])
    elif ':' in s:
        s = ''.join(s.split())
        tokens = s.split(':')
        if len(tokens)>0 and len(tokens[1])>0:
            if tokens[1][0].isnumeric():
                return int(tokens[1][0])
    return np.nan

filter_gleason = df_path['path_prpt_p1'].fillna('').str.contains('Gleason',case=False)
df_path_gleason = df_path[filter_gleason].copy()
print('Abstracting Gleason scores')
df_path_gleason['Gleason'] = df_path_gleason['path_prpt_p1'].apply(extractGleason)
df_save = df_path_gleason[COLS_SAVE]

# Do last cleaning -- Gleason scores should not be under 6. Convert 1's to 10 
# TODO: Fix regex to grab 10s
df_save.loc[df_save['Gleason'] == 1] = 10
df_save.loc[df_save['Gleason'] < 6] = np.NaN
df_save = df_save[df_save['Gleason'].notnull() & df_save['MRN'].notnull()]
df_save = convert_to_int(df=df_save, list_cols=['MRN', 'Gleason'])


print('Saving %s' % FNAME_SAVE)
obj_minio.save_obj(
    df=df_save, 
    path_object=FNAME_SAVE, 
    sep='\t'
)
# bytedata=.to_csv(index=False).encode('utf-8')
# bufferdata=BytesIO(bytedata)

# client.put_object('cdm-data', 'pathology/pathology_gleason_calls.tsv',
#                   data=bufferdata,
#                   length=len(bytedata),content_type='application/csv')
