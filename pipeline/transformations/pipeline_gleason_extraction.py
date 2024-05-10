import numpy as np
import pandas as pd

from msk_cdm.data_processing import convert_to_int
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from msk_cdm.minio import MinioAPI
from pathology_report_segmentation.annotations import extractGleason

## Constants
FNAME_SAVE = c_dar.fname_path_gleason
FNAME_PATH = c_dar.fname_pathology
FNAME_MINIO_ENV = c_dar.minio_env
COLS_SAVE = ['MRN','Accession Number','Path Procedure Date','Gleason']

def main():

    print('Loading %s' % FNAME_PATH)
    obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
    obj = obj_minio.load_obj(path_object=FNAME_PATH)
    df_path = pd.read_csv(obj, sep='\t', low_memory=False)

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

if __name__ == '__main__':
    main()

