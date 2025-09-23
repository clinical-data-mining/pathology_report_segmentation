import argparse
import numpy as np
import pandas as pd

from msk_cdm.data_processing import convert_to_int
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from msk_cdm.minio import MinioAPI
from pathology_report_segmentation.annotations import extractGleason

## Constants
FNAME_SAVE = c_dar.fname_path_gleason
FNAME_PATH = c_dar.fname_pathology
COLS_SAVE = ['MRN','Accession Number','Path Procedure Date','Gleason']

def main():
    parser = argparse.ArgumentParser(description="pipeline_gleason_extraction.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    print('Loading %s' % FNAME_PATH)
    obj_minio = MinioAPI(fname_minio_env=args.minio_env)
    obj = obj_minio.load_obj(path_object=FNAME_PATH)
    df_path = pd.read_csv(obj, sep='\t', low_memory=False)

    filter_gleason = df_path['PATH_REPORT_NOTE'].fillna('').str.contains('Gleason',case=False)
    df_path_gleason = df_path[filter_gleason].copy()
    print('Abstracting Gleason scores')
    df_path_gleason['Gleason'] = df_path_gleason['PATH_REPORT_NOTE'].apply(extractGleason)
    df_path_gleason = df_path_gleason.rename(
        columns={
            'ACCESSION_NUMBER': 'Accession Number',
            'DTE_PATH_PROCEDURE':'Path Procedure Date'
        }
    )
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

