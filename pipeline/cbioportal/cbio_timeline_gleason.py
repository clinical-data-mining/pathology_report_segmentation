""""
cbio_timeline_gleason.py

"""
#Import the requisite library
import argparse
import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
from msk_cdm.data_processing import convert_to_int


fname_gleason = config_cdm.fname_path_gleason
fname_timeline_gleason = config_cdm.fname_path_gleason_cbio_timeline
_col_order_gleason = [
    'MRN', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'SUBTYPE', 
    'SOURCE', 
    'GLEASON_SCORE'
]

    
def main():
    parser = argparse.ArgumentParser(description="cbio_timeline_gleason.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()
    
    obj_minio = MinioAPI(fname_minio_env=args.minio_env)

    obj = obj_minio.load_obj(path_object=fname_gleason)
    df_gleason = pd.read_csv(obj, sep='\t')
    df_gleason = convert_to_int(df=df_gleason, list_cols=['Gleason'])
    df_gleason = df_gleason.drop(columns=['Accession Number'])

    df_gleason = df_gleason.rename(columns={'Path Procedure Date': 'START_DATE', 'Gleason':'GLEASON_SCORE'})
    df_gleason = df_gleason.assign(STOP_DATE='')
    df_gleason = df_gleason.assign(EVENT_TYPE='PATHOLOGY')
    df_gleason = df_gleason.assign(SUBTYPE='Gleason Score')
    df_gleason = df_gleason.assign(SOURCE='Pathology Reports (NLP)')
    
    df_gleason = df_gleason[df_gleason['GLEASON_SCORE'].notnull() & (df_gleason['GLEASON_SCORE'] != '')].copy()
    df_gleason = df_gleason[_col_order_gleason].copy()
    
    # Save to MinIO
    print('Saving %s' % fname_timeline_gleason)
    obj_minio.save_obj(
        df=df_gleason, 
        path_object=fname_timeline_gleason, 
        sep='\t'
    )


if __name__ == '__main__':
    main()
