""""
cbio_timeline_mmr.py



"""
#Import the requisite library
import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm


fname_mmr = config_cdm.fname_path_mmr
fname_timeline_mmr = "pathology/table_timeline_mmr_calls.tsv"
fname_minio_env = config_cdm.minio_env
_col_order_mmr = [
    'MRN',
    'START_DATE',
    'STOP_DATE',
    'EVENT_TYPE',
    'SUBTYPE',
    'SOURCE',
    'MMR_ABSENT'
]


def main():

    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    obj = obj_minio.load_obj(path_object=fname_mmr)
    df_mmr = pd.read_csv(obj, sep='\t')

    df_mmr = df_mmr.rename(columns={'Path Procedure Date': 'START_DATE'})
    df_mmr = df_mmr.assign(STOP_DATE='')
    df_mmr = df_mmr.assign(EVENT_TYPE='PATHOLOGY')
    df_mmr = df_mmr.assign(SUBTYPE='MMR Status')
    df_mmr = df_mmr.assign(SOURCE='CDM')

    # TODO add color for positive and negative

    df_mmr = df_mmr[df_mmr['MMR_ABSENT'].notnull() & (df_mmr['MMR_ABSENT'] != '')].copy()
    df_mmr = df_mmr[_col_order_mmr]

    # Save to MinIO
    print('Saving %s' % fname_timeline_mmr)
    obj_minio.save_obj(
        df=df_mmr,
        path_object=fname_timeline_mmr,
        sep='\t'
    )


if __name__ == '__main__':
    main()
