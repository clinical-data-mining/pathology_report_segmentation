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
    'MMR'
]


def main():

    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    obj = obj_minio.load_obj(path_object=fname_mmr)
    df_mmr = pd.read_csv(obj, sep='\t')

    cols_rename = {
        'Path Procedure Date': 'START_DATE',
        'MMR_ABSENT': 'MMR'
    }
    df_mmr = df_mmr.rename(columns=cols_rename)
    df_mmr = df_mmr.assign(STOP_DATE='')
    df_mmr = df_mmr.assign(EVENT_TYPE='PATHOLOGY')
    df_mmr = df_mmr.assign(SUBTYPE='MMR Deficiency')
    df_mmr = df_mmr.assign(SOURCE='Pathology Reports (NLP)')

    df_mmr = df_mmr.replace({'MMR': {True: 'deficient', False: 'proficient'}})

    # TODO add color for positive and negative

    df_mmr = df_mmr[df_mmr['MMR'].notnull() & (df_mmr['MMR'] != '')].copy()
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
