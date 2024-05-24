#Import the requisite library
import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as var


FNAME_MMR = 'pathology/table_timeline_mmr_calls.tsv'
FNAME_SAVE_PATIENT = 'pathology/table_summary_mmr_patient.tsv'
FNAME_MINIO_ENV = var.minio_env


def _load_data(
        obj_minio,
        fname_mmr
):
    print('Loading %s' % fname_mmr)
    obj = obj_minio.load_obj(path_object=fname_mmr)
    df_mmr = pd.read_csv(obj, sep='\t')
    df_mmr['START_DATE'] = pd.to_datetime(df_mmr['START_DATE'], errors='coerce')

    return df_mmr


def _clean_data_patient(df_mmr):
    df_mmr = df_mmr.sort_values(by=['MRN', 'START_DATE'])
    reps = {True:'Yes', False:'No'}
    list_mrns_mmr = df_mmr.loc[df_mmr['MMR_ABSENT'] == True, 'MRN']
    df_mmr_summary = df_mmr[['MRN']].drop_duplicates()
    df_mmr_summary['HISTORY_OF_D_MMR'] = df_mmr_summary['MRN'].isin(list_mrns_mmr).replace(reps)

    return df_mmr_summary



def create_dmmr_summary(
        fname_minio_env,
        fname_mmr,
        fname_save_patient
):
    # Create minio object
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    # Load data
    df_mmr = _load_data(
        obj_minio=obj_minio,
        fname_mmr=fname_mmr
    )

    # Create summaries
    ## Patient summary
    df_mmr_p = _clean_data_patient(df_mmr=df_mmr)

    # Save data
    ## Patient summary
    print('Saving %s' % fname_save_patient)
    obj_minio.save_obj(
        df=df_mmr_p,
        path_object=fname_save_patient,
        sep='\t'
    )

    return df_mmr_p

def main():
    fname_minio_env = FNAME_MINIO_ENV
    fname_mmr = FNAME_MMR
    fname_save_patient = FNAME_SAVE_PATIENT

    print('Creating dMMR Summaries')
    create_dmmr_summary(
        fname_minio_env=fname_minio_env,
        fname_mmr=fname_mmr,
        fname_save_patient=fname_save_patient
    )

if __name__ == '__main__':
    main()

