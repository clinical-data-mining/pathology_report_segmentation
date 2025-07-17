import pandas as pd

from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
from msk_cdm.minio import MinioAPI
from msk_cdm.data_processing import mrn_zero_pad

fname_minio = config_cdm.minio_env
FNAME_ACCESSION_NUMBER_SAVE = 'epic_ddp_concat/pathology/path_accessions.tsv'
FNAME_ACCESSION_NUMBER_COMBINED_SAVE = 'epic_ddp_concat/pathology/path_accessions_idb_epic_combined.tsv'
f_accessions_idb = 'pathology/path_accessions.tsv'
list_cols_keep = [
    'MRN',
    'ACCESSION_NUMBER',
    'SPECIMEN_NUMBER',
    'SOURCE_ACCESSION_NUMBER_0',
    'SOURCE_SPEC_NUM_0',
    'SOURCE_ACCESSION_NUMBER_0b',
    'SOURCE_SPEC_NUM_0b'
]


def combine_idb_epic_accession(
        fname_minio_env,
        fname_epic,
        fname_idb,
        fname_save
    ):

    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    print(f"Loading {fname_idb}")
    obj = obj_minio.load_obj(fname_idb)
    df_idb = pd.read_csv(obj, sep='\t')
    print(df_idb.sample())

    print(f"Loading {fname_epic}")
    obj = obj_minio.load_obj(fname_epic)
    df_f = pd.read_csv(obj, sep='\t')
    print(df_f.sample())

    df_f = mrn_zero_pad(df=df_f, col_mrn='MRN')
    df_idb = mrn_zero_pad(df=df_idb, col_mrn='MRN')

    df_f_bfilled = df_f.merge(
        right=df_idb,
        how='left',
        left_on=['MRN', 'PDRX_ACCESSION_NO'],
        right_on=['MRN', 'ACCESSION_NUMBER']
    )
    print(df_f_bfilled.sample())

    # backfill idb data
    df_f_bfilled = df_f_bfilled.drop(columns=['ACCESSION_NUMBER'])
    df_f_bfilled['SOURCE_ACCESSION_NUMBER_0'] = df_f_bfilled['SOURCE_ACCESSION_NUMBER_0_y'].fillna(
        df_f_bfilled['SOURCE_ACCESSION_NUMBER_0_x'])
    df_f_bfilled['SOURCE_SPEC_NUM_0'] = df_f_bfilled['SOURCE_SPEC_NUM_0_y'].fillna(df_f_bfilled['SOURCE_SPEC_NUM_0_x'])
    df_f_bfilled = df_f_bfilled.rename(columns={'PDRX_ACCESSION_NO': 'ACCESSION_NUMBER'})
    df_f_bfilled_clean = df_f_bfilled[list_cols_keep].copy()

    df_f_bfilled_clean['SPECIMEN_NUMBER'] = df_f_bfilled_clean['SPECIMEN_NUMBER'].fillna(1)

    print(f"Saving {fname_save}")
    obj_minio.save_obj(df=df_f_bfilled_clean, path_object=fname_save, sep='\t')

    print("Saved!")

    return df_f_bfilled_clean

def main():
    # Extract DOP
    df_f_bfilled_clean = combine_idb_epic_accession(
        fname_minio_env=fname_minio,
        fname_epic=FNAME_ACCESSION_NUMBER_SAVE,
        fname_idb=f_accessions_idb,
        fname_save=FNAME_ACCESSION_NUMBER_COMBINED_SAVE
    )

    print(df_f_bfilled_clean.head())

    tmp = 0

if __name__ == '__main__':
    main()