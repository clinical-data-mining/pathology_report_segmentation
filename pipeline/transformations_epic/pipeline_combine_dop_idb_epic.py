import pandas as pd

from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
from msk_cdm.minio import MinioAPI

fname_minio = config_cdm.minio_env
FNAME_DOP_SAVE = 'epic_ddp_concat/pathology/pathology_spec_part_dop_combined.tsv'
FNAME_DOP_EPIC = 'epic_ddp_concat/pathology/pathology_spec_part_dop.tsv'
f_dop_idb = 'pathology/pathology_spec_part_dop.tsv'

def combine_idb_epic_dop(
        fname_minio_env,
        fname_epic,
        fname_idb,
        fname_save
    ):

    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    print(f"Loading {fname_idb}")
    obj = obj_minio.load_obj(fname_idb)
    df_dop_idb = pd.read_csv(obj, sep='\t')

    print(f"Loading {fname_epic}")
    obj = obj_minio.load_obj(fname_epic)
    df_f = pd.read_csv(obj, sep='\t')

    df_f_bfilled = df_f.merge(right=df_dop_idb, how='left', left_on='PDRX_ACCESSION_NO', right_on='ACCESSION_NUMBER')
    df_f_bfilled['DATE_OF_PROCEDURE_SURGICAL'] = df_f_bfilled['DATE_OF_PROCEDURE_SURGICAL_y'].fillna(df_f_bfilled['DATE_OF_PROCEDURE_SURGICAL_x'])
    df_f_bfilled["DATE_OF_PROCEDURE_SURGICAL"] = pd.to_datetime(df_f_bfilled["DATE_OF_PROCEDURE_SURGICAL"]).dt.date
    df_f_bfilled_clean = df_f_bfilled[['PDRX_ACCESSION_NO', 'SPECIMEN_NUMBER', 'DATE_OF_PROCEDURE_SURGICAL', 'DOP_DATE_ERROR_x']].copy()
    df_f_bfilled_clean = df_f_bfilled_clean.rename(columns={'PDRX_ACCESSION_NO': 'ACCESSION_NUMBER', 'DOP_DATE_ERROR_x': 'DOP_DATE_ERROR'})
    df_f_bfilled_clean['SPECIMEN_NUMBER'] = df_f_bfilled_clean['SPECIMEN_NUMBER'].fillna(1)

    print(f"Saving {fname_save}")
    obj_minio.save_obj(df=df_f_bfilled_clean, path_object=fname_save, sep='\t')

    print("Saved!")

def main():
    # Extract DOP
    obj_p = combine_idb_epic_dop(
        fname_minio_env=fname_minio,
        fname_epic=FNAME_DOP_EPIC,
        fname_idb=f_dop_idb,
        fname_save=FNAME_DOP_SAVE
    )

    df = obj_p.return_df()

    print(df.head())

    tmp = 0

if __name__ == '__main__':
    main()