import argparse
from msk_cdm.minio import MinioAPI
from pathology_report_segmentation.annotations_epic import PathologyExtractAccessionEpic

## Constants
col_label_1 = 'SAMPLE_ID'
col_label_2 = 'PDRX_ACCESSION_NO'
col_spec_sub = 'PART_DESCRIPTION'

fname_path = 'epic_ddp_concat/id-mapping/epic_ddp_id_mapping_pathology.tsv'
FNAME_ACCESSION_NUMBER_SAVE = 'epic_ddp_concat/pathology/path_accessions.tsv'


def main():
    parser = argparse.ArgumentParser(description="pipeline_extract_accession.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    # Extract DOP
    obj_path = PathologyExtractAccessionEpic(
        fname_minio_env=args.minio_env,
        fname=fname_path,
        list_col_index=[col_label_1, col_label_2],
        col_spec_sub=col_spec_sub
    )

    df_dop = obj_path.return_df()
    df_orig = obj_path.return_df_original()
    df_f = df_orig.merge(right=df_dop, how='left', on=['SAMPLE_ID', 'PDRX_ACCESSION_NO'])
    df_f = df_f.drop(columns=['DMP_ID'])

    df_f = df_f.rename(
        columns={
            'SOURCE_ACCESSION_NUMBER': 'SOURCE_ACCESSION_NUMBER_0',
            'SPECIMEN_NUMBER': 'SOURCE_SPEC_NUM_0'
        })

    # Save data
    obj_minio = MinioAPI(fname_minio_env=args.minio_env)

    print(f"Saving {FNAME_ACCESSION_NUMBER_SAVE}")
    obj_minio.save_obj(df=df_f, path_object=FNAME_ACCESSION_NUMBER_SAVE, sep='\t')
    print(df_f.head())

    tmp = 0

if __name__ == '__main__':
    main()