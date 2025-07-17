from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations_epic import PathologyExtractDOPEpic
from msk_cdm.minio import MinioAPI


## Constants
col_label_1 = 'SAMPLE_ID'
col_label_2 = 'PDRX_ACCESSION_NO'
col_spec_sub = 'PART_DESCRIPTION'
fname_path = 'epic_ddp_concat/id-mapping/epic_ddp_id_mapping_pathology.tsv'
FNAME_DOP_SAVE = 'epic_ddp_concat/pathology/pathology_spec_part_dop.tsv'


def main():
    # Extract DOP
    obj_p = PathologyExtractDOPEpic(
            fname_minio_env=c_dar.minio_env,
            fname=fname_path,
            list_col_index=[col_label_1, col_label_2],
            col_spec_sub=col_spec_sub
        )

    df_dop = obj_p.return_df()
    df_orig = obj_p.return_df_original()

    df_f = df_orig.merge(right=df_dop, how='left', on=[col_label_1, col_label_2, col_spec_sub])
    df_f = df_f.drop(columns=['DMP_ID'])
    df_f = df_f.rename(columns={'PDRX_ACCESSION_NO': 'ACCESSION_NUMBER'})

    print(f"Saving {FNAME_DOP_SAVE}")
    obj_minio = MinioAPI(fname_minio_env=c_dar.minio_env)
    obj_minio.save_obj(df=df_f, path_object=FNAME_DOP_SAVE, sep='\t')

    print("Saved!")

    print(df_f.head())

    tmp = 0

if __name__ == '__main__':
    main()