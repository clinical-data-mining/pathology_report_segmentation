from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations_epic import PathologyExtractAccessionEpic

## Constants
col_label_1 = 'SAMPLE_ID'
col_label_2 = 'PDRX_ACCESSION_NO'
col_spec_sub = 'PART_DESCRIPTION'

fname_minio = c_dar.minio_env
fname_path = 'epic_ddp_concat/id-mapping/epic_ddp_id_mapping_pathology.tsv'
FNAME_ACCESSION_NUMBER_SAVE = 'epic_ddp_concat/pathology/path_accessions.tsv'


def main():
    # Extract DOP
    obj_path = PathologyExtractAccessionEpic(
        fname_minio_env=fname_minio,
        fname=fname_path,
        list_col_index=[col_label_1, col_label_2],
        col_spec_sub=col_spec_sub
    )

    df_dop = obj_path.return_df()

    print(df.head())

    tmp = 0

if __name__ == '__main__':
    main()