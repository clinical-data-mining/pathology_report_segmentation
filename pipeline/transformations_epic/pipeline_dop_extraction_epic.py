from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations_epic import PathologyExtractDOPEpic

## Constants
col_label_1 = 'SAMPLE_ID'
col_label_2 = 'PDRX_ACCESSION_NO'
col_spec_sub = 'PART_DESCRIPTION'
fname_path = 'epic_ddp_concat/id-mapping/epic_ddp_id_mapping_pathology.tsv'


def main():
    # Extract DOP
    obj_p = PathologyExtractDOPEpic(
            fname_minio_env=c_dar.minio_env,
            fname=fname_path,
            list_col_index=[col_label_1, col_label_2],
            col_spec_sub=col_spec_sub
        )

    df = obj_p.return_df()

    print(df.head())

    tmp = 0

if __name__ == '__main__':
    main()