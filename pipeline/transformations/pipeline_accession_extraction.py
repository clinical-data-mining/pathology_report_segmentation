from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations import PathologyExtractAccession


## Constants
col_label_access_num = 'ACCESSION_NUMBER'
col_label_spec_num = 'SPECIMEN_NUMBER'
col_spec_sub = 'SPECIMEN_SUBMITTED'


def main():

    # Extract source accession number
    obj_p = PathologyExtractAccession(
        fname_minio_env=c_dar.minio_env,
        fname=c_dar.fname_darwin_path_col_spec_sub,
        col_label_access_num=col_label_access_num,
        col_label_spec_num=col_label_spec_num,
        col_spec_sub=col_spec_sub,
        fname_out=c_dar.fname_path_accessions
    )

    df = obj_p.return_df()

    tmp = 0

if __name__ == '__main__':
    main()