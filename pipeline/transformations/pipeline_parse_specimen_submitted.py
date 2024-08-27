from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.segmentation import PathologyParseSpecSubmitted


def main():

    obj_mol = PathologyParseSpecSubmitted(
        fname_minio_env=c_dar.minio_env,
        fname_path_parsed=c_dar.fname_path_clean,
        col_spec_sub='SPECIMEN_SUBMISSION_LIST',
        list_cols_id=['MRN', 'ACCESSION_NUMBER'],
        fname_save=c_dar.fname_darwin_path_col_spec_sub
    )

    df_m = obj_mol.return_df()

    tmp = 0

if __name__ == '__main__':
    main()
