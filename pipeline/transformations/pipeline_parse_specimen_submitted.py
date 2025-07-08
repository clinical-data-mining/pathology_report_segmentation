import argparse
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.segmentation import PathologyParseSpecSubmitted


def main():
    parser = argparse.ArgumentParser(description="pipeline_parse_specimen_submitted.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    obj_mol = PathologyParseSpecSubmitted(
        fname_minio_env=args.minio_env,
        fname_path_parsed=c_dar.fname_path_clean,
        col_spec_sub='SPECIMEN_SUBMISSION_LIST',
        list_cols_id=['MRN', 'ACCESSION_NUMBER'],
        fname_save=c_dar.fname_darwin_path_col_spec_sub
    )

    df_m = obj_mol.return_df()

    tmp = 0

if __name__ == '__main__':
    main()
