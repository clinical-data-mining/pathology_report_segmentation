import argparse
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations import PathologyExtractDOP

## Constants
col_label_access_num = 'ACCESSION_NUMBER'
col_label_spec_num = 'SPECIMEN_NUMBER'
col_spec_sub = 'SPECIMEN_SUBMITTED'


def main():
    parser = argparse.ArgumentParser(description="pipeline_dop_extraction.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    # Extract DOP
    obj_p = PathologyExtractDOP(
        fname_minio_env=args.minio_env,
        fname=c_dar.fname_darwin_path_col_spec_sub,
        col_label_access_num=col_label_access_num,
        col_label_spec_num=col_label_spec_num,
        col_spec_sub=col_spec_sub,
        list_accession=None,
        fname_save=c_dar.fname_spec_part_dop
    )

    df = obj_p.return_df()

    tmp = 0

if __name__ == '__main__':
    main()