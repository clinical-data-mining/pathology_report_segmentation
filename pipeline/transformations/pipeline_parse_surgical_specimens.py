import argparse
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar

from pathology_report_segmentation.segmentation import ParseSurgicalPathologySpecimens


def main():
    parser = argparse.ArgumentParser(description="pipeline_parse_surgical_specimens.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    obj_parse = ParseSurgicalPathologySpecimens(
        fname_minio_env=args.minio_env,
        fname_darwin_pathology_parsed=c_dar.fname_darwin_path_surgical,
        fname_save=c_dar.fname_darwin_path_clean_parsed_specimen
    )

    df_surg_path_parsed_spec = obj_parse.return_df()

if __name__ == '__main__':
    main()


