import argparse
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.segmentation import InitCleanPathology

def main():
    parser = argparse.ArgumentParser(description="pipeline_clean_pathology.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    obj_path = InitCleanPathology(
        fname_minio_env=args.minio_env,
        fname=c_dar.fname_pathology,
        fname_save=c_dar.fname_path_clean
    )

    df = obj_path.return_df()
    tmp = 0

if __name__ == '__main__':
    main()