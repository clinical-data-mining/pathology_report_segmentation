import argparse
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations import create_id_mapping_pathology

def main():
    parser = argparse.ArgumentParser(description="pipeline_id_mapping.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    # Extract DOP
    df_mapping = create_id_mapping_pathology(
        fname_minio_env=args.minio_env,
        fname_path=c_dar.fname_path_clean,
        fname_out_mapping=c_dar.fname_id_map
    )

if __name__ == '__main__':
    main()