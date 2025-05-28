import argparse
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations import CombineAccessionDOPImpact


def main():
    parser = argparse.ArgumentParser(description="pipeline_dop_wrapper.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    # Extract source accession number
    obj_p = CombineAccessionDOPImpact(
        fname_minio_env=args.minio_env,
        fname_accession=c_dar.fname_path_accessions,
        fname_dop=c_dar.fname_spec_part_dop,
        fname_path=c_dar.fname_path_clean,
        fname_save=c_dar.fname_combine_dop_accession
    )

    df = obj_p.return_df()

    tmp = 0

if __name__ == '__main__':
    main()