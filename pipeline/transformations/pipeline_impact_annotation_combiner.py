import argparse
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations import PathologyImpactDOPAnno


def main():
    parser = argparse.ArgumentParser(description="pipeline_impact_annotation_combiner.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    objd = PathologyImpactDOPAnno(
        fname_minio_env=args.minio_env,
        fname_path_summary=c_dar.fname_combine_dop_accession,
        fname_surgery=c_dar.fname_surg,
        fname_ir=c_dar.fname_ir,
        fname_save = c_dar.fname_dop_anno
    )
    df_out = objd.return_summary()

    tmp = 0

if __name__ == '__main__':
    main()