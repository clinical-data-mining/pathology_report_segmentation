import argparse
from pathology_report_segmentation.annotations_epic import PathologyImpactDOPAnnoEpic


FNAME_DOP_SUMMARY_EPIC = 'epic_ddp_concat/pathology/pathology_dop_impact_summary_epic_idb_combined.tsv'
fname_idb_prior = 'pathology/table_pathology_impact_sample_summary_dop_anno.tsv'
fname_procedures = 'epic_ddp_concat/surgery/t06_epic_ddp_surg_procedures.tsv'
fname_save_summary_anno = 'epic_ddp_concat/pathology/table_pathology_impact_sample_summary_dop_anno_epic_idb_combined.tsv'


def main():
    parser = argparse.ArgumentParser(description="pipeline_impact_annotation_combiner_idb_epic_combined.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()

    fname_minio_env = args.minio_env

    # Hardcoded input/output paths
    config = {
        "fname_summary": FNAME_DOP_SUMMARY_EPIC,
        "fname_prior_anno": fname_idb_prior,
        "fname_procedures": fname_procedures,
        "fname_save": fname_save_summary_anno
    }

    estimator = PathologyImpactDOPAnnoEpic(
        fname_minio_env=fname_minio_env,
        config=config
    )
    df = estimator.process()
    print(f"Surgical date estimation complete. Output shape: {df.shape}")

if __name__ == "__main__":
    main()
