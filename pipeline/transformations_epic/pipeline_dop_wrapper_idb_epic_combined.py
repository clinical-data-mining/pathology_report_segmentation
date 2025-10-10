import argparse
from pathology_report_segmentation.annotations_epic import CombineAccessionDOPImpactEpic


def main():
    parser = argparse.ArgumentParser(description="pipeline_dop_wrapper_idb_epic_combined.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    fname_minio_env = args.minio_env
    fname_dbx_env = args.databricks_env

    # Static input/output paths
    config = {
        "fname_accession": "epic_ddp_concat/pathology/path_accessions.tsv",
        "fname_dop": "epic_ddp_concat/pathology/pathology_spec_part_dop.tsv",
        "fname_idb": "pathology/pathology_dop_impact_summary.tsv",
        "fname_map": "epic_ddp_concat/id-mapping/epic_ddp_id_mapping_pathology.tsv",
        "table_surg": "cdsi_prod.cdm_epic_impact_pipeline_prod.t14_epic_impact_pathology_reports",
        "table_mole": "cdsi_prod.cdm_epic_impact_pipeline_prod.t14_1_epic_impact_pathology_molecular_reports",
        "fname_save": "epic_ddp_concat/pathology/pathology_dop_impact_summary_epic_idb_combined.tsv",
    }

    processor = CombineAccessionDOPImpactEpic(
        fname_minio_env=fname_minio_env,
        fname_dbx_env=fname_dbx_env,
        config=config
    )

    df_result = processor.process()
    print(f"Processing complete. Output shape: {df_result.shape}")
    print(df_result.notnull().sum()/df_result.shape[0])


if __name__ == "__main__":
    main()
