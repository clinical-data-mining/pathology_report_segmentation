import argparse
from annotations import PathologyImpactDOPAnnoEpic


FNAME_DOP_SUMMARY_EPIC = 'epic_ddp_concat/pathology/pathology_dop_impact_summary_epic_idb_combined.tsv'
fname_idb_prior = 'pathology/table_pathology_impact_sample_summary_dop_anno.tsv'
fname_procedures = 'epic_ddp_concat/surgery/t06_epic_ddp_surg_procedures.tsv'
fname_save_summary_anno = 'epic_ddp_concat/pathology/table_pathology_impact_sample_summary_dop_anno_epic_idb_combined.tsv'

# Table configuration (dummy variables for now)
TABLE_DOP_SUMMARY = 'cdsi_prod.cdm_epic_impact_pipeline_prod.pathology_dop_impact_summary_epic_idb_combined'
OUTPUT_TABLE_CATALOG = 'cdsi_prod'
OUTPUT_TABLE_SCHEMA = 'cdm_epic_impact_pipeline_prod'
OUTPUT_TABLE_NAME = 'table_pathology_impact_sample_summary_dop_anno_epic_idb_combined'


def main():
    parser = argparse.ArgumentParser(description="pipeline_impact_annotation_combiner_idb_epic_combined.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    fname_databricks_env = args.databricks_env

    # Hardcoded input/output paths
    config = {
        "table_summary": TABLE_DOP_SUMMARY,  # Query from table created in previous step
        "fname_summary": FNAME_DOP_SUMMARY_EPIC,  # Fallback to file if table not available
        "fname_prior_anno": fname_idb_prior,
        "fname_procedures": fname_procedures,
        "fname_save": fname_save_summary_anno,
        "output_table_config": {
            'catalog': OUTPUT_TABLE_CATALOG,
            'schema': OUTPUT_TABLE_SCHEMA,
            'table': OUTPUT_TABLE_NAME
        }
    }

    estimator = PathologyImpactDOPAnnoEpic(
        fname_databricks_env=fname_databricks_env,
        config=config
    )
    df = estimator.process()
    print(f"Surgical date estimation complete. Output shape: {df.shape}")

if __name__ == "__main__":
    main()
