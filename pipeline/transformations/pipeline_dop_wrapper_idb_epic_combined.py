import argparse
from annotations import CombineAccessionDOPImpactEpic


# Table configuration (dummy variables for now)
OUTPUT_TABLE_CATALOG = 'cdsi_prod'
OUTPUT_TABLE_SCHEMA = 'cdm_epic_impact_pipeline_prod'
OUTPUT_TABLE_NAME = 'pathology_dop_impact_summary_epic_idb_combined'


def main():
    parser = argparse.ArgumentParser(description="pipeline_dop_wrapper_idb_epic_combined.py")
    parser.add_argument(
        "--databricks_env",
        dest="databricks_env",
        required=True,
        help="location of Databricks environment file",
    )
    args = parser.parse_args()

    fname_dbx_env = args.databricks_env

    # Static input/output paths
    # Step 1 outputs are now tables (queried directly), legacy data are files
    config = {
        "table_accession": "cdsi_prod.cdm_epic_impact_pipeline_prod.path_accessions",
        "table_dop": "cdsi_prod.cdm_epic_impact_pipeline_prod.pathology_spec_part_dop",
        "fname_idb": "pathology/pathology_dop_impact_summary.tsv",  # Legacy file
        "fname_map": "epic_ddp_concat/id-mapping/epic_ddp_id_mapping_pathology.tsv",
        "table_surg": "cdsi_prod.cdm_epic_impact_pipeline_prod.t14_epic_impact_pathology_reports",
        "table_mole": "cdsi_prod.cdm_epic_impact_pipeline_prod.t14_1_epic_impact_pathology_molecular_reports",
        "fname_save": "epic_ddp_concat/pathology/pathology_dop_impact_summary_epic_idb_combined.tsv",
        "output_table_config": {
            'catalog': OUTPUT_TABLE_CATALOG,
            'schema': OUTPUT_TABLE_SCHEMA,
            'table': OUTPUT_TABLE_NAME
        }
    }

    processor = CombineAccessionDOPImpactEpic(
        fname_databricks_env=fname_dbx_env,
        config=config
    )

    df_result = processor.process()
    print(f"Processing complete. Output shape: {df_result.shape}")
    print(df_result.notnull().sum()/df_result.shape[0])


if __name__ == "__main__":
    main()
