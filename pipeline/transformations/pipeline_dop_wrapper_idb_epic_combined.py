#!/usr/bin/env python3
"""
Combine EPIC and IDB pathology DOP (Date of Procedure) summary data.
Uses the CombineAccessionDOPImpactEpic annotation class to merge data from multiple sources.
"""
import argparse
import sys
import os

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_external_source_table, get_output_table_config
from databricks_io import DatabricksIO

from annotations import CombineAccessionDOPImpactEpic


def main():
    parser = argparse.ArgumentParser(
        description="Combine EPIC and IDB pathology DOP summary data"
    )
    parser.add_argument(
        "--databricks_env",
        required=True,
        help="Path to Databricks environment file",
    )
    parser.add_argument(
        "--config_yaml",
        required=True,
        help="Path to YAML configuration file",
    )
    args = parser.parse_args()

    # Load configuration
    yaml_config = load_config(args.config_yaml)

    # Get table names from config
    table_surg = get_external_source_table(yaml_config, 'pathology_reports')
    table_mole = get_external_source_table(yaml_config, 'molecular_reports')

    # Get output table config
    output_config = get_output_table_config(
        yaml_config, 'step2_combining', 'pathology_dop_impact_summary_epic_idb_combined'
    )

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Script-specific configuration for annotation class
    script_config = {
        "table_surg": table_surg,
        "table_mole": table_mole,
        "output_table_config": output_config
    }

    # Initialize and run processor
    processor = CombineAccessionDOPImpactEpic(
        db_io=db_io,
        config=script_config,
        yaml_config=yaml_config
    )

    df_result = processor.process()
    print(f"Processing complete. Output shape: {df_result.shape}")
    print("Column completeness:")
    print(df_result.notnull().sum() / df_result.shape[0])


if __name__ == "__main__":
    main()
