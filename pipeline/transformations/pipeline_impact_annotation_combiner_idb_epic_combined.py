#!/usr/bin/env python3
"""
Annotate and backfill surgical procedure dates for MSK-IMPACT sequenced samples.
Uses the PathologyImpactDOPAnnoEpic annotation class to estimate dates by matching
Epic surgical procedures with pathology report dates.
"""
import argparse
import sys
import os

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_output_table_config
from databricks_io import DatabricksIO

from annotations import PathologyImpactDOPAnnoEpic


def main():
    parser = argparse.ArgumentParser(
        description="Annotate surgical dates for IMPACT pathology samples"
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

    # Get output table config
    output_config = get_output_table_config(
        yaml_config,
        'step2_combining',
        'table_pathology_impact_sample_summary_dop_anno_epic_idb_combined'
    )

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Script-specific configuration for annotation class
    script_config = {
        "output_table_config": output_config
    }

    # Initialize and run estimator
    estimator = PathologyImpactDOPAnnoEpic(
        db_io=db_io,
        config=script_config,
        yaml_config=yaml_config
    )

    df = estimator.process()
    print(f"Surgical date estimation complete. Output shape: {df.shape}")


if __name__ == "__main__":
    main()
