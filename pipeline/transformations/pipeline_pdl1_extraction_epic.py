#!/usr/bin/env python3
"""
Extract PD-L1 biomarker annotations from Epic pathology reports.
Uses NLP patterns to identify and extract PD-L1 expression levels.
"""
import argparse
import sys
import os

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_external_source_table, get_output_table_config
from databricks_io import DatabricksIO

from annotations import PathologyExtractPDL1Epic

# Constants
COL_TEXT = 'path_prpt_p1'
COL_ACCESSION = 'ACCESSION_NUMBER'


def main():
    parser = argparse.ArgumentParser(
        description="Extract PD-L1 annotations from Epic pathology reports"
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
    config = load_config(args.config_yaml)

    # Get input table from config
    source_table = get_external_source_table(config, 'pathology_reports')

    # Get output table config
    output_config = get_output_table_config(config, 'step1_extraction', 'pathology_pdl1_calls_epic')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Load pathology reports
    print(f"Loading pathology reports from {source_table}")
    df_pathology_reports_epic = db_io.read_table(source_table)

    # Get impact mapping table from config
    impact_mapping_config = get_output_table_config(config, 'step2_combining', 'table_pathology_impact_sample_summary_dop_anno_epic_idb_combined')
    impact_mapping_table = impact_mapping_config.fully_qualified_table

    print(f"Loading impact mapping from {impact_mapping_table}")
    df_impact_mapping = db_io.read_table(impact_mapping_table)

    print("Extracting PD-L1 from reports")
    # Extract PD-L1
    obj_p = PathologyExtractPDL1Epic(
        df_pathology_reports=df_pathology_reports_epic,
        df_impact_mapping=df_impact_mapping,
        col_text=COL_TEXT,
        col_accession=COL_ACCESSION
    )

    df_pdl1 = obj_p.return_extraction()
    print("Sample of extracted PD-L1 annotations:")
    print(df_pdl1.sample())

    # Save to both volume file and create table
    print(f"Saving {len(df_pdl1):,} PD-L1 annotations to {output_config.volume_path}")
    print(f"Creating table {output_config.fully_qualified_table}")
    db_io.write_table(df=df_pdl1, table_config=output_config)


if __name__ == '__main__':
    main()