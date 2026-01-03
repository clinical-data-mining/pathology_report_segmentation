#!/usr/bin/env python3
"""
Extract MMR (Mismatch Repair) biomarker annotations from Epic pathology reports.
Uses NLP patterns to identify deficient MMR proteins (MLH1, PMS2, MSH2, MSH6).
"""
import argparse
import sys
import os

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_external_source_table, get_output_table_config
from databricks_io import DatabricksIO

from annotations import _extractMMR_from_str

# Constants
COL_TEXT = 'path_prpt_p1'
COLS_SAVE = ['MRN', 'Accession Number', 'Path Procedure Date', 'MMR_ABSENT']


def extractMMR(df):
    """Extract MMR annotations from pathology reports."""
    filter_mmr = df[COL_TEXT].fillna('').str.contains('MLH1|PMS2|MSH2|MSH6', regex=True, case=False)
    filter_mnumber = ~df['ACCESSION_NUMBER'].str.contains('M')
    df_mmr = df[filter_mmr & filter_mnumber].copy()
    df_mmr['MMR_ABSENT'] = df_mmr[COL_TEXT].apply(_extractMMR_from_str)
    df_mmr = df_mmr.rename(
        columns={
            'ACCESSION_NUMBER': 'Accession Number',
            'DTE_PATH_PROCEDURE': 'Path Procedure Date'
        }
    )
    df_save = df_mmr[COLS_SAVE]

    return df_save


def main():
    parser = argparse.ArgumentParser(
        description="Extract MMR annotations from Epic pathology reports"
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
    output_config = get_output_table_config(config, 'step1_extraction', 'pathology_mmr_calls_epic')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Load pathology reports
    print(f"Loading pathology reports from {source_table}")
    df_path = db_io.read_table(source_table)

    print("Extracting MMR annotations")
    df_save = extractMMR(df=df_path)

    # Save to both volume file and create table
    print(f"Saving {len(df_save):,} MMR annotations to {output_config.volume_path}")
    print(f"Creating table {output_config.fully_qualified_table}")
    db_io.write_table(df=df_save, table_config=output_config)

    return 0


if __name__ == '__main__':
    main()