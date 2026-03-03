#!/usr/bin/env python3
"""
Generate cBioPortal timeline files for Gleason score events.
"""
import argparse
import sys
import os
import pandas as pd

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_combined_table, get_output_table_config
from databricks_io import DatabricksIO

from msk_cdm.data_processing import convert_to_int


COL_GLEASON = 'GLEASON'
# Column order for cBioPortal timeline format
_col_order_gleason = [
    'MRN',
    'START_DATE',
    'STOP_DATE',
    'EVENT_TYPE',
    'SUBTYPE',
    'SOURCE',
    'ACCESSION_NUMBER',
    'GLEASON_SCORE'
]


def main():
    parser = argparse.ArgumentParser(
        description="Generate cBioPortal timeline for Gleason score events"
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
    source_table = get_combined_table(config, 'pathology_gleason_calls_epic_idb_combined')

    # Get output table config
    output_config = get_output_table_config(config, 'step3_cbioportal', 'timeline_gleason_scores')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Load data from table
    print(f'Loading {source_table}')
    df_gleason = db_io.read_table(source_table)
    df_gleason = convert_to_int(df=df_gleason, list_cols=[COL_GLEASON])
    df_gleason = df_gleason.drop(columns=['ACCESSION_NUMBER'])

    # Transform to timeline format
    df_gleason = df_gleason.rename(columns={'DTE_PATH_PROCEDURE': 'START_DATE', COL_GLEASON: 'GLEASON_SCORE'})
    df_gleason = df_gleason.assign(STOP_DATE='')
    df_gleason = df_gleason.assign(EVENT_TYPE='PATHOLOGY')
    df_gleason = df_gleason.assign(SUBTYPE='Gleason Score')
    df_gleason = df_gleason.assign(SOURCE='Pathology Reports (NLP)')

    df_gleason = df_gleason[df_gleason['GLEASON_SCORE'].notnull() & (df_gleason['GLEASON_SCORE'] != '')].copy()
    df_gleason = df_gleason[_col_order_gleason].copy()

    # Save to both volume file and create table
    print(f"Saving {len(df_gleason):,} Gleason timeline events to {output_config.volume_path}")
    print(f"Creating table {output_config.fully_qualified_table}")
    db_io.write_table(df=df_gleason, table_config=output_config)


if __name__ == '__main__':
    main()
