#!/usr/bin/env python3
"""
Extract Gleason scores from Epic pathology reports.
Uses NLP patterns to identify and extract Gleason scores from prostate pathology reports.
"""
import argparse
import sys
import os
import numpy as np

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_external_source_table, get_output_table_config
from databricks_io import DatabricksIO

from msk_cdm.data_processing import convert_to_int
from annotations import extractGleason

# Constants
COL_TEXT = 'path_prpt_p1'
COLS_SAVE = ['MRN', 'Accession Number', 'Path Procedure Date', 'Gleason']


def main():
    parser = argparse.ArgumentParser(
        description="Extract Gleason scores from Epic pathology reports"
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
    output_config = get_output_table_config(config, 'step1_extraction', 'pathology_gleason_calls_epic')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Load pathology reports
    print(f"Loading pathology reports from {source_table}")
    df_path = db_io.read_table(source_table)

    # Filter for Gleason-containing reports
    filter_gleason = df_path[COL_TEXT].fillna('').str.contains('Gleason', case=False)
    df_path_gleason = df_path[filter_gleason].copy()

    print('Abstracting Gleason scores')
    df_path_gleason['Gleason'] = df_path_gleason[COL_TEXT].apply(extractGleason)
    df_path_gleason = df_path_gleason.rename(
        columns={
            'ACCESSION_NUMBER': 'ACCESSION_NUMBER',
            'DTE_PATH_PROCEDURE': 'DTE_PATH_PROCEDURE'
        }
    )
    df_save = df_path_gleason[['MRN', 'ACCESSION_NUMBER', 'DTE_PATH_PROCEDURE', 'Gleason']]

    # Do last cleaning -- Gleason scores should not be under 6. Convert 1's to 10
    # TODO: Fix regex to grab 10s
    df_save.loc[df_save['Gleason'] == 1] = 10
    df_save.loc[df_save['Gleason'] < 6] = np.NaN
    df_save = df_save[df_save['Gleason'].notnull() & df_save['MRN'].notnull()]
    df_save = convert_to_int(df=df_save, list_cols=['MRN', 'Gleason'])

    # Save to both volume file and create table
    print(f"Saving {len(df_save):,} Gleason scores to {output_config.volume_path}")
    print(f"Creating table {output_config.fully_qualified_table}")
    db_io.write_table(df=df_save, table_config=output_config)


if __name__ == '__main__':
    main()

