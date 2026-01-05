#!/usr/bin/env python3
"""
Generate cBioPortal timeline files for MMR biomarker events.
"""
import argparse
import sys
import os
import pandas as pd

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config_loader import load_config, get_step2_table, get_output_table_config
from databricks_io import DatabricksIO

# Column order for cBioPortal timeline format
_col_order_mmr = [
    'MRN',
    'START_DATE',
    'STOP_DATE',
    'EVENT_TYPE',
    'SUBTYPE',
    'SOURCE',
    'MMR'
]


def main():
    parser = argparse.ArgumentParser(
        description="Generate cBioPortal timeline for MMR events"
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
    source_table = get_step2_table(config, 'pathology_mmr_calls_epic_idb_combined')

    # Get output table config
    output_config = get_output_table_config(config, 'step3_cbioportal', 'timeline_mmr_calls')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Load data from table
    print(f'Loading {source_table}')
    df_mmr = db_io.read_table(source_table)

    # Transform to timeline format
    cols_rename = {
        'DTE_PATH_PROCEDURE': 'START_DATE',
        'MMR_ABSENT': 'MMR'
    }
    df_mmr = df_mmr.rename(columns=cols_rename)
    df_mmr = df_mmr.assign(STOP_DATE='')
    df_mmr = df_mmr.assign(EVENT_TYPE='PATHOLOGY')
    df_mmr = df_mmr.assign(SUBTYPE='MMR Deficiency')
    df_mmr = df_mmr.assign(SOURCE='Pathology Reports (NLP)')

    df_mmr = df_mmr.replace({'MMR': {True: 'deficient', False: 'proficient'}})

    # TODO add color for positive and negative

    df_mmr = df_mmr[df_mmr['MMR'].notnull() & (df_mmr['MMR'] != '')].copy()
    df_mmr = df_mmr[_col_order_mmr]

    # Save to both volume file and create table
    print(f"Saving {len(df_mmr):,} MMR timeline events to {output_config.volume_path}")
    print(f"Creating table {output_config.fully_qualified_table}")
    db_io.write_table(df=df_mmr, table_config=output_config)


if __name__ == '__main__':
    main()
