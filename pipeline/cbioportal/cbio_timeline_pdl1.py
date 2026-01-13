#!/usr/bin/env python3
"""
Generate cBioPortal timeline files for PD-L1 biomarker events.
"""
import argparse
import sys
import os
import pandas as pd

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_step2_table, get_output_table_config
from databricks_io import DatabricksIO

# Column order for cBioPortal timeline format
_col_order_pdl1 = [
    'MRN',
    'START_DATE',
    'STOP_DATE',
    'EVENT_TYPE',
    'SUBTYPE',
    'SOURCE',
    'PDL1_POSITIVE',
    'PDL1_TPS_NLP'
]


def main():
    parser = argparse.ArgumentParser(
        description="Generate cBioPortal timeline for PD-L1 events"
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
    source_table = get_step2_table(config, 'pathology_pdl1_calls_epic_idb_combined')

    # Get output table config
    output_config = get_output_table_config(config, 'step3_cbioportal', 'timeline_pdl1_calls')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Load data from table
    print(f'Loading {source_table}')
    df_pdl1 = db_io.read_table(source_table)

    # Transform to timeline format
    df_pdl1 = df_pdl1.rename(columns={'DTE_PATH_PROCEDURE': 'START_DATE'})
    df_pdl1 = df_pdl1.assign(STOP_DATE='')
    df_pdl1 = df_pdl1.assign(EVENT_TYPE='PATHOLOGY')
    df_pdl1 = df_pdl1.assign(SUBTYPE='PD-L1 Positive')
    df_pdl1 = df_pdl1.assign(SOURCE='CDM')

    df_pdl1['PDl1_PERCENTAGE_EST'] = pd.to_numeric(df_pdl1['PDl1_PERCENTAGE_EST'], errors='coerce')
    df_pdl1['PDl1_TPS_1_EST'] = pd.to_numeric(df_pdl1['PDl1_TPS_1_EST'], errors='coerce')
    df_pdl1['PDl1_TPS_2_EST'] = pd.to_numeric(df_pdl1['PDl1_TPS_2_EST'], errors='coerce')

    df_pdl1['PDL1_TPS_NLP'] = df_pdl1[['PDl1_PERCENTAGE_EST', 'PDl1_TPS_1_EST', 'PDl1_TPS_2_EST']].max(axis=1)

    df_pdl1 = df_pdl1[df_pdl1['PDL1_POSITIVE'].notnull() & (df_pdl1['PDL1_POSITIVE'] != '')].copy()
    df_pdl1 = df_pdl1[_col_order_pdl1]

    # Save to both volume file and create table
    print(f"Saving {len(df_pdl1):,} PD-L1 timeline events to {output_config.volume_path}")
    print(f"Creating table {output_config.fully_qualified_table}")
    db_io.write_table(df=df_pdl1, table_config=output_config)


if __name__ == '__main__':
    main()
