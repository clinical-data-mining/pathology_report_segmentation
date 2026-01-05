#!/usr/bin/env python3
"""
Create cBioPortal summary table for MMR at patient level.
"""
import argparse
import sys
import os
import pandas as pd

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_step3_table, get_output_table_config
from databricks_io import DatabricksIO


def _load_data(db_io, table_mmr):
    """Load MMR timeline data."""
    print(f'Loading {table_mmr}')
    df_mmr = db_io.read_table(table_mmr)
    df_mmr['START_DATE'] = pd.to_datetime(df_mmr['START_DATE'], errors='coerce')

    return df_mmr


def _clean_data_patient(df_mmr):
    """Create patient-level summary."""
    df_mmr = df_mmr.sort_values(by=['MRN', 'START_DATE'])
    reps = {True: 'deficient', False: 'proficient'}
    list_mrns_mmr = df_mmr.loc[df_mmr['MMR'] == 'deficient', 'MRN']
    df_mmr_summary = df_mmr[['MRN']].drop_duplicates()
    df_mmr_summary['HISTORY_OF_D_MMR'] = df_mmr_summary['MRN'].isin(list_mrns_mmr).replace(reps)

    return df_mmr_summary


def create_dmmr_summary(db_io, table_mmr, output_config_patient):
    """Create and save dMMR patient summary."""
    # Load data
    df_mmr = _load_data(db_io, table_mmr)

    # Create patient summary
    df_mmr_p = _clean_data_patient(df_mmr=df_mmr)

    # Save data
    print(f"Saving {len(df_mmr_p):,} patient records to {output_config_patient.volume_path}")
    print(f"Creating table {output_config_patient.fully_qualified_table}")
    db_io.write_table(df=df_mmr_p, table_config=output_config_patient)

    return df_mmr_p


def main():
    parser = argparse.ArgumentParser(
        description="Create MMR summaries for cBioPortal"
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

    # Get input table from config (reads from Step 3 timeline table)
    table_mmr = get_step3_table(config, 'timeline_mmr_calls')

    # Get output table config
    output_config_patient = get_output_table_config(config, 'step3_cbioportal', 'summary_mmr_patient')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    print('Creating dMMR Summaries')
    create_dmmr_summary(db_io, table_mmr, output_config_patient)


if __name__ == '__main__':
    main()
