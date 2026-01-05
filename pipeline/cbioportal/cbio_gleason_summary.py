#!/usr/bin/env python3
"""
Create cBioPortal summary tables for Gleason scores at patient and sample levels.
"""
import argparse
import sys
import os
import pandas as pd

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_step2_table, get_output_table_config
from databricks_io import DatabricksIO

RENAME_SAMPLE = {'Gleason': 'GLEASON_SAMPLE_LEVEL'}


def _load_data(db_io, table_gleason, table_map):
    """Load Gleason and mapping data."""
    print(f'Loading {table_gleason}')
    df_gleason = db_io.read_table(table_gleason)
    df_gleason['DTE_PATH_PROCEDURE'] = pd.to_datetime(df_gleason['DTE_PATH_PROCEDURE'], errors='coerce')

    print(f'Loading {table_map}')
    sql_map = f"SELECT SAMPLE_ID, SOURCE_ACCESSION_NUMBER_0 FROM {table_map}"
    df_map = db_io.api.query_from_sql(sql=sql_map)

    return df_gleason, df_map


def _clean_data_patient(df_gleason):
    """Create patient-level summary."""
    df_gleason = df_gleason.sort_values(by=['MRN', 'DTE_PATH_PROCEDURE'])
    gleason_highest = df_gleason.groupby(['MRN'])['Gleason'].max().rename('GLEASON_HIGHEST_REPORTED').reset_index()
    gleason_first = df_gleason.groupby(['MRN']).first().reset_index()
    gleason_first = gleason_first.rename(columns={'Gleason': 'GLEASON_FIRST_REPORTED'})
    gleason_first = gleason_first[['MRN', 'GLEASON_FIRST_REPORTED']]

    df_gleason_patient = gleason_first.merge(right=gleason_highest, how='inner', on='MRN')

    return df_gleason_patient


def _clean_data_sample(df_gleason, df_map):
    """Create sample-level summary."""
    df_gleason_s1 = df_gleason.merge(
        right=df_map[['SAMPLE_ID', 'SOURCE_ACCESSION_NUMBER_0']],
        how='inner',
        left_on='ACCESSION_NUMBER',
        right_on='SOURCE_ACCESSION_NUMBER_0'
    )

    df_gleason_s = df_gleason_s1[['SAMPLE_ID', 'Gleason']].rename(columns=RENAME_SAMPLE)
    df_gleason_s['DMP_ID'] = df_gleason_s['SAMPLE_ID'].apply(lambda x: x[:9])

    return df_gleason_s


def create_gleason_summaries(db_io, table_gleason, table_map, output_config_patient, output_config_sample):
    """Create and save Gleason patient and sample summaries."""
    # Load data
    df_gleason, df_map = _load_data(db_io, table_gleason, table_map)

    # Create summaries
    df_gleason_p = _clean_data_patient(df_gleason=df_gleason)
    df_gleason_s = _clean_data_sample(df_gleason=df_gleason, df_map=df_map)

    # Save data
    print(f"Saving {len(df_gleason_p):,} patient records to {output_config_patient.volume_path}")
    print(f"Creating table {output_config_patient.fully_qualified_table}")
    db_io.write_table(df=df_gleason_p, table_config=output_config_patient)

    print(f"Saving {len(df_gleason_s):,} sample records to {output_config_sample.volume_path}")
    print(f"Creating table {output_config_sample.fully_qualified_table}")
    db_io.write_table(df=df_gleason_s, table_config=output_config_sample)

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Create Gleason score summaries for cBioPortal"
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

    # Get input tables from config
    table_gleason = get_step2_table(config, 'pathology_gleason_calls_epic_idb_combined')
    table_map = get_step2_table(config, 'table_pathology_impact_sample_summary_dop_anno_epic_idb_combined')

    # Get output table configs
    output_config_patient = get_output_table_config(config, 'step3_cbioportal', 'summary_gleason_patient')
    output_config_sample = get_output_table_config(config, 'step3_cbioportal', 'summary_gleason_sample')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    print('Creating Gleason Score Summaries')
    create_gleason_summaries(db_io, table_gleason, table_map, output_config_patient, output_config_sample)


if __name__ == '__main__':
    main()
