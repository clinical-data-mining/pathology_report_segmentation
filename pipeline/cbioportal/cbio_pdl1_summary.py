#!/usr/bin/env python3
"""
Create cBioPortal summary tables for PD-L1 at patient and sample levels.
"""
import argparse
import sys
import os
import pandas as pd

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config_loader import load_config, get_step2_table, get_output_table_config
from databricks_io import DatabricksIO


def _load_data(db_io, table_pdl1, table_map):
    """Load PD-L1 and mapping data."""
    print(f'Loading {table_pdl1}')
    df_pdl1 = db_io.read_table(table_pdl1)
    df_pdl1['DTE_PATH_PROCEDURE'] = pd.to_datetime(df_pdl1['DTE_PATH_PROCEDURE'], errors='coerce')

    print(f'Loading {table_map}')
    sql_map = f"SELECT SAMPLE_ID, SOURCE_ACCESSION_NUMBER_0 FROM {table_map}"
    df_map = db_io.api.query_from_sql(sql=sql_map)

    return df_pdl1, df_map


def _clean_data_patient(df_pdl1):
    """Create patient-level summary."""
    df_pdl1 = df_pdl1.sort_values(by=['MRN', 'DTE_PATH_PROCEDURE'])
    reps = {True: 'Yes', False: 'No'}
    list_mrns_pdl1 = df_pdl1.loc[df_pdl1['PDL1_POSITIVE'] == 'Yes', 'MRN']
    df_pdl1_summary = df_pdl1[['MRN']].drop_duplicates()
    df_pdl1_summary['HISTORY_OF_PDL1'] = df_pdl1_summary['MRN'].isin(list_mrns_pdl1).replace(reps)

    return df_pdl1_summary


def _clean_data_sample(df_pdl1, df_map):
    """Create sample-level summary."""
    df_pdl1_s1 = df_pdl1.merge(
        right=df_map[['SAMPLE_ID', 'SOURCE_ACCESSION_NUMBER_0']],
        how='inner',
        left_on='ACCESSION_NUMBER',
        right_on='SOURCE_ACCESSION_NUMBER_0'
    )

    df_pdl1_s = df_pdl1_s1[['SAMPLE_ID', 'PDL1_POSITIVE']].copy()
    df_pdl1_s['DMP_ID'] = df_pdl1_s['SAMPLE_ID'].apply(lambda x: x[:9])

    return df_pdl1_s


def create_pdl1_summaries(db_io, table_pdl1, table_map, output_config_patient, output_config_sample):
    """Create and save PD-L1 patient and sample summaries."""
    # Load data
    df_pdl1, df_map = _load_data(db_io, table_pdl1, table_map)

    # Create summaries
    df_pdl1_p = _clean_data_patient(df_pdl1=df_pdl1)
    df_pdl1_s = _clean_data_sample(df_pdl1=df_pdl1, df_map=df_map)

    # Save data
    print(f"Saving {len(df_pdl1_p):,} patient records to {output_config_patient.volume_path}")
    print(f"Creating table {output_config_patient.fully_qualified_table}")
    db_io.write_table(df=df_pdl1_p, table_config=output_config_patient)

    print(f"Saving {len(df_pdl1_s):,} sample records to {output_config_sample.volume_path}")
    print(f"Creating table {output_config_sample.fully_qualified_table}")
    db_io.write_table(df=df_pdl1_s, table_config=output_config_sample)

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Create PD-L1 summaries for cBioPortal"
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
    table_pdl1 = get_step2_table(config, 'pathology_pdl1_calls_epic_idb_combined')
    table_map = get_step2_table(config, 'table_pathology_impact_sample_summary_dop_anno_epic_idb_combined')

    # Get output table configs
    output_config_patient = get_output_table_config(config, 'step3_cbioportal', 'summary_pdl1_patient')
    output_config_sample = get_output_table_config(config, 'step3_cbioportal', 'summary_pdl1_sample')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    print('Creating PD-L1 Summaries')
    create_pdl1_summaries(db_io, table_pdl1, table_map, output_config_patient, output_config_sample)


if __name__ == '__main__':
    main()
