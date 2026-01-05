#!/usr/bin/env python3
"""
Generate cBioPortal timeline files for date of surgery for sequenced samples.
Creates timeline events showing when surgical specimens were collected.
"""
import argparse
import sys
import os
import pandas as pd

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_step2_table, get_output_table_config
from databricks_io import DatabricksIO

from msk_cdm.data_processing import convert_to_int

# Column order for cBioPortal timeline format
COL_ORDER_SEQ = [
    'MRN',
    'START_DATE',
    'STOP_DATE',
    'EVENT_TYPE',
    'SUBTYPE',
    'SAMPLE_ID'
]


def sample_acquisition_timeline(db_io, source_table, output_config):
    """Create timeline data for specimen acquisition dates."""
    # Load IMPACT sample summary data
    col_use = [
        'MRN',
        'SAMPLE_ID',
        'DATE_OF_PROCEDURE_SURGICAL_EST'
    ]
    print(f'Loading {source_table}')
    cols_str = ', '.join(col_use)
    sql = f"SELECT {cols_str} FROM {source_table}"
    df_samples_seq = db_io.api.query_from_sql(sql=sql)
    df_samples_seq['DATE_OF_PROCEDURE_SURGICAL_EST'] = pd.to_datetime(
        df_samples_seq['DATE_OF_PROCEDURE_SURGICAL_EST'],
        format='mixed',
        errors='coerce'
    )
    df_samples_seq = df_samples_seq.rename(columns={'DATE_OF_PROCEDURE_SURGICAL_EST': 'START_DATE'})

    # Convert MRN column from float or str to int
    df_samples_seq['MRN_numeric'] = pd.to_numeric(df_samples_seq['MRN'], errors='coerce')
    df_samples_seq = df_samples_seq.dropna(subset=['MRN_numeric'])
    df_samples_seq['MRN'] = df_samples_seq['MRN_numeric']
    df_samples_seq = df_samples_seq.drop(columns=['MRN_numeric'])
    df_samples_seq = convert_to_int(df=df_samples_seq, list_cols=['MRN'])

    # Filter for tumor samples only
    df_samples_seq = df_samples_seq[df_samples_seq['SAMPLE_ID'].notnull() & df_samples_seq['SAMPLE_ID'].str.contains('T')]

    # Add cBioPortal timeline fields
    df_samples_seq = df_samples_seq.assign(STOP_DATE='')
    df_samples_seq = df_samples_seq.assign(EVENT_TYPE='Sample acquisition')
    df_samples_seq = df_samples_seq.assign(SUBTYPE='')

    # Reorder columns
    df_samples_seq_f = df_samples_seq[COL_ORDER_SEQ]

    # Save timeline to both volume file and create table
    print(f"Saving {len(df_samples_seq_f):,} timeline events to {output_config.volume_path}")
    print(f"Creating table {output_config.fully_qualified_table}")
    db_io.write_table(df=df_samples_seq_f, table_config=output_config)

    return df_samples_seq_f


def main():
    parser = argparse.ArgumentParser(
        description="Generate cBioPortal timeline for specimen surgical dates"
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
    source_table = get_step2_table(config, 'table_pathology_impact_sample_summary_dop_anno_epic_idb_combined')

    # Get output table config
    output_config = get_output_table_config(config, 'step3_cbioportal', 'timeline_specimen_surgery')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    df_seq_timeline = sample_acquisition_timeline(db_io, source_table, output_config)
    print("Sample of timeline data:")
    print(df_seq_timeline.sample())
    

if __name__ == '__main__':
    main()



    
