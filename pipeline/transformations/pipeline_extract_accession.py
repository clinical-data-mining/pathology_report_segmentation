#!/usr/bin/env python3
"""
Extract pathology accession numbers from ID mapping table.
Parses IMPACT sample part descriptions to extract source accession numbers and specimen numbers.
"""
import argparse
import sys
import os

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_external_source_table, get_output_table_config
from databricks_io import DatabricksIO

from annotations import PathologyExtractAccessionEpic

# Constants
COL_LABEL_1 = 'SAMPLE_ID'
COL_LABEL_2 = 'PDRX_ACCESSION_NO'
COL_SPEC_SUB = 'PART_DESCRIPTION'


def main():
    parser = argparse.ArgumentParser(
        description="Extract pathology accession numbers from ID mapping"
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
    source_table = get_external_source_table(config, 'id_mapping')

    # Get output table config
    output_config = get_output_table_config(config, 'step1_extraction', 'path_accessions')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Extract accession numbers
    print(f"Loading ID mapping from {source_table}")
    obj_path = PathologyExtractAccessionEpic(
        fname_databricks_env=args.databricks_env,
        fname=source_table,
        list_col_index=[COL_LABEL_1, COL_LABEL_2],
        col_spec_sub=COL_SPEC_SUB
    )

    df_dop = obj_path.return_df()
    df_orig = obj_path.return_df_original()
    df_f = df_orig.merge(right=df_dop, how='left', on=['SAMPLE_ID', 'PDRX_ACCESSION_NO'])
    df_f = df_f.drop(columns=['DMP_ID'])

    df_f = df_f.rename(
        columns={
            'SOURCE_ACCESSION_NUMBER': 'SOURCE_ACCESSION_NUMBER_0',
            'SPECIMEN_NUMBER': 'SOURCE_SPEC_NUM_0'
        })

    # Save to both volume file and create table
    print(f"Saving {len(df_f):,} rows to {output_config.volume_path}")
    print(f"Creating table {output_config.fully_qualified_table}")
    db_io.write_table(df=df_f, table_config=output_config)

    print("Sample of extracted accession numbers:")
    print(df_f.head())


if __name__ == '__main__':
    main()