#!/usr/bin/env python3
"""
Extract date of procedure (DOP) from pathology specimen part descriptions.
Parses IMPACT sample metadata to extract surgical procedure dates.
"""
import argparse
import sys
import os

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_external_source_table, get_output_table_config
from databricks_io import DatabricksIO

from annotations import PathologyExtractDOPEpic

# Constants
COL_LABEL_1 = 'SAMPLE_ID'
COL_LABEL_2 = 'PDRX_ACCESSION_NO'
COL_SPEC_SUB = 'PART_DESCRIPTION'


def main():
    parser = argparse.ArgumentParser(
        description="Extract date of procedure (DOP) from pathology specimens"
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
    output_config = get_output_table_config(config, 'step1_extraction', 'pathology_spec_part_dop')

    # Initialize DatabricksIO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Extract DOP
    print(f"Loading ID mapping from {source_table}")
    obj_p = PathologyExtractDOPEpic(
        fname_databricks_env=args.databricks_env,
        table=source_table,
        list_col_index=[COL_LABEL_1, COL_LABEL_2],
        col_spec_sub=COL_SPEC_SUB
    )

    df_dop = obj_p.return_df()
    df_orig = obj_p.return_df_original()

    df_f = df_orig[[COL_LABEL_1, COL_LABEL_2]].merge(right=df_dop, how='left', on=[COL_LABEL_1, COL_LABEL_2])
    df_f = df_f.rename(columns={'PDRX_ACCESSION_NO': 'ACCESSION_NUMBER'})

    # Save to both volume file and create table
    print(f"Saving {len(df_f):,} rows to {output_config.volume_path}")
    print(f"Creating table {output_config.fully_qualified_table}")
    db_io.write_table(df=df_f, table_config=output_config)

    print("Saved successfully!")
    print("Sample of extracted DOP data:")
    print(df_f.head())


if __name__ == '__main__':
    main()