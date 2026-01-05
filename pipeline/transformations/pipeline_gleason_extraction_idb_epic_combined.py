#!/usr/bin/env python3
"""
Combine EPIC and IDB Gleason call tables from Databricks tables, align shared columns,
zero-pad MRNs, and save a single combined TSV.
"""
import argparse
import sys
import os
from typing import List

import pandas as pd

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import load_config, get_step1_table, get_legacy_table, get_output_table_config
from databricks_io import DatabricksIO

from msk_cdm.data_processing import set_debug_console, mrn_zero_pad

sort_columns = ["MRN", "DTE_PATH_PROCEDURE"]


def combine_gleason_tables(
    df_epic: pd.DataFrame,
    df_idb: pd.DataFrame,
    sort_cols: List[str] = ("MRN", "DTE_PATH_PROCEDURE"),
) -> pd.DataFrame:
    """
    Align shared columns and vertically concatenate EPIC and IDB PD-L1 tables.

    Args:
        df_epic: DataFrame from EPIC pipeline.
        df_idb: DataFrame from IDB pipeline.
        sort_cols: Column names to sort by after concatenation.

    Returns:
        Combined and sorted DataFrame containing rows from both sources,
        restricted to their shared columns. MRN is zero-padded.
    """
    # Zero-pad MRN if present
    if "MRN" in df_epic.columns:
        df_epic = mrn_zero_pad(df=df_epic, col_mrn="MRN")
    if "MRN" in df_idb.columns:
        df_idb = mrn_zero_pad(df=df_idb, col_mrn="MRN")

    # Change date of procuedre column name for IDB legacy data
    df_idb = df_idb.rename(columns={'PATH_PROCEDURE_DATE': 'DTE_PATH_PROCEDURE'})

    # Use intersection of columns (preserve IDB column order)
    shared = list(set(df_idb.columns).intersection(set(df_epic.columns)))
    cols = [c for c in df_idb.columns if c in shared]

    combined = pd.concat(
        [df_idb[cols], df_epic[cols]],
        axis=0,
        ignore_index=True,
    )

    # Sort if all requested sort columns exist
    sort_by = [c for c in sort_cols if c in combined.columns]
    if sort_by:
        combined = combined.sort_values(by=sort_by, kind="mergesort")  # stable

    return combined


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine EPIC and IDB Gleason call tables."
    )
    parser.add_argument(
        "--databricks_env",
        required=True,
        help="Path to Databricks environment file.",
    )
    parser.add_argument(
        "--config_yaml",
        required=True,
        help="Path to YAML configuration file.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    set_debug_console()

    # Load configuration
    config = load_config(args.config_yaml)

    # Get table names from config
    table_gleason_epic = get_step1_table(config, 'pathology_gleason_calls_epic')
    table_gleason_idb = get_legacy_table(config, 'gleason_calls')
    output_config = get_output_table_config(config, 'step2_combining', 'pathology_gleason_calls_epic_idb_combined')

    # Init Databricks IO
    db_io = DatabricksIO(fname_databricks_env=args.databricks_env)

    # Load EPIC data from table
    print(f"Loading EPIC Gleason data from {table_gleason_epic}")
    df_epic = db_io.read_table(table_gleason_epic)

    # Load IDB data from table (NO MORE read_files()!)
    print(f"Loading IDB Gleason data from {table_gleason_idb}")
    df_idb = db_io.read_table(table_gleason_idb)

    # Combine
    df_out = combine_gleason_tables(
        df_epic=df_epic,
        df_idb=df_idb,
        sort_cols=sort_columns
    )

    print(df_out.sample())

    print(
        f"Saving {len(df_out):,} rows and {df_out.shape[1]} columns to: {output_config.volume_path}"
    )
    print(f"Creating table: {output_config.fully_qualified_table}")

    # Save to both volume file and create table
    db_io.write_table(df=df_out, table_config=output_config)

    return 0


if __name__ == "__main__":
    sys.exit(main())
