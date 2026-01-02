#!/usr/bin/env python3
"""
Combine EPIC and IDB Gleason call tables from Databricks tables, align shared columns,
zero-pad MRNs, and save a single combined TSV.
"""
import argparse
import sys
from typing import List

import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import set_debug_console, mrn_zero_pad

sort_columns = ["MRN", "DTE_PATH_PROCEDURE"]
fname_save = 'epic_ddp_concat/pathology/pathology_gleason_calls_epic_idb_combined.tsv'

# Table configuration
TABLE_GLEASON_EPIC = 'cdsi_prod.cdm_epic_impact_pipeline_prod.pathology_gleason_calls_epic'
fname_gleason_idb = 'pathology/pathology_gleason_calls.tsv'

OUTPUT_TABLE_CATALOG = 'cdsi_prod'
OUTPUT_TABLE_SCHEMA = 'cdm_epic_impact_pipeline_prod'
OUTPUT_TABLE_NAME = 'pathology_gleason_calls_epic_idb_combined'


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
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    set_debug_console()

    # Init Databricks
    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Load EPIC data from table
    print(f"Loading EPIC Gleason data from {TABLE_GLEASON_EPIC}")
    sql_epic = f"SELECT * FROM {TABLE_GLEASON_EPIC}"
    df_epic = obj_db.query_from_sql(sql=sql_epic)

    # Load IDB data from file
    print(f"Loading IDB Gleason data from {fname_gleason_idb}")
    sql_idb = f"SELECT * FROM read_files('{fname_gleason_idb}', format => 'csv', sep => '\\t', header => true)"
    df_idb = obj_db.query_from_sql(sql=sql_idb)

    # Combine
    df_out = combine_gleason_tables(
        df_epic=df_epic,
        df_idb=df_idb,
        sort_cols=sort_columns
    )

    print(df_out.sample())

    print(
        f"Saving {len(df_out):,} rows and {df_out.shape[1]} columns to: {fname_save}"
    )

    # Save to both volume file and create table
    obj_db.write_db_obj(
        df=df_out,
        volume_path=fname_save,
        sep='\t',
        overwrite=True,
        dict_database_table_info={
            'catalog': OUTPUT_TABLE_CATALOG,
            'schema': OUTPUT_TABLE_SCHEMA,
            'table': OUTPUT_TABLE_NAME,
            'volume_path': fname_save,
            'sep': '\t'
        }
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
