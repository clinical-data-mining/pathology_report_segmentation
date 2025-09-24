#!/usr/bin/env python3
"""
Combine EPIC and IDB PD-L1 call tables from MinIO, align shared columns,
zero-pad MRNs, and save a single combined TSV back to MinIO.

Example:
    python my_func.py \
        --minio-env /gpfs/mindphidata/fongc2/minio_env.txt \
        --in-epic epic_ddp_concat/pathology/pathology_pdl1_calls_epic.tsv \
        --in-idb pathology/pathology_pdl1_calls.tsv \
        --out epic_ddp_concat/pathology/pathology_pdl1_calls_epic_idb_combined.tsv
"""
import argparse
import sys
from typing import List

import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_processing import set_debug_console, mrn_zero_pad

sort_columns = ["MRN", "DTE_PATH_PROCEDURE"]
fname_save = 'epic_ddp_concat/pathology/pathology_pdl1_calls_epic_idb_combined.tsv'
fname_pdl1_epic = 'epic_ddp_concat/pathology/pathology_pdl1_calls_epic.tsv'
fname_pdl1_idb = 'pathology/pathology_pdl1_calls.tsv'


def load_minio_tsv(minio: MinioAPI, path: str) -> pd.DataFrame:
    """Load a TSV from MinIO into a DataFrame."""
    obj = minio.load_obj(path_object=path)
    df = pd.read_csv(obj, sep="\t", low_memory=False)
    return df


def combine_pdl1_tables(
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
        description="Combine EPIC and IDB PD-L1 call tables from MinIO."
    )
    parser.add_argument(
        "--minio_env",
        help="Path to MinIO environment file.",
    )
    parser.add_argument(
        "--fname_pdl1_epic",
        default=fname_pdl1_epic,
        help="MinIO path for EPIC PD-L1 TSV.",
    )
    parser.add_argument(
        "--fname_pdl1_idb",
        default=fname_pdl1_idb,
        help="MinIO path for IDB PD-L1 TSV.",
    )
    parser.add_argument(
        "--fname_save",
        default=fname_save,
        help="MinIO path to save combined TSV.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    set_debug_console()

    # Init MinIO
    minio = MinioAPI(fname_minio_env=args.fname_minio)

    # Load inputs
    df_epic = load_minio_tsv(minio, args.fname_pdl1_epic)
    df_idb = load_minio_tsv(minio, args.fname_pdl1_idb)

    # Combine
    df_out = combine_pdl1_tables(
        df_epic=df_epic,
        df_idb=df_idb,
        sort_cols=sort_columns
    )

    print(df_out.sample())

    print(
        f"Saving {len(df_out):,} rows and {df_out.shape[1]} columns to: {args.fname_save}"
    )

    # Save
    minio.save_obj(df=df_out, path_object=args.fname_save, sep="\t")


    return 0


if __name__ == "__main__":
    sys.exit(main())
