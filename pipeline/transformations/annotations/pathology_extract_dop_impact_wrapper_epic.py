# ==========================================================
# CombineAccessionDOPImpactEpic
# ----------------------------------------------------------
# This class consolidates pathology metadata — including
# dates of surgical procedures and report accession numbers —
# for MSK-IMPACT sequenced tumor samples. It integrates data
# from:
# - Epic pathology reports (via Databricks SQL)
# - DMP sequencing records
# - ID mapping tables from CVR
# - Parsed specimen metadata (DOP)
# - Pathology accessions connected to IMPACT specimens (mentioned in part descriptions)
# ==========================================================

import pandas as pd
import sys
import os

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config_loader import get_legacy_table, get_external_source_table, get_step1_table
from databricks_io import DatabricksIO

from msk_cdm.data_processing import mrn_zero_pad

# Common column names
COL_SAMPLE_ID = 'SAMPLE_ID'
COL_ACCESSION_NO = 'ACCESSION_NUMBER'
COL_PDRX_ACCESSION_NO = 'PDRX_ACCESSION_NO'
COL_SOURCE_ACCESSION = 'SOURCE_ACCESSION_NUMBER_0'
COL_SOURCE_SPEC_NUM = 'SOURCE_SPEC_NUM_0'
COL_DATE_SURG = 'DATE_OF_PROCEDURE_SURGICAL'
COL_DATE_REPORT = 'REPORT_CMPT_DATE_SOURCE_0'
COL_DATE_SEQ = 'DATE_SEQUENCING_REPORT'
COL_SPEC_COLLECT_DATE = 'Specimen_Collected_Date'
COL_SPEC_NUM_DMP = 'SPECIMEN_NUMBER_DMP'
COL_ACCESSION_DMP = 'ACCESSION_NUMBER_DMP'

class CombineAccessionDOPImpactEpic:
    def __init__(self, db_io, config, yaml_config):
        """
        Initialize the CombineAccessionDOPImpactEpic.

        Parameters:
        - db_io: DatabricksIO instance for database operations
        - config: Dictionary containing script-specific configuration (table names, output paths)
        - yaml_config: Loaded YAML configuration for table lookups
        """
        self.db_io = db_io
        self.config = config
        self.yaml_config = yaml_config

    def load_pathology_dates_from_db(self):
        df_path_surg = self.db_io.api.query_from_sql(
            sql=f"SELECT {COL_ACCESSION_NO}, DTE_PATH_PROCEDURE FROM {self.config['table_surg']}"
        ).drop_duplicates()
        df_path_surg_g = df_path_surg.groupby(COL_ACCESSION_NO)['DTE_PATH_PROCEDURE'].first().reset_index()

        df_path_mole = self.db_io.api.query_from_sql(
            sql=f"SELECT Accession_Number, {COL_SPEC_COLLECT_DATE} FROM {self.config['table_mole']}"
        )

        return df_path_surg_g, df_path_mole

    def load_data(self):
        """
        Load data from Databricks tables (NO MORE read_files()!).

        Returns:
        - df_idb_prior: Legacy IDB DOP summary data
        - df_accession: Pathology accession numbers
        - df_dop: Date of procedure data
        - df_map: ID mapping table
        """
        # Load legacy IDB data from table
        table_idb = get_legacy_table(self.yaml_config, 'dop_summary')
        print(f"Loading legacy IDB DOP data from {table_idb}")
        df_idb_prior = self.db_io.read_table(table_idb).drop_duplicates()

        # Load Step 1 outputs from tables
        table_accession = get_step1_table(self.yaml_config, 'path_accessions')
        print(f"Loading accession data from {table_accession}")
        df_accession = self.db_io.read_table(table_accession)
        df_accession[COL_SOURCE_ACCESSION] = df_accession[COL_SOURCE_ACCESSION].str.strip()

        table_dop = get_step1_table(self.yaml_config, 'pathology_spec_part_dop')
        print(f"Loading DOP data from {table_dop}")
        df_dop = self.db_io.read_table(table_dop)

        # Load ID mapping from external source
        table_map = get_external_source_table(self.yaml_config, 'id_mapping')
        print(f"Loading ID mapping from {table_map}")
        df_map = self.db_io.read_table(table_map)
        df_map = mrn_zero_pad(df=df_map, col_mrn='MRN')

        return df_idb_prior, df_accession, df_dop, df_map

    def merge_pathology_data(self, df_map, df_accession, df_dop, df_idb):
        df_map[COL_PDRX_ACCESSION_NO] = df_map[COL_PDRX_ACCESSION_NO].str.strip()
        df_map = df_map[['MRN', COL_SAMPLE_ID, COL_PDRX_ACCESSION_NO]].copy()
        df_accession[COL_PDRX_ACCESSION_NO] = df_accession[COL_PDRX_ACCESSION_NO].str.strip()
        df_dop[COL_ACCESSION_NO] = df_dop[COL_ACCESSION_NO].str.strip()

        df_accession_f = df_accession[[COL_SAMPLE_ID, COL_PDRX_ACCESSION_NO, COL_SOURCE_ACCESSION, COL_SOURCE_SPEC_NUM]].copy()
        df_dop_f = df_dop[[COL_SAMPLE_ID, COL_ACCESSION_NO, COL_DATE_SURG]].copy()

        df = df_map.merge(df_accession_f, how='left', on=[COL_SAMPLE_ID, COL_PDRX_ACCESSION_NO])
        df = df.merge(df_dop_f, how='left', left_on=[COL_SAMPLE_ID, COL_PDRX_ACCESSION_NO], right_on=[COL_SAMPLE_ID, COL_ACCESSION_NO])
        df = df.drop(columns=[COL_ACCESSION_NO]).rename(columns={COL_PDRX_ACCESSION_NO: COL_ACCESSION_DMP})
        df = df.merge(df_idb.drop(columns=['MRN', 'DTE_TUMOR_SEQUENCING']), how='left', on=[COL_SAMPLE_ID, COL_ACCESSION_DMP])
        df[COL_SPEC_NUM_DMP] = df[COL_SPEC_NUM_DMP].fillna(1)

        df[COL_SOURCE_ACCESSION] = df[f'{COL_SOURCE_ACCESSION}_x'].fillna(df[f'{COL_SOURCE_ACCESSION}_y'])
        df = df.drop(columns=[f'{COL_SOURCE_ACCESSION}_x', f'{COL_SOURCE_ACCESSION}_y'])
        df[COL_SOURCE_SPEC_NUM] = df[f'{COL_SOURCE_SPEC_NUM}_x'].fillna(df[f'{COL_SOURCE_SPEC_NUM}_y'])
        df = df.drop(columns=[f'{COL_SOURCE_SPEC_NUM}_x', f'{COL_SOURCE_SPEC_NUM}_y'])
        df[COL_DATE_SURG] = df[f'{COL_DATE_SURG}_x'].fillna(df[f'{COL_DATE_SURG}_y'])
        df = df.drop(columns=[f'{COL_DATE_SURG}_x', f'{COL_DATE_SURG}_y'])

        return df

    def merge_report_dates(self, df, df_path_surg_g, df_path_mole):
        df = df.merge(df_path_surg_g, how='left', left_on=COL_SOURCE_ACCESSION, right_on=COL_ACCESSION_NO)
        df[COL_DATE_REPORT] = df[COL_DATE_REPORT].fillna(df['DTE_PATH_PROCEDURE'])
        df = df.drop(columns=[COL_ACCESSION_NO, 'DTE_PATH_PROCEDURE'])

        df = df.merge(df_path_mole, how='left', left_on=COL_ACCESSION_DMP, right_on='Accession_Number')
        df[COL_DATE_SEQ] = df[COL_DATE_SEQ].fillna(df[COL_SPEC_COLLECT_DATE])
        df = df.drop(columns=['Accession_Number', COL_SPEC_COLLECT_DATE])

        return df

    def process(self):
        """
        Run the full DOP combination pipeline:
        - Load inputs from tables
        - Merge pathology data
        - Merge report dates
        - Save result to volume and create table

        Returns:
        - df_combined: Final combined DataFrame
        """
        df_path_surg_g, df_path_mole = self.load_pathology_dates_from_db()
        df_idb, df_accession, df_dop, df_map = self.load_data()
        df_combined = self.merge_pathology_data(df_map, df_accession, df_dop, df_idb)
        df_combined = self.merge_report_dates(df_combined, df_path_surg_g, df_path_mole)

        # Save to both volume file and create table using DatabricksIO
        if 'output_table_config' in self.config:
            print(f"Saving to {self.config['output_table_config'].volume_path}")
            print(f"Creating table {self.config['output_table_config'].fully_qualified_table}")
            self.db_io.write_table(df=df_combined, table_config=self.config['output_table_config'])

        return df_combined
