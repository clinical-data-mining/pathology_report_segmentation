# ==========================================================
# CombineAccessionDOPImpactEpic
# ----------------------------------------------------------
# This class consolidates pathology metadata — including
# dates of surgical procedures and report accession numbers —
# for MSK-IMPACT sequenced tumor samples. It integrates data
# from:
# - Epic pathology reports (via Databricks SQL)
# - DMP sequencing records
# - ID mapping files from CVR
# - Parsed specimen metadata (DOP)
# - Pathology accessions connected to IMPACT specimens (mentioned in part descriptions)
# ==========================================================

import pandas as pd

from msk_cdm.data_processing import mrn_zero_pad
from msk_cdm.databricks import DatabricksAPI

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
    def __init__(self, fname_databricks_env, config):
        self.obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)
        self.config = config

    def load_pathology_dates_from_db(self):
        df_path_surg = self.obj_db.query_from_sql(
            sql=f"SELECT {COL_ACCESSION_NO}, DTE_PATH_PROCEDURE FROM {self.config['table_surg']}"
        ).drop_duplicates()
        df_path_surg_g = df_path_surg.groupby(COL_ACCESSION_NO)['DTE_PATH_PROCEDURE'].first().reset_index()

        df_path_mole = self.obj_db.query_from_sql(
            sql=f"SELECT Accession_Number, {COL_SPEC_COLLECT_DATE} FROM {self.config['table_mole']}"
        )

        return df_path_surg_g, df_path_mole

    def load_data(self):
        # Load legacy IDB data from file
        sql_idb = f"SELECT * FROM read_files('{self.config['fname_idb']}', format => 'csv', sep => '\t', header => true)"
        df_idb_prior = self.obj_db.query_from_sql(sql=sql_idb).drop_duplicates()

        # Load Step 1 outputs from tables (if available), otherwise from files
        if 'table_accession' in self.config:
            sql_accession = f"SELECT * FROM {self.config['table_accession']}"
        else:
            sql_accession = f"SELECT * FROM read_files('{self.config['fname_accession']}', format => 'csv', sep => '\t', header => true)"
        df_accession = self.obj_db.query_from_sql(sql=sql_accession)
        df_accession[COL_SOURCE_ACCESSION] = df_accession[COL_SOURCE_ACCESSION].str.strip()

        if 'table_dop' in self.config:
            sql_dop = f"SELECT * FROM {self.config['table_dop']}"
        else:
            sql_dop = f"SELECT * FROM read_files('{self.config['fname_dop']}', format => 'csv', sep => '\t', header => true)"
        df_dop = self.obj_db.query_from_sql(sql=sql_dop)

        sql_map = f"SELECT * FROM read_files('{self.config['fname_map']}', format => 'csv', sep => '\t', header => true)"
        df_map = self.obj_db.query_from_sql(sql=sql_map)
        df_map = mrn_zero_pad(df=df_map, col_mrn='MRN')

        return df_idb_prior, df_accession, df_dop, df_map

    def merge_pathology_data(self, df_map, df_accession, df_dop, df_idb):
        df_map[COL_PDRX_ACCESSION_NO] = df_map[COL_PDRX_ACCESSION_NO].str.strip()
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
        df_path_surg_g, df_path_mole = self.load_pathology_dates_from_db()
        df_idb, df_accession, df_dop, df_map = self.load_data()
        df_combined = self.merge_pathology_data(df_map, df_accession, df_dop, df_idb)
        df_combined = self.merge_report_dates(df_combined, df_path_surg_g, df_path_mole)

        # Save to both volume file and create table
        dict_table_info = None
        if 'output_table_config' in self.config:
            dict_table_info = {
                'catalog': self.config['output_table_config']['catalog'],
                'schema': self.config['output_table_config']['schema'],
                'table': self.config['output_table_config']['table'],
                'volume_path': self.config['fname_save'],
                'sep': '\t'
            }

        self.obj_db.write_db_obj(
            df=df_combined,
            volume_path=self.config['fname_save'],
            sep='\t',
            overwrite=True,
            dict_database_table_info=dict_table_info
        )

        return df_combined
