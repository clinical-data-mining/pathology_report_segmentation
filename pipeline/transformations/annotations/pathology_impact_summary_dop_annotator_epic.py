# ==========================================================
# PathologyImpactDOPAnnoEpic
# ----------------------------------------------------------
# Annotates and backfills surgical procedure dates for MSK-
# IMPACT sequenced samples by checking whether the Epic
# surgical procedure date matches the pathology report date.
# ==========================================================
import pandas as pd
import sys
import os

# Add pipeline to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config_loader import get_legacy_table, get_external_source_table, get_step2_table
from databricks_io import DatabricksIO

from msk_cdm.data_processing import mrn_zero_pad


# Common column names
COL_SAMPLE_ID = 'SAMPLE_ID'
COL_ACCESSION_DMP = 'ACCESSION_NUMBER_DMP'
COL_DOP_EST = 'DATE_OF_PROCEDURE_SURGICAL_EST'
COL_DOP_SOURCE = 'DOP_COMPUTE_SOURCE'
COL_REPORT_DATE = 'REPORT_CMPT_DATE_SOURCE_0'
COL_DOP = 'DATE_OF_PROCEDURE_SURGICAL'
COL_PROCEDURE_DATE = 'DATE_OF_PROCEDURE'
COL_PROC_DESC = 'PROCEDURE_DESCRIPTION'
COL_MRN = 'MRN'

class PathologyImpactDOPAnnoEpic:
    """
    This class loads prior annotations, Epic pathology reports, and Epic surgical procedures
    and backfills or estimates surgical procedure dates (DATE_OF_PROCEDURE_SURGICAL_EST)
    for MSK-IMPACT samples.

    If a surgical procedure occurs on the same day as a pathology report, that date
    is inferred to be the date of the surgical procedure.
    """
    def __init__(self, db_io, config, yaml_config):
        """
        Initialize the PathologyImpactDOPAnnoEpic.

        Parameters:
        - db_io: DatabricksIO instance for database operations
        - config: Dictionary containing script-specific configuration (table names, output paths)
        - yaml_config: Loaded YAML configuration for table lookups
        """
        self.db_io = db_io
        self.config = config
        self.yaml_config = yaml_config

    def load_data(self):
        """
        Load data from Databricks tables (NO MORE read_files()!):
        - Prior pathology annotations with existing DOP estimates
        - Combined pathology summary output from prior processing
        - Epic surgical procedure table (preprocessed)

        Returns:
        - df_prior: Prior annotation DataFrame
        - df_summary: Combined summary from PathologyDataProcessor
        - df_proc_g: Grouped surgical procedures by MRN + date
        """
        # Load prior annotations from legacy IDB table
        table_prior = get_legacy_table(self.yaml_config, 'prior_annotations')
        print(f"Loading prior annotations from {table_prior}")
        df_prior = self.db_io.read_table(table_prior)
        df_prior = df_prior[[
            COL_SAMPLE_ID, COL_ACCESSION_DMP,
            COL_DOP_EST, COL_DOP_SOURCE
        ]].drop_duplicates()

        # Load summary from Step 2 combining output table
        table_summary = get_step2_table(self.yaml_config, 'pathology_dop_impact_summary_epic_idb_combined')
        print(f"Loading summary data from {table_summary}")
        df_summary = self.db_io.read_table(table_summary)
        df_summary[COL_ACCESSION_DMP] = df_summary[COL_ACCESSION_DMP].str.strip()
        df_summary[COL_REPORT_DATE] = pd.to_datetime(df_summary[COL_REPORT_DATE], errors='coerce')
        df_summary = mrn_zero_pad(df=df_summary, col_mrn=COL_MRN)

        # Load surgical procedures from external source table
        table_proc = get_external_source_table(self.yaml_config, 'surgical_procedures')
        print(f"Loading surgical procedures from {table_proc}")
        df_proc = self.db_io.read_table(table_proc)
        df_proc = mrn_zero_pad(df=df_proc, col_mrn=COL_MRN)
        df_proc[COL_PROCEDURE_DATE] = pd.to_datetime(df_proc[COL_PROCEDURE_DATE], errors='coerce')

        df_proc_g = df_proc.groupby([COL_MRN, COL_PROCEDURE_DATE])[COL_PROC_DESC].count().reset_index()

        return df_prior, df_summary, df_proc_g

    def estimate_surgical_dates(self, df_summary, df_prior, df_proc_g):
        """
        Apply logic to estimate or backfill surgical procedure dates:

        1. If DATE_OF_PROCEDURE_SURGICAL exists and no prior source exists,
           it's backfilled with source 'Specimens Submitted'.

        2. If Epic surgical procedure date matches the pathology report date,
           the report date is assigned to DATE_OF_PROCEDURE_SURGICAL_EST with source 'Surgical'.

        Parameters:
        - df_summary: Summary table from previous pathology processing
        - df_prior: Prior annotations with existing surgical estimates
        - df_proc_g: Surgical procedure counts by MRN and date

        Returns:
        - df: Annotated and backfilled DataFrame
        """
        df = df_summary.merge(df_prior, how='left', on=[COL_SAMPLE_ID, COL_ACCESSION_DMP])
        df = df.merge(df_proc_g, how='left', left_on=[COL_MRN, COL_REPORT_DATE], right_on=[COL_MRN, COL_PROCEDURE_DATE])

        df[COL_PROC_DESC] = df[COL_PROC_DESC].fillna(0)

        # Backfill from parsed pathology reports
        log1 = df[COL_DOP].notnull() & df[COL_DOP_SOURCE].isnull()
        df.loc[log1, COL_DOP_SOURCE] = 'Specimens Submitted'
        df.loc[log1, COL_DOP_EST] = df.loc[log1, COL_DOP]

        # Backfill when pathology report date == surgical procedure date
        log_proc = df[COL_PROC_DESC] > 0
        log_report = df[COL_REPORT_DATE].notnull()
        log_missing = df[COL_DOP_EST].isnull()
        log = log_proc & log_report & log_missing

        df.loc[log, COL_DOP_SOURCE] = 'Surgical'
        df.loc[log, COL_DOP_EST] = df.loc[log, COL_REPORT_DATE]

        df = df.drop(columns=[COL_PROCEDURE_DATE, COL_PROC_DESC])

        return df

    def process(self):
        """
        Run the full surgical DOP annotation pipeline:
        - Load inputs from tables
        - Annotate surgical dates from matched procedures
        - Save result to volume and create table

        Returns:
        - df_final: Final annotated DataFrame
        """
        df_prior, df_summary, df_proc_g = self.load_data()
        df_final = self.estimate_surgical_dates(df_summary, df_prior, df_proc_g)

        # Save to both volume file and create table using DatabricksIO
        if 'output_table_config' in self.config:
            print(f"Saving to {self.config['output_table_config'].volume_path}")
            print(f"Creating table {self.config['output_table_config'].fully_qualified_table}")
            self.db_io.write_table(df=df_final, table_config=self.config['output_table_config'])

        return df_final
