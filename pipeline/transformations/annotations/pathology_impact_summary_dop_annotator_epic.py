# ==========================================================
# PathologyImpactDOPAnnoEpic
# ----------------------------------------------------------
# Annotates and backfills surgical procedure dates for MSK-
# IMPACT sequenced samples by checking whether the Epic
# surgical procedure date matches the pathology report date.
# ==========================================================
import pandas as pd

from msk_cdm.data_processing import mrn_zero_pad
from msk_cdm.minio import MinioAPI


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
    def __init__(self, fname_minio_env, config):
        """
        Initialize the PathologyImpactDOPAnnoEpic.

        Parameters:
        - fname_minio_env: Path to MinIO environment credentials file.
        - config: Dictionary containing all MinIO paths for input/output TSVs.
        """
        self.obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
        self.config = config

    def load_data(self):
        """
        Load data from MinIO:
        - Prior pathology annotations with existing DOP estimates
        - Combined pathology summary output from prior processing
        - Epic surgical procedure table (preprocessed)

        Returns:
        - df_prior: Prior annotation DataFrame
        - df_summary: Combined summary from PathologyDataProcessor
        - df_proc_g: Grouped surgical procedures by MRN + date
        """
        df_prior = pd.read_csv(self.obj_minio.load_obj(self.config['fname_prior_anno']), sep='\t', low_memory=False)
        df_prior = df_prior[[
            COL_SAMPLE_ID, COL_ACCESSION_DMP,
            COL_DOP_EST, COL_DOP_SOURCE
        ]].drop_duplicates()

        df_summary = pd.read_csv(self.obj_minio.load_obj(self.config['fname_summary']), sep='\t')
        df_summary[COL_ACCESSION_DMP] = df_summary[COL_ACCESSION_DMP].str.strip()
        df_summary[COL_REPORT_DATE] = pd.to_datetime(df_summary[COL_REPORT_DATE], errors='coerce')
        df_summary = mrn_zero_pad(df=df_summary, col_mrn=COL_MRN)

        df_proc = pd.read_csv(self.obj_minio.load_obj(self.config['fname_procedures']), sep='\t')
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
        - Load inputs
        - Annotate surgical dates from matched procedures
        - Save result to MinIO

        Returns:
        - df_final: Final annotated DataFrame
        """
        df_prior, df_summary, df_proc_g = self.load_data()
        df_final = self.estimate_surgical_dates(df_summary, df_prior, df_proc_g)
        self.obj_minio.save_obj(df=df_final, path_object=self.config['fname_save'], sep='\t')
        return df_final
