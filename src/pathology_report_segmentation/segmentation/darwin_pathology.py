""""
darwin_pathology.py

 Requires data from Darwin Digital Platform, and the columns provided form the pathology endpoint
"""
from typing import Optional

import pandas as pd
import numpy as np

from msk_cdm.minio import MinioAPI

pd.set_option("future.no_silent_downcasting", True)

COL_DATE_SEQ = 'DTE_TUMOR_SEQUENCING'
COL_PATH_RPT = 'PATH_REPORT_NOTE'
COL_ACCESSION_NUMBER = 'ACCESSION_NUMBER'
COL_RPT_TYPE = 'PATH_REPORT_TYPE'
COL_SPECIMEN_SUBMISSION_LIST = 'SPECIMEN_SUBMISSION_LIST'


class InitCleanPathology(object):
    """
    A class to initialize and clean pathology data from the Darwin Digital Platform.

    This class handles loading, cleaning, and saving pathology data, which includes processing
    pathology reports, normalizing labels, and extracting relevant information based on
    provided columns and specifications.

    Attributes:
        fname_minio_env (str): The environment configuration for Minio API.
        fname (str): The path to the input data file.
        fname_save (str, optional): The path to save the cleaned data. Defaults to None.

    Methods:
        return_df(): Returns the cleaned dataframe.
    """
    def __init__(
            self,
            fname_minio_env: str,
            fname: str,
            fname_save: Optional[str] = None
    ):
        """
        Initializes the InitCleanPathology class with file paths and Minio environment.

        Args:
            fname_minio_env: The environment configuration for Minio API.
            fname: The path to the input data file.
            fname_save: The path to save the cleaned data. Defaults to None.
        """
        self._fname = fname
        self._fname_out = fname_save
        self._df = None
        self._obj_minio = None
        self._bucket = None

        self._obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

        self._process_data()

    def _process_data(self):
        """
        Loads, cleans, and saves the pathology data.

        This method loads the pathology data from Minio, cleans the data by filtering and
        normalizing columns, and then saves the cleaned data if a save path is provided.
        """
        # Use different loading process if clean path data set is accessible
        df_path = self._load_data()
        df_path = self._clean_data(df=df_path)

        # Save data
        if self._fname_out is not None:
            print('Saving %s' % self._fname_out)
            self._obj_minio.save_obj(
                df=df_path,
                bucket_name=self._bucket,
                path_object=self._fname_out,
                sep='\t'
            )

        # Set as a member variable
        self._df = df_path

    def return_df(self):
        """
        Returns the cleaned dataframe.

        Returns:
            pd.DataFrame: The cleaned pathology data.
        """
        return self._df

    def _load_data(
            self
    ) -> pd.DataFrame:
        """
        Loads the pathology data from Minio.

        Returns:
            pd.DataFrame: The loaded pathology data.
        """
        # Load pathology table
        print('Loading %s' % self._fname)
        obj = self._obj_minio.load_obj(
            bucket_name=self._bucket,
            path_object=self._fname
        )
        df = pd.read_csv(
            obj,
            header=0,
            low_memory=False,
            sep='\t'
        )

        return df

    def _clean_data(
            self,
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Cleans the pathology data by filtering, normalizing, and removing duplicates.

        Args:
            df (pd.DataFrame): The loaded pathology data.

        Returns:
            pd.DataFrame: The cleaned pathology data.
        """
        # The goal for cleaning is to
        # first find path reports with an impact sample attached to it.
        # Then, find all surgical and slide pathology reports associated with it.
        # Clean date of sequencing
        # Finally, using the original surgical path reports, find all associated reports - may contain Cytology reports

        # Get specimen submitted
        df = self._specimen_submitted(df=df)

        # -----------
        # From all pathology reports, only take the surgical or cytology reports
        df_path = self._select_pathology_columns(df=df)

        # Normalize labels of path report type
        df_path = self._split_path_data(df=df_path)

        # Compute length of note
        rpt_len = df_path.loc[df_path[COL_PATH_RPT].notnull(), COL_PATH_RPT].apply(lambda x: len(x))
        df_path = df_path.assign(RPT_CHAR_LEN=rpt_len)
        df_path['RPT_CHAR_LEN'] = df_path['RPT_CHAR_LEN'].fillna(0)

        # Clean date of sequencing
        df_path[COL_DATE_SEQ] = pd.to_datetime(df_path[COL_DATE_SEQ], errors='coerce').dt.date

        # Remove duplicate and dated reports
        df_path = df_path.drop_duplicates()

        return df_path

    def _specimen_submitted(
            self,
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Extracts and adds a list of specimen submissions from the pathology reports.

        Args:
            df: The loaded pathology data.

        Returns:
            pd.DataFrame: The dataframe with added specimen submission list.
        """
        logic_1 = df[COL_RPT_TYPE].str.upper().str.contains('SURGICAL').fillna(False)
        #logic for M and DMG pathology reports
        logic_2 = df[COL_ACCESSION_NUMBER].str.contains('M').fillna(False)
        log_diag1 = df[COL_PATH_RPT].fillna('').str.contains(r'DIAGNOSTIC\sINTERPRETATION')
        log_diag2 = df[COL_PATH_RPT].fillna('').str.contains(r'I\sATTEST\sTHAT\sTHE\sABOVE')
        log_diag2_f = ~log_diag1 & log_diag2
        log_diag = log_diag1 | log_diag2_f
        logic_3 = df[COL_RPT_TYPE].str.upper().str.contains('CYTOLOGY').fillna(False)
        logic_4 = df[COL_RPT_TYPE].str.upper().str.contains('CYTOGENETICS').fillna(False)

        regex_pat_1 = r"(Specimens Submitted:.*?[\w\W]*)DIAGNOSIS:"
        regex_pat_2a = r"(Specimens Submitted:.*?[\w\W]*)(?=DIAGNOSTIC\sINTERPRETATION)"
        regex_pat_2b = r"(Specimens Submitted:.*?[\w\W]*)(?=I\sATTEST\sTHAT\sTHE\sABOVE)"
        regex_pat_3 = r"(Specimens Submitted:.*?[\w\W]*)CYTOLOGIC DIAGNOSIS:"
        regex_pat_4 = r"(Specimens Submitted:.*?[\w\W]*)(?=\r\n\r\n)"

        df[COL_SPECIMEN_SUBMISSION_LIST] = ''

        df_sub_list1 = df.loc[logic_1, COL_PATH_RPT].str.extract(regex_pat_1)
        index_1 = df_sub_list1.index
        # For M and DMG pathology reports
        df_sub_list2a = df.loc[logic_2 & (log_diag1), COL_PATH_RPT].str.extract(regex_pat_2a)
        index_2a = df_sub_list2a.index
        df_sub_list2b = df.loc[logic_2 & (log_diag2_f), COL_PATH_RPT].str.extract(regex_pat_2b)
        index_2b = df_sub_list2b.index

        df_sub_list3 = df.loc[logic_3, COL_PATH_RPT].str.extract(regex_pat_3)
        index_3 = df_sub_list3.index
        df_sub_list4 = df.loc[logic_4, COL_PATH_RPT].str.extract(regex_pat_4)
        index_4 = df_sub_list4.index

        df.loc[index_1, COL_SPECIMEN_SUBMISSION_LIST] = df_sub_list1.values
        df.loc[index_2a, COL_SPECIMEN_SUBMISSION_LIST] = df_sub_list2a.values
        df.loc[index_2b, COL_SPECIMEN_SUBMISSION_LIST] = df_sub_list2b.values
        df.loc[index_3, COL_SPECIMEN_SUBMISSION_LIST] = df_sub_list3.values
        df.loc[index_4, COL_SPECIMEN_SUBMISSION_LIST] = df_sub_list4.values

        df[COL_SPECIMEN_SUBMISSION_LIST] = df[COL_SPECIMEN_SUBMISSION_LIST].str.strip()

        return df

    def _select_pathology_columns(
            self,
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Renames and selects relevant columns within the pathology report.

        Args:
            df: The loaded pathology data.

        Returns:
            pd.DataFrame: The dataframe with selected and renamed columns.
        """
        df = df.rename(
            columns={
                'ASSOCIATED_REPORTS': 'ASSOCIATED_PATH_REPORT_ID'
            }
        )

        return df

    def _split_path_data(
            self,
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Normalizes and splits pathology report types into general categories.

        Args:
            df : The cleaned pathology data.

        Returns:
            pd.DataFrame: The dataframe with normalized pathology report types.
        """
        path_types = {'Surgical': 'Surgical',
                      'Cytology': 'Cyto',
                      'Molecular': 'Molecular',
                      'Hematopathology': 'Hemato'}
        col_report_type = COL_RPT_TYPE

        # path = self.pathname
        df = df.assign(PATH_REPORT_TYPE_GENERAL=np.NaN)

        for i, current_path_key in enumerate(path_types.keys()):
            val = path_types[current_path_key]
            logic_report_type = df[col_report_type].str.contains(val).fillna(False)
            df.loc[logic_report_type, 'PATH_REPORT_TYPE_GENERAL'] = current_path_key

        return df

