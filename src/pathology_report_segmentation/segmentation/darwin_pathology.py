""""
darwin_pathology.py

 Requires data from Darwin Digital Platform, and the columns provided form the pathology endpoint
"""
import pandas as pd
import numpy as np

from msk_cdm.minio import MinioAPI

pd.set_option("future.no_silent_downcasting", True)


class InitCleanPathology(object):
    def __init__(
            self,
            fname_minio_env,
            fname,
            fname_save=None
    ):
        self._fname = fname
        self._fname_out = fname_save
        self._df = None
        self._obj_minio = None
        self._bucket = None

        self._obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

        self._col_path_rpt = 'PATH_REPORT_NOTE'
        self._col_accession_num = 'ACCESSION_NUMBER'

        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        df_path = self._load_data()
        df_path = self._clean_data(df=df_path)

        # Save data
        if self._fname_out is not None:
            print('Saving %s' % self._fname_out)
            self._obj_minio.save_obj(df=df_path, 
                                     bucket_name=self._bucket, 
                                     path_object=self._fname_out, 
                                     sep='\t')

        # Set as a member variable
        self._df = df_path

    def return_df(self):
        return self._df

    def _load_data(self):
        # Load pathology table
        print('Loading %s' % self._fname)
        obj = self._obj_minio.load_obj(bucket_name=self._bucket, path_object=self._fname)
        df = pd.read_csv(obj, header=0, low_memory=False, sep='\t')

        return df

    def _clean_data(self, df):
        # The goal for cleaning is to
        # first find path reports with an impact sample attached to it.
        # Then, find all surgical and slide pathology reports associated with it.
        # Finally, using the original surgical path reports, find all associated reports - may contain Cytology reports

        # Get specimen submitted
        df = self._specimen_submitted(df=df)

        # -----------
        # From all pathology reports, only take the surgical or cytology reports
        df_path = self._select_pathology_columns(df=df)

        # Note - only 65% of the surgical procedure dates are provided
        # Tested 40 reports of date of collection vs date of procedure of the report - All had same dates
        # Find more surg procedure dates

        # Normalize labels of path report type
        df_path = self._split_path_data(df=df_path)

        # Compute length of note
        rpt_len = df_path.loc[df_path[self._col_path_rpt].notnull(), self._col_path_rpt].apply(lambda x: len(x))
        df_path = df_path.assign(RPT_CHAR_LEN=rpt_len)
        df_path['RPT_CHAR_LEN'] = df_path['RPT_CHAR_LEN'].fillna(0)

        # Remove rows where reports dont exist
#         logic_exists = df_path['RPT_CHAR_LEN'] > 0
#         df_path_1 = df_path[logic_exists]

        # Remove duplicate and dated reports
        df_path = df_path.drop_duplicates()

        return df_path

    def _specimen_submitted(self, df):
        logic_1 = df['PATH_REPORT_TYPE'].str.upper().str.contains('SURGICAL').fillna(False)
        #logic for M and DMG pathology reports
        logic_2 = df['ACCESSION_NUMBER'].str.contains('M').fillna(False)
        log_diag1 = df['PATH_REPORT_NOTE'].fillna('').str.contains(r'DIAGNOSTIC\sINTERPRETATION')
        log_diag2 = df['PATH_REPORT_NOTE'].fillna('').str.contains(r'I\sATTEST\sTHAT\sTHE\sABOVE')
        log_diag2_f = ~log_diag1 & log_diag2
        log_diag = log_diag1 | log_diag2_f
        logic_3 = df['PATH_REPORT_TYPE'].str.upper().str.contains('CYTOLOGY').fillna(False)
        logic_4 = df['PATH_REPORT_TYPE'].str.upper().str.contains('CYTOGENETICS').fillna(False)

        regex_pat_1 = r"(Specimens Submitted:.*?[\w\W]*)DIAGNOSIS:"
        regex_pat_2a = r"(Specimens Submitted:.*?[\w\W]*)(?=DIAGNOSTIC\sINTERPRETATION)"
        regex_pat_2b = r"(Specimens Submitted:.*?[\w\W]*)(?=I\sATTEST\sTHAT\sTHE\sABOVE)"
        regex_pat_3 = r"(Specimens Submitted:.*?[\w\W]*)CYTOLOGIC DIAGNOSIS:"
        regex_pat_4 = r"(Specimens Submitted:.*?[\w\W]*)(?=\r\n\r\n)"

        df['SPECIMEN_SUBMISSION_LIST'] = ''

        df_sub_list1 = df.loc[logic_1, 'PATH_REPORT_NOTE'].str.extract(regex_pat_1)
        index_1 = df_sub_list1.index
        # For M and DMG pathology reports
        df_sub_list2a = df.loc[logic_2 & (log_diag1), 'PATH_REPORT_NOTE'].str.extract(regex_pat_2a)
        index_2a = df_sub_list2a.index
        df_sub_list2b = df.loc[logic_2 & (log_diag2_f), 'PATH_REPORT_NOTE'].str.extract(regex_pat_2b)
        index_2b = df_sub_list2b.index

        df_sub_list3 = df.loc[logic_3, 'PATH_REPORT_NOTE'].str.extract(regex_pat_3)
        index_3 = df_sub_list3.index
        df_sub_list4 = df.loc[logic_4, 'PATH_REPORT_NOTE'].str.extract(regex_pat_4)
        index_4 = df_sub_list4.index

        df.loc[index_1, 'SPECIMEN_SUBMISSION_LIST'] = df_sub_list1.values
        df.loc[index_2a, 'SPECIMEN_SUBMISSION_LIST'] = df_sub_list2a.values
        df.loc[index_2b, 'SPECIMEN_SUBMISSION_LIST'] = df_sub_list2b.values
        df.loc[index_3, 'SPECIMEN_SUBMISSION_LIST'] = df_sub_list3.values
        df.loc[index_4, 'SPECIMEN_SUBMISSION_LIST'] = df_sub_list4.values

        df['SPECIMEN_SUBMISSION_LIST'] = df['SPECIMEN_SUBMISSION_LIST'].str.strip()

        return df

    def _select_pathology_columns(self, df):
        # This function will select relevant columns within the pathology report

        # -----------
        # Change name of IMPACT report date from 'Path Procedure Date' to 'DATE_OF_IMPACT_RPT' and
        # date of collection procedure to age
        # df = df.rename(columns={'Path Procedure Date': 'DTE_PATH_PROCEDURE',
        #                         'Path Report Type': 'PATH_REPORT_TYPE',
        #                         'PDRX_DMP_PATIENT_ID': 'DMP_ID',
        #                         'PDRX_DMP_SAMPLE_ID': 'SAMPLE_ID',
        #                         'SPEC_SUB_PRE': 'SPECIMEN_SUBMISSION_LIST',
        #                         'path_prpt_p1': self._col_path_rpt,
        #                         'Accession Number': self._col_accession_num,
        #                         'Associated Reports': 'ASSOCIATED_PATH_REPORT_ID',
        #                         'PRPT_PATH_RPT_ID': 'PATH_RPT_ID'
        #                         })
        df = df.rename(
            columns={
                'ASSOCIATED_REPORTS': 'ASSOCIATED_PATH_REPORT_ID'
            }
        )

        return df

    def _split_path_data(self, df):
        path_types = {'Surgical': 'Surgical',
                      'Cytology': 'Cyto',
                      'Molecular': 'Molecular',
                      'Hematopathology': 'Hemato'}
        col_report_type = 'PATH_REPORT_TYPE'

        # path = self.pathname
        df = df.assign(PATH_REPORT_TYPE_GENERAL=np.NaN)

        for i, current_path_key in enumerate(path_types.keys()):
            val = path_types[current_path_key]
            logic_report_type = df[col_report_type].str.contains(val).fillna(False)
            df.loc[logic_report_type, 'PATH_REPORT_TYPE_GENERAL'] = current_path_key

        return df

