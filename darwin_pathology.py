""""
darwin_pathology.py

By Chris Fong - MSKCC 2018

 Requires data from Darwin Digital Platform, and the columns provided form the pathology endpoint
"""
import os
import sys
sys.path.insert(0, '/mind_data/fongc2/cdm-utilities/')
sys.path.insert(0, '/mind_data/fongc2/cdm-utilities/minio_api')
import pandas as pd
import numpy as np
from minio_api import MinioAPI
from utils import read_minio_api_config


class InitCleanPathology(object):
    def __init__(self, fname_minio_env, fname, fname_save=None):
        self._fname_minio_env = fname_minio_env
        self._fname = fname
        self._fname_out = fname_save
        self._df = None
        self._obj_minio = None
        self._bucket = None

        self._col_path_rpt = 'PATH_REPORT_NOTE'
        self._col_accession_num = 'ACCESSION_NUMBER'

        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        self._init_minio()
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
    
    def _init_minio(self):
        # Setup Minio configuration
        minio_config = read_minio_api_config(fname_env=self._fname_minio_env)
        ACCESS_KEY = minio_config['ACCESS_KEY']
        SECRET_KEY = minio_config['SECRET_KEY']
        CA_CERTS = minio_config['CA_CERTS']
        URL_PORT = minio_config['URL_PORT']
        BUCKET = minio_config['BUCKET']
        self._bucket = BUCKET

        self._obj_minio = MinioAPI(ACCESS_KEY=ACCESS_KEY, 
                                     SECRET_KEY=SECRET_KEY, 
                                     ca_certs=CA_CERTS, 
                                     url_port=URL_PORT)
        return None

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

        # Combine path note part1 and part2
        df = self._combine_path_note_parts(df=df)

        # Select and drop columns for summary table
        df = df.rename(columns={'PDRX_DMP_SAMPLE_ID': 'SAMPLE_ID'})

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
        df_path = df_path.sort_values(by=['PATH_RPT_ID', 'RPT_CHAR_LEN'], ascending=True)

        return df_path

    def _combine_path_note_parts(self, df):
        # Combine path note part1 and part2
        logic_subset = df['path_prpt_p2'].notnull()
        path_note_combined = df.loc[logic_subset, 'path_prpt_p1'] + df.loc[df.path_prpt_p2.notnull(), 'path_prpt_p2']
        df.loc[logic_subset, 'path_prpt_p1'] = path_note_combined
        # Drop the path note part 2 column
        df = df.drop(columns=['path_prpt_p2'])

        return df

    def _select_pathology_columns(self, df):
        # This function will select relevant columns within the pathology report

        # -----------
        # Change name of IMPACT report date from 'Path Procedure Date' to 'DATE_OF_IMPACT_RPT' and
        # date of collection procedure to age
        df = df.rename(columns={'Path Procedure Date': 'DTE_PATH_PROCEDURE',
                                'Path Report Type': 'PATH_REPORT_TYPE',
                                'PDRX_DMP_PATIENT_ID': 'DMP_ID',
                                'PDRX_DMP_SAMPLE_ID': 'SAMPLE_ID',
                                'SPEC_SUB_PRE': 'SPECIMEN_SUBMISSION_LIST',
                                'path_prpt_p1': self._col_path_rpt,
                                'Accession Number': self._col_accession_num,
                                'Associated Reports': 'ASSOCIATED_PATH_REPORT_ID',
                                'PRPT_PATH_RPT_ID': 'PATH_RPT_ID'
                                })

        # Drop some of the sample rpt df columns
        cols_drop = ['Aberrations', 'Aberration Count', 'Report Type']
        cols_drop1 = [x for x in df.columns if x in cols_drop]
        df = df.drop(columns=cols_drop1)

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

def main():
    import constants_darwin_pathology as c_dar
    

    obj_path = InitCleanPathology(fname_minio_env=c_dar.minio_env,
                             fname=c_dar.fname_path_ddp,
                             fname_save=c_dar.fname_darwin_path_clean)

    df = obj_path.return_df()
    tmp = 0

if __name__ == '__main__':
    main()

