""""
pathology_extract_dop.py

By Chris Fong - MSKCC 2020

This script will extract accession numbers that are
buried in specimen submitted columns, or another specified column of text data

"""
import os
import sys 
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
import re
import pandas as pd
import numpy as np
from minio_api import MinioAPI
from utils import read_minio_api_config


class PathologyExtractDOP(object):
    def __init__(self, fname_minio_env, fname, col_label_access_num, col_label_spec_num, col_spec_sub, fname_save=None, list_accession=None):
        self._fname_minio_env = fname_minio_env
        self._fname = fname
        self._fname_save = fname_save
        self._list_accession = list_accession
        self._obj_minio = None
        self._bucket = None

        # Column headers
        self._col_label_access_num = col_label_access_num
        self._col_label_spec_num = col_label_spec_num
        self._col_spec_sub = col_spec_sub

        self._df = None
        self._df_original = None
        
        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        self._init_minio()
        df_path = self._load_data()

        # Take subset of accession numbers
        if self._list_accession is not None:
            df_path = df_path[df_path[self._col_label_access_num].isin(self._list_accession)]

        self._df_original = df_path

        df_path = self._compute_dop(df=df_path)

        # Save data
        if self._fname_save is not None:
            print('Saving %s' % self._fname_save)
            self._obj_minio.save_obj(df=df_path, bucket_name=self._bucket, path_object=self._fname_save, sep='\t')

        # Set as a member variable
        self._df = df_path

    def return_df(self):
        return self._df

    def return_df_original(self):
        return self._df_original
    
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
        print('Loading %s' % self._fname)
        obj = self._obj_minio.load_obj(bucket_name=self._bucket, path_object=self._fname)
        df = pd.read_csv(obj, header=0, low_memory=False, sep='\t')

        return df

    def _compute_dop(self, df):
        # Extract dates of procedure from a specified column of text, typically the specimen submitted section.
        # This script will first search for dates that fall after the text 'DOP:'
        # Next, if a date is not found, any date is considered a DOP, therefore giving precedence to text with 'DOP:'

        ## Constants
        col_label_access_num = self._col_label_access_num
        col_label_spec_num = self._col_label_spec_num
        col_spec_sub = self._col_spec_sub
        col_DOP_0 = 'DATE_EXTRACTED_0'
        col_DOP_mod_0 = col_DOP_0 + '_all'
        col_DOP_final = 'DATE_OF_PROCEDURE_SURGICAL'

        ### Load spec sub data from all path files (DDP column)
        print('Extracting Date of Procedure in specimen submitted column')

        # Fill specimen part numbers with 1 if null
        df[col_label_spec_num] = df[col_label_spec_num].fillna(1)

        ### Use DOP within regex phrase first
        # Regex rule for dates with DOP in front
        regex_str = r'DOP:[ ]*[\d]{1,2}/[\d]{1,2}/[\d]{2,4}'
        df_dop = self._extract_date_of_procedure(df=df,
                                                regex_str=regex_str,
                                                col_spec_sub=col_spec_sub,
                                                col_label_access_num=col_label_access_num,
                                                col_label_spec=col_label_spec_num)
        # Take the first date
        df_dop = df_dop[[col_label_access_num, col_label_spec_num, col_DOP_0]]
        col_dop = list(set(df_dop.columns) - set([col_label_access_num, col_label_spec_num]))

        # Remove 'DOP:' from series
        df_dop[col_dop] = df_dop[col_dop].fillna('')
        df_dop_clean = df_dop.apply(lambda x: x.str.replace('DOP:', '').str.strip() if x.name in col_dop else x)
        df_dop_clean = df_dop_clean.replace('', np.NaN)

        ### Regex rule for dates WITHOUT DOP in front
        regex_str = r'[ ]*[\d]{1,2}/[\d]{1,2}/[\d]{2,4}'
        df_dop_mod = self._extract_date_of_procedure(df=df,
                                                    regex_str=regex_str,
                                                    col_spec_sub=col_spec_sub,
                                                    col_label_access_num=col_label_access_num,
                                                    col_label_spec=col_label_spec_num)
        df_dop_mod = df_dop_mod.rename(columns={col_DOP_0: col_DOP_mod_0})
        df_dop_mod = df_dop_mod[[col_label_access_num, col_label_spec_num, col_DOP_mod_0]]
        df_dop_mod[col_DOP_mod_0] = df_dop_mod[col_DOP_mod_0].fillna(value=np.NaN)

        # Merge the two df with DOPs
        df_dop_clean[col_label_spec_num] = df_dop_clean[col_label_spec_num].astype(int).astype(object)
        df_dop_clean = df_dop_clean.reset_index(drop=True)
        df_dop_mod[col_label_spec_num] = df_dop_mod[col_label_spec_num].astype(int).astype(object)
        df_dop_mod = df_dop_mod.reset_index(drop=True)
        # Merge
        df_dop_clean_f = pd.concat([df_dop_clean, df_dop_mod[col_DOP_mod_0]], axis=1, sort=False)

        # Create test df for QC
        df[col_label_spec_num] = df[col_label_spec_num].astype(int).astype(object)
        r = df_dop_clean_f.merge(right=df, how='left', on=[col_label_access_num, col_label_spec_num])
        t = r[r[col_DOP_0].isnull() & r[col_DOP_mod_0].notnull()]

        # Fill blanks from df1 with df2
        df_dop_clean_f[col_DOP_0] = df_dop_clean_f[col_DOP_0].fillna(df_dop_clean_f[col_DOP_mod_0])

        # Drop column that was used to fill nulls.
        df_dop_clean_f = df_dop_clean_f.drop(columns=[col_DOP_mod_0])

        # Rename dop column
        df_dop_clean_f = df_dop_clean_f.rename(columns={col_DOP_0: col_DOP_final})

        # Convert to datetime
        dop_error = df_dop_clean_f[col_DOP_final]
        df_dop_clean_f[col_DOP_final] = pd.to_datetime(df_dop_clean_f[col_DOP_final], errors='coerce')

        # Create column for miswritten text
        dop_error_f = dop_error[df_dop_clean_f[col_DOP_final].isnull() & dop_error.notnull()]
        df_dop_clean_f = df_dop_clean_f.assign(DOP_DATE_ERROR=dop_error_f)

        return df_dop_clean_f

    def _extract_date_of_procedure(self, df, regex_str, col_spec_sub, col_label_access_num, col_label_spec):
        # Extract DOP based on provided regex pattern.

        # Find the marker where the header of the substring ends
        # Extract string using regex input
        regex_series = df[col_spec_sub].apply(lambda x: re.findall(regex_str, str(x)))

        ln = regex_series.apply(lambda x: len(x))

        # Split the series if there are multiple accession numbers in the text find
        series_expand_all = pd.DataFrame(regex_series.values.tolist(), index=regex_series.index)

        cols_new = ['DATE_EXTRACTED_' + str(x) for x in series_expand_all.columns]
        dict_cols = dict(zip(series_expand_all.columns, cols_new))
        series_expand_all = series_expand_all.rename(columns=dict_cols)

        # Merge
        df_sample_rpt_list1 = pd.concat([df[[col_label_access_num, col_label_spec]], series_expand_all],
                                        axis=1, sort=False)

        return df_sample_rpt_list1

def main():
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'cdm-utilities')))
    from data_classes_cdm import CDMProcessingVariables as c_dar

    ## Constants
    col_label_access_num = 'ACCESSION_NUMBER'
    col_label_spec_num = 'SPECIMEN_NUMBER'
    col_spec_sub = 'SPECIMEN_SUBMITTED'

    # Extract DOP
    obj_p = PathologyExtractDOP(fname_minio_env=c_dar.minio_env,
                                fname=c_dar.fname_darwin_path_col_spec_sub,
                                col_label_access_num=col_label_access_num,
                                col_label_spec_num=col_label_spec_num,
                                col_spec_sub=col_spec_sub,
                                list_accession=None,
                                fname_save=c_dar.fname_spec_part_dop)

    df = obj_p.return_df()

    tmp = 0

if __name__ == '__main__':
    main()


