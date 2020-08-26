""""
darwin_pathology.py

By Chris Fong - MSKCC 2018

 Requires data from Darwin Digital Platform, and the columns provided form the pathology endpoint
"""
import os
import pandas as pd
import numpy as np
from utils_darwin_etl import save_appended_df


class DarwinDiscoveryPathology(object):
    def __init__(self, pathname, fname, fname_out=None):
        self.pathname = pathname
        self.fname = fname

        self._df = None
        self._fname_out = fname_out

        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        df_path = self._load_data()

        df_path = self._clean_data(df=df_path)

        # Save data
        if self._fname_out is not None:
            save_appended_df(df=df_path, filename=self._fname_out, pathname=self.pathname)

        # Set as a member variable
        self._df = df_path

    def return_df(self):
        return self._df

    def _load_data(self):
        # Load pathology table
        print('Loading raw pathology table')
        pathfilename = os.path.join(self.pathname, self.fname)
        df = pd.read_csv(pathfilename, header=0, low_memory=False, sep='\t', nrows=100000)

        return df

    def _clean_data(self, df):
        # The goal for cleaning is to make each row an accession number

        # -----------
        # Fill 0s for MRNs
        df['MRN'] = df['MRN'].astype(str).str.zfill(8)

        # Group by accession numbers so sample ids are in a list
        df_g = df[df['SAMPLE_ID'].notnull()].groupby(['ACCESSION_NUMBER'])['SAMPLE_ID'].apply(list).reset_index()

        # Reformat table and add sample IDs
        df_ = df.drop(columns=['SAMPLE_ID']).drop_duplicates()
        df_ = df_.merge(right=df_g, how='left', on='ACCESSION_NUMBER')

        # Normalize labels of path report type
        df_path = self._split_path_data(df=df_)

        # # Compute length of note
        # rpt_len = df_path.loc[df_path['PATH_REPORT_NOTE'].notnull(), 'PATH_REPORT_NOTE'].apply(lambda x: len(x))
        # df_path = df_path.assign(RPT_CHAR_LEN=rpt_len)
        # df_path['RPT_CHAR_LEN'] = df_path['RPT_CHAR_LEN'].fillna(0)

        return df_path

    def _split_path_data(self, df):
        path_types = {'Surgical': 'Surgical',
                      'Cytology': 'Cyto',
                      'Molecular': 'Molecular',
                      'Hematopathology': 'Hemato'}
        col_report_type = 'REPORT_TYPE'

        # path = self.pathname
        df = df.assign(PATH_REPORT_TYPE_GENERAL=np.NaN)

        for i, current_path_key in enumerate(path_types.keys()):
            val = path_types[current_path_key]
            logic_report_type = df[col_report_type].str.contains(val).fillna(False)
            df.loc[logic_report_type, 'PATH_REPORT_TYPE_GENERAL'] = current_path_key

        return df


def main():
    import constants_darwin as c_dar
    from utils_darwin_etl import set_debug_console


    set_debug_console()
    DarwinDiscoveryPathology(pathname=c_dar.pathname,
                             fname='table_pathology.tsv',
                             fname_out=c_dar.fname_darwin_path_clean)

    tmp = 0

if __name__ == '__main__':
    main()

