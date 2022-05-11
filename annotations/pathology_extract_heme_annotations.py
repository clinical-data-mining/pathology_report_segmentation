""""
 key_value_pair_extraction

 By Chris Fong - MSKCC 2021

    This object will read in a file (dataframe) containing
"""
import os
import sys
import re
sys.path.insert(0, '/mind_data/fongc2/pathology_report_segmentation/')
import pandas as pd
import numpy as np
from utils_pathology import save_appended_df


class PathologyKeyValuePairExtraction(object):
    def __init__(self, Series, ):
        self._fname = fname
        self._fname_out = fname_out

        # Column headers
        self._col_dx_spec_desc = col_dx_spec_desc
        self._list_col_index = list_col_index
        self._col_out_dict = col_out_dict

        self._df_key_value = df
        self._regex_key_rmv = [':', '-']
        self._regex_key = ['[\\r\\n]{0,3}[- ]*[ \w\(\)\-\+\;\,\'\.]+[:]+']

        self._process_data()
        
    def return_df(self):
        return self._df_key_value

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        if self._df_key_value is None:
            df_path = self._load_data()

            df_test = df_path[df_path[self._col_dx_spec_desc].notnull()]
            kv_dict = df_path[self._col_dx_spec_desc].apply(lambda x: self._create_key_value_pairs(dx=x))
            kwargs = {self._col_out_dict : lambda x: kv_dict}
            df_path = df_path.assign(**kwargs)
            
            df_path = df_path[self._list_col_index + [self._col_out_dict]]

            self._df_key_value = df_path

            if self._fname_out is not None:
                save_appended_df(df=df_path, filename=self._fname_out, pathname='')
        
    def _load_data(self):
        # Load pathology table
        df = pd.read_csv(self._fname, header=0, low_memory=False, sep=',')

        return df
        
    def _create_key_value_pairs(self, dx):
        subs = self._regex_key
        regex = re.compile(r"({0})\s*(.*?)(?=\s*(?:{0}|$))".format("|".join(subs)))
        matches = dict(regex.findall(dx))
        dict_out = self._strip_results(dictionary=matches)

        return dict_out
    
    def _strip_results(self, dictionary):
        rmv_list = self._regex_key_rmv
        keys = [word.translate({ord(x): ' ' for x in rmv_list}).strip() for word, initial in dictionary.items()]
        values = [initial.strip() for initial, initial in dictionary.items()]
        dict_out = dict(zip(keys, values))

        return dict_out
    
    
    def extract_term(self, list_key_terms, key_label='VARIABLE', fname_save=None):
        value_name = 'RESULT_' + key_label
        var_name = 'COL_NAME_' + key_label
        
        df = self.return_df()
        df_test = df[df[self._col_out_dict].astype(str).str.upper().str.contains('|'.join(list_key_terms))]

        
        new_df = pd.DataFrame(df_test[self._col_out_dict].tolist(), index=df_test.index)
        new_df = pd.concat([df_test[self._list_col_index], new_df], axis=1)
        
        logic_cols_use = new_df.columns.str.upper().str.contains('|'.join(list_key_terms))
        cols_use = new_df.columns[logic_cols_use].to_list()
        t = new_df[self._list_col_index + cols_use].dropna(how='all', axis=0)
        t_melt = t.melt(id_vars=self._list_col_index, value_vars=cols_use, value_name=value_name, var_name=var_name)
        t_melt = t_melt[t_melt[value_name].notnull()]
        
        if fname_save is not None:
            save_appended_df(df=t_melt, filename=fname_save, pathname='')
        
        return t_melt
    

    
    