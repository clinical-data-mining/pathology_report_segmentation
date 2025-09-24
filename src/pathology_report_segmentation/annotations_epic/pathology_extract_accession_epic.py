""""
pathology_extract_accession_epic.py

This script will extract accession numbers that are
buried in the part description column
"""
from msk_cdm.databricks import DatabricksAPI
from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
from msk_cdm.data_processing import mrn_zero_pad, set_debug_console, print_df_without_index

import re

import pandas as pd
import numpy as np

from msk_cdm.minio import MinioAPI


class PathologyExtractAccessionEpic(object):
    def __init__(
            self,
            fname_minio_env,
            fname,
            list_col_index,
            col_spec_sub,
            fname_save=None
    ):
        self._fname_minio_env = fname_minio_env
        self._fname = fname
        self._fname_save = fname_save
        self._obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

        # Column headers
        self._list_col_index = list_col_index
        self._col_spec_sub = col_spec_sub

        self._df = None
        self._df_original = None

        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        df_path = self._load_data()

        self._df_original = df_path

        df_path = self._compute_dop(df=df_path)

        # Save data
        if self._fname_save is not None:
            print('Saving %s' % self._fname_save)
            self._obj_minio.save_obj(df=df_path, path_object=self._fname_save, sep='\t')

        # Set as a member variable
        self._df = df_path

    def return_df(self):
        return self._df

    def return_df_original(self):
        return self._df_original

    def _load_data(self):
        print('Loading %s' % self._fname)
        obj = self._obj_minio.load_obj(path_object=self._fname)
        df = pd.read_csv(obj, header=0, low_memory=False, sep='\t')

        return df

    def _compute_dop(self, df):
        # Extract dates of procedure from a specified column of text, typically the specimen submitted section.
        # This script will first search for dates that fall after the text 'DOP:'
        # Next, if a date is not found, any date is considered a DOP, therefore giving precedence to text with 'DOP:'

        ## Constants
        list_col_index = self._list_col_index
        col_spec_sub = self._col_spec_sub
        col_DOP_0 = 'ACCESSION_EXTRACTED_0'
        col_DOP_mod_0 = col_DOP_0 + '_all'
        col_DOP_final = 'ACCESSION_NUMBER_EXTRACTED'

        ### Load spec sub data from all path files (DDP column)
        print('Extracting accession number from part description column')

        ### Use DOP within regex phrase first
        # Regex rule for dates with DOP in front
        # regex_str = r'DOP:[ ]*[\d]{1,2}/[\d]{1,2}/[\d]{2,4}'
        regex_rule_surg_accession = r'[MSKmsk]{3}[ ]{0,1}[\#]{0,1}[\"]{0,1}[\:]{0,1}[ ]*[s|S|c|C|h|H|m|M|f|F|DMG][\d]{1,2}[\-|\/]{1}[\d]{1,6}[\/\#]{0,1}[ ]{0,1}[\d]{0,2}'
        regex_rule_surg_accession_mod = r'[ ]*[A-Za-z]*[s|S|c|C|h|H|m|M][\d]{1,2}[\-|\/]{1}[\d]{1,6}[\/\#]{0,1}[ ]{0,1}[\d]{0,2}'

        df_dop = self._extract_accession_number(
            df=df,
            regex_str=regex_rule_surg_accession,
            col_spec_sub=col_spec_sub,
            list_col_index=list_col_index,
        )
        # Take the first date
        df_dop = df_dop[list_col_index + [col_DOP_0]]
        col_dop = list(set(df_dop.columns) - set(list_col_index))

        # Remove 'DOP:' from series
        df_dop[col_dop] = df_dop[col_dop].fillna('')
        df_dop_clean = df_dop.apply(lambda x: x.str.replace('MSK', '').str.replace('msk', '').str.replace(':',
                                                                                                          '').str.strip() if x.name in col_dop else x)
        df_dop_clean = df_dop_clean.replace('', np.NaN)

        ### Regex rule for dates WITHOUT MSK in front
        df_dop_mod = self._extract_accession_number(
            df=df,
            regex_str=regex_rule_surg_accession_mod,
            col_spec_sub=col_spec_sub,
            list_col_index=list_col_index
        )
        df_dop_mod = df_dop_mod.rename(columns={col_DOP_0: col_DOP_mod_0})
        df_dop_mod = df_dop_mod[list_col_index + [col_DOP_mod_0]]
        df_dop_mod[col_DOP_mod_0] = df_dop_mod[col_DOP_mod_0].fillna(value=np.NaN)

        # Merge the two df with DOPs
        df_dop_clean_f = pd.concat([df_dop_clean, df_dop_mod[col_DOP_mod_0]], axis=1, sort=False)

        # Create test df for QC
        r = df_dop_clean_f.merge(right=df, how='left', on=list_col_index)
        t = r[r[col_DOP_0].isnull() & r[col_DOP_mod_0].notnull()]

        # Fill blanks from df1 with df2
        df_dop_clean_f[col_DOP_0] = df_dop_clean_f[col_DOP_0].fillna(df_dop_clean_f[col_DOP_mod_0])

        # Drop column that was used to fill nulls.
        df_dop_clean_f = df_dop_clean_f.drop(columns=[col_DOP_mod_0])

        # Rename dop column
        df_dop_clean_f = df_dop_clean_f.rename(columns={col_DOP_0: col_DOP_final})

        # Split accession number and specimen number
        df_dop_clean_f[["SOURCE_ACCESSION_NUMBER", "SPECIMEN_NUMBER"]] = df_dop_clean_f[
            "ACCESSION_NUMBER_EXTRACTED"].str.extract(r'^([^/]+)(?:/(\d+))?$')

        return df_dop_clean_f

    def _extract_accession_number(
            self,
            df,
            regex_str,
            col_spec_sub,
            list_col_index
    ):
        # Extract DOP based on provided regex pattern.

        # Find the marker where the header of the substring ends
        # Extract string using regex input
        regex_series = df[col_spec_sub].apply(lambda x: re.findall(regex_str, str(x)))

        ln = regex_series.apply(lambda x: len(x))

        # Split the series if there are multiple accession numbers in the text find
        series_expand_all = pd.DataFrame(regex_series.values.tolist(), index=regex_series.index)

        cols_new = ['ACCESSION_EXTRACTED_' + str(x) for x in series_expand_all.columns]
        dict_cols = dict(zip(series_expand_all.columns, cols_new))
        series_expand_all = series_expand_all.rename(columns=dict_cols)

        # Merge
        df_sample_rpt_list1 = pd.concat(
            [df[list_col_index], series_expand_all],
            axis=1,
            sort=False
        )

        return df_sample_rpt_list1









