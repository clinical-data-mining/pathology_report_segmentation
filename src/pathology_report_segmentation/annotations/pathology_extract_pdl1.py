""""
pathology_extract_pdl1.py

This script will extract PD-L1 related annotations

"""
import re

import pandas as pd
import numpy as np

from msk_cdm.minio import MinioAPI
from msk_cdm.data_processing import mrn_zero_pad, drop_cols



class PathologyExtractPDL1(object):
    def __init__(
        self,
        minio_env, 
        fname, 
        col_text, 
        fname_save=None
    ):
        self._fname = fname
        self._col_text = col_text
        self._fname_save = fname_save
        self._df_extracted = None
        self._obj_minio = MinioAPI(fname_minio_env=minio_env)
        
        self._cols_drop = [
             'PATH_RPT_ID', 
             'ASSOCIATED_PATH_REPORT_ID', 
             'DMP_ID', 
             'SAMPLE_ID', 
             'SPECIMEN_SUBMISSION_LIST', 
             'PATH_REPORT_TYPE_GENERAL', 
             'RPT_CHAR_LEN']
        
        self._cols_logicals = [
            'HAS_PDL1_POS', 
            'HAS_PDL1_NEG', 
            'HAS_PDL1_PERC', 
            'HAS_PDL1_CPS_1', 
            'HAS_PDL1_CPS_2', 
            'HAS_PDL1_TPS_1',
            'HAS_PDL1_TPS_2',
            'HAS_PDL1_IPS'
        ]
        
        self._cols_extract = [
            'PDl1_PERCENTAGE', 
            'PDl1_CPS_1', 
            'PDl1_CPS_2', 
            'PDl1_TPS_1', 
            'PDl1_TPS_2', 
            'PDl1_IPS'
        ]
        
        self._col_text_split = 'PDL1_TEXT'
        self._col_text_split_display = 'PDL1_TEXT_DISPLAY'
        self._col_mentions_pdl1 = 'MENTIONS_PDL1'
        self._text_buffer = 200  # Character buffer when splitting text for "PD-L1"
        
        # Regex
        self._regex_pdl1 = r'pd[-]*l[-]*1'
        self._regex_perc = r"([<>=]*[ ]*[\d+\.\d+-]*\d+[\.\d+]*?[ ]*%)"
        self._regex_digit_match = r"([<>=]*\d+[\.\d+]*[/]*[\d]*)"
        self._regex_neg = r"(?:no[\s]*labeling[\s]*is[\s]*seen[\s]*in[\s]*tumor[\s]*cells|negative)"
        self._regex_cps_1 = "([<>=]*\d+[\.\d]*?)(?=\s*\D*of 100)" 
        self._regex_cps_2 = r"combined[\s]*positiv[eity]+[\s]*score[^><=0-9]*"
        self._regex_tps_1 = r"tumor[\s]*proportion[\s]*score[^><=0-9]*"
        self._regex_tps_2 = r"[percentagecontribution]{10,12}[\s]*of[\s]*tumor[\s]*cells[^><=0-9]*"
        self._regex_ips = r"[percentagecontribution]{10,12}[\s]*of[\s]*[inflammatoryimmune]{6,12}[^><=0-9]*"
        self._regex_pos = r"positive \(cps >=1|positive \(>=1"
        
        self._process_data()

    def _process_data(self):
        df = self._load_data()
        
        # Add annotation for PD-L1 mentions
        df = self._add_pdl1_mention(df)
        
        # Add annotations for antibody used
        df = self._add_antibody(df=df)
        
        # Add annotations for key words
        df = self._add_regex_available(df)
        
        # Add regex extracted data
        df = self._add_annotations(df=df)
        
        # Add column to indicate extraction needs debugging
        df = self._add_needs_review_anno(df=df)
        
        # Clean columns
        cols_drop = self._cols_drop
        df = drop_cols(df=df, cols=cols_drop)
        df = df[df[self._col_mentions_pdl1] == True]
        
        # Add pd-l1 positive annotation
        df = self._combine_pdl1_score(
            df_pdl1=df
        )

        # Save data
        if self._fname_save is not None:
            print('Saving %s' % self._fname_save)
            self._obj_minio.save_obj(
                df=df, 
                path_object=self._fname_save, 
                sep='\t'
            )

        # Set as a member variable
        self._df_extracted = df

    def return_extraction(self):
        return self._df_extracted
    
    def _load_data(self):
        obj = self._obj_minio.load_obj(path_object=self._fname)
        df = pd.read_csv(obj, sep='\t', low_memory=False)
        df[self._col_text] = df[self._col_text].str.lower()
        
        df = mrn_zero_pad(df=df, col_mrn='MRN')
        
        return df
    
    def _add_pdl1_mention(self, df):
        # Adds relevant text and T/F annotation for availability
        has_pdl1 = df[self._col_text].str.contains(self._regex_pdl1, regex=True)
        
        get_pdl1_text = lambda x: ''.join(['MARKER' + y[:self._text_buffer] for y in re.split(self._regex_pdl1, x)[1:]])
        get_pdl1_text_display = lambda x: ''.join(['PD-L1' + y[:self._text_buffer] for y in re.split(self._regex_pdl1, x)[1:]])
        
        text = df.loc[has_pdl1, self._col_text].apply(lambda z: get_pdl1_text(z))
        text = text.rename(self._col_text_split)
        
        text_display = df.loc[has_pdl1, self._col_text].apply(lambda z: get_pdl1_text_display(z))
        text_display = text.rename(self._col_text_split_display)
        
        kwargs = {self._col_mentions_pdl1: has_pdl1}
        df = df.assign(**kwargs)
        df = pd.concat([df, text, text_display], axis=1, sort=False)
        
        return df
    
    def _add_regex_available(self, df):
        pattern_neg = re.compile(self._regex_neg)
        pattern_cps_2 = re.compile(self._regex_cps_2)
        pattern_tps = re.compile(self._regex_tps_1)
        pattern_tps_2 = re.compile(self._regex_tps_2)
        pattern_ips = re.compile(self._regex_ips)
        pattern_pos = re.compile(self._regex_pos)
        
        series_text = df.loc[df[self._col_mentions_pdl1] == True, self._col_text_split]        
        logic_available = series_text.apply(lambda y: [re.search(pattern_pos, y) is not None, \
                                                       re.search(pattern_neg, y) is not None, \
                                                       '%' in y, \
                                                       'of 100' in y, re.search(pattern_cps_2, y) is not None, 
                                                       re.search(pattern_tps, y) is not None, 
                                                       re.search(pattern_tps_2, y) is not None, 
                                                       re.search(pattern_ips, y) is not None
                                                      ] 
                                           )

        df_available = pd.DataFrame(logic_available.tolist(), index= logic_available.index, columns=self._cols_logicals)
        df_path_f = pd.concat([df, df_available], axis=1, sort=False)
        df_path_f[self._cols_logicals] = df_path_f[self._cols_logicals].fillna(False)

        return df_path_f
    
    def _add_anno_template(self, df, regex_pattern, col_text, col_rename, col_filter_by):
        a = df[df[col_filter_by] == True]
        pattern = re.compile(regex_pattern, re.IGNORECASE)
        pdl1_anno = a[col_text].apply(lambda z: re.findall(regex_pattern, z)).rename(col_rename)

        df = pd.concat([df, pdl1_anno], axis=1, sort=False)

        return df
    
    def _add_annotations(self, df):
        # Add percentages
        regex_pattern=self._regex_perc
        df = self._add_anno_template(df=df, regex_pattern=regex_pattern, col_filter_by='HAS_PDL1_PERC', col_text=self._col_text_split, col_rename='PDl1_PERCENTAGE')
        
        # Add CPS (v1)
        regex_pattern=self._regex_cps_1
        df = self._add_anno_template(df=df, regex_pattern=regex_pattern, col_filter_by='HAS_PDL1_CPS_1', col_text=self._col_text_split, col_rename='PDl1_CPS_1')
        
        # Add CPS (v2)
        regex_pattern=self._regex_cps_2+self._regex_digit_match
        df = self._add_anno_template(df=df, regex_pattern=regex_pattern, col_filter_by='HAS_PDL1_CPS_2', col_text=self._col_text_split, col_rename='PDl1_CPS_2')
        
        # Add TPS (v1)
        regex_pattern=self._regex_tps_1+self._regex_digit_match
        df = self._add_anno_template(df=df, regex_pattern=regex_pattern, col_filter_by='HAS_PDL1_TPS_1', col_text=self._col_text_split, col_rename='PDl1_TPS_1')
        
        # Add TPS (v2)
        regex_pattern=self._regex_tps_2+self._regex_digit_match
        df = self._add_anno_template(df=df, regex_pattern=regex_pattern, col_filter_by='HAS_PDL1_TPS_2', col_text=self._col_text_split, col_rename='PDl1_TPS_2')
    
        # Add IPS
        regex_pattern=self._regex_ips+self._regex_digit_match
        df = self._add_anno_template(df=df, regex_pattern=regex_pattern, col_filter_by='HAS_PDL1_IPS', col_text=self._col_text_split, col_rename='PDl1_IPS')
        
        return df
    
    def _add_antibody(self, df):
        df['E1L3N'] = df[self._col_text].str.contains('e1l3n')
        df['SP-142'] = df[self._col_text].str.contains('sp-142|sp142',regex=True)
        df['SP-263'] = df[self._col_text].str.contains('sp-263|sp263',regex=True)
        df['CD-2764'] = df[self._col_text].str.contains('cd2764',regex=True)
        df['22C3'] = df[self._col_text].str.contains('22c3',regex=True)
        
        df[self._col_text_split] = df[self._col_text_split].str.replace(r'e1l3n|sp-142|sp142|sp-263|sp263|cd2764|22c3', 'ANTIBODY')
        
        return df
    
    def _add_needs_review_anno(self, df):
        l1 = (df['MENTIONS_PDL1'] == True) 
        l2 = (df['HAS_PDL1_POS'] == False)
        l3 = (df['HAS_PDL1_NEG'] == False)
        l4 = (df['PDl1_PERCENTAGE'].isnull()) 
        l5 = (df['PDl1_CPS_1'].isnull())
        l6 = (df['PDl1_CPS_2'].isnull())
        l7 = (df['PDl1_TPS_1'].isnull())
        l8 = (df['PDl1_TPS_2'].isnull())
        l10 = (df['PDl1_IPS'].isnull())
        l9 = (df['PATH_REPORT_TYPE'].str.contains('Surg'))
        logic_missing = l1 & l2 & l3 & l4 & l5 & l6 & l7 & l8 & l9 & l10
        df = df.assign(DEBUG_REQUIRED=logic_missing)
        
        return df
    
    def _combine_pdl1_score(self, df_pdl1):
        """
        This function cycles through columns where % were collected and cleans the values.
        PDL1_POSITIVE is a Yes/No annotation where Yes occurs when 'PDl1_PERCENTAGE_EST', 'PDl1_TPS_1_EST' or 'PDl1_TPS_2_EST' >=1
        """
        cols_extract = self._cols_extract
        df_pdl1_anno = df_pdl1.copy()

        for col_current in cols_extract:
            new_col = col_current + '_EST'
            # Get usable rows in current column
            length_current = df_pdl1_anno.loc[df_pdl1_anno[col_current].notnull(), col_current].apply(lambda x: len(x))
            index_current = length_current[length_current >0].index
            
            # Clean values
            pct_current = df_pdl1_anno.loc[index_current, col_current].apply(lambda x: x[0]).str.strip()
            pct_current = pct_current.str.replace('%', '').str.replace(' ', '').str.replace('<1', '0').str.replace('>1', '1').str.replace('>=1', '1').str.replace('>', '').str.replace('<', '').str.replace('=', '')

            # Takes second value in range
            pct_range = pct_current[pct_current.str.contains('-')].str.split('-').apply(lambda x: x[1])
            pct_current[pct_current.str.contains('-')] = pct_range


            pct_current = pd.to_numeric(pct_current, errors='coerce')
            df_pdl1_anno[new_col] = pct_current

        PDL1_POSITIVE = (df_pdl1_anno['PDl1_PERCENTAGE_EST'] >= 1) | (df_pdl1_anno['PDl1_TPS_1_EST'] >= 1) | (df_pdl1_anno['PDl1_TPS_2_EST'] >= 1)
        PDL1_POSITIVE_NULL = df_pdl1_anno['PDl1_PERCENTAGE_EST'].isnull() & df_pdl1_anno['PDl1_TPS_1_EST'].isnull() & df_pdl1_anno['PDl1_TPS_2_EST'].isnull()
        df_pdl1_anno['PDL1_POSITIVE'] = PDL1_POSITIVE
        df_pdl1_anno = df_pdl1_anno.replace({'PDL1_POSITIVE': {True: 'Yes', False: 'No'}})
        df_pdl1_anno.loc[PDL1_POSITIVE_NULL, 'PDL1_POSITIVE'] = np.NaN
        
        return df_pdl1_anno
    

