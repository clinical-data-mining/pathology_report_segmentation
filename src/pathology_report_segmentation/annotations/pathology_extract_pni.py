""""
pathology_extract_pni.py

This script will search a pathology report text column and locate patient with PNI diagnosis

"""
import re
import pandas as pd


class PerineuralInvasionAnnotation(object):
    def __init__(self, df, col_pathology_note):
        # Filenames loaded in
        self._df_input = df
        self.col_pathology_note = col_pathology_note

        self._df_clean = None
        self._df_pni = None
        self._process_data()

    def _process_data(self):
        # Load the dataframe and/or manipulations object
        df = self._df_input

        # Clean text of PNI
        df_clean = self._clean_pni_text(df=df)

        # Classify PNI
        df = self._classify_pni(df=df_clean)

        self._df_pni = df
        self._df_clean = df_clean

    def return_df(self):
        return self._df_pni

    def _clean_pni_text(self, df):
        # Clean the dataset
        col_pathology_note = self.col_pathology_note
        logic_filter = df[self.col_pathology_note].notnull()
        df = df[logic_filter].copy()

        # Make all text upper case for consistency
        df[col_pathology_note] = df[col_pathology_note].str.upper()

        # Find the first 'DIAGNOSIS:' text and split
        # df[col_pathology_note] = df[col_pathology_note].apply(lambda x: x.split('DIAGNOSIS:', 1)[-1])

        bool_PNI1 = df[col_pathology_note].str.contains('NEURAL INVASION').fillna(False)
        bool_PNI2 = df[col_pathology_note].str.contains('(?<!\w)(PNI)(?!\w)').fillna(False)
        bool_PNI3 = df[col_pathology_note].str.contains('NERVE INV').fillna(False)

        bool_PNI = bool_PNI1 | bool_PNI2 | bool_PNI3

        df = df[bool_PNI].reset_index(drop=True)

        # Replace carriage return, multiple spaces, and colon with space
        df[col_pathology_note] = df[col_pathology_note].apply(lambda x: re.sub("(\:\ +\r\n)|(\:\r\n\ +)", ': ', x))
        # Replace carriage return and multiple spaces with space
        df[col_pathology_note] = df[col_pathology_note].apply(lambda x: re.sub("(\ +\r\n)|(\r\n\ +)", ' ', x))
        # Remove multiple spaces
        df[col_pathology_note] = df[col_pathology_note].apply(lambda x: re.sub(' +', ' ', x))
        # Remove space inbetween two carriage returns
        df[col_pathology_note] = df[col_pathology_note].apply(lambda x: re.sub("(?<=\r\n)(' ')(?=\r\n)", lambda y: y.group(0).replace(" ", ""), x))
        # Replace new line/carriage return with space if between letters or numbers
        # df[col_pathology_note] = df[col_pathology_note].apply(lambda x: re.sub("(?<=(\w)|(\d)|(\,))(\r\n)(?=(\w)|(\d))", lambda y: y.group(0).replace("\r\n", ' '), x))
        # Replace multiple new lines with one
        df[col_pathology_note] = df[col_pathology_note].apply(lambda x: re.sub("((\r\n)+)", "\r\n", x))
        # Replace ' - ' with '\n\r- ' (new line/dash/space)
        df[col_pathology_note] = df[col_pathology_note].apply(lambda x: re.sub("(\ \-\ )|(\ \-)", "\r\n\t- ", x))
        # Create new line after each sentence
        df[col_pathology_note] = df[col_pathology_note].apply(lambda x: re.sub("(\D\.\ )", lambda y: y.group(0).replace(". ", ".\r\n"), x))

        return df

    def _classify_pni(self, df):
        # Extract line containing the key terms
        obj_search = df[self.col_pathology_note].apply(lambda x: self._get_line_from_text(x=x))

        # Compute positive PNI
        key_pos = ['PERINEURAL INVASION IS SEEN', ': IDENTIFIED', 'PERINEURAL INVASION IS PRESENT',
                   'PERINEURAL INVASION IDENTIFIED', 'PERINEURAL INVASION IS IDENTIFIED', 'PERINEURAL INVASION PRESENT',
                   ': PRESENT', 'PERINEURAL INVASION IS NOTED', 'PERINEURAL INVASION POSITIVE',
                   'PERINEURAL INVASION ARE PRESENT', 'PERINEURAL INVASIONS ARE PRESENT', 'PERINEURAL INVASION NOTED',
                   'PERINEURAL INVASION ARE IDENTIFIED', 'PERINEURAL INVASION SEEN', 'IS ALSO PRESENT']
        key_neg = ['NOT IDENTIFIED', 'NO PERINEURAL INVASION IS SEEN', 'NO PERINEURAL INVASION IS IDENTIFIED',
                   'NO PERINEURAL INVASION IDENTIFIED', 'NO PERINEURAL INVASION IS PRESENT', ': ABSENT',
                   'NO PERINEURAL INVASION SEEN', 'NO PERINEURAL INVASION', ': NOT PRESENT', 'IS NOT SEEN']

        bool_pos = obj_search.str.contains('|'.join(key_pos)) & ~obj_search.str.contains('NO ')
        bool_neg = obj_search.str.contains('|'.join(key_neg))

        bool_pni = bool_pos | bool_neg
        bool_pni_test = bool_pos & bool_neg

        ind_neg = bool_neg[bool_neg == True].index
        ind_pos = bool_pos[bool_pos == True].index
        bad_classification_ind = len(set.intersection(set(ind_pos), set(ind_neg)))

        # Define PNI columns
        df = df.assign(PNI_PRESENT=pd.NA)
        df = df.assign(PNI_TEXT_SEGMENT=obj_search)

        df.loc[bool_pos, 'PNI_PRESENT'] = True
        df.loc[bool_neg, 'PNI_PRESENT'] = False

        bool_ext = (df.PNI_TEXT_SEGMENT.str.contains('EXTENSIVE')) & \
                   (df['PNI_PRESENT'] == True)
        bool_focal = (df.PNI_TEXT_SEGMENT.str.contains('|'.join(['FOCAL', 'SINGLE FOCUS']))) & \
                     (df['PNI_PRESENT'] == True)
        df = df.assign(PNI_EXTENSIVE=bool_ext)
        df = df.assign(PNI_FOCAL=bool_focal)

        return df

    def _get_line_from_text(self, x):
        # Search for line containing keywords
        y = re.search("[\\r\\n\\t]*[ \d\w-]+(NEURAL INVASION|PNI|NERVE INV)[\w\.: ]*[\\r\\n]*", x)
        # Get the line containing keywords
        try:
            l = y.span()
        except:
            l = 0
        s = x[l[0]:l[1]]
        # Remove leading and trailing zeros
        s = s.strip()

        return s

