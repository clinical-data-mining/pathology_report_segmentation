""""
    pathology_parsing_surgical_specimens.py

    By Chris Fong - MSKCC 2019

"""
import os
import re
import pandas as pd
import numpy as np
from utils_pathology import save_appended_df
import sys
# import builder


class ParseSurgicalPathologySpecimens(object):
    def __init__(self, pathname, fname_darwin_pathology_parsed, fname_out_pathology_specimens_parsed=None):
        # Member variables
        self.pathname = pathname
        self.fname_darwin_pathology_parsed = fname_darwin_pathology_parsed
        self.fname_darwin_path_clean_parsed_specimen = fname_out_pathology_specimens_parsed

        # Data frames
        self._df_path_parsed = None
        self._df_path_parsed_specimens_long = None

        # Process the data -- load the impact pathology file (cleaned), and then parse the table
        self._process_data()

    def return_df_parsed(self):
        return self._df_path_parsed

    def return_df_parsed_spec(self):
        return self._df_path_parsed_specimens_long

    def _load_data(self):
        pathfilename = os.path.join(self.pathname, self.fname_darwin_pathology_parsed)
        df_path = pd.read_csv(pathfilename, header=0)

        return df_path

    def get_synoptic(dx):
        """
        :param dx: diagnosis (string)
        :return: concept, value (dictionary)
        """
        if type(dx) == str:
            dx = '"""\n' + dx + '\n"""'

            keys = builder.get_key(dx)

            dict_of_k_v = {}

            for ind, i in enumerate(range(len(keys))):
                try:
                    if i != len(keys) - 1:
                        current_item, next_item = keys[i], keys[i + 1]
                    else:
                        current_item, next_item = keys[i], '"""'
                except Exception:
                    print('Could not find end key')
                    sys.exit()
                current_item_updated = builder.cleanup_for_regex(current_item)
                next_item_updated = builder.cleanup_for_regex(next_item)

                key = re.findall("([A-Za-z0-9].*[^:])", keys[i])
                value = builder.get_value(current_item_updated, next_item_updated, dx)

                if len(value) == 0:
                    value = ['EMPTY']
                else:
                    value = [' '.join(value[0].split())]

                if len(key) == 0:
                    key = ['EMPTY']

                dict_of_k_v[key[0].strip(':').strip()] = value[0].strip()
                dx = builder.set_key_in_item(current_item_updated, dx)

            return dict_of_k_v

    def _process_data(self):
        # Process the data -- load the impact pathology file (cleaned), and then parse the table
        # Load the data, if parsed data exists, load that first
        df_path_parsed = self._load_data()
        self._df_path_parsed = df_path_parsed

        # TODO: Refactor surgical_pathology_parsing such that specimen submitted parsing is done in this class

        # Parse path dx section for specimen
        col_name = 'PATH_NOTE_PATH_DX'
        df_path_parsed = self._parse_path_dx_section(df=df_path_parsed, col_name=col_name)

        # Elongate the specimen descriptions from dicts to a longer format dataframe
        df_path_parsed_long = self._elongate_path_spec(df=df_path_parsed)

        if self.fname_darwin_path_clean_parsed_specimen is not None:
            fname_parsed_spec = self.fname_darwin_path_clean_parsed_specimen
            save_appended_df(df=df_path_parsed_long, filename=fname_parsed_spec, pathname=self.pathname)

        # Make member variable
        self._df_path_parsed_specimens_long = df_path_parsed_long

    def _parse_path_dx_section(self, df, col_name):
        # Add number of specimens by finding sample number in text with \n and :, then find largest number
        # Remove white space at ends of path dx note
        # df = df.head(500)
        print('Parsing Pathology Diagnosis Section')
        logic_notnull = df[col_name].notnull()
        df = df.groupby(['ACCESSION_NUMBER']).first().reset_index()

        df[col_name] = df.loc[logic_notnull, col_name].apply(lambda x: x.strip())

        # Clean path DX section
        # Take bracketed segments and remove new line carriage return
        regex_brackets = '\([^)]*\)'
        regex = re.compile(regex_brackets)
        fn_rmv_cr = lambda x: regex.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
        df[col_name] = df.loc[logic_notnull, col_name].apply(fn_rmv_cr)

        # regex_new line removal - TODO: Redo this just for the title
        regex_nl = '([a-z](\\r\\n){1}[a-z])'
        regex_2 = re.compile(regex_nl)
        fn_rmv_ln = lambda x: regex_2.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
        df[col_name] = df.loc[logic_notnull, col_name].apply(fn_rmv_ln)

        # regex_new line removal around measurements
        regex_nl = '((\\r\\n){1}[\d]{1}.[\d]{1})'
        regex_2 = re.compile(regex_nl)
        fn_rmv_ln = lambda x: regex_2.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
        df[col_name] = df.loc[logic_notnull, col_name].apply(fn_rmv_ln)

        # regex_new line removal that are around dates
        regex_nl2 = '([:]{1}[ ]*[\\r\\n]+[ ]*[\d]{1,2}[/]{1}[\d]{1,2}[/]{1}[\d]{1,4}[ ]*[:]{1})'
        regex_3 = re.compile(regex_nl2)
        fn_rmv_ln2 = lambda x: regex_3.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
        df[col_name] = df.loc[logic_notnull, col_name].apply(fn_rmv_ln2)

        # Find segments of specimens and their descriptions, typically like 1. (Description) : \r\n2. (etc)
        regex_rule_spec_num = '(^[\d]{1}[.)]{1}[ A-Za-z]+[\da-zA-Z.,\'";:#/() \-\&]+[:.]{0,1}[ ]*[\\r\\n]+|[\\r\\n]+[ ]*[\d]{1,2}[.)]{1}[ A-Za-z]+[\da-zA-Z.,\'";:#/() \-\&]+[:.]{0,1}[ ]*[\\r\\n]+)'
        regex_spec_titles = re.compile(regex_rule_spec_num)
        df_path_dx_list = df.loc[logic_notnull, col_name]

        # Find specimen description in list
        lambda_find_title = lambda str: regex_spec_titles.findall(str)
        list_spec_header_list = df_path_dx_list.apply(lambda_find_title)

        # Clean list_spec_header_list by removing white space and new lines
        regex_1 = '^[\d]{1,2}[.)]{1}'
        regex_num_header = re.compile(regex_1)
        lambda_parse_header = lambda x: [regex_num_header.sub('', y.strip()).strip() for y in x]
        list_spec_header_list_clean1 = list_spec_header_list.apply(lambda_parse_header)

        # Remove the colon at the end of the headers
        regex_2 = ':$'
        regex_end_colon = re.compile(regex_2)
        lambda_remove_colon = lambda x: [regex_end_colon.sub('', y) for y in x]
        list_spec_header_list_clean = list_spec_header_list_clean1.apply(lambda_remove_colon)

        # Compute number of samples according to parsed samples in path dx
        list_spec_length = list_spec_header_list_clean.apply(lambda x: len(x))

        # Get specimen number used and make into a list
        regex_rule_spec_num2 = '^[\d]{1,2}'
        r = re.compile(regex_rule_spec_num2)
        lambda_get_spec_num_str = lambda y: list(map(lambda x: r.findall(x.strip())[0], y))
        list_spec_number_listing_str = list_spec_header_list.apply(lambda_get_spec_num_str)
        # Convert to int
        lambda_spec_num_int = lambda y: list(map(lambda x: int(x), y))
        list_spec_number_listing = list_spec_number_listing_str.apply(lambda_spec_num_int)
        # Compute the MAX sample number in the list -- this will help determine if there is header out of format
        lambda_spec_num_max = lambda x: max(x) if len(x) > 0 else 0
        list_spec_number_max = list_spec_number_listing[list_spec_length != 0].apply(lambda_spec_num_max)

        # Add the spec count attributes to df
        df = df.assign(PATH_DX_SPEC_LENGTH=list_spec_length)
        df = df.assign(PATH_DX_SPEC_MAX=list_spec_number_max)

        df['PATH_DX_SPEC_LENGTH'] = df['PATH_DX_SPEC_LENGTH'].fillna(0)
        df['PATH_DX_SPEC_MAX'] = df['PATH_DX_SPEC_MAX'].fillna(0)

        # # Create dictionaries for specimen label number and descriptions
        # s = pd.Series(index=list_spec_number_listing.index, dtype=object)
        # for ind in list_spec_number_listing.index:
        #     if len(list_spec_number_listing.loc[ind]) > 0:
        #         d = dict(zip(list_spec_number_listing_str.loc[ind], list_spec_header_list_clean.loc[ind]))
        #         s[ind] = d
        # df = df.assign(PATH_DX_SPEC_TITLES=s)

        # Extract the specimen notes from the individual specimens by
        # Splitting the path dx text
        # Locate the titles of the specimens in the split
        # Use list comprehension to extract the note
        # Create a dictionary of the header to the text below it

        # Split path dx note by title
        lambda_split_path_dx = lambda str: regex_spec_titles.split(str)
        split_list_path_dx = df_path_dx_list.apply(lambda_split_path_dx)

        # Remove first item in list of the split since it's always blank
        split_list_path_dx = split_list_path_dx.apply(lambda x: x[1:])

        # Use list comprehension to extract only the "even" (index= 1,3,5...) items in list, which is the specimen notes
        lambda_extract_even_index = lambda y: [y[x].strip() for x in range(1, len(y)) if x % 2 == 1]
        path_dx_notes = split_list_path_dx.apply(lambda_extract_even_index)

        size_list = list_spec_header_list_clean.shape[0]
        df = df.assign(PATH_DX_DICT=None)
        for k in range(size_list):
            dct = {x: {str(y): str(z)} for x, y, z in zip(list_spec_number_listing.iloc[k], list_spec_header_list_clean.iloc[k], path_dx_notes.iloc[k])}

            # Put into dataframe
            df.at[k, 'PATH_DX_DICT'] = dct

        # # For notes that could not be parsed due to no specimen number label, take entire text
        # logic_spec_num = (df['PATH_DX_SPEC_LENGTH'] == 0) & \
        #                  (df['PATH_SPEC_SUBMITTED_LENGTH'] == 1)
        # index_1_spec_note = df.loc[logic_spec_num].index
        # list_spec_number_listing.loc[index_1_spec_note] = list_spec_number_listing.loc[index_1_spec_note].apply(lambda x: [1])
        # path_dx_notes.loc[index_1_spec_note] = df_path_dx_list.loc[index_1_spec_note]

        # # Create dictionaries for specimen label number and descriptions
        # s = pd.Series(index=list_spec_number_listing.index, dtype=object)
        # for ind in list_spec_number_listing.index:
        #     if len(list_spec_number_listing.loc[ind]) > 0:
        #         d = dict(zip(list_spec_number_listing_str.loc[ind], path_dx_notes.loc[ind]))
        #         s[ind] = d
        # df = df.assign(PATH_DX_SPEC_NOTES=s)

        return df

    def _elongate_path_spec(self, df):
        print('Elongating Parsed Pathology Data')
        col_access = 'ACCESSION_NUMBER'

        # # Elongate the dataframe so each row is a specimen
        # logic_notnull = df['PATH_SPEC_SUBMITTED_DICT'].notnull()
        # df.loc[logic_notnull, 'PATH_SPEC_SUBMITTED_DICT'] = df.loc[logic_notnull, 'PATH_SPEC_SUBMITTED_DICT'].apply(lambda x: eval(x))

        size_list = df.shape[0]
        list_df_path_dx = [None]*size_list
        list_df_spec_sub = [None]*size_list
        col_df_spec = [col_access, 'PATH_DX_SPEC_NUM', 'PATH_DX_SPEC_TITLE', 'PATH_DX_SPEC_DESC']
        for k in range(size_list):
            accession_num = df[col_access].iloc[k]
            if df['PATH_DX_SPEC_LENGTH'].iloc[k] > 0:
                dct = df['PATH_DX_DICT'].iloc[k]

                # Make path dx into dataframe
                t = pd.DataFrame.from_dict({i: [accession_num, i, j, dct[i][j]] for i in dct.keys() for j in dct[i].keys()},
                                           orient='index',
                                           columns=col_df_spec)

            else:
                t = pd.DataFrame.from_dict({-1: [accession_num, np.NaN, np.NaN, np.NaN]},
                                           orient='index',
                                           columns=col_df_spec)
            #
            # # Unpack the specimen submitted column
            # col_spec_sub = ['PATH_SPEC_SUB_SPEC_NUM', 'PATH_SPEC_SUBMITTED', col_access]
            # # log1 = df['PATH_SPEC_SUBMITTED_LENGTH'].iloc[k] == df['PATH_SPEC_SUBMITTED_MAX'].iloc[k]
            # log2 = df['PATH_DX_SPEC_LENGTH'].iloc[k] == df['PATH_DX_SPEC_MAX'].iloc[k]
            # # log3 = df['PATH_SPEC_SUBMITTED_LENGTH'].iloc[k] == df['PATH_DX_SPEC_LENGTH'].iloc[k]
            #
            # # if log1 & log2 & log3 & logic_notnull[k]:
            # if log2:
            #     t_spec = pd.DataFrame.from_dict(df['PATH_SPEC_SUBMITTED_DICT'].iloc[k], orient='index')
            #     t_spec = t_spec.reset_index()
            #     t_spec = t_spec.rename(columns={'index': col_spec_sub[0],
            #                                     0: col_spec_sub[1]})
            #     t_spec = t_spec.assign(ACCESSION_NUMBER=accession_num)
            # else:
            #     t_spec = pd.DataFrame.from_dict({-1: [np.NaN, np.NaN, accession_num]},
            #                                     orient='index',
            #                                     columns=col_spec_sub)
            #

            # Put dataframe into list
            list_df_path_dx[k] = t
            # list_df_spec_sub[k] = t_spec

        df_spec = pd.concat(list_df_path_dx)
        # df_spec_sub = pd.concat(list_df_spec_sub)

        # Reset index to obtain accession numbers that weren't parsed
        df_spec = df_spec.reset_index()
        # df_spec_sub = df_spec_sub.reset_index()

        # # Create column for incomplete parsing
        # parsing_completed = df_spec['index'] == -1
        # df_spec = df_spec.assign(PARSING_INCOMPLETE_PATH_DX=parsing_completed)
        df_spec = df_spec.drop(columns=['index'])

        # parsing_completed_sub = df_spec_sub['index'] == -1
        # df_spec_sub = df_spec_sub.assign(PARSING_INCOMPLETE_SPEC_SUB=parsing_completed_sub)
        # df_spec_sub = df_spec_sub.drop(columns=['index'])

        df_spec['PATH_DX_SPEC_NUM'] = df_spec['PATH_DX_SPEC_NUM'].fillna(0)
        # df_spec_sub['PATH_SPEC_SUB_SPEC_NUM'] = df_spec_sub['PATH_SPEC_SUB_SPEC_NUM'].apply(lambda x: float(x))
        # df_spec_sub['PATH_SPEC_SUB_SPEC_NUM'] = df_spec_sub['PATH_SPEC_SUB_SPEC_NUM'].fillna(0)

        # # Merge parsed path dx and spec submitted
        # L = df_spec.merge(right=df_spec_sub, how='left',
        #                   right_on=[col_access, 'PATH_SPEC_SUB_SPEC_NUM'],
        #                   left_on=[col_access, 'PATH_DX_SPEC_NUM'])
        # L['PARSING_INCOMPLETE_SPEC_SUB'] = L['PARSING_INCOMPLETE_SPEC_SUB'].fillna(True)

        # For incomplete parsing, merge dictionary
        # ids_missing_path_dx = L.loc[L['PARSING_INCOMPLETE_PATH_DX'] == True, col_access]
        # path_dx_note_missing = df.loc[df[col_access].isin(ids_missing_path_dx), [col_access, 'PATH_NOTE_PATH_DX']]

        # ids_missing_spec_sub = L.loc[L['PARSING_INCOMPLETE_SPEC_SUB'] == True, col_access]
        # path_spec_sub_missing = df.loc[df[col_access].isin(ids_missing_spec_sub), [col_access, 'PATH_NOTE_SPEC_SUB']]

        # L1 = L.merge(right=path_dx_note_missing, how='left', on=col_access)
        # L2 = L1.merge(right=path_spec_sub_missing, how='left', on=col_access)

        # # For notes that could not be parsed due to no specimen number label, take entire text
        # logic_spec_num = (df['PATH_DX_SPEC_LENGTH'] == 0) & \
        #                  (df['PATH_SPEC_SUBMITTED_LENGTH'] == 1)
        # index_1_spec_note = df.loc[logic_spec_num].index
        # list_spec_number_listing.loc[index_1_spec_note] = list_spec_number_listing.loc[index_1_spec_note].apply(lambda x: [1])
        # path_dx_notes.loc[index_1_spec_note] = df_path_dx_list.loc[index_1_spec_note]

        return df_spec

    def _check_spec_header_info(self, df):
        # Compute statistics on completeness and correctness across the multiple sources that contains the
        # description of the specimen
        col_baseline = 'SAMPLE_COLLECTION_RPT_SPEC_COUNT'
        df[col_baseline] = df[col_baseline].fillna(0)
        df_check = df[['SAMPLE_COLLECTION_SPEC_SUB', 'PATH_SPEC_SUBMITTED_DICT', 'PATH_DX_SPEC_TITLES',
                       col_baseline, 'PATH_SPEC_SUBMITTED_LENGTH', 'PATH_SPEC_SUBMITTED_MAX',
                       'PATH_DX_SPEC_LENGTH', 'PATH_DX_SPEC_MAX']]

        # Count which column has most complete info on spec
        count_nulls = df_check[
            ['SAMPLE_COLLECTION_SPEC_SUB', 'PATH_SPEC_SUBMITTED_DICT', 'PATH_DX_SPEC_TITLES']].isnull().sum()

        # Check the specimen counts for each category
        df_check1 = df[df[col_baseline] != 0]
        t1 = (df_check1[col_baseline] != df_check1['PATH_SPEC_SUBMITTED_LENGTH']).sum()
        t2 = (df_check1[col_baseline] != df_check1['PATH_SPEC_SUBMITTED_MAX']).sum()
        t3 = (df_check1[col_baseline] != df_check1['PATH_DX_SPEC_LENGTH']).sum()
        t4 = (df_check1[col_baseline] != df_check1['PATH_DX_SPEC_MAX']).sum()

        logic_check1 = df['PATH_SPEC_SUBMITTED_LENGTH'] != 0
        logic_check2 = df['PATH_DX_SPEC_LENGTH'] != 0
        df_check2 = df[logic_check1 & logic_check2]
        s1 = (df_check2['PATH_SPEC_SUBMITTED_LENGTH'] != df_check2['PATH_SPEC_SUBMITTED_MAX']).sum()
        s2 = (df_check2['PATH_DX_SPEC_LENGTH'] != df_check2['PATH_DX_SPEC_MAX']).sum()
        s3 = (df_check2['PATH_SPEC_SUBMITTED_LENGTH'] != df_check2['PATH_DX_SPEC_LENGTH']).sum()
        s4 = (df_check2['PATH_SPEC_SUBMITTED_MAX'] != df_check2['PATH_DX_SPEC_MAX']).sum()

        num_missing_path_spec = (~logic_check1).sum()
        num_missing_path_dx = (~logic_check2).sum()

        return None


def main():
    import constants_darwin_pathology as cd
    from utils_pathology import set_debug_console

    set_debug_console()

    
    pathname = cd.pathname
    fname_out_pathology_specimens_parsed = cd.fname_darwin_path_clean_parsed_specimen
    fname_darwin_pathology_parsed = cd.fname_darwin_path_surgical
    obj_parse = ParseSurgicalPathologySpecimens(pathname=pathname,
                                                fname_darwin_pathology_parsed=fname_darwin_pathology_parsed,
                                                fname_out_pathology_specimens_parsed=fname_out_pathology_specimens_parsed)

    df_surg_path_parsed_spec = obj_parse.return_df_parsed_spec()
    print(df_surg_path_parsed_spec.head())

if __name__ == '__main__':
    main()
