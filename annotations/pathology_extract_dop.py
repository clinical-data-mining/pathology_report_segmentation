""""
pathology_extract_dop.py

By Chris Fong - MSKCC 2020

This script will extract accession numbers that are
buried in specimen submitted columns, or another specified column of text data

"""
import os
import re
import pandas as pd
from utils_pathology import save_appended_df


class PathologyExtractDOP(object):
    def __init__(self, pathname, fname, col_label_access_num, col_label_spec_num, col_spec_sub, fname_out=None, list_accession=None):
        self.pathname = pathname
        self.fname = fname
        self._list_accession = list_accession

        # Column headers
        self._col_label_access_num = col_label_access_num
        self._col_label_spec_num = col_label_spec_num
        self._col_spec_sub = col_spec_sub

        self._df = None
        self._df_original = None
        self._fname_out = fname_out

        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        df_path = self._load_data()

        # Take subset of accession numbers
        if self._list_accession is not None:
            df_path = df_path[df_path[self._col_label_access_num].isin(self._list_accession)]

        self._df_original = df_path

        df_path = self._compute_dop(df=df_path)

        # Save data
        if self._fname_out is not None:
            save_appended_df(df=df_path, filename=self._fname_out, pathname=self.pathname)

        # Set as a member variable
        self._df = df_path

    def return_df(self):
        return self._df

    def return_df_original(self):
        return self._df_original

    def _load_data(self):
        # Load pathology table
        pathfilename = os.path.join(self.pathname, self.fname)
        df = pd.read_csv(pathfilename, header=0, low_memory=False, sep=',')

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
        df_dop_clean = df_dop_clean.replace('', pd.np.nan)

        ### Regex rule for dates WITHOUT DOP in front
        regex_str = r'[ ]*[\d]{1,2}/[\d]{1,2}/[\d]{2,4}'
        df_dop_mod = self._extract_date_of_procedure(df=df,
                                                    regex_str=regex_str,
                                                    col_spec_sub=col_spec_sub,
                                                    col_label_access_num=col_label_access_num,
                                                    col_label_spec=col_label_spec_num)
        df_dop_mod = df_dop_mod.rename(columns={col_DOP_0: col_DOP_mod_0})
        df_dop_mod = df_dop_mod[[col_label_access_num, col_label_spec_num, col_DOP_mod_0]]
        df_dop_mod[col_DOP_mod_0] = df_dop_mod[col_DOP_mod_0].fillna(value=pd.np.nan)

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
    import constants_darwin_pathology as c_dar
    from utils_darwin_etl import set_debug_console
    from darwin_pathology import DarwinDiscoveryPathology

    ## Constants
    col_label_access_num = 'ACCESSION_NUMBER'
    col_label_spec_num = 'SPECIMEN_NUMBER'
    col_spec_sub = 'SPECIMEN_SUBMITTED'

    set_debug_console()

    # Extract DOP
    obj_p = PathologyExtractDOP(pathname=c_dar.pathname,
                                fname=c_dar.fname_darwin_path_col_spec_sub,
                                col_label_access_num=col_label_access_num,
                                col_label_spec_num=col_label_spec_num,
                                col_spec_sub=col_spec_sub,
                                list_accession=None,
                                fname_out='pathology_spec_part_dop.csv')

    df = obj_p.return_df()

    tmp = 0

if __name__ == '__main__':
    main()


