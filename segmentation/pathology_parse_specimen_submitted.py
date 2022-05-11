""""
pathology_parse_specimen_submitted.py

By Chris Fong - MSKCC 2019

Parses specimen submitted column into individual parts

"""
import os
import sys  
sys.path.insert(0, '/mind_data/fongc2/pathology_report_segmentation')
import pandas as pd
from utils_pathology import parse_specimen_info, save_appended_df


class PathologyParseSpecSubmitted(object):
    def __init__(self, pathname, fname_path_parsed, col_spec_sub, list_cols_id, fname_save=None):
        self._pathname = pathname
        self._fname_path_parsed = fname_path_parsed
        self.fname_save = fname_save
        self._col_spec_sub = col_spec_sub
        self._list_cols_id = list_cols_id

        self._df_input = None
        self._df_parsed_spec_sub = None

        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        #Load data_conv
        self._load_data()

        df_sample_rpt = self._df_input
        print('Parsing Specimen List')

        # Remove Amended Diagnosis
        df_sample_rpt = self._remove_amended_diagnosis(df=df_sample_rpt)

        # Parse by number
        df, df_spec_parse_list_note = parse_specimen_info(df=df_sample_rpt, col_name=self._col_spec_sub)

        # Reorganize to place into a long format
        cols_id = self._list_cols_id
        df_spec_list = pd.concat([df[cols_id], df_spec_parse_list_note], axis=1, sort=False)
        df_spec_list_melt = pd.melt(frame=df_spec_list,
                                    value_vars=df_spec_parse_list_note.columns,
                                    id_vars=cols_id,
                                    value_name='SPECIMEN_SUBMITTED',
                                    var_name='SPECIMEN_NUMBER')
        # Remove rows with NA in SPECIMEN_SUBMITTED column
        df_spec_list_melt1 = df_spec_list_melt[df_spec_list_melt['SPECIMEN_SUBMITTED'].notnull()]
        # Merge with original acceesion number list to find gaps in parsing
        df_spec_list_f = df[cols_id].merge(right=df_spec_list_melt1, how='left', on=cols_id)

        self._df_parsed_spec_sub = df_spec_list_f

        # Save data
        if self.fname_save is not None:
            save_appended_df(df=self._df_parsed_spec_sub, filename=self.fname_save, pathname=self._pathname)

    def _remove_amended_diagnosis(self, df):
        key = 'amended diagnosis'
        log_nonull = df[self._col_spec_sub].notnull()
        ind_contains = df[self._col_spec_sub].str.lower().str.contains(key)

        if ind_contains.sum() > 0:
            t = df[log_nonull & ind_contains]
            ind = t[self._col_spec_sub].str.lower().str.find(key)
            t = t.assign(INDEX = ind)
            p = t.apply(lambda x: x[self._col_spec_sub][:x['INDEX']], axis=1)

            df.loc[p.index, self._col_spec_sub] = p

        return df

    def return_df(self):
        return self._df_parsed_spec_sub

    def _load_data(self):
        # Load pathology table
        print('Loading pathology table containing specimen submitted info')
        pathfilename = os.path.join(self._pathname, self._fname_path_parsed)
        df = pd.read_csv(pathfilename, header=0, low_memory=False)
        self._df_input = df

def main():
    import os
    import constants_darwin_pathology as c_dar
    from utils_pathology import set_debug_console


    set_debug_console()
    obj_mol = PathologyParseSpecSubmitted(pathname=c_dar.pathname,
                                          fname_path_parsed=c_dar.fname_darwin_path_clean,
                                          col_spec_sub='SPECIMEN_SUBMISSION_LIST',
                                          list_cols_id=['MRN', 'ACCESSION_NUMBER'],
                                          fname_save=c_dar.fname_darwin_path_col_spec_sub)

    df_m = obj_mol.return_df()

    tmp = 0

if __name__ == '__main__':
    main()
