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

    def _data_model(self):
        self._col_path_rpt = 'PATH_REPORT_NOTE'
        self._col_accession_num = 'ACCESSION_NUMBER'

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
        df = pd.read_csv(pathfilename, header=0, low_memory=False, sep='\t')

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

        # Fix bad IDs
        df_path = self._fix_bad_ids(df=df_path)

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
        logic_exists = df_path['RPT_CHAR_LEN'] > 0
        df_path_1 = df_path[logic_exists]

        # Remove duplicate and dated reports
        df_path_1 = df_path_1.drop_duplicates()
        df_path_1 = df_path_1.sort_values(by=['PATH_RPT_ID', 'RPT_CHAR_LEN'], ascending=True)
        t = df_path_1['PATH_RPT_ID'].duplicated(keep=False)
        df_path_1a = df_path_1[~t]
        df_path_1b = df_path_1[t][df_path_1[t].duplicated(keep='last')]
        if df_path_1b.shape[0] > 0:
            df_path_f = pd.concat([df_path_1a, df_path_1b], axis=0, sort=False)
        else:
            df_path_f = df_path_1a

        return df_path_f

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
        df = df.drop(columns=cols_drop)

        return df

    def _fix_bad_ids(self, df):
        # fix ids
        fix1 = {1672375: 'P-0002845',
                1980825: 'P-0029444',
                1748211: 'P-0007944',
                1376523: pd.np.NaN}
        fix2 = {'P-0000000': pd.np.NaN,
                }
        fix3 = {'P-0000518': 'P-0009406',
                'P-0000213': 'P-0000306',
                'P-0000111': 'P-0008213'}

        t = df[['P_ID', 'SAMPLE_ID']]
        t1 = t[t['SAMPLE_ID'].notnull()]
        t1['DMP_ID'] = t1['SAMPLE_ID'].str[:9]
        df_ids = t1[['P_ID', 'DMP_ID']].drop_duplicates()

        df_ids.loc[df_ids['DMP_ID'] == list(fix2.keys())[0], 'DMP_ID'] = pd.np.NaN
        df_ids1 = df_ids[df_ids['DMP_ID'].notnull()]

        df_fix1 = pd.DataFrame.from_dict(fix1, orient='index').reset_index()
        for i in range(df_fix1.shape[0]):
            p_id = df_fix1.loc[i, 'index']
            dmp_id = df_fix1.loc[i, 0]
            df_ids1.loc[df_ids1['P_ID'] == p_id, 'DMP_ID'] = dmp_id

        df_fix3 = pd.DataFrame.from_dict(fix3, orient='index').reset_index()
        for i in range(df_fix3.shape[0]):
            dmp_id1 = df_fix3.loc[i, 'index']
            dmp_id2 = df_fix3.loc[i, 0]
            df_ids1.loc[df_ids1['DMP_ID'] == dmp_id1, 'DMP_ID'] = dmp_id2

        df_ids1 = df_ids1.drop_duplicates()

        # Drop old column and merge
        df = df.drop(columns='DMP_ID')
        df_f = df_ids1.merge(right=df, how='left', on='P_ID')

        return df_f

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
    import constants_darwin as c_dar
    from utils_darwin_etl import set_debug_console


    set_debug_console()
    obj_path = DarwinDiscoveryPathology(pathname=c_dar.pathname,
                             fname='Darwin_Discovery_Pathology_Reports_20201014.csv',
                             fname_out=c_dar.fname_darwin_path_clean)

    df = obj_path.return_df()
    tmp = 0

if __name__ == '__main__':
    main()

