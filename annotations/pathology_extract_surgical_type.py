""""
 pathology_extraction_method.py

 By Chris Fong - MSKCC 2018

 This script is a test to extract biopsy and resection for all impact samples
"""
import os
import pandas as pd
import numpy as np
from utils_darwin_etl import save_appended_df


class PathologyImpactSpecimenExtraction(object):
    def __init__(self, pathname, fname_path_summary, list_cols_spec, fname_save):
        self._pathname = pathname
        self._fname_path_summary = fname_path_summary
        self._fname_save = fname_save

        # Columns to analyze
        self._list_cols_spec = list_cols_spec
        self._col_name_biopsy = 'IS_BIOPSY'
        self._col_name_resection = 'IS_RESECTION'
        self._col_id = 'SAMPLE_ID'

        # Dataframes
        self._df_path_summary = None

        # Keywoards
        self._resec_keywords = None
        self._bx_keywords = None

        self._process_data()

    def _process_data(self):
        # Load data
        df_path_summary = self._load_data()

        # Load keywords
        self._load_keywords()

        # Create annotations
        df = self._create_annotations(df_path_summary=df_path_summary)

        # Set as member variable
        self._df_path_summary = df

        # Save appended path summary table
        save_appended_df(df=df, pathname=self._pathname, filename=self._fname_save)

        return None

    def _load_keywords(self):
        resec_keywords = ['resec', 'ctomy', 'turp', 'turbt', 'segment', 'excis', 'whipple', 'seminal vesicles',
                          'dissection', 'mass', 'uterus, cervix, bilateral', ' and ovary', 'wedge',
                          'uterus and cervix', 'sigmoid and rectum', 'pancreas and spleen']

        bx_keywords = ['biop', 'biopsy', 'biosy', 'boispy', 'biospy', 'bx', 'needle', 'punch', 'core', 'washing',
                       'cyto', 'shave', 'sentinel', 'CT guided', 'incisional', 'aspiration', 'fna']

        self._resec_keywords = resec_keywords
        self._bx_keywords = bx_keywords

    def _load_data(self):
        pathfilename_summary = os.path.join(self._pathname, self._fname_path_summary)
        df_path_summary = pd.read_csv(pathfilename_summary, header=0)

        return df_path_summary

    def return_summary(self):
        return self._df_path_summary

    def _create_annotations(self, df_path_summary):
        # Add column if is a resection
        resec_keywords = self._resec_keywords
        bx_keywords = self._bx_keywords

        # Create copy to manipulate and then merge later
        df_path = df_path_summary.copy()

        col_name_biopsy = self._col_name_biopsy
        col_name_resection = self._col_name_resection
        col_id = self._col_id
        # TODO: Create loop to analyze multiple columns, for now assume there is only 1 value in list
        col_names_text = self._list_cols_spec

        # Remove leading text from dmp spec submitted list
        df_path[col_names_text[0]] = df_path[col_names_text[0]].str.replace('Specimens Submitted:\r\n', '')

        is_resection1 = df_path[col_names_text[0]].str.lower().str.contains('|'.join(resec_keywords))
        is_resection = is_resection1
        # is_resection2 = df_path[col_names_text[1]].str.lower().str.contains('|'.join(resec_keywords))
        # is_resection3 = df_path[col_names_text[2]].str.lower().str.contains('|'.join(resec_keywords))
        # is_resection = is_resection1 | is_resection2 | is_resection3
        kwargs = {col_name_resection: is_resection}
        df_path = df_path.assign(**kwargs)
        # df_path = df_path.assign(IS_RESECTION=is_resection)

        # Add column if procedure was a biopsy
        is_biopsy1 = df_path[col_names_text[0]].str.lower().str.contains('|'.join(bx_keywords))
        is_biopsy = is_biopsy1
        # is_biopsy2 = df_path[col_names_text[1]].str.lower().str.contains('|'.join(bx_keywords))
        # is_biopsy3 = df_path[col_names_text[2]].str.lower().str.contains('|'.join(bx_keywords))
        # is_biopsy = is_biopsy1 | is_biopsy2 | is_biopsy3
        kwargs = {col_name_biopsy: is_biopsy}
        df_path = df_path.assign(**kwargs)
        # df_path = df_path.assign(IS_BIOPSY=is_biopsy)

        cols_path_subset = [col_id, col_name_biopsy, col_name_resection]
        df_path_subset = df_path[cols_path_subset]

        df_path_summary = df_path_summary.merge(right=df_path_subset, how='left', on='SAMPLE_ID')

        # If IS_BIOPSY and IS_RESECTION are both false, then change to resection
        unkn_logic = (df_path_summary[col_name_biopsy] == False) & (df_path_summary[col_name_resection] == False)
        df_path_summary.loc[unkn_logic, col_name_biopsy] = pd.np.NaN
        df_path_summary.loc[unkn_logic, col_name_resection] = pd.np.NaN
        # Likely biopsy
        likely_biopsy_logic = (df_path_summary[col_name_biopsy] == True) & (df_path_summary[col_name_resection] == True)
        df_path_summary.loc[likely_biopsy_logic, col_name_biopsy] = True
        df_path_summary.loc[likely_biopsy_logic, col_name_resection] = False

        # Replace values
        df_path_summary[col_name_biopsy] = df_path_summary[col_name_biopsy].replace({True: 'biopsy', False: np.NaN})

        # Combine the two columns
        df_path_summary = df_path_summary.assign(IMPACT_SPECIMEN_TYPE=df_path_summary[col_name_biopsy])
        df_path_summary.loc[df_path_summary[col_name_resection] == 1, 'IMPACT_SPECIMEN_TYPE'] = 'resection'

        # Drop columns
        df_path_summary = df_path_summary.drop(columns=col_names_text[1:] + [col_name_biopsy, col_name_resection])

        return df_path_summary

def main():
    import constants_darwin as c_dar
    from utils_darwin_etl import set_debug_console

    set_debug_console()
    fname_summary = c_dar.fname_darwin_path_impact_summary_annotated
    list_cols_spec = ['SPECIMEN_SUBMITTED']
    objd = PathologyImpactSpecimenExtraction(pathname=c_dar.pathname,
                                             fname_path_summary=fname_summary,
                                             list_cols_spec=list_cols_spec,
                                             fname_save=c_dar.fname_darwin_path_impact_summary_annotated2)
    df_out = objd.return_summary()

    print(df_out.head())

if __name__ == '__main__':
    main()
