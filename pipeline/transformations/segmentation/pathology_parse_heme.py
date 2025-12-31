""""
    pathology_parse_heme.py

    By Chris Fong - MSKCC 2022

    This class is used to take in a free text heme pathology report with list of samples names and IDs
    to parse all of the specimen pathology info. Added entry to label corresponding impact sample that is within the
    report. For each heme pathology report, the specimens are parsed into a dictionary listing all specimens.
    
    These are the (main) sections of interest for initial parsing:
    - Clinical Diagnosis & History:
    - Specimens Submitted:
    - DIAGNOSIS:
    - BONE MARROW BIOPSY
    - BONE MARROW ASPIRATE SMEAR
    - PERIPHERAL BLOOD
    - IMMUNOHISTOCHEMISTRY
    - FLOW CYTOMETRIC ANALYSIS, PERIPHERAL BLOOD 
    - FLOW CYTOMETRIC ANALYSIS, BONE MARROW
    - CYTOGENETIC STUDIES
    - MOLECULAR STUDIES
    - (Ending) I ATTEST THAT THE ABOVE DIAGNOSIS IS BASED UPON MY PERSONAL EXAMINATION OF
    THE SLIDES (AND/OR OTHER MATERIAL), AND THAT I HAVE REVIEWED AND APPROVED
    THIS REPORT.
"""
import argparse
import os
import sys  
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
import re
import pandas as pd
from minio_api import MinioAPI
from utils import read_minio_api_config


class ParseHemePathology(object):
    def __init__(self, fname_minio_env, fname_path_clean, fname_save=None):
        # Member variables
        self._fname_minio_env = fname_minio_env
        self._fname_darwin_pathology_clean = fname_path_clean
        self._fname_save = fname_save
        self._obj_minio = None
        self._bucket = None

        # Data frames
        self._df_path_heme = None
        self.df_surg_path_parsed = None

        # Header group
        self._headers_clinical_dx = None
        self._headers_spec_sub = None
        self._headers_path_dx = None
        self._headers_path_dx_end = None
        self._headers_bone_marrow_bx = None
        self._headers_bone_marrow_smear = None
        self._headers_periph_blood = None
        self._headers_ihc = None
        self._headers_flow_blood = None
        self._headers_flow_bm = None
        self._headers_cyto = None
        self._headers_molecular = None
             
        # self._headers_proc_addenda = None
        # self._headers_dx_addenda = None
        # self._headers_gross_desc = None
        # self._headers_sect_summary = None
        # self._headers_spec_studies = None
        # self._headers_intraop_consult = None
        # self._headers_sign_out = None

        # Process the data -- load the impact pathology file (cleaned), and then parse the table
        self._process_data()

    def _header_names(self):
        self.col_path_note = 'PATH_REPORT_NOTE'
        self.col_accession = 'ACCESSION_NUMBER'
        self._col_id_darwin = 'MRN'
        # self._col_path_date = 'REPORT_CMPT_DATE'
        self._col_path_date = 'DTE_PATH_PROCEDURE'
        self._report_type = 'Hematopathology'        
        self._headers_clinical_dx = ['Clinical Diagnosis & History:', 'Clinical Diagnosis and History:', 'CLINICAL DIAGNOSIS AND HISTORY:']
        self._headers_spec_sub = ['Specimens Submitted:', 'SpecimensSubmitted:', 'SPECIMENS SUBMITTED:']
        self._headers_path_dx = ['DIAGNOSIS:']
        self._headers_bone_marrow_bx = ['BONE MARROW BIOPSY']
        self._headers_bone_marrow_smear = ['BONE MARROW ASPIRATE SMEAR']
        self._headers_periph_blood = ['PERIPHERAL BLOOD']
        self._headers_ihc = ['IMMUNOHISTOCHEMISTRY']
        self._headers_flow_blood = ['FLOW CYTOMETRIC ANALYSIS, PERIPHERAL BLOOD']
        self._headers_flow_bm = ['FLOW CYTOMETRIC ANALYSIS, BONE MARROW']
        self._headers_cyto = ['CYTOGENETIC STUDIES']
        self._headers_molecular = ['MOLECULAR STUDIES']
        
        self._headers_path_dx_end = ['I ATTEST THAT THE ABOVE DIAGNOSIS']
#         self._headers_proc_addenda = ['Procedures/Addenda:']
#         self._headers_dx_addenda = ['Addendum Diagnosis', '\* Addendum \*', 'ADDENDUM:']
#         self._headers_dx_amended = ['AMENDED DIAGNOSIS']
#         self._headers_gross_desc = ['Gross Description:']
#         self._headers_sect_summary = ['Summary of sections:', 'Summary of Sections:']
#         self._headers_spec_studies = ['Special Studies:']
#         self._headers_intraop_consult = ['Intraoperative Consultation:']
#         self._headers_sign_out = ['Signed out by ']

        self.path_header_ind_col_names = ['CLINICAL_DX', 'SPEC_SUB', 'PATH_DX', 'BONE_MARROW_BIOPSY', 'BONE_MARROW_ASPIRATE_SMEAR',
                                          'PERIPHERAL_BLOOD', 'IMMUNOHISTOCHEMISTRY', 'FLOW_CYTOMETRIC_ANALYSIS_PERIPHERAL_BLOOD',
                                          'FLOW_CYTOMETRIC_ANALYSIS_BONE_MARROW',
                                          'CYTOGENETIC_STUDIES', 'MOLECULAR STUDIES','SIGNED_OUT_BY_TO_END']

    def _load_data(self):
        print('Loading %s' % self._fname_darwin_pathology_clean)
        obj = self._obj_minio.load_obj(bucket_name=self._bucket, path_object=self._fname_darwin_pathology_clean)
        df = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        
        return df

    def return_df(self):
        return self._df_path_heme

    def return_df_summary(self):
        return self.df_surg_path_parsed
    
    def _init_minio(self):
        # Setup Minio configuration
        minio_config = read_minio_api_config(fname_env=self._fname_minio_env)
        ACCESS_KEY = minio_config['ACCESS_KEY']
        SECRET_KEY = minio_config['SECRET_KEY']
        CA_CERTS = minio_config['CA_CERTS']
        URL_PORT = minio_config['URL_PORT']
        BUCKET = minio_config['BUCKET']
        self._bucket = BUCKET

        self._obj_minio = MinioAPI(ACCESS_KEY=ACCESS_KEY, 
                                     SECRET_KEY=SECRET_KEY, 
                                     ca_certs=CA_CERTS, 
                                     url_port=URL_PORT)
        return None

    def _process_data(self):
        # Process the data -- load the impact pathology file (cleaned), and then parse the table
        # Get header names
        self._header_names()
        
        # Load the data, if parsed data exists, load that first
        self._init_minio()
        df_path = self._load_data()

        # Select heme path reports
        df_path_heme = self._get_heme_path_reports(df=df_path)
        
        self._df_path_heme = df_path_heme

        # Parse the report notes at the main header level
        df_path_heme_parsed = self._parse_report_sections(df=df_path_heme)

        # Save data
        if self._fname_save is not None:
            print('Saving %s' % self._fname_save)
            self._obj_minio.save_obj(df=df_path_heme_parsed, 
                                     bucket_name=self._bucket, 
                                     path_object=self._fname_save, 
                                     sep='\t')

        # Make member variable
        self.df_surg_path_parsed = df_path_heme_parsed

    def _parse_report_sections(self, df):
        cols = [self._col_id_darwin, self.col_accession, self._col_path_date, self.col_path_note]
        df_path = df[cols]

        # Select heme reports
        df_surg = self._get_unique_reports(df=df_path)

        # Find index of pathology report sections
        df_path_indices = self._get_path_headers_main_indices(df_path=df_surg)

        # Clean pathology report indices
        df_path_indices = self._fix_path_indices(df_path_indices=df_path_indices)

        # Parse pathology reports for all sections
        df_path_parsed = self._parse_path_headers(df_indices=df_path_indices, df=df_surg)

        # sort_values
        df_path_parsed = df_path_parsed.sort_values(by=[self._col_id_darwin, self.col_accession])

        return df_path_parsed

    def _get_unique_reports(self, df):
        df_surg = df.drop_duplicates()
        # Drop duplicated accession numbers
        df_surg = df_surg[~df_surg[self.col_accession].duplicated(keep='last')]

        return df_surg

    def _get_heme_path_reports(self, df):
        ## This function will create the pathology dataframe connecting heme and molecular path reports
        # First, the molecular path reports will be filtered by impact samples only
        # the summary data will filter the molecular path reports,
        # and the corresponding heme pathology reports
        # Finally, the data frames will be merged

        df_path_surg = df[df['PATH_REPORT_TYPE_GENERAL'] == self._report_type]

        return df_path_surg

    def _get_path_headers_main_indices(self, df_path):
        # This function will parse the primary sections within the heme pathology reports
        # This primarily consists of
        # - Clinical diagnosis and history
        # - Specimens submitted
        # - Diagnosis
        # - Addendum

        col_path_note = self.col_path_note

        df_path_indices = df_path[[self._col_id_darwin, self.col_accession]].copy()

        headers_path_notes = [self._headers_clinical_dx,
                               self._headers_spec_sub,
                               self._headers_path_dx,
                               self._headers_bone_marrow_bx,
                               self._headers_bone_marrow_smear,
                               self._headers_periph_blood,
                               self._headers_ihc,
                               self._headers_flow_blood,
                               self._headers_flow_bm,
                               self._headers_cyto,
                               self._headers_molecular,
                              self._headers_path_dx_end
                             ]


        col_names_index = ['IND_' + x for x in self.path_header_ind_col_names]

        # Find the clinical diagnosis and history section
        # Find the Specimens Submitted section
        # Find the DIAGONSIS section
        for i, header_name in enumerate(headers_path_notes):
            df_path = self._get_header_index(df=df_path, col_path_notes=col_path_note,
                                             header_name=header_name,
                                             col_name=col_names_index[i])

        # Create table of indices
        cols_index = list(df_path.columns[df_path.columns.str.contains('|'.join(col_names_index))])
        df_path_indices = pd.concat([df_path_indices, df_path[cols_index]], axis=1, sort=False)

        # Drop columns from heme path frame
        df_path = df_path.drop(columns=cols_index)

        # Get index of last character of path note
        note_length = df_path[col_path_note].apply(lambda x: len(x))
        df_path_indices = df_path_indices.assign(IND_END_NOTE=note_length)
        df_path_indices['IND_END_NOTE'] = df_path_indices['IND_END_NOTE'].fillna(-1).astype(int)

        return df_path_indices

    def _get_header_index(self, df, col_path_notes, header_name, col_name):
        path_notes = df[col_path_notes]

        for i, key in enumerate(header_name):
            regex_rule_header3 = key + '[ ]*[\r\n]*'

            path_dx_sub_header_loc = path_notes.apply(lambda x: re.search(regex_rule_header3, str(x)))
            index_tuple = path_dx_sub_header_loc[path_dx_sub_header_loc.notnull()].apply(lambda x: x.span())

            # Find start and end points: _0, _1
            cols = [col_name + '_0', col_name + '_1']
            df_index_split = pd.DataFrame(index_tuple.loc[index_tuple.notnull()].tolist(),
                                          index=index_tuple.loc[index_tuple.notnull()].index,
                                          columns=cols)

            if i == 0:
                # kwargs = {col_name: index_tuple}
                # df = df.assign(**kwargs)
                df = pd.concat([df, df_index_split], axis=1)
            else:
                df[cols[0]] = df[cols[0]].fillna(df_index_split[cols[0]])
                df[cols[1]] = df[cols[1]].fillna(df_index_split[cols[1]])

        # df[cols] = df[cols].fillna(-1).astype(int)

        return df

    def _fix_path_indices(self, df_path_indices):
        # From the index table, compute header index and end of path dx section
        df_path_indices = df_path_indices.assign(IND_HEADER_0=0)
        cols_ind = list(df_path_indices.columns[df_path_indices.columns.str.contains('IND')])
        ind_header_1 = df_path_indices[df_path_indices[cols_ind] > 0].min(axis=1) - 1
        df_path_indices = df_path_indices.assign(IND_HEADER_1=ind_header_1)
        
        # Put header columns in the front
        

        return df_path_indices

    def _parse_path_headers(self, df_indices, df):
        df_parsed = df_indices[[self._col_id_darwin, self.col_accession]].copy().reset_index(drop=True)
        
        # Merge notes with the indices
        df_indices_notes = df[[self.col_accession, self.col_path_note]].merge(right=df_indices,
                                      how='right',
                                      on=self.col_accession)

        next_col = 'IND_END_SECTION'
        logic_cols_next = df_indices_notes.columns.str.contains('IND_') & df_indices_notes.columns.str.contains('_0')
        list_next_col = list(df_indices_notes.columns[logic_cols_next])
        for i, column_name in enumerate(self.path_header_ind_col_names):
            print('Parsing section: %s' % column_name)
            logic_cols_current = df_indices_notes.columns.str.contains(column_name) & df_indices_notes.columns.str.contains('IND_') & df_indices_notes.columns.str.contains('_1')
            current_col = df_indices_notes.columns[logic_cols_current][0]

            # Compute start and stop indices for text
            current_df = df_indices_notes[[self.col_path_note, current_col]].copy()
            logic_ind1 = current_df[current_col].notnull()
            current_df[current_col] = current_df[current_col].fillna(-1).astype(int)

            if i == (len(self.path_header_ind_col_names) - 1):
                current_df[next_col] = df_indices_notes['IND_END_NOTE'].fillna(-1).astype(int)
            else:
                ind_diff = df_indices_notes[list_next_col].apply(lambda x: x - current_df[current_col])
                ind_end = ind_diff[ind_diff > 0].min(axis=1).fillna(-1).astype(int)
                current_df[next_col] = current_df[current_col] + ind_end

            # Parse section
            fn_slice_hit = lambda x: x[self.col_path_note][x[current_col]:x[next_col]]
            df_path_clin_dx = current_df[logic_ind1].apply(fn_slice_hit, axis=1)
            df_path_clin_dx = df_path_clin_dx.str.strip()

            # Add segmented text to a new column
            kwargs = {column_name: df_path_clin_dx}
            df_parsed = df_parsed.assign(**kwargs)
            
        return df_parsed

def main():
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'cdm-utilities')))
    from data_classes_cdm import CDMProcessingVariables as c_dar

    parser = argparse.ArgumentParser(description="pathology_parse_heme.py")
    parser.add_argument(
        "--minio_env",
        dest="minio_env",
        required=True,
        help="location of Minio environment file",
    )
    args = parser.parse_args()
    
    obj_s = ParseHemePathology(fname_minio_env=args.minio_env,
                               fname_path_clean=c_dar.fname_path_clean,
                               fname_save=c_dar.fname_darwin_path_heme)
    df = obj_s.return_df()

    tmp = 0

if __name__ == '__main__':
    main()
