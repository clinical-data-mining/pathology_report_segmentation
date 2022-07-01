""""
    pathology_parse_heme_section.py

    By Chris Fong - MSKCC 2022


"""
import os
import sys  
sys.path.insert(0, '/mind_data/fongc2/pathology_report_segmentation')
import re
import pandas as pd
from utils_pathology import save_appended_df


class ParseHemePathologySectionBMAS(object):
    def __init__(self, pathname, fname_path_clean, fname_save=None):
        # Member variables
        self.pathname = pathname
        self.fname_darwin_pathology_clean = fname_path_clean
        self.fname_save = fname_save

        # Data frames
        self._df_path_heme = None
        self._df_attributes = None

        # Header group
        self._dict_input = None
        self._dict_clean = None
        self._column_extract = None
        self._list_column_index = None

        self._process_data()

    def _variables_extraction(self):
        self._column_extract = 'BONE_MARROW_ASPIRATE_SMEAR'
        self._list_column_index = ['ACCESSION_NUMBER']
        self._dict_input = {'Cellularity:': 'CELLULARITY',
              'Quality:': 'QUALITY',
             'Myeloid lineage:': 'MYELOID_LINEAGE',
             'Erythroid lineage:': 'ERYTHROID_LINEAGE',
             'Megakaryocytes:': 'MEGAKARYOCYTES',
             'Plasma cells:': 'PLASMA_CELLS',
             'Congo red:': 'CONGO_RED',
             'Aspirate differential:': 'ASPIRATE_DIFFERENTIAL',
             'Blasts': 'BLASTS',
             'Promyelocytes': 'PROMYELOCYTES',
             'Myelocytes': 'MYELOCYTES',
             'Metamyelocytes': 'METAMYELOCYTES1',
              'Meta': 'METAMYELOCYTES2',
              'Band/PMN Neutrophils': 'NEUTROPHILS_BANDS2',
             'Neutrophils/Bands': 'NEUTROPHILS_BANDS1',
             'Monocytes': 'MONOCYTES',
             'Eosinophils': 'EOSINOPHILS',
             'Basophils': 'BASOPHILS',
             'Erythroid Precursors': 'ERYTHROID_PRECURSORS',
             'Plasma Cells': 'PLASMA_CELLS',
             'Lymphocytes': 'LYMPHOCYTES',
              'Nucleated RBC': 'NUCLEATED_RBC',
             'Number of Cells Counted': 'NUMBER_OF_CELLS_COUNTED',
              'Clot section': 'CLOT_SECTION',
             'M: E Ratio': 'ME_RATIO1',
             'M:E Ratio': 'ME_RATIO2'}

        self._dict_clean = {'ME_RATIO': ['ME_RATIO2', 'ME_RATIO1'],
             'METAMYELOCYTES': ['METAMYELOCYTES1', 'METAMYELOCYTES2'],
             'NEUTROPHILS_BANDS': ['NEUTROPHILS_BANDS1', 'NEUTROPHILS_BANDS2']}
        
    def _header_names(self):
        self.col_path_note = 'BONE MARROW ASPIRATE SMEAR'
        self.col_accession = 'ACCESSION_NUMBER'
        self._col_id_darwin = 'MRN'
        # self._col_path_date = 'REPORT_CMPT_DATE'
        self._col_path_date = 'DTE_PATH_PROCEDURE'
        self._report_type = 'Hematopathology'        
        self._headers_clinical_dx = ['Clinical Diagnosis & History:', 'Clinical Diagnosis and History:', 'CLINICAL DIAGNOSIS AND HISTORY:']
        self._headers_spec_sub = ['Specimens Submitted:', 'SpecimensSubmitted:', 'SPECIMENS SUBMITTED:']
        self._headers_quality = ['Quality:', 'Bone Marrow Aspirate Smear Quality:']
        self._headers_diff = ['Differential']
        self._headers_morph = ['Morphology:']
        self._headers_hist_stain = ['Histochemical stains:']
        self._headers_ihc = ['IMMUNOHISTOCHEMISTRY']
        self._headers_flow_blood = ['FLOW CYTOMETRIC ANALYSIS, PERIPHERAL BLOOD']
        self._headers_flow_bm = ['FLOW CYTOMETRIC ANALYSIS, BONE MARROW']
        self._headers_cyto = ['CYTOGENETIC STUDIES']
        self._headers_molecular = ['MOLECULAR STUDIES']

        self._headers_path_dx_end = ['I ATTEST THAT THE ABOVE DIAGNOSIS']

    def _load_data(self):
        pathfilename = os.path.join(self.pathname, self.fname_darwin_pathology_clean)
        df_path = pd.read_csv(pathfilename, header=0, low_memory=False)

        return df_path

    def return_input(self):
        return self._df_path_heme

    def return_output(self):
        return self._df_attributes

    def _process_data(self):
        # Process the data -- load the impact pathology file (cleaned), and then parse the table
        # Get header names
        self._variables_extraction()
        
        # Load the data, if parsed data exists, load that first
        df = self._load_data()

        dict_input = self._dict_input
        col_attr = self._column_extract
        cols_index = self._list_column_index
        dict_clean = self._dict_clean

        cols = cols_index + [col_attr]
        df_path = df[cols]
        self._df_path_heme = df_path
        
        # Parse the report notes at the main header level
        df_path_heme_parsed = self._extract_attributes(df=df_path, 
                                                       col_attr=col_attr, 
                                                       dict_input=dict_input)
        
        # Clean variables with multiple ontologies
        df_path_heme_parsed = self._clean_attributes(df=df_path_heme_parsed, dict_clean=dict_clean)
        
        # Save data
        if self.fname_save is not None:
            save_appended_df(df=df_path_heme_parsed, filename=self.fname_save, pathname=self.pathname)

        # Make member variable
        self._df_attributes = df_path_heme_parsed

    def _get_attribute(self, series, str_attr):
        str_reg = '(?<=' + str_attr + ').*?(?=\n)'
        extract_current = series.str.findall(str_reg)
        extract_current = extract_current[extract_current.notnull()]
        extract_current_len = extract_current.apply(lambda x: len(x))
        extract_current_str = extract_current[extract_current_len > 0].apply(lambda x: x[0].strip())

        return extract_current_str
    
    def _extract_attributes(self, df, col_attr, dict_input):
        series = df[col_attr]
        for current_attr, col_name in dict_input.items():
            series_parsed = self._get_attribute(series=series, str_attr=current_attr)
            kwargs = {col_name : series_parsed}
            df = df.assign(**kwargs)
            
        return df
    
    def _clean_attributes(self, df, dict_clean):
        for col_name, list_col_name in dict_clean.items():
            series_comb = df[list_col_name[0]].fillna(df[list_col_name[1]])
            kwargs = {col_name : series_comb}
            df = df.assign(**kwargs)
            df = df.drop(columns=list_col_name)
            
        return df

def main():
    import constants_darwin_pathology as c_dar
    from utils_pathology import set_debug_console


    set_debug_console()
    obj_s = ParseHemePathologySection(pathname=c_dar.pathname,
                                   fname_path_clean=c_dar.fname_darwin_path_heme,
                                   fname_save=c_dar.fname_darwin_path_heme_parse_bm_biopsy)
    df = obj_s.return_output()

    tmp = 0

if __name__ == '__main__':
    main()

