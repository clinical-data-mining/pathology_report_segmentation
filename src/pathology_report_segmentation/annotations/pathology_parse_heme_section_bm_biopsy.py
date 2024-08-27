""""
    pathology_parse_heme_section_bm_biopsy.py

"""
import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar

class ParseHemePathologySectionBMBx(object):
    def __init__(self, fname_minio_env, fname_path_heme, fname_save=None):
        # Member variables
        self._fname_darwin_pathology_clean = fname_path_heme
        self._fname_save = fname_save

        self._obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

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
        self._column_extract = 'BONE_MARROW_BIOPSY'
        self._list_column_index = ['ACCESSION_NUMBER']
        self._dict_input = {
            'Cellularity:': 'CELLULARITY',
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

    def _load_data(self):
        print('Loading %s' % self._fname_darwin_pathology_clean)
        obj = self._obj_minio.load_obj(path_object=self._fname_darwin_pathology_clean)
        df_path = pd.read_csv(obj, header=0, 
                              low_memory=False, sep='\t') 

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
        if self._fname_save is not None:
            print('Saving %s' % self._fname_save)
            self._obj_minio.save_obj(df=df_path_heme_parsed,
                                     path_object=self._fname_save, 
                                     sep='\t')

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

    obj_s = ParseHemePathologySectionBMBx(fname_minio_env=c_dar.minio_env,
                                       fname_path_heme=c_dar.fname_darwin_path_heme,
                                       fname_save=c_dar.fname_darwin_path_heme_parse_bm_biopsy)
    df = obj_s.return_output()

    tmp = 0

if __name__ == '__main__':
    main()

