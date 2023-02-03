""""
pathology_parse_molecular.py

By Chris Fong - MSKCC 2019

Selects molecular pathology reports based on list of SAMPLE IDs and
parses DMP reports at the main header level. The parsed reports are
written to file.

"""
import os
import sys  
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
import pandas as pd
from utils_pathology import get_path_headers_main_indices
from minio_api import MinioAPI
from utils import read_minio_api_config


class ParseMolecularPathology(object):
    def __init__(self, fname_minio_env, fname_path_clean, fname_save):
        self._fname_minio_env = fname_minio_env
        self._fname_path_clean = fname_path_clean
        self._fname_save = fname_save
        self._obj_minio = None
        self._bucket = None

        self._df_path = None
        self._df_dmp = None

        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        self._init_minio()
        df_path = self._load_data()

        # Header info
        self._header_names()

        # Parse the report notes at the main header level
        df_path_dmp = self._parse_report_sections(df=df_path)

        # Save data
        if self._fname_save is not None:
            print('Saving %s' % self._fname_save)
            self._obj_minio.save_obj(df=df_path_dmp, 
                                     bucket_name=self._bucket, 
                                     path_object=self._fname_save, 
                                     sep='\t')

        # Set as a member variable
        self._df_dmp = df_path_dmp

    def _header_names(self):
        self.col_path_note = 'PATH_REPORT_NOTE'
        self.col_accession = 'ACCESSION_NUMBER'
        self._col_id_darwin = 'MRN'
        self._col_id_impact = 'DMP_ID'
        self._col_id_sample = 'SAMPLE_ID'
        # self._col_path_date = 'REPORT_CMPT_DATE'
        self._col_path_date = 'REPORT_DATE'

        self._headers_spec_sub = ['Specimens Submitted:', 'SpecimensSubmitted:', 'SPECIMENS SUBMITTED:']
        self._headers_path_dx = ['DIAGNOSTIC INTERPRETATION:', 'DIAGNOSTICINTERPRETATION:']
        self._headers_path_method = ['Methodology:', 'TEST AND METHODOLOGY:']
        self._headers_path_test = ['Test Performed:']

        self.path_header_ind_col_names = ['SPEC_SUB', 'PATH_DX']

    def return_df(self):
        return self._df_dmp

    def _load_data(self):
        print('Loading %s' % self._fname_path_clean)
        obj = self._obj_minio.load_obj(bucket_name=self._bucket, path_object=self._fname_path_clean)
        df = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        
        return df
    
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

    def _parse_report_sections(self, df):
        # The goal for cleaning is to
        # first find path reports with an impact sample attached to it.
        # Then, find all surgical and slide pathology reports associated with it.
        # Finally, using the original surgical path reports, find all associated reports - may contain Cytology reports

        col_accession = self.col_accession
        col_report_note = self.col_path_note
        # -----------
        # Filter by pathology reports that are diagnostic impact reports,
        df_sample_rpt1 = self._get_sample_reports(df=df)
        df_sample_rpt = df_sample_rpt1[[col_accession, col_report_note]]

        # Get indices of headers from reports
        # Headers/sections to build indices against.  Add to this list to parse more sections
        headers_path_notes = [self._headers_spec_sub,
                              self._headers_path_dx]
        path_header_ind_col_names = self.path_header_ind_col_names

        df_path_indices = get_path_headers_main_indices(df=df_sample_rpt,
                                                        col_path_note=col_report_note,
                                                        cols_index_list=[col_accession],
                                                        headers_path_notes=headers_path_notes,
                                                        path_header_ind_col_names=path_header_ind_col_names)
        # Remove indices that are null
        col_ind0 = df_path_indices.columns[df_path_indices.columns.str.contains('IND_')][0]
        df_path_indices = df_path_indices[df_path_indices[col_ind0].notnull()]

        # Parse main sections
        # -----------
        # Parse specimen submitted section from text note
        df_path_parsed = self._parse_section_spec_submitted(df_indices=df_path_indices, df_path=df_sample_rpt)
        # df_sample_rpt = df_sample_rpt.merge(right=df_path_parsed, how='left', on=col_accession)

        # Parse dx and other sections from text note
        df_path_parsed_other = self._parse_section_other(df_indices=df_path_indices, df_path=df_sample_rpt)
        # df_sample_rpt = df_sample_rpt.merge(right=df_path_parsed, how='left', on=col_accession)

        # Parse report header from text note
        df_path_parsed_header = self._parse_section_report_header(df_indices=df_path_indices, df_path=df_sample_rpt)

        # Merge parsed sections into a single dataframe
        df_dmp_parsed = df_sample_rpt1[[self._col_id_darwin, col_accession]].drop_duplicates()
        df_dmp_parsed = df_dmp_parsed.merge(right=df_path_parsed_header, how='left', on=col_accession)
        df_dmp_parsed = df_dmp_parsed.merge(right=df_path_parsed, how='left', on=col_accession)
        df_dmp_parsed = df_dmp_parsed.merge(right=df_path_parsed_other, how='left', on=col_accession)

        # sort_values
        df_dmp_parsed = df_dmp_parsed.sort_values(by=[self._col_id_darwin, self.col_accession])

        return df_dmp_parsed

    def _get_sample_reports(self, df):
        # Filter by samples in cbioportal
        # sample_type_bool = df[self._col_id_sample].isin(self._df_sample_ids)
        sample_type_bool = df['PATH_REPORT_TYPE_GENERAL'] == 'Molecular'
        sum_sample = sample_type_bool.sum()
        print('Number of samples in list: %i' % sum_sample)
        df_sample_rpt = df[sample_type_bool]

        return df_sample_rpt

    def _parse_section_report_header(self, df_indices, df_path):
        # Parse the specimen submitted section from notes
        col_path_note = self.col_path_note

        col = 'DMP_NOTE_RPT_HEADER'

        df_indices = df_indices.assign(IND_HEADER_0=0)
        df_indices = df_indices.merge(right=df_path,
                                      how='left',
                                      on=self.col_accession)

        # Parse spec submitted section
        col_name = 'IND_HEADER_0'
        col_name_end = col_name + '_END'
        logic_ind1 = df_indices[col_name].notnull()
        ind_end = df_indices.loc[logic_ind1, ['IND_SPEC_SUB_0', 'IND_PATH_DX_0']].min(axis=1)

        kwargs = {col_name_end: ind_end}
        df_indices = df_indices.assign(**kwargs)

        df_indices[[col_name, col_name_end]] = df_indices[[col_name, col_name_end]].fillna(-1).astype(int)
        fn_slice_hit = lambda x: x[col_path_note][x[col_name]:x[col_name_end]]
        print('Extracting Section: %s' % col_name)
        df_path_spec_sub = df_indices[logic_ind1].apply(fn_slice_hit, axis=1)
        df_indices = df_indices.assign(DMP_NOTE_RPT_HEADER=df_path_spec_sub)

        # Strip text data
        log_ind = df_indices[col].notnull()
        df_indices.loc[log_ind, col] = df_indices.loc[log_ind, col].apply(lambda x: x.strip())
        df_indices = df_indices[[self.col_accession, col]]

        return df_indices

    def _parse_section_spec_submitted(self, df_indices, df_path):
        # Parse the specimen submitted section from notes
        col_path_note = self.col_path_note

        col = 'DMP_NOTE_SPEC_SUB'

        df_indices = df_indices.assign(IND_HEADER_0=0)
        df_indices = df_indices.merge(right=df_path,
                                      how='left',
                                      on=self.col_accession)

        # Parse spec submitted section
        col_name = 'IND_SPEC_SUB_1'
        col_name_end = col_name + '_END'
        logic_ind1 = df_indices[col_name].notnull()
        ind_end = df_indices.loc[logic_ind1, ['IND_END_NOTE', 'IND_PATH_DX_0']].min(axis=1)

        kwargs = {col_name_end: ind_end}
        df_indices = df_indices.assign(**kwargs)

        df_indices[[col_name, col_name_end]] = df_indices[[col_name, col_name_end]].fillna(-1).astype(int)
        fn_slice_hit = lambda x: x[col_path_note][x[col_name]:x[col_name_end]]
        print('Extracting Section: %s' % col_name)
        df_path_spec_sub = df_indices[logic_ind1].apply(fn_slice_hit, axis=1)
        df_indices = df_indices.assign(DMP_NOTE_SPEC_SUB=df_path_spec_sub)

        # Strip text data
        log_ind = df_indices[col].notnull()
        df_indices.loc[log_ind, col] = df_indices.loc[log_ind, col].apply(lambda x: x.strip())
        df_indices = df_indices[[self.col_accession, col]]

        return df_indices

    def _parse_section_other(self, df_indices, df_path):
        # Parse the specimen submitted section from notes
        col_path_note = self.col_path_note

        col = 'DMP_NOTE_DX_AND_OTHER'

        df_indices = df_indices.assign(IND_HEADER_0=0)
        df_indices = df_indices.merge(right=df_path,
                                      how='left',
                                      on=self.col_accession)

        # Parse spec submitted section
        col_name = 'IND_PATH_DX_1'
        col_name_end = col_name + '_END'
        logic_ind1 = df_indices[col_name].notnull()
        ind_end = df_indices.loc[logic_ind1, 'IND_END_NOTE']

        kwargs = {col_name_end: ind_end}
        df_indices = df_indices.assign(**kwargs)

        df_indices[[col_name, col_name_end]] = df_indices[[col_name, col_name_end]].fillna(-1).astype(int)
        fn_slice_hit = lambda x: x[col_path_note][x[col_name]:x[col_name_end]]
        print('Extracting Section: %s' % col_name)
        df_path_spec_sub = df_indices[logic_ind1].apply(fn_slice_hit, axis=1)
        df_indices = df_indices.assign(DMP_NOTE_DX_AND_OTHER=df_path_spec_sub)

        # Strip text data
        log_ind = df_indices[col].notnull()
        df_indices.loc[log_ind, col] = df_indices.loc[log_ind, col].apply(lambda x: x.strip())
        df_indices = df_indices[[self.col_accession, col]]

        return df_indices

def main():
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'cdm-utilities')))
    from data_classes_cdm import CDMProcessingVariables as c_dar
    
    
    obj_m = ParseMolecularPathology(fname_minio_env=c_dar.minio_env,
                                    fname_path_clean=c_dar.fname_path_clean,
                                    fname_save=c_dar.fname_darwin_path_molecular)

    df = obj_m.return_df()

    tmp = 0

if __name__ == '__main__':
    main()