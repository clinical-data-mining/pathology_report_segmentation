""""
pathology_parse_molecular.py

By Chris Fong - MSKCC 2019

Selects molecular pathology reports based on list of SAMPLE IDs and
parses DMP reports at the main header level. The parsed reports are
written to file.

"""
import os
import pandas as pd
import constants_darwin
from utils_darwin_etl import save_appended_df
from utils_pathology import extract_specimen_submitted_column, parse_specimen_info, clean_date_column, get_path_headers_main_indices


class ParseMolecularPathology(object):
    def __init__(self, pathname, fname_path_clean, fname_save, fname_sample_ids=None):
        self.pathname = pathname
        self.fname_path_clean = fname_path_clean
        self.fname_sample_ids = fname_sample_ids
        self.fname_save = fname_save

        self._df_path = None
        self._df_dmp = None

        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        # self._load_sid_data()

        self._load_data()

        # Header info
        self._header_names()

        # Parse the report notes at the main header level
        df_path_dmp = self._parse_report_sections()

        # Save data
        if self.fname_save is not None:
            save_appended_df(df=df_path_dmp, filename=self.fname_save, pathname=self.pathname)

        # Set as a member variable
        self._df_dmp = df_path_dmp

    def _header_names(self):
        self.col_path_note = 'PATH_REPORT'
        self.col_accession = 'ACCESSION_NUMBER'
        self._col_id_darwin = 'DARWIN_PATIENT_ID'
        self._col_id_impact = 'DMP_ID'
        self._col_id_sample = 'SAMPLE_ID'
        # self._col_path_date = 'REPORT_CMPT_DATE'
        self._col_path_date = 'REPORT_DATE'

        self._headers_spec_sub = ['Specimens Submitted:', 'SpecimensSubmitted:', 'SPECIMENS SUBMITTED:']
        self._headers_path_dx = ['DIAGNOSTIC INTERPRETATION:', 'DIAGNOSTICINTERPRETATION:']
        self._headers_path_method = ['Methodology:', 'TEST AND METHODOLOGY:']
        self._headers_path_test = ['Test Performed:']

        self.path_header_ind_col_names = ['SPEC_SUB', 'PATH_DX']

    def return_df_summary(self):
        return self._df_dmp

    def return_df(self):
        return self._df_path

    def _load_sid_data(self):
        df = pd.read_csv(self.fname_sample_ids, header=0, low_memory=False, sep='\t')
        self._df_sample_ids = df['Sample ID']

    def _load_data(self):
        # Load pathology table
        print('Loading clean pathology table')
        pathfilename = os.path.join(self.pathname, self.fname_path_clean)
        df = pd.read_csv(pathfilename, header=0, low_memory=False)
        self._df_path = df

    def _parse_report_sections(self):
        # The goal for cleaning is to
        # first find path reports with an impact sample attached to it.
        # Then, find all surgical and slide pathology reports associated with it.
        # Finally, using the original surgical path reports, find all associated reports - may contain Cytology reports
        df = self.return_df()

        col_accession = self.col_accession
        col_report_note = self.col_path_note
        # -----------
        # Filter by pathology reports that are diagnostic impact reports,
        df_sample_rpt1 = self._get_sample_reports(df=df)
        # Drop duplicate rows, not sure why they exist at query
        df_sample_rpt2 = df_sample_rpt1[~df_sample_rpt1[self._col_id_sample].duplicated(keep='last')]
        df_sample_rpt = df_sample_rpt2[[col_accession, col_report_note]]

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
        df_dmp_parsed = df_sample_rpt2[[self._col_id_darwin, col_accession]].drop_duplicates()
        df_dmp_parsed = df_dmp_parsed.merge(right=df_path_parsed_header, how='left', on=col_accession)
        df_dmp_parsed = df_dmp_parsed.merge(right=df_path_parsed, how='left', on=col_accession)
        df_dmp_parsed = df_dmp_parsed.merge(right=df_path_parsed_other, how='left', on=col_accession)

        # sort_values
        df_dmp_parsed = df_dmp_parsed.sort_values(by=[self._col_id_darwin, self.col_accession])

        return df_dmp_parsed





        # Fill nas with note extracted dops
        df['ACCESSION_NUM_PATH_REPORT_0'] = df['ACCESSION_NUM_PATH_REPORT_0'].fillna(df['ACCESSION_NUM_PATH_REPORT_NOTE_0'])
        df = df.drop(columns='ACCESSION_NUM_PATH_REPORT_NOTE_0')

        # -----------
        # TODO move this to separate file/function -- DOP extraction
        # Perform the same task of extraction on the date of procedure 'DOP:'
        # Regex for matching date of procedure 'DOP:'
        print('Extracting Date of Procedure')
        # regex_rule_dop = r'[DOP]{3}\:[ ]*[\d]{1,2}/[\d]{1,2}/[\d]{2,4}'
        regex_rule_dop = r'[ ]*[\d]{1,2}/[\d]{1,2}/[\d]{2,4}'
        col_label_dop = 'DATE_OF_PROCEDURE_COLLECTED'
        df_dop, _, _ = extract_specimen_submitted_column(df_spec_listing=df_sample_rpt_list,
                                                                         regex_str=regex_rule_dop,
                                                                         col_label=col_label_dop)
        df = df.assign(DATE_OF_PROCEDURE_COLLECTED=df_dop)
        #
        # # Regex for matching date of procedure 'DOP:'
        # print('Extracting Date of Procedure in Note')
        # col_label_dop = 'DATE_OF_PROCEDURE_COLLECTED_NOTE'
        # df_dop_note, _, _ = extract_specimen_submitted_column(df_spec_listing=df_sample_rpt_list_note,
        #                                                  regex_str=regex_rule_dop,
        #                                                  col_label=col_label_dop)
        # df = df.assign(DATE_OF_PROCEDURE_COLLECTED_NOTE=df_dop_note)
        #
        # # Fill nas with note extracted dops
        # df['DATE_OF_PROCEDURE_COLLECTED'] = df['DATE_OF_PROCEDURE_COLLECTED'].fillna(df['DATE_OF_PROCEDURE_COLLECTED_NOTE'])
        # df = df.drop(columns='DATE_OF_PROCEDURE_COLLECTED_NOTE')
        #
        # ## Create table of specimen extraction ------------------------------------------------------------------------
        # # TODO move this to separate file/function -- Summary file
        # cols_keep = [self._col_id_darwin, self._col_id_sample, self.col_accession, 'SPECIMEN_COUNT_DMP_RPT','ACCESSION_NUM_PATH_REPORT_0',
        #              'PATH_REPORT_SPEC_NUM_0', 'DATE_OF_PROCEDURE_COLLECTED', 'SPEC_SUB_DICT']
        # df_dmp = df[cols_keep]
        #
        # ### CLEAN resulting table
        # # -----------
        # print('Cleaning Table')
        # # Rename columns
        # df_dmp = df_dmp.rename(columns={self.col_accession: 'ACCESSION_NUM_DMP_REPORT',
        #                                 'DTE_PATH_PROCEDURE': 'DTE_DMP_PATH_RPT',
        #                                 'Path Report Type': 'DMP_REPORT_TYPE',
        #                                 'DATE_OF_PROCEDURE_COLLECTED': 'DATE_OF_PROCEDURE_COLLECTED_DMP'})
        #
        # # Drop duplicates
        # df_dmp = df_dmp.sort_values(by=[self._col_id_sample])
        #
        # # Convert columns for datetime
        # # Clean the one entry that was entered incorrectly
        # col_name_dop = 'DATE_OF_PROCEDURE_COLLECTED_DMP'
        # df_dmp = clean_date_column(df=df_dmp, col_date=col_name_dop)
        #
        # return df_dmp

    def _get_sample_reports(self, df):
        # Filter by samples in cbioportal
        # sample_type_bool = df[self._col_id_sample].isin(self._df_sample_ids)
        sample_type_bool = df[self._col_id_sample].notnull()
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
    import constants_darwin as c_dar
    from utils_darwin_etl import set_debug_console


    set_debug_console()
    ParseMolecularPathology(pathname=c_dar.pathname,
                           fname_path_clean=c_dar.fname_darwin_path_clean,
                           fname_save=c_dar.fname_darwin_path_molecular)

    tmp = 0

if __name__ == '__main__':
    main()