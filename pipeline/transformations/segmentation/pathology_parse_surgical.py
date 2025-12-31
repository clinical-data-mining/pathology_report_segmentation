""""
    pathology_parse_surgical.py

    ParseSurgicalPathology
    This class is used to take in a free text surgical pathology report with list of samples names and IDs
    to parse all of the specimen pathology info. Added entry to label corresponding impact sample that is within the
    report. For each surgical pathology report, the specimens are parsed into a dictionary listing all specimens.
"""
import re
from typing import Optional

import pandas as pd

from msk_cdm.minio import MinioAPI


class ParseSurgicalPathology(object):
    """
    A class to parse surgical pathology reports and extract specimen-related information.

    Attributes:
        fname_minio_env (str): The environment filename for Minio API.
        fname_path_clean (str): The filename of the cleaned pathology data.
        fname_save (str): The filename where the processed data will be saved.
        _obj_minio (MinioAPI): An instance of the MinioAPI class for data handling.
        _df_path_surgical (pd.DataFrame): DataFrame to hold the surgical pathology data.
        df_surg_path_parsed (pd.DataFrame): DataFrame to hold the parsed surgical pathology data.

    Methods:
        return_df(): Returns the loaded surgical pathology data.
        return_df_summary(): Returns the parsed surgical pathology summary data.
    """
    def __init__(
            self,
            fname_minio_env: str,
            fname_path_clean: str,
            fname_save: Optional[str]=None
    ):
        """
        Initializes the ParseSurgicalPathology class with filenames for Minio environment,
        cleaned pathology data, and optional save location.

        Args:
            fname_minio_env: The environment filename for Minio API.
            fname_path_clean: The filename of the cleaned pathology data.
            fname_save: The filename where the processed data will be saved. Defaults to None.
        """
        # Member variables
        self._fname_minio_env = fname_minio_env
        self._fname = fname_path_clean
        self._fname_save = fname_save
        self._obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

        # Data frames
        self._df_path_surgical = None
        self.df_surg_path_parsed = None

        # Header group
        self._headers_clinical_dx = None
        self._headers_spec_sub = None
        self._headers_path_dx = None
        self._headers_path_dx_end = None
        self._headers_proc_addenda = None
        self._headers_dx_addenda = None
        self._headers_gross_desc = None
        self._headers_sect_summary = None
        self._headers_spec_studies = None
        self._headers_intraop_consult = None
        self._headers_sign_out = None

        # Process the data -- load the impact pathology file (cleaned), and then parse the table
        self._process_data()

    def _header_names(self):
        """
        Initializes the header names and column names used to parse the pathology reports.
        """
        self.col_path_note = 'PATH_REPORT_NOTE'
        self.col_accession = 'ACCESSION_NUMBER'
        self._col_id_darwin = 'MRN'
        # self._col_path_date = 'REPORT_CMPT_DATE'
        self._col_path_date = 'DTE_PATH_PROCEDURE'

        self._headers_clinical_dx = ['Clinical Diagnosis & History:', 'Clinical Diagnosis and History:', 'CLINICAL DIAGNOSIS AND HISTORY:']
        self._headers_spec_sub = ['Specimens Submitted:', 'SpecimensSubmitted:', 'SPECIMENS SUBMITTED:']
        self._headers_path_dx = ['DIAGNOSIS:']
        self._headers_path_dx_end = ['I ATTEST THAT THE ABOVE DIAGNOSIS IS BASED', 'I ATTEST THAT THEABOVE DIAGNOSIS', 'Report Electronically Signed Out', 'Report ElectronicallySigned Out']
        self._headers_proc_addenda = ['Procedures/Addenda:']
        self._headers_dx_addenda = ['Addendum Diagnosis', '\* Addendum \*', 'ADDENDUM:']
        self._headers_dx_amended = ['AMENDED DIAGNOSIS']
        self._headers_gross_desc = ['Gross Description:']
        self._headers_sect_summary = ['Summary of sections:', 'Summary of Sections:']
        self._headers_spec_studies = ['Special Studies:']
        self._headers_intraop_consult = ['Intraoperative Consultation:']
        self._headers_sign_out = ['Signed out by ']

        self.path_header_ind_col_names = ['CLINICAL_DX', 'SPEC_SUB', 'AMENDED_DIAGNOSIS', 'PATH_DX', 'PATH_DX_END', 'ADDENDA_PROC',
                                          'ADDENDA_DX', 'GROSS_DESC', 'SUM_SECT', 'SPECIAL_STUDIES',
                                          'INTRAOP_CONSULT', 'SIGNED_OUT_BY_TO_END']

    def _load_data(self):
        """
        Loads the cleaned pathology data from Minio storage.

        Returns:
            pd.DataFrame: The loaded pathology data.
        """
        print('Loading %s' % self._fname)
        obj = self._obj_minio.load_obj(path_object=self._fname)
        df = pd.read_csv(obj, header=0, low_memory=False, sep='\t')

        return df

    def return_df(self):
        """
        Returns the loaded surgical pathology data.

        Returns:
            pd.DataFrame: The surgical pathology data.
        """
        return self._df_path_surgical

    def return_df_summary(self):
        """
        Returns the parsed surgical pathology summary data.

        Returns:
            pd.DataFrame: The parsed surgical pathology data.
        """
        return self.df_surg_path_parsed

    def _process_data(self):
        """
        Processes the surgical pathology data by loading, filtering, and parsing the reports.
        Saves the parsed data if a save location is provided.
        """
        # Process the data -- load the impact pathology file (cleaned), and then parse the table
        # Load the data, if parsed data exists, load that first
        df_path = self._load_data()

        # Select surgical path reports
        df_path_surg = self._get_surgical_path_reports(df=df_path)
        self._df_path_surgical = df_path_surg

        # Get header names
        self._header_names()

        # Parse the report notes at the main header level
        df_path_surg = self._parse_report_sections()

        # Save data
        if self._fname_save is not None:
            print('Saving %s' % self._fname_save)
            self._obj_minio.save_obj(
                df=df_path_surg,
                path_object=self._fname_save,
                sep='\t'
            )

        # Make member variable
        self.df_surg_path_parsed = df_path_surg

    def _parse_report_sections(self):
        """
        Parses the main sections of the pathology reports, such as clinical diagnosis, specimens submitted,
        and pathology diagnosis.

        Returns:
            pd.DataFrame: The DataFrame with parsed sections of the surgical pathology reports.
        """
        df_path1 = self.return_df()

        cols = [self._col_id_darwin, self.col_accession, self._col_path_date, self.col_path_note]
        df_path = df_path1[cols]

        # Select surgical reports
        df_surg = self._get_unique_reports(df=df_path)

        # Find index of pathology report sections
        df_path_indices = self._get_path_headers_main_indices(df_path=df_surg)

        # Clean pathology report indices
        df_path_indices = self._fix_path_indices(df_path_indices=df_path_indices)

        # Parse pathology reports for all sections, for now do important parts
        df_path_parsed = self._parse_path_headers_select(df_indices=df_path_indices)

        # sort_values
        df_path_parsed = df_path_parsed.sort_values(by=[self._col_id_darwin, self.col_accession])

        return df_path_parsed

    def _get_unique_reports(
            self,
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Removes duplicate pathology reports, retaining only unique ones.

        Args:
            df: DataFrame containing pathology reports.

        Returns:
            pd.DataFrame: DataFrame with unique pathology reports.
        """
        df_surg = df.drop_duplicates()
        # Drop duplicated accession numbers
        df_surg = df_surg[~df_surg[self.col_accession].duplicated(keep='last')]

        return df_surg

    def _get_surgical_path_reports(
            self,
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Filters the pathology reports to retain only surgical pathology reports.

        Args:
            df: DataFrame containing all types of pathology reports.

        Returns:
            pd.DataFrame: DataFrame containing only surgical pathology reports.
        """
        ## This function will create the pathology dataframe connecting surgical and molecular path reports
        # First, the molecular path reports will be filtered by impact samples only
        # the summary data will filter the molecular path reports,
        # and the corresponding surgical pathology reports
        # Finally, the data frames will be merged

        df_path_surg = df[df['PATH_REPORT_TYPE_GENERAL'] == 'Surgical']

        return df_path_surg

    def _get_path_headers_main_indices(
            self,
            df_path
    ):
        # This function will parse the primary sections within the surgical pathology reports
        # This primarily consists of
        # - Clinical diagnosis and history
        # - Specimens submitted
        # - Diagnosis
        # - Addendum

        col_path_note = self.col_path_note

        df_path_indices = df_path[[self._col_id_darwin, self.col_accession]].copy()

        headers_path_notes_main = [
            self._headers_clinical_dx,
            self._headers_spec_sub,
            self._headers_dx_amended,
            self._headers_path_dx,
            self._headers_path_dx_end
        ]

        # TODO: Parse other sections, only find index points and group remaining path note into single column
        header_path_notes_other = [
            self._headers_proc_addenda,
            self._headers_dx_addenda,
            self._headers_gross_desc,
            self._headers_sect_summary,
            self._headers_spec_studies,
            self._headers_intraop_consult,
            self._headers_sign_out
        ]

        headers_path_notes = headers_path_notes_main + header_path_notes_other


        col_names_index = ['IND_' + x for x in self.path_header_ind_col_names]

        # Find the clinical diagnosis and history section
        # Find the Specimens Submitted section
        # Find the DIAGONSIS section
        for i, header_name in enumerate(headers_path_notes):
            df_path = self._get_header_index(
                df=df_path,
                col_path_notes=col_path_note,
                header_name=header_name,
                col_name=col_names_index[i]
            )

        # Create table of indices
        cols_index = list(df_path.columns[df_path.columns.str.contains('|'.join(col_names_index))])
        df_path_indices = pd.concat([df_path_indices, df_path[cols_index]], axis=1, sort=False)

        # Drop columns from surgical path frame
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
            df_index_split = pd.DataFrame(
                index_tuple.loc[index_tuple.notnull()].tolist(),
                index=index_tuple.loc[index_tuple.notnull()].index,
                columns=cols
            )

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
        cols_ind = list(df_path_indices.columns[df_path_indices.columns.str.contains('IND')])
        ind_header_1 = df_path_indices[df_path_indices[cols_ind] > 0].min(axis=1) - 1
        df_path_indices = df_path_indices.assign(IND_HEADER_1=ind_header_1)

        # Compute end index of path dx section
        log_path_indices = df_path_indices[cols_ind].copy()
        for i, col in enumerate(cols_ind):
            log_path_indices[col] = df_path_indices[col] > df_path_indices['IND_PATH_DX_1']

        ind_end_path_dx = df_path_indices[log_path_indices].min(axis=1) - 1
        df_path_indices = df_path_indices.assign(IND_PATH_DX_END=ind_end_path_dx)

        return df_path_indices

    def _parse_path_headers_select(self, df_indices):
        df_path = self._df_path_surgical
        col_path_note = self.col_path_note

        df_indices = df_indices.assign(IND_HEADER_0=0)
        df_indices = df_indices.merge(
            right=df_path[[self.col_accession, self.col_path_note]],
            how='left',
            on=self.col_accession
        )

        # Parse header section
        df_indices[['IND_HEADER_0', 'IND_HEADER_1']] = df_indices[['IND_HEADER_0', 'IND_HEADER_1']].astype(int)
        fn_slice_hit = lambda x: x[col_path_note][x['IND_HEADER_0']:x['IND_HEADER_1']]
        print('Extracting Section: %s' % 'HEADER')
        df_path_header = df_indices.apply(fn_slice_hit, axis=1)
        df_indices = df_indices.assign(PATH_NOTE_HEADER=df_path_header)

        # Parse clinical dx section, if it exists
        col_name = 'IND_CLINICAL_DX_1'
        col_name_end = col_name + '_END'
        logic_ind1 = df_indices[col_name].notnull()
        ind_end = df_indices.loc[logic_ind1, ['IND_SPEC_SUB_0', 'IND_PATH_DX_0']].min(axis=1)

        kwargs = {col_name_end: ind_end}
        df_indices = df_indices.assign(**kwargs)

        df_indices[[col_name, col_name_end]] = df_indices[[col_name, col_name_end]].fillna(-1).astype(int)
        fn_slice_hit = lambda x: x[col_path_note][x[col_name]:x[col_name_end]]
        print('Extracting Section: %s' % col_name)
        df_path_clin_dx = df_indices[logic_ind1].apply(fn_slice_hit, axis=1)
        df_indices = df_indices.assign(PATH_NOTE_CLINICAL_DX=df_path_clin_dx)

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
        df_indices = df_indices.assign(PATH_NOTE_SPEC_SUB=df_path_spec_sub)

        # Parse path dx section
        col_name = 'IND_PATH_DX_1'
        col_name_end = col_name + '_END'
        logic_ind1 = df_indices[col_name].notnull()
        ind_end = df_indices.loc[logic_ind1, ['IND_END_NOTE', 'IND_PATH_DX_END_0']].min(axis=1)

        kwargs = {col_name_end: ind_end}
        df_indices = df_indices.assign(**kwargs)

        df_indices[[col_name, col_name_end]] = df_indices[[col_name, col_name_end]].fillna(-1).astype(int)
        fn_slice_hit = lambda x: x[col_path_note][x[col_name]:x[col_name_end]]
        print('Extracting Section: %s' % col_name)
        df_path_dx = df_indices[logic_ind1].apply(fn_slice_hit, axis=1)
        df_indices = df_indices.assign(PATH_NOTE_PATH_DX=df_path_dx)

        # Parse the rest of the note text in the other column
        print('Parsing Remaining Text')
        fn_slice_hit = lambda x: x[col_path_note][x[col_name_end]:x['IND_END_NOTE']]
        df_path_other = df_indices.apply(fn_slice_hit, axis=1)
        df_indices = df_indices.assign(PATH_NOTE_OTHER=df_path_other)

        # Drop column containing entire path text, since it is parsed at this point
        # df_indices = df_indices.drop(columns=col_path_note)
        cols = ['PATH_NOTE_HEADER', 'PATH_NOTE_CLINICAL_DX',
                'PATH_NOTE_SPEC_SUB', 'PATH_NOTE_PATH_DX',
                'PATH_NOTE_OTHER']

        # Strip blanks from text
        for j, col in enumerate(cols):
            log_ind = df_indices[col].notnull()
            df_indices.loc[log_ind, col] = df_indices.loc[log_ind, col].apply(lambda x: x.strip())

        # Take relevant columns and merge with original set of accession
        df_path_parsed1 = df_indices[[self.col_accession] + cols]
        df_path2 = df_path[[self._col_id_darwin, self.col_accession]]
        df_path_parsed = df_path2.merge(right=df_path_parsed1,
                                        how='left',
                                        on=self.col_accession)

        return df_path_parsed

    def _parse_path_headers_all(self, df_indices):
        df_path = self._df_path_surgical
        col_path_note = self.col_path_note
        groups = ['IND_CLINICAL_DX_1', 'IND_SPEC_SUB_1', 'IND_PATH_DX_1']
        col_ind_end = 'IND_END'

        df_indices = df_indices.assign(IND_HEADER_0=0)
        df_indices = df_indices.merge(
            right=df_path[[self.col_accession, self.col_path_note]],
            how='left',
            on=self.col_accession
        )
        cols_ind = list(df_indices.columns[df_indices.columns.str.contains('IND')])

        # Parse header section
        df_indices[['IND_HEADER_0', 'IND_HEADER_1']] = df_indices[['IND_HEADER_0', 'IND_HEADER_1']].astype(int)
        fn_slice_hit = lambda x: x[col_path_note][x['IND_HEADER_0']:x['IND_HEADER_1']]
        df_path_header = df_indices.apply(fn_slice_hit, axis=1)
        df_indices = df_indices.assign(PATH_NOTE_HEADER=df_path_header)

        # Parse headers in variable "groups"
        for i, col_name in enumerate(groups):
            # Get clinical dx section -------------------------------------------------------------------------------------
            print('Extracting Section: %s' % groups[i])
            logic_ind1 = df_indices[col_name].notnull()

            log_path_indices = df_indices.loc[logic_ind1, cols_ind].copy()
            # df_indices2 = df_indices[logic_ind1].copy()
            # df_indices2[col_name] = df_indices2[col_name].astype(int)
            for j, col in enumerate(cols_ind):
                # log_path_indices[col] = df_indices2[col].fillna(0) > df_indices2[col_name]
                log_path_indices[col] = df_indices.loc[logic_ind1, col].fillna(0) > df_indices.loc[logic_ind1, col_name]

            ind_end = df_indices.loc[logic_ind1, cols_ind][log_path_indices].min(axis=1)

            col_name_end = col_name.replace('IND_', 'IND_END_')
            kwargs = {col_name_end: ind_end}
            df_indices = df_indices.assign(**kwargs)
            df_indices[col_name_end] = df_indices[col_name_end].fillna(-1).astype(int)
            df_indices[col_name] = df_indices[col_name].fillna(-1).astype(int)
            # df_indices2 = df_indices2.assign(IND_END=ind_end)

            # Current workaround using a lambda function that specifies the start & end columns.
            # fn_slice_hit = lambda x: x[col_path_note][x[col_name]:x[col_ind_end]]
            fn_slice_hit = lambda x: x[col_path_note][x[col_name]:x[col_name_end]]
            # apply the slice function to the dataframe
            series_clinical_dx = df_indices[logic_ind1].apply(fn_slice_hit, axis=1)

            # Add series to path summary frame
            col_name_ind = col_ind_end + '_' + col_name
            col_name_parsed_note = col_name.replace('IND_', 'PATH_NOTE_')
            # df_indices2 = df_indices2.rename(columns={col_ind_end: col_name_ind})
            df_indices.loc[series_clinical_dx.index, col_name_parsed_note] = series_clinical_dx

        # Parse the rest of the note text in the other column
        fn_slice_hit = lambda x: x[col_path_note][x[col_name_end]:x['IND_END_NOTE']]
        df_path_other = df_indices.apply(fn_slice_hit, axis=1)
        df_indices = df_indices.assign(PATH_NOTE_OTHER=df_path_other)

        # Drop column containing entire path text, since it is parsed at this point
        # df_indices = df_indices.drop(columns=col_path_note)

        return df_indices

    def _get_pathdoc_version(self, df_path, col_path_note):
        # Compute PathDoc version number
        headers_path_version = ['PathDoc Version']
        regex_rule_doc_version = headers_path_version[0] + '[ ]*[\d]{1,2}[.]{1}[\d]{1,2}'  # ex PathDoc Version 1.1
        version_note = df_path[col_path_note].apply(lambda x: re.findall(regex_rule_doc_version, str(x)))
        regex_rule = headers_path_version[0] + '[ ]*'
        version_num = version_note.apply(lambda x: float(re.sub(regex_rule, '', x[0])) if len(x) > 0 else '')
        df_path = df_path.assign(PATHDOC_VERSION=version_num)

        return df_path

    def _check_spec_header_info(self, df):
        # Compute statistics on completeness and correctness across the multiple sources that contains the
        # description of the specimen
        col_baseline = 'SAMPLE_COLLECTION_RPT_SPEC_COUNT'
        df[col_baseline] = df[col_baseline].fillna(0)
        df_check = df[['SAMPLE_COLLECTION_SPEC_SUB', 'PATH_SPEC_SUBMITTED_DICT', 'PATH_DX_SPEC_TITLES',
                       col_baseline, 'PATH_SPEC_SUBMITTED_LENGTH', 'PATH_SPEC_SUBMITTED_MAX',
                       'PATH_DX_SPEC_LENGTH', 'PATH_DX_SPEC_MAX']]

        # Count which column has most complete info on spec
        count_nulls = df_check[['SAMPLE_COLLECTION_SPEC_SUB', 'PATH_SPEC_SUBMITTED_DICT', 'PATH_DX_SPEC_TITLES']].isnull().sum()

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


