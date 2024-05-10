""""
pathology_extract_accession.py

This script will extract accession numbers that are
buried in specimen submitted columns
"""
from msk_cdm.minio import MinioAPI
from pathology_report_segmentation.data_processing import extract_specimen_submitted_column
import pandas as pd
import numpy as np


class PathologyExtractAccession(object):
    def __init__(
            self,
            fname_minio_env,
            fname,
            col_label_access_num,
            col_label_spec_num,
            col_spec_sub,
            fname_out=None
    ):
        self._fname_minio_env = fname_minio_env
        self.fname = fname

        # Column headers
        self._col_label_access_num = col_label_access_num
        self._col_label_spec_num = col_label_spec_num
        self._col_spec_sub = col_spec_sub

        self._df = None
        self._df_original = None
        self._fname_out = fname_out
        
        self._obj_minio = MinioAPI(fname_minio_env=self._fname_minio_env)
        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        df_path = self._load_data()
        self._df_original = df_path

        # Load sample ID mapping between sample ID and accession numbers.
        # df_path_orig = self._load_sample_id_map()
        df_path_orig = None

        # Extract accession numbers from specimen submitted section
        col_accession = 'SOURCE_ACCESSION_NUMBER_0'
        col_spec_num = 'SOURCE_SPEC_NUM_0'
        df_path = self._extract_accession(df_sample_rpt_list1=df_path, df_path_orig=df_path_orig)
        # Note: This is a hack here... Remove columns indicating more than source accession number (aka SOURCE_ACCESSION_NUMBER_1, SOURCE_ACCESSION_NUMBER_2, etc..)
        cols_keep = ['MRN', 'ACCESSION_NUMBER', 'SPECIMEN_NUMBER',  col_accession, col_spec_num]
        df_path = df_path[cols_keep].copy()

        # Remove source accessions that aren't in patient profile at MSK - may be outside accessions
        df_path = self._clean_source_accessions(
            df_path=df_path,
            df_path_orig=df_path_orig,
            col_accession=col_accession,
            col_spec_sub=col_spec_num
        )

        # Fill spec num with 1 if there is only 1 specimen in report
        df_path = self._fill_single_parts(
            df=df_path,
            col_accession=col_accession,
            col_spec_num=col_spec_num
        )
        
        # Find subsource accession.
        df_copy = df_path.copy()
        df_copy = df_copy.rename(
            columns={
                col_accession: 'SOURCE_ACCESSION_NUMBER_0b',
                col_spec_num: 'SOURCE_SPEC_NUM_0b',
                'ACCESSION_NUMBER': 'ACCESSION_NUMBER_b',
                'SPECIMEN_NUMBER': 'SPECIMEN_NUMBER_b'
            }
        )
        df_path_f = df_path.merge(
            right=df_copy,
            how='left',
            left_on=['MRN','SOURCE_ACCESSION_NUMBER_0', 'SOURCE_SPEC_NUM_0'],
            right_on=['MRN', 'ACCESSION_NUMBER_b', 'SPECIMEN_NUMBER_b']
        )
        # Drop columns that duplicates.
        df_path_f = df_path_f.drop(columns=['ACCESSION_NUMBER_b', 'SPECIMEN_NUMBER_b'])

        # Fill spec num with 1 if there is only 1 specimen in report
        col_accession = 'SOURCE_ACCESSION_NUMBER_0b'
        col_spec_num = 'SOURCE_SPEC_NUM_0b'
        df_path_f = self._clean_source_accessions(
            df_path=df_path_f,
            df_path_orig=df_path_orig,
            col_accession=col_accession,
            col_spec_sub=col_spec_num
        )

        # Fill spec num with 1 if there is only 1 specimen in report
        df_path_f = self._fill_single_parts(
            df=df_path_f,
            col_accession=col_accession,
            col_spec_num=col_spec_num
        )

        # Save data
        fname_save = self._fname_out
        if fname_save is not None:
            print('Saving %s' % fname_save)
            self._obj_minio.save_obj(
                df=df_path_f,
                path_object=fname_save,
                sep='\t'
            )

        # Set as a member variable
        self._df = df_path_f

    def _fill_single_parts(self, df, col_accession, col_spec_num):
        # Compute number of parts in report
        df_sample_rpt_list1 = self.return_df_original()
        df_sample_count = df_sample_rpt_list1.groupby(['ACCESSION_NUMBER'])['SPECIMEN_NUMBER'].count().reset_index()

        # For accession numbers that don't have a spec number, fill with '1' if only 1 spec was submitted.
        log2 = (df[col_accession].notnull()) & (df[col_spec_num].isnull())
        ids_change1 = df.loc[log2, col_accession]
        ids_change2 = df_sample_count.loc[df_sample_count['SPECIMEN_NUMBER'] == 1, 'ACCESSION_NUMBER']
        ids_change_f = list(set.intersection(set(ids_change1), set(ids_change2)))

        # Replace SOURCE_SPEC_NUM_0 with blanks with 1 if that accession has only 1 sample submitted
        df.loc[df[col_accession].isin(ids_change_f), col_spec_num] = 1.0

        return df

    def return_df(self):
        return self._df

    def return_df_original(self):
        return self._df_original

    def _load_data(self):
        # Load pathology table
        fname = self.fname
        print('Loading %s' % fname)
        obj = self._obj_minio.load_obj(path_object=fname)
        df = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        df['SPECIMEN_NUMBER'] = df['SPECIMEN_NUMBER'].fillna(1)

        return df

#     def _load_sample_id_map(self):
#         # Load path object to get external matching sample ids and accession number
#         obj_path = DarwinDiscoveryPathology(pathname=self.pathname, fname='table_pathology.tsv')
#         df_path_orig = obj_path.return_df_original()

#         return df_path_orig

    def _extract_accession(self, df_sample_rpt_list1, df_path_orig):
        col_label_access_num = self._col_label_access_num
        col_label_spec_num = self._col_label_spec_num
        col_spec_sub = self._col_spec_sub

        # df_path_orig_1 = df_path_orig[['ACCESSION_NUMBER', 'SAMPLE_ID']]
        # df_path_orig_impact = df_path_orig_1[df_path_orig_1['SAMPLE_ID'].notnull()]

        # Extract MSK surgical accession number with 'MSK:'
        # Regex for matching accession number for surgical procedure
        # regex_rule_surg_accession = r'[MSK]{3}\:[ ]*[S][\d]{2}-[\d]{3,6}'
        print('Extracting Matching Accession Numbers')
        regex_rule_surg_accession = r'[MSKmsk]{3}[ ]{0,1}[\#]{0,1}[\"]{0,1}[\:]{0,1}[ ]*[s|S|c|C|h|H|m|M|f|F|DMG][\d]{1,2}[\-|\/]{1}[\d]{1,6}[\/\#]{0,1}[ ]{0,1}[\d]{0,2}'
        regex_rule_surg_accession_mod = r'[ ]*[A-Za-z]*[s|S|c|C|h|H|m|M][\d]{1,2}[\-|\/]{1}[\d]{1,6}[\/\#]{0,1}[ ]{0,1}[\d]{0,2}'

        # Compute number of parts in report
        df_sample_count = df_sample_rpt_list1.groupby(['ACCESSION_NUMBER'])['SPECIMEN_NUMBER'].count().reset_index()
        # df_sample_rpt_list = df_sample_rpt_list1.merge(right=df_path_orig_impact, how='right', on='ACCESSION_NUMBER')
        df_sample_rpt_list = df_sample_rpt_list1

        df_access_num_source = extract_specimen_submitted_column(
            df_spec_listing=df_sample_rpt_list,
            regex_str=regex_rule_surg_accession,
            col_spec_sub=col_spec_sub,
            col_label_access_num=col_label_access_num,
            col_label_spec=col_label_spec_num
        )

        df_access_num_source_mod = extract_specimen_submitted_column(
            df_spec_listing=df_sample_rpt_list,
            regex_str=regex_rule_surg_accession_mod,
            col_spec_sub=col_spec_sub,
            col_label_access_num=col_label_access_num,
            col_label_spec=col_label_spec_num
        )

        # Replace blanks with nan and convert to float
        df_access_num_source['SOURCE_SPEC_NUM_0'] = df_access_num_source['SOURCE_SPEC_NUM_0'].str.strip()
        df_access_num_source.loc[df_access_num_source['SOURCE_SPEC_NUM_0'] == '', 'SOURCE_SPEC_NUM_0'] = np.NaN
        df_access_num_source_mod['SOURCE_SPEC_NUM_0'] = df_access_num_source_mod['SOURCE_SPEC_NUM_0'].str.strip()
        df_access_num_source_mod.loc[
            df_access_num_source_mod['SOURCE_SPEC_NUM_0'] == '', 'SOURCE_SPEC_NUM_0'] = np.NaN

        # Combine accession extraction methods
        log1 = df_access_num_source_mod['SOURCE_ACCESSION_NUMBER_0'].notnull() & df_access_num_source[
            'SOURCE_ACCESSION_NUMBER_0'].isnull()
        ind_rep = df_access_num_source[log1].index
        df_access_num_source.loc[ind_rep, 'SOURCE_ACCESSION_NUMBER_0'] = df_access_num_source_mod.loc[
            log1, 'SOURCE_ACCESSION_NUMBER_0']
        df_access_num_source.loc[ind_rep, 'SOURCE_SPEC_NUM_0'] = df_access_num_source_mod.loc[log1, 'SOURCE_SPEC_NUM_0']

        # For accession numbers that don't have a spec number, fill with '1' if only 1 spec was submitted.
        log2 = (df_access_num_source['SOURCE_ACCESSION_NUMBER_0'].notnull()) & (
            df_access_num_source['SOURCE_SPEC_NUM_0'].isnull())
        ids_change1 = df_access_num_source.loc[log2, 'SOURCE_ACCESSION_NUMBER_0']
        ids_change2 = df_sample_count.loc[df_sample_count['SPECIMEN_NUMBER'] == 1, 'ACCESSION_NUMBER']
        ids_change_f = list(set.intersection(set(ids_change1), set(ids_change2)))

        # Replace SOURCE_SPEC_NUM_0 with blanks with 1 if that accession has only 1 sample submitted
        df_access_num_source.loc[df_access_num_source['SOURCE_ACCESSION_NUMBER_0'].isin(ids_change_f), 'SOURCE_SPEC_NUM_0'] = 1.0

        # Merge with patient ID
        t = df_sample_rpt_list1[['MRN', 'ACCESSION_NUMBER']].drop_duplicates()
        df_access_num_source = t.merge(right=df_access_num_source, how='right', on='ACCESSION_NUMBER')

        df_access_num_source['SPECIMEN_NUMBER'] = df_access_num_source['SPECIMEN_NUMBER'].astype(int).astype(object)

        return df_access_num_source

    def _clean_source_accessions(self, df_path, df_path_orig, col_accession, col_spec_sub):
        # Clean source accession numbers -- Remove any cases that are outside accessions
        df_n = df_path.copy()
        df_n = df_n[['MRN', col_accession]].dropna()
        df_n = df_n.rename(columns={col_accession: 'ACCESSION_NUMBER'})
        df_n = df_n.assign(P=1)
        df_n = df_n.drop_duplicates()

        accessions_good = df_n.loc[df_n['P'] == 1, 'ACCESSION_NUMBER'].drop_duplicates()

        remove_accessions = list(set(df_path[col_accession].dropna()) - set(accessions_good))
        log_accession_rmv = df_path[col_accession].isin(remove_accessions)
        df_path.loc[log_accession_rmv, col_accession] = np.NaN
        df_path.loc[log_accession_rmv, col_spec_sub] = np.NaN

        return df_path

    def _add_spec_submitted(self):
        # LAbel if submitted slides.
        # rule1 = 'outside'
        # rule2 = 'msk'
        # rule3 = '|'.join(['ssl', 'sbl'])
        # submitted_slides = df_sample_rpt_list['SPECIMEN_SUBMITTED'].str.lower().str.contains(rule3).fillna(False)
        # at_msk = df_sample_rpt_list['SPECIMEN_SUBMITTED'].str.lower().str.contains(rule2).fillna(False)
        # outside = df_sample_rpt_list['SPECIMEN_SUBMITTED'].str.lower().str.contains(rule1).fillna(False)
        # submitted_slides_f = submitted_slides | (outside & ~at_msk)
        # df_sample_rpt_list = df_sample_rpt_list.assign(NO_MSK=submitted_slides_f)
        # df_sample_rpt_list = df_sample_rpt_list.merge(right=df_path_orig_slides, how='left', on='ACCESSION_NUMBER')
        # df_sample_rpt_list['NO_MSK'] = df_sample_rpt_list['NO_MSK'] | df_sample_rpt_list['SUBMITTED_SLIDES']
        # df_sample_rpt_list = df_sample_rpt_list.drop(columns=['SUBMITTED_SLIDES'])

        return None


