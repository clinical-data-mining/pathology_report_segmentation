""""
pathology_extract_dop_impact_wrapper.py

By Chris Fong - MSKCC 2020

This script will extract accession numbers that are
buried in specimen submitted columns

Steps:
- Get dmp accessions for impact samples, date of report for dmps
- Extract DOP from dmp accessions/parts
- Compute source accession numbers for dmp reports, date of report of surg path
- Extract DOP from surg path reports
- Extract any date found in DMP reports


"""
import os
import pandas as pd
from utils_darwin_etl import save_appended_df


class CombineAccessionDOPImpact(object):
    def __init__(self, pathname, fname_accession, fname_dop, fname_path, fname_out=None):
        self.pathname = pathname
        self.fname_path = fname_path
        self.fname_accession = fname_accession
        self.fname_dop = fname_dop

        # Column headers
        self._col_label_access_num = None
        self._col_label_spec_num = None
        self._col_spec_sub = None
        self._col_sample_id1 = None
        self._col_sample_id2 = None
        self._col_id1 = None
        self._col_id2 = None

        self._df = None
        self._fname_out = fname_out

        self._constants()
        self._process_data()

    def _process_data(self):
        # Load data
        df_path, df_dop, df_accession = self._load_data()

        # Load constants
        self._constants()

        # Manipulate data
        df_dop1, df_accession1, df_impact_map = self._organize_data(df_path, df_dop, df_accession)

        # Merge data in a summary table
        # df = self._merge_data(df_dop1=df_dop1, df_accession1=df_accession1, df_impact_map=df_impact_map, df_path=df_path)
        df1 = self._merge_sample_ids(df_accession1=df_accession1, df_impact_map=df_impact_map)
        df3 = self._merge_dop( df1=df1, df_dop1=df_dop1)
        df4 = self._merge_report_dates(df3=df3, df_path=df_path)

        # Rename column names and drop some columns
        df = self._final_clean(df4=df4)

        # Save data
        if self._fname_out is not None:
            save_appended_df(df=df, filename=self._fname_out, pathname=self.pathname)

        self._df = df

    def return_df(self):
        return self._df

    def _constants(self):
        ## Constants
        self._col_label_access_num = 'ACCESSION_NUMBER'
        self._col_label_spec_num_m = 'SPECIMEN_NUMBER'
        self._col_label_spec_num = 'SOURCE_SPEC_NUM_0'
        self._col_label_spec_num_b = 'SOURCE_SPEC_NUM_0b'
        self._col_spec_sub = 'SOURCE_ACCESSION_NUMBER_0'
        self._col_spec_sub_b = 'SOURCE_ACCESSION_NUMBER_0b'
        self._col_sample_id1 = 'SAMPLE_ID'
        self._col_sample_id2 = 'Sample ID'
        self._col_id1 = 'DMP_ID'
        self._col_id2 = 'Patient ID'

        return None

    def _load_data(self):
        # Load pathology table

        ### Load files needed to extract DOP
        pathfilename = os.path.join(self.pathname, self.fname_path)
        df_path = pd.read_csv(pathfilename, header=0, low_memory=False)

        # Load parsed specimen submitted list
        pathfilename = os.path.join(self.pathname, self.fname_dop)
        df_dop = pd.read_csv(pathfilename, header=0, low_memory=False)

        # Load connecting path accessions
        pathfilename = os.path.join(self.pathname, self.fname_accession)
        df_accession = pd.read_csv(pathfilename, header=0, low_memory=False)

        return df_path, df_dop, df_accession

    def _organize_data(self, df_path, df_dop, df_accession):
        df_path_impact = df_path.loc[df_path[self._col_sample_id1].notnull(), ['P_ID', self._col_label_access_num, self._col_sample_id1, 'DTE_PATH_PROCEDURE']]
        df_impact_map = df_path_impact.rename(columns={self._col_label_access_num: 'ACCESSION_NUMBER_DMP',
                                                       'DTE_PATH_PROCEDURE': 'DATE_SEQUENCING_REPORT'})

        # Load relevant accession numbers to pull DOP from
        df_accession1 = df_accession[df_accession[self._col_label_access_num].isin(df_impact_map['ACCESSION_NUMBER_DMP'])]
        df_accession1 = df_accession1.drop_duplicates().reset_index(drop=True)
        # Convert data types
        df_accession1[self._col_label_spec_num_m] = df_accession1[self._col_label_spec_num_m].astype(int).astype(str)
        df_accession1[self._col_label_spec_num] = df_accession1[self._col_label_spec_num].fillna(0).astype(int).astype(str)
        df_accession1[self._col_label_spec_num_b] = df_accession1[self._col_label_spec_num_b].fillna(0).astype(int).astype(str)
        df_accession1.loc[df_accession1[self._col_label_spec_num] == '0', self._col_label_spec_num] = pd.np.NaN
        df_accession1.loc[df_accession1[self._col_label_spec_num_b] == '0', self._col_label_spec_num_b] = pd.np.NaN

        key = df_accession1[[self._col_label_access_num, self._col_label_spec_num_m]].apply(lambda x: '-'.join(x), axis=1)
        df_accession1 = df_accession1.assign(KEY=key)

        # Compute all reports/accession numbers associated with the impact sample
        df_a = pd.concat([df_accession1[self._col_label_access_num],
                          df_accession1[self._col_spec_sub],
                          df_accession1[self._col_spec_sub_b]], axis=0, sort=False)
        list_a = list(df_a.dropna().drop_duplicates().reset_index(drop=True))

        # Filter the DOP table by the same accession number list
        df_dop1 = df_dop[df_dop[self._col_label_access_num].isin(list_a)]
        df_dop1 = df_dop1.drop(columns=['DOP_DATE_ERROR'])
        df_dop1 = df_dop1[df_dop1['DATE_OF_PROCEDURE_SURGICAL'].notnull()]
        df_dop1[self._col_label_spec_num_m] = df_dop1[self._col_label_spec_num_m].astype(int).astype(str)
        key = df_dop1[[self._col_label_access_num, self._col_label_spec_num_m]].apply(lambda x: '-'.join(x), axis=1)
        df_dop1 = df_dop1.assign(KEY=key)

        return df_dop1, df_accession1, df_impact_map

    def _merge_sample_ids(self, df_accession1, df_impact_map):
        # MERGE 1 -- Merge Sample IDs with DMP SPEC number and submitted description
        df = df_accession1.merge(right=df_impact_map, how='right',
                                 left_on=self._col_label_access_num,
                                 right_on='ACCESSION_NUMBER_DMP')
        # Clean columns
        df = df.rename(columns={self._col_label_spec_num_m: 'SPECIMEN_NUMBER_DMP',
                                'DTE_PATH_PROCEDURE': 'REPORT_DATE_DMP'})
        df = df.drop(columns=[self._col_label_access_num])
        df1 = df.groupby(['P_ID', 'SAMPLE_ID']).first().reset_index()

        return df1

    def _merge_dop(self, df1, df_dop1):
        # MERGE 2 -- Merge with DOP
        df1['SPECIMEN_NUMBER_DMP'] = df1['SPECIMEN_NUMBER_DMP'].astype(int)
        df0 = df1.merge(right=df_dop1, how='left', on='KEY')
        df0 = df0.drop_duplicates()
        df0 = df0.drop(columns=['KEY', self._col_label_access_num, self._col_label_spec_num_m])
        df0 = df0.rename(columns={'DATE_OF_PROCEDURE_SURGICAL': 'DATE_OF_PROCEDURE_SURGICAL_DMP'})

        # MERGE 3 -- Merge Source accession number (1) with dates of procedure
        key = df0[[self._col_spec_sub, self._col_label_spec_num]].dropna().apply(lambda x: '-'.join(x), axis=1)
        df2 = pd.concat([df0, key], axis=1, sort=False)
        df2 = df2.rename(columns={0: 'KEY'})
        df2 = df2.merge(right=df_dop1, how='left', on='KEY')
        df2 = df2.drop(columns=['KEY', self._col_label_access_num, self._col_label_spec_num_m])
        df2 = df2.rename(columns={'DATE_OF_PROCEDURE_SURGICAL': 'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0'})

        # MERGE 4 -- Merge Source accession number (2) with dates of procedure
        key = df2[[self._col_spec_sub_b, self._col_label_spec_num_b]].dropna().apply(lambda x: '-'.join(x), axis=1)
        df3 = pd.concat([df2, key], axis=1, sort=False)
        df3 = df3.rename(columns={0: 'KEY'})
        df3 = df3.merge(right=df_dop1, how='left', on='KEY')
        df3 = df3.drop(columns=['KEY', self._col_label_access_num, self._col_label_spec_num_m])
        df3 = df3.rename(columns={'DATE_OF_PROCEDURE_SURGICAL': 'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0b'})

        return df3

    def _merge_report_dates(self, df3, df_path):
        # Report dates
        df_report_date = df_path[[self._col_label_access_num, 'DTE_PATH_PROCEDURE']].drop_duplicates()

        # MERGE 5 -- Merge Source accession number with dates of REPORTS
        df3_a = df3.loc[df3[self._col_spec_sub].notnull(), [self._col_spec_sub]]
        rpt_date0 = df3_a.merge(right=df_report_date, how='left', left_on=self._col_spec_sub,
                                right_on=self._col_label_access_num)
        rpt_date0 = rpt_date0.rename(columns={'DTE_PATH_PROCEDURE': 'REPORT_CMPT_DATE_SOURCE_0'})
        rpt_date0 = rpt_date0.drop(columns=[self._col_label_access_num])

        df3_b = df3.loc[df3[self._col_spec_sub_b].notnull(), [self._col_spec_sub_b]]
        rpt_date0b = df3_b.merge(right=df_report_date, how='left', left_on=self._col_spec_sub_b,
                                 right_on=self._col_label_access_num)
        rpt_date0b = rpt_date0b.rename(columns={'DTE_PATH_PROCEDURE': 'REPORT_CMPT_DATE_SOURCE_0b'})
        rpt_date0b = rpt_date0b.drop(columns=[self._col_label_access_num])

        df4 = df3.merge(right=rpt_date0, how='left', on=self._col_spec_sub)
        df4 = df4.drop_duplicates()
        df4 = df4.merge(right=rpt_date0b, how='left', on=self._col_spec_sub_b)

        return df4

    def _merge_data(self, df_dop1, df_accession1, df_impact_map, df_path):
        ### Merge data frames linking accession numbers to impact IDs

        # Report dates
        df_report_date = df_path[[self._col_label_access_num, 'DTE_PATH_PROCEDURE']].drop_duplicates()

        # MERGE 1 -- Merge Sample IDs with DMP SPEC number and submitted description
        df = df_accession1.merge(right=df_impact_map, how='right',
                                 left_on=self._col_label_access_num,
                                 right_on='ACCESSION_NUMBER_DMP')
        # Clean columns
        df = df.rename(columns={self._col_label_spec_num_m: 'SPECIMEN_NUMBER_DMP',
                                'DTE_PATH_PROCEDURE': 'REPORT_DATE_DMP'})
        df = df.drop(columns=[self._col_label_access_num])
        df1 = df.groupby(['SAMPLE_ID']).first().reset_index()

        # MERGE 2 -- Merge with DOP
        df1['SPECIMEN_NUMBER_DMP'] = df1['SPECIMEN_NUMBER_DMP'].astype(int)
        df1 = df1.merge(right=df_dop1, how='left', on='KEY')
        df1 = df1.drop_duplicates()
        df1 = df1.drop(columns=['KEY', self._col_label_access_num, self._col_label_spec_num_m])

        # MERGE 3 -- Merge Source accession number (1) with dates of procedure
        key = df1[[self._col_spec_sub, self._col_label_spec_num]].dropna().apply(lambda x: '-'.join(x), axis=1)
        df2 = pd.concat([df1, key], axis=1, sort=False)
        df2 = df2.rename(columns={0: 'KEY'})
        df2 = df2.merge(right=df_dop1, how='left', on='KEY')
        df2 = df2.drop(columns=['KEY', self._col_label_access_num, self._col_label_spec_num])
        df2 = df2.rename(columns={'DATE_OF_PROCEDURE_SURGICAL_x': 'DATE_OF_PROCEDURE_SURGICAL_DMP',
                                  'DATE_OF_PROCEDURE_SURGICAL_y': 'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0'})

        # MERGE 4 -- Merge Source accession number (2) with dates of procedure
        key = df2[[self._col_spec_sub_b, self._col_label_spec_num_b]].dropna().apply(lambda x: '-'.join(x), axis=1)
        df3 = pd.concat([df2, key], axis=1, sort=False)
        df3 = df3.rename(columns={0: 'KEY'})
        df3 = df3.merge(right=df_dop1, how='left', on='KEY')
        df3 = df3.drop(columns=['KEY', self._col_label_access_num, self._col_label_spec_num_m])
        df3 = df3.rename(columns={'DATE_OF_PROCEDURE_SURGICAL': 'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0b'})

        # MERGE 5 -- Merge Source accession number with dates of REPORTS
        df3_a = df3.loc[df3[self._col_spec_sub].notnull(), [self._col_spec_sub]]
        rpt_date0 = df3_a.merge(right=df_report_date, how='left', left_on=self._col_spec_sub,
                                right_on=self._col_label_access_num)
        rpt_date0 = rpt_date0.rename(columns={'DTE_PATH_PROCEDURE': 'REPORT_CMPT_DATE_SOURCE_0'})
        rpt_date0 = rpt_date0.drop(columns=[self._col_label_access_num])

        df3_b = df3.loc[df3[self._col_spec_sub_b].notnull(), [self._col_spec_sub_b]]
        rpt_date0b = df3_b.merge(right=df_report_date, how='left', left_on=self._col_spec_sub_b,
                                 right_on=self._col_label_access_num)
        rpt_date0b = rpt_date0b.rename(columns={'DTE_PATH_PROCEDURE': 'REPORT_CMPT_DATE_SOURCE_0b'})
        rpt_date0b = rpt_date0b.drop(columns=[self._col_label_access_num])

        df4 = df3.merge(right=rpt_date0, how='left', on=self._col_spec_sub)
        df4 = df4.drop_duplicates()
        df4 = df4.merge(right=rpt_date0b, how='left', on=self._col_spec_sub_b)

        return df4

    def _final_clean(self, df4):
        ### Clean columns
        DATE_OF_PROCEDURE_SURGICAL = df4['DATE_OF_PROCEDURE_SURGICAL_DMP'].fillna(df4['DATE_OF_PROCEDURE_SURGICAL_SOURCE_0b'])
        DATE_OF_PROCEDURE_SURGICAL = DATE_OF_PROCEDURE_SURGICAL.fillna(df4['DATE_OF_PROCEDURE_SURGICAL_SOURCE_0'])
        # DATE_OF_PROCEDURE_SURGICAL = df3['DATE_OF_PROCEDURE_SURGICAL_DMP'].fillna(df3['DATE_OF_PROCEDURE_SURGICAL_SOURCE_0b'])
        # DATE_OF_PROCEDURE_SURGICAL = DATE_OF_PROCEDURE_SURGICAL.fillna(df3['DATE_OF_PROCEDURE_SURGICAL_SOURCE_0'])
        df4 = df4.assign(DATE_OF_PROCEDURE_SURGICAL=DATE_OF_PROCEDURE_SURGICAL)
        # Drop DOP that were used to combine.
        df4 = df4.drop(columns=['DATE_OF_PROCEDURE_SURGICAL_DMP',
                                'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0',
                                'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0b'])

        return df4



def main():
    import constants_darwin as c_dar
    from utils_darwin_etl import set_debug_console

    set_debug_console()

    # Extract source accession number
    obj_p = CombineAccessionDOPImpact(pathname=c_dar.pathname,
                                      fname_accession='path_accessions.csv',
                                      fname_dop='pathology_spec_part_dop.csv',
                                      fname_path=c_dar.fname_darwin_path_clean,
                                      fname_out='pathology_dop_impact_summary.csv')

    df = obj_p.return_df()

    tmp = 0

if __name__ == '__main__':
    main()


