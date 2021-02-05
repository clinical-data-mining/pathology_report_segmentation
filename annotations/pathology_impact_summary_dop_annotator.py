""""
pathology_impact_summary_dop_annotator.py

By Chris Fong - MSKCC 2019


"""
import os
import pandas as pd
from utils_pathology import save_appended_df


class PathologyImpactDOPAnno(object):
    def __init__(self, pathname, fname_path_summary, fname_surgery, fname_ir, fname_save=None):
        self.pathname = pathname
        self._fname_path_summary = fname_path_summary
        self._fname_surgery = fname_surgery
        self._fname_save = fname_save
        self._fname_ir = fname_ir

        self._df_summary = None
        self._df_surg_unique = None
        self._df_ir_unique = None

        self._col_surg_date = 'PROC_DATE_SURG'
        self._col_ir_date = 'PROC_DATE_IR'

        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        df_path_summary1, df_surg, df_ir = self._load_data()
        log1 = ~df_path_summary1['ACCESSION_NUMBER_DMP'].duplicated(keep=False)
        log2 = ~df_path_summary1['ACCESSION_NUMBER_DMP'].duplicated(keep='first')
        df_path_summary = df_path_summary1[log1 | log2]

        # Compute unique surgery dates for patients
        df_with_dates = self._add_proc_dates(df=df_path_summary, df_surg=df_surg, df_ir=df_ir)

        # Clean dates
        cols_dates = ['DATE_SEQUENCING_REPORT', 'REPORT_CMPT_DATE_SOURCE_0',
                      'REPORT_CMPT_DATE_SOURCE_0b', 'DATE_OF_PROCEDURE_SURGICAL', 'PROC_DATE_SURG',
                      self._col_surg_date, self._col_ir_date]
        for i in cols_dates:
            df_with_dates[i] = pd.to_datetime(df_with_dates[i])

        # Compute number of days between surg and report
        df_surg_path0 = (df_with_dates[self._col_surg_date] - df_with_dates['REPORT_CMPT_DATE_SOURCE_0']).dt.days == 0
        df_surg_path0b = (df_with_dates[self._col_surg_date] - df_with_dates['REPORT_CMPT_DATE_SOURCE_0b']).dt.days == 0
        df_surg_path_r = (df_with_dates[self._col_surg_date] - df_with_dates['DATE_SEQUENCING_REPORT']).dt.days == 0

        df_with_dates = df_with_dates.assign(MATCH_SURG=(df_surg_path0 | df_surg_path0b | df_surg_path_r))

        # Compute number of days between IR and report
        df_ir_path0 = (df_with_dates[self._col_ir_date] - df_with_dates['REPORT_CMPT_DATE_SOURCE_0']).dt.days == 0
        df_ir_path0b = (df_with_dates[self._col_ir_date] - df_with_dates['REPORT_CMPT_DATE_SOURCE_0b']).dt.days == 0
        df_ir_path_r = (df_with_dates[self._col_ir_date] - df_with_dates['DATE_SEQUENCING_REPORT']).dt.days == 0

        df_with_dates = df_with_dates.assign(MATCH_IR=(df_ir_path0 | df_ir_path0b | df_ir_path_r))

        df_with_dates = df_with_dates.assign(DATE_OF_PROCEDURE_SURGICAL_EST=df_with_dates['DATE_OF_PROCEDURE_SURGICAL'])

        # Fill in missing dates from surg
        log1 = df_with_dates['DATE_OF_PROCEDURE_SURGICAL'].isnull()
        log2 = df_with_dates['MATCH_SURG'] == True
        log3 = df_with_dates['MATCH_IR'] == True
        df_surg_fill = df_with_dates[log1 & log2].copy()
        df_ir_fill = df_with_dates[log1 & log3].copy()

        # Fill in estimated dates into surg/ir tables.
        col_est = 'DATE_OF_PROCEDURE_SURGICAL_EST'
        df_surg_fill[col_est] = df_surg_fill[col_est].fillna(df_surg_fill['PROC_DATE_SURG'])
        df_ir_fill[col_est] = df_ir_fill[col_est].fillna(df_ir_fill['PROC_DATE_IR'])

        # Fill in estimated dates into main table.
        df_with_dates[col_est] = df_with_dates[col_est].fillna(df_surg_fill[col_est])
        df_with_dates[col_est] = df_with_dates[col_est].fillna(df_ir_fill[col_est])

        # Compute the first surg/ir date to match report
        cols_g = ['SAMPLE_ID', 'ACCESSION_NUMBER_DMP', 'SPECIMEN_NUMBER_DMP']
        df_with_dates_g = df_with_dates.groupby(cols_g)[col_est].first()
        df_with_dates_g = df_with_dates_g.reset_index()

        # Merge estimated DOP
        df_path_summary_f = df_path_summary.merge(right=df_with_dates_g, how='left', on=cols_g)

        # Create label for source of DOP
        df_path_summary_f = self._create_source_label(df=df_path_summary_f, df_surg=df_surg_fill, df_ir=df_ir_fill)

        # Set as member variable
        self._df_summary = df_path_summary_f

        # Save data
        if self._fname_save is not None:
            save_appended_df(df=df_path_summary_f, pathname=self.pathname, filename=self._fname_save)

    def return_summary(self):
        return self._df_summary

    def _add_proc_dates(self, df, df_surg, df_ir):
        # Get unique ages of surgery for patient
        col_proc_date = 'DATE_OF_PROCEDURE'
        col_id = 'DARWIN_PATIENT_ID'
        col_proc_date_ir = 'REPORT_DATE'
        cols_keep = [col_id, col_proc_date]
        df_surg1 = df_surg.loc[df_surg[col_proc_date].notnull(), cols_keep].drop_duplicates()
        df_surg1 = df_surg1.rename(columns={col_proc_date: self._col_surg_date})
        # df_surg_unique = df_surg1.groupby(['DMP_ID', col_proc_date])['CPT Code'].apply(lambda x: '|'.join(x)).reset_index()

        cols_keep = [col_id, col_proc_date_ir]
        df_ir1 = df_ir.loc[df_ir[col_proc_date_ir].notnull(), cols_keep].drop_duplicates()
        df_ir1 = df_ir1.rename(columns={col_proc_date_ir: self._col_ir_date})
        # df_ir_unique = df_ir1.groupby(['DMP_ID', 'AGE_AT_PROCEDURE'])['Procedure Desc'].apply(lambda x: '|'.join(x)).reset_index()

        # Merge
        df = df.rename(columns={'P_ID': 'DARWIN_PATIENT_ID'})
        df = df.merge(right=df_surg1, how='left', on='DARWIN_PATIENT_ID')
        df = df.merge(right=df_ir1, how='left', on='DARWIN_PATIENT_ID')

        return df

    def _load_data(self):
        # Load tables
        pathfilename = os.path.join(self.pathname, self._fname_path_summary)
        df_path_summary = pd.read_csv(pathfilename, header=0, low_memory=False)

        pathfilename_surg = os.path.join(self.pathname, self._fname_surgery)
        df_surg = pd.read_csv(pathfilename_surg, header=0, low_memory=False, sep='\t')

        pathfilename_ir = os.path.join(self.pathname, self._fname_ir)
        df_ir = pd.read_csv(pathfilename_ir, header=0, low_memory=False, sep='\t')

        return df_path_summary, df_surg, df_ir

    def _create_source_label(self, df, df_surg, df_ir):
        # Create label for source of DOP
        logic_sid_spec_sub = df['DATE_OF_PROCEDURE_SURGICAL'].notnull()
        logic_sid_surg = df['SAMPLE_ID'].isin(df_surg['SAMPLE_ID'])
        logic_sid_ir = df['SAMPLE_ID'].isin(df_ir['SAMPLE_ID'])
        logic_sid_both = logic_sid_surg & logic_sid_ir
        df = df.assign(DOP_COMPUTE_SOURCE=pd.np.NaN)
        df.loc[logic_sid_surg, 'DOP_COMPUTE_SOURCE'] = 'Surgical'
        df.loc[logic_sid_ir, 'DOP_COMPUTE_SOURCE'] = 'IR'
        df.loc[logic_sid_both, 'DOP_COMPUTE_SOURCE'] = 'Surgical & IR'
        df.loc[logic_sid_spec_sub, 'DOP_COMPUTE_SOURCE'] = 'Specimens Submitted'

        return df

def main():
    import constants_darwin as c_dar
    from utils_pathology import set_debug_console

    set_debug_console()
    fname_summary = 'pathology_dop_impact_summary.csv'
    objd = PathologyImpactDOPAnno(pathname=c_dar.pathname,
                                  fname_path_summary=fname_summary,
                                  fname_surgery='table_surgery.tsv',
                                  fname_ir='table_investigational_radiology.tsv',
                                  fname_save = c_dar.fname_darwin_path_impact_summary_annotated)
    df_out = objd.return_summary()

    tmp = 0

if __name__ == '__main__':
    main()