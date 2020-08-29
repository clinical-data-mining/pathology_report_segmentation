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
import constants_darwin as c_dar
from pathology_extract_dop import PathologyExtractDOP
from utils_pathology import clean_date_column
from utils_darwin_etl import set_debug_console
from darwin_pathology import DarwinDiscoveryPathology
import plotly.express as px


set_debug_console()

## Constants
col_label_access_num = 'ACCESSION_NUMBER'
col_label_spec_num = 'SPECIMEN_NUMBER'
col_spec_sub = 'SPECIMEN_SUBMITTED'
col_sample_id1 = 'SAMPLE_ID'
col_sample_id2 = 'Sample ID'
col_id1 = 'DMP_ID'
col_id2 = 'Patient ID'

### Load files needed to extract DOP
# Load IMPACT samples
pathname = c_dar.pathname
fname = 'mskimpact_clinical_data.tsv'
pathfilename = os.path.join(pathname, fname)
df_impact = pd.read_csv(pathfilename, header=0, low_memory=False, sep='\t')
df_impact1 = df_impact[[col_id2, col_sample_id2]].drop_duplicates()
df_impact1 = df_impact1.rename(columns={col_id2: col_id1, col_sample_id2: col_sample_id1})

obj_path = DarwinDiscoveryPathology(pathname=c_dar.pathname, fname='table_pathology.tsv')
df_path = obj_path.return_df_original()
df_path_impact = df_path.loc[df_path[col_sample_id1].notnull(), [col_label_access_num, col_sample_id1, 'REPORT_CMPT_DATE']]
df_impact_map = df_impact1.merge(right=df_path_impact, how='inner', on=col_sample_id1)
df_impact_map = df_impact_map.rename(columns={'ACCESSION_NUMBER': 'ACCESSION_NUMBER_DMP',
                                              'REPORT_CMPT_DATE': 'DATE_SEQUENCING_REPORT'})

# Load parsed specimen submitted list
pathname = c_dar.pathname
pathfilename = c_dar.fname_darwin_path_col_spec_sub
obj_dop = PathologyExtractDOP(pathname=pathname,
                              fname=pathfilename,
                              col_label_access_num=col_label_access_num,
                              col_label_spec_num=col_label_spec_num,
                              col_spec_sub=col_spec_sub,
                              fname_out=None)
series_expand_all = obj_dop.return_df()
df_sample_rpt_list1 = obj_dop.return_df_original()

# Load connecting path accessions
pathname = c_dar.pathname
fname = 'path_accessions.csv'
pathfilename = os.path.join(pathname, fname)
df_accessions = pd.read_csv(pathfilename, header=0, low_memory=False)
# Clean columns TODO: do this in accession extraction
df_accessions = df_accessions.drop(columns=['SOURCE_ACCESSION_NUMBER_1', 'SOURCE_SPEC_NUM_1'])


# Desired columns:
# DMP_ID
# SAMPLE_ID
# ACCESSION_NUMBER (DMP) = ACCESSION_NUMBER_DMP
# SPECIMEN_NUMBER (DMP) = SPECIMEN_NUMBER_DMP
# REPORT_CMPT_DATE (DMP) = DATE_SEQUENCING_REPORT
# ACCESSION_NUMBER (Source 0/1/2)
# SPECIMEN_NUMBER (Source 0/1/2)
# REPORT_CMPT_DATE (Source 0/1/2)
# DATE_OF_PROCEDURE_SURGICAL
# SPECIMEN_SUBMITTED

### Merge data frames linking accession numbers to impact IDs

# MERGE 1 -- Merge Sample IDs with DMP SPEC number and submitted description
df = df_impact_map.merge(right=df_sample_rpt_list1, how='left',
                         left_on='ACCESSION_NUMBER_DMP',
                         right_on='ACCESSION_NUMBER')
# Clean columns
df = df.rename(columns={'SPECIMEN_NUMBER': 'SPECIMEN_NUMBER_DMP',
                        'SPECIMEN_SUBMITTED': 'SPECIMEN_SUBMITTED_DMP'})
df = df.drop(columns=['ACCESSION_NUMBER'])

# MERGE 2 -- Merge with DOP
df['SPECIMEN_NUMBER_DMP'] = df['SPECIMEN_NUMBER_DMP'].astype(int).astype(object)
series_expand_all['SPECIMEN_NUMBER'] = series_expand_all['SPECIMEN_NUMBER'].astype(int).astype(object)
df = df.merge(right=series_expand_all, how='left',
              left_on=['ACCESSION_NUMBER_DMP', 'SPECIMEN_NUMBER_DMP'],
              right_on=[col_label_access_num, col_label_spec_num])

# MERGE 3 -- Merge Source accession number(s) with dates of procedure
df_source_accession_dop = df_accessions.merge(right=series_expand_all, how='left',
                                              left_on=['SOURCE_ACCESSION_NUMBER_0', 'SOURCE_SPEC_NUM_0'],
                                              right_on=[col_label_access_num, col_label_spec_num])
# Clean columns
df_source_accession_dop = df_source_accession_dop.rename(columns={'ACCESSION_NUMBER_x': 'ACCESSION_NUMBER',
                                                                  'SPECIMEN_NUMBER_x': 'SPECIMEN_NUMBER',
                                                                  'DATE_OF_PROCEDURE_SURGICAL': 'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0'})
df_source_accession_dop = df_source_accession_dop.drop(columns=['ACCESSION_NUMBER_y', 'SPECIMEN_NUMBER_y'])
# Do the same with SOURCE_ACCESSION_NUMBER_0b
df_source_accession_dop = df_source_accession_dop.merge(right=series_expand_all, how='left',
                                                          left_on=['SOURCE_ACCESSION_NUMBER_0b', 'SOURCE_SPEC_NUM_0b'],
                                                          right_on=[col_label_access_num, col_label_spec_num])
# Clean columns
df_source_accession_dop = df_source_accession_dop.rename(columns={'ACCESSION_NUMBER_x': 'ACCESSION_NUMBER',
                                                                  'SPECIMEN_NUMBER_x': 'SPECIMEN_NUMBER',
                                                                  'DATE_OF_PROCEDURE_SURGICAL': 'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0b'})
df_source_accession_dop = df_source_accession_dop.drop(columns=['ACCESSION_NUMBER_y', 'SPECIMEN_NUMBER_y'])


# Get accession numbers and dates of dmp reports for impact ids
df_impact_dmp = df_impact[[col_sample_id2]].merge(right=df_path[[col_sample_id1, col_label_access_num, 'REPORT_CMPT_DATE']],
                                               how='left', left_on=col_sample_id2, right_on=col_sample_id1)
df_impact_dmp1 = df_impact_dmp[df_impact_dmp[col_sample_id1].notnull()]

# Merge with parsed specimen submitted
df_impact_dmp2 = df_impact_dmp1.merge(right=df_sample_rpt_list1, how='left', on=col_label_access_num)



# Clean df_impact_dmp3 columns
df_impact_dmp3 = df_impact_dmp3.drop(columns=['DATE_EXTRACTED_1', 'DATE_EXTRACTED_2', col_sample_id2])

### Connect impact dmp accessions to source accessions
# Compute source accession for all impact reports
df_impact_surg = df_impact_dmp3[[col_label_access_num, col_label_spec_num]].merge(right=df_accessions, how='left', on=[col_label_access_num, col_label_spec_num])

## Compute DOP for source accessions
df_impact_surg1 = df_impact_surg[['SOURCE_ACCESSION_NUMBER_0', 'SOURCE_SPEC_NUM_0']].merge(right=series_expand_all, how='left',
                                       right_on=[col_label_access_num, col_label_spec_num],
                                       left_on=['SOURCE_ACCESSION_NUMBER_0', 'SOURCE_SPEC_NUM_0'])



# Compute date of report for source surg path reports
df_surg_access = df_impact_surg[['SOURCE_ACCESSION_NUMBER_0']].drop_duplicates()
df_surg_access1 = df_surg_access.merge(right=df_path[[col_label_access_num, 'REPORT_CMPT_DATE']],
                                                  how='left', left_on='SOURCE_ACCESSION_NUMBER_0', right_on=col_label_access_num)
df_surg_access1 = df_surg_access1.drop(columns=col_label_access_num)
df_surg_access1 = df_surg_access1.rename(columns={'REPORT_CMPT_DATE': 'DTE_SOURCE_0_PATH_PROCEDURE'})
df_surg_access1 = df_surg_access1[df_surg_access1['SOURCE_ACCESSION_NUMBER_0'].notnull()]

df_surg_access_b = df_impact_surg[['SOURCE_ACCESSION_NUMBER_0b']].drop_duplicates()
df_surg_access1b = df_surg_access_b.merge(right=df_path[[col_label_access_num, 'REPORT_CMPT_DATE']],
                                                  how='left', left_on='SOURCE_ACCESSION_NUMBER_0b', right_on=col_label_access_num)
df_surg_access1b = df_surg_access1b.drop(columns=col_label_access_num)
df_surg_access1b = df_surg_access1b.rename(columns={'REPORT_CMPT_DATE': 'DTE_SOURCE_0b_PATH_PROCEDURE'})
df_surg_access1b = df_surg_access1b[df_surg_access1b['SOURCE_ACCESSION_NUMBER_0b'].notnull()]

df_impact_surg1 = df_impact_surg.merge(right=df_surg_access1, how='left', on='SOURCE_ACCESSION_NUMBER_0')
df_impact_surg1 = df_impact_surg1.merge(right=df_surg_access1b, how='left', on='SOURCE_ACCESSION_NUMBER_0b')

# Merge source accessions with DOP
df_impact_surg2 = df_impact_surg1.merge(right=series_expand_all[[col_label_access_num, col_label_spec_num, 'DATE_EXTRACTED_0', 'DATE_EXTRACTED_0_all']], how='left',
                      left_on=['SOURCE_ACCESSION_NUMBER_0', 'SOURCE_SPEC_NUM_0'],
                      right_on=[col_label_access_num, col_label_spec_num])

df_impact_surg3 = df_impact_surg2.merge(right=series_expand_all[[col_label_access_num, col_label_spec_num, 'DATE_EXTRACTED_0', 'DATE_EXTRACTED_0_all']], how='left',
                      left_on=['SOURCE_ACCESSION_NUMBER_0b', 'SOURCE_SPEC_NUM_0b'],
                      right_on=[col_label_access_num, col_label_spec_num])

# Clean and remove columns
cols_drop = [col_label_access_num, col_label_spec_num, 'SOURCE_ACCESSION_NUMBER_1',
             'SOURCE_SPEC_NUM_1', 'ACCESSION_NUMBER_y', 'SPECIMEN_NUMBER_y']
df_impact_surg3 = df_impact_surg3.drop(columns=cols_drop)

cols_rename = {'ACCESSION_NUMBER_x': col_label_access_num,
               'SPECIMEN_NUMBER_x': col_label_spec_num,
               'DATE_EXTRACTED_0_x': 'DATE_EXTRACTED_SOURCE_0',
               'DATE_EXTRACTED_0_y': 'DATE_EXTRACTED_SOURCE_0b',
               'DATE_EXTRACTED_0_all_x': 'DATE_EXTRACTED_SOURCE_0_all',
               'DATE_EXTRACTED_0_all_y': 'DATE_EXTRACTED_SOURCE_0b_all'
               }
df_impact_surg3 = df_impact_surg3.rename(columns=cols_rename)


### Merge datasets
t = df_impact_dmp3.merge(right=df_impact_surg3, how='left', on=[col_label_access_num, col_label_spec_num])
# Fill in blanks where possible
t = t.assign(DTE_IMPACT_COLLECTED=t['DATE_EXTRACTED_0'])
t['DTE_IMPACT_COLLECTED'] = t['DTE_IMPACT_COLLECTED'].fillna(t['DATE_EXTRACTED_SOURCE_0'])
t['DTE_IMPACT_COLLECTED'] = t['DTE_IMPACT_COLLECTED'].fillna(t['DATE_EXTRACTED_SOURCE_0b'])
t['DTE_IMPACT_COLLECTED'] = t['DTE_IMPACT_COLLECTED'].fillna(t['DATE_EXTRACTED_0_all'])
t['DTE_IMPACT_COLLECTED'] = t['DTE_IMPACT_COLLECTED'].fillna(t['DATE_EXTRACTED_SOURCE_0_all'])
t['DTE_IMPACT_COLLECTED'] = t['DTE_IMPACT_COLLECTED'].fillna(t['DATE_EXTRACTED_SOURCE_0b_all'])

t['DTE_IMPACT_COLLECTED'].isnull().sum()

# Drop columns that were used to fill null values
cols_drop = ['DATE_EXTRACTED_0', 'DATE_EXTRACTED_SOURCE_0', 'DATE_EXTRACTED_SOURCE_0b', 'DATE_EXTRACTED_0_all',
             'DATE_EXTRACTED_SOURCE_0_all', 'DATE_EXTRACTED_SOURCE_0b_all']
t = t.drop(columns=cols_drop)

# Clean date columns
t['DTE_IMPACT_COLLECTED'] = t['DTE_IMPACT_COLLECTED'].str.replace('DOP:', '').str.strip()
t = clean_date_column(df=t, col_date='DTE_IMPACT_COLLECTED')

t['DTE_IMPACT_COLLECTED'] = pd.to_datetime(t['DTE_IMPACT_COLLECTED'])
t['REPORT_CMPT_DATE'] = pd.to_datetime(t['REPORT_CMPT_DATE'])
t['DTE_SOURCE_0_PATH_PROCEDURE'] = pd.to_datetime(t['DTE_SOURCE_0_PATH_PROCEDURE'])
t['DTE_SOURCE_0b_PATH_PROCEDURE'] = pd.to_datetime(t['DTE_SOURCE_0b_PATH_PROCEDURE'])

id_from_sample = t[col_sample_id1].apply(lambda x: x[:9])
log_id_match = t[col_id1] == id_from_sample
t_f = t[log_id_match]

### Save data
fname = 'pathology_dop.csv'
pathname = c_dar.pathname
pathfilename_save = os.path.join(pathname, fname)
t_f.to_csv(pathfilename_save, index=False)


tmp = 0
