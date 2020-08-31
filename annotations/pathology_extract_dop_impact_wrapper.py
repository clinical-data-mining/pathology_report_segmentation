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
obj_path = DarwinDiscoveryPathology(pathname=c_dar.pathname, fname='table_pathology.tsv')
df_path = obj_path.return_df_original()
df_path_impact = df_path.loc[df_path[col_sample_id1].notnull(), [col_label_access_num, col_sample_id1, 'REPORT_CMPT_DATE']]
df_impact_map = df_path_impact.rename(columns={'ACCESSION_NUMBER': 'ACCESSION_NUMBER_DMP',
                                              'REPORT_CMPT_DATE': 'DATE_SEQUENCING_REPORT'})
df_report_date = df_path[['ACCESSION_NUMBER', 'REPORT_CMPT_DATE']].drop_duplicates()

# Load parsed specimen submitted list
pathname = c_dar.pathname
fname = 'pathology_spec_part_dop.csv'
pathfilename = os.path.join(pathname, fname)
df_dop = pd.read_csv(pathfilename, header=0, low_memory=False)

# Load connecting path accessions
pathname = c_dar.pathname
fname = 'path_accessions.csv'
pathfilename = os.path.join(pathname, fname)
df_accession = pd.read_csv(pathfilename, header=0, low_memory=False)

# Load relevant accession numbers to pull DOP from
df_accession1 = df_accession[df_accession[col_label_access_num].isin(df_impact_map['ACCESSION_NUMBER_DMP'])]
df_accession1 = df_accession1.drop_duplicates().reset_index(drop=True)
# Convert data types
df_accession1[col_label_spec_num] = df_accession1[col_label_spec_num].astype(int).astype(str)
df_accession1['SOURCE_SPEC_NUM_0'] = df_accession1['SOURCE_SPEC_NUM_0'].fillna(0).astype(int).astype(str)
df_accession1['SOURCE_SPEC_NUM_0b'] = df_accession1['SOURCE_SPEC_NUM_0b'].fillna(0).astype(int).astype(str)
df_accession1.loc[df_accession1['SOURCE_SPEC_NUM_0'] == '0', 'SOURCE_SPEC_NUM_0'] = pd.np.NaN
df_accession1.loc[df_accession1['SOURCE_SPEC_NUM_0b'] == '0', 'SOURCE_SPEC_NUM_0b'] = pd.np.NaN

key = df_accession1[[col_label_access_num, col_label_spec_num]].apply(lambda x: '-'.join(x), axis=1)
df_accession1 = df_accession1.assign(KEY=key)

# Compute all reports associated with the impact sample
df_a = pd.concat([df_accession1[col_label_access_num], df_accession1['SOURCE_ACCESSION_NUMBER_0'],
                  df_accession1['SOURCE_ACCESSION_NUMBER_0b']], axis=0, sort=False)
list_a = list(df_a.dropna().drop_duplicates().reset_index(drop=True))

# Filter the DOP table by this list
df_dop1 = df_dop[df_dop[col_label_access_num].isin(list_a)]
df_dop1 = df_dop1.drop(columns=['DOP_DATE_ERROR'])
df_dop1 = df_dop1[df_dop1['DATE_OF_PROCEDURE_SURGICAL'].notnull()]
df_dop1[col_label_spec_num] = df_dop1[col_label_spec_num].astype(int).astype(str)
key = df_dop1[[col_label_access_num, col_label_spec_num]].apply(lambda x: '-'.join(x), axis=1)
df_dop1 = df_dop1.assign(KEY=key)
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
df = df_accession1.merge(right=df_impact_map, how='right', left_on='ACCESSION_NUMBER', right_on='ACCESSION_NUMBER_DMP')
# Clean columns
df = df.rename(columns={'SPECIMEN_NUMBER': 'SPECIMEN_NUMBER_DMP',
                        'REPORT_CMPT_DATE': 'REPORT_CMPT_DATE_DMP'})
df = df.drop(columns=['ACCESSION_NUMBER'])
df1 = df.groupby(['SAMPLE_ID']).first().reset_index()

# MERGE 2 -- Merge with DOP
df1['SPECIMEN_NUMBER_DMP'] = df1['SPECIMEN_NUMBER_DMP'].astype(int)
df1 = df1.merge(right=df_dop1, how='left', on='KEY')
df1 = df1.drop_duplicates()
df1 = df1.drop(columns=['KEY', 'ACCESSION_NUMBER', 'SPECIMEN_NUMBER'])

# MERGE 3 -- Merge Source accession number (1) with dates of procedure
key = df1[['SOURCE_ACCESSION_NUMBER_0', 'SOURCE_SPEC_NUM_0']].dropna().apply(lambda x: '-'.join(x), axis=1)
df2 = pd.concat([df1, key], axis=1, sort=False)
df2 = df2.rename(columns={0: 'KEY'})
df2 = df2.merge(right=df_dop1, how='left', on='KEY')
df2 = df2.drop(columns=['KEY', 'ACCESSION_NUMBER', 'SPECIMEN_NUMBER'])
df2 = df2.rename(columns={'DATE_OF_PROCEDURE_SURGICAL_x': 'DATE_OF_PROCEDURE_SURGICAL_DMP',
                          'DATE_OF_PROCEDURE_SURGICAL_y': 'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0'})

# MERGE 4 -- Merge Source accession number (2) with dates of procedure
key = df2[['SOURCE_ACCESSION_NUMBER_0b', 'SOURCE_SPEC_NUM_0b']].dropna().apply(lambda x: '-'.join(x), axis=1)
df3 = pd.concat([df2, key], axis=1, sort=False)
df3 = df3.rename(columns={0: 'KEY'})
df3 = df3.merge(right=df_dop1, how='left', on='KEY')
df3 = df3.drop(columns=['KEY', 'ACCESSION_NUMBER', 'SPECIMEN_NUMBER'])
df3 = df3.rename(columns={'DATE_OF_PROCEDURE_SURGICAL': 'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0b'})

# MERGE 5 -- Merge Source accession number with dates of REPORTS
df3_a = df3.loc[df3['SOURCE_ACCESSION_NUMBER_0'].notnull(), ['SOURCE_ACCESSION_NUMBER_0']]
rpt_date0 = df3_a.merge(right=df_report_date, how='left', left_on='SOURCE_ACCESSION_NUMBER_0', right_on='ACCESSION_NUMBER')
rpt_date0 = rpt_date0.rename(columns={'REPORT_CMPT_DATE': 'REPORT_CMPT_DATE_SOURCE_0'})
rpt_date0 = rpt_date0.drop(columns=['ACCESSION_NUMBER'])

df3_b = df3.loc[df3['SOURCE_ACCESSION_NUMBER_0b'].notnull(), ['SOURCE_ACCESSION_NUMBER_0b']]
rpt_date0b = df3_b.merge(right=df_report_date, how='left', left_on='SOURCE_ACCESSION_NUMBER_0b', right_on='ACCESSION_NUMBER')
rpt_date0b = rpt_date0b.rename(columns={'REPORT_CMPT_DATE': 'REPORT_CMPT_DATE_SOURCE_0b'})
rpt_date0b = rpt_date0b.drop(columns=['ACCESSION_NUMBER'])

df4 = df3.merge(right=rpt_date0, how='left', on='SOURCE_ACCESSION_NUMBER_0')
df4 = df4.drop_duplicates()
df4 = df4.merge(right=rpt_date0b, how='left', on='SOURCE_ACCESSION_NUMBER_0b')

# Clean columns
DATE_OF_PROCEDURE_SURGICAL = df3['DATE_OF_PROCEDURE_SURGICAL_DMP'].fillna(df3['DATE_OF_PROCEDURE_SURGICAL_SOURCE_0b'])
DATE_OF_PROCEDURE_SURGICAL = DATE_OF_PROCEDURE_SURGICAL.fillna(df3['DATE_OF_PROCEDURE_SURGICAL_SOURCE_0'])
df4 = df4.assign(DATE_OF_PROCEDURE_SURGICAL=DATE_OF_PROCEDURE_SURGICAL)
# Drop DOP that were used to combine.
df4  = df4.drop(columns=['DATE_OF_PROCEDURE_SURGICAL_DMP', 'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0', 'DATE_OF_PROCEDURE_SURGICAL_SOURCE_0b'])


### Save data
fname = 'pathology_dop_impact_summary.csv'
pathname = c_dar.pathname
pathfilename_save = os.path.join(pathname, fname)
df4.to_csv(pathfilename_save, index=False)


tmp = 0
