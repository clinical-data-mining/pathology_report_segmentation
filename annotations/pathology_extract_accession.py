""""
pathology_extract_accession.py

By Chris Fong - MSKCC 2020

This script will extract accession numbers that are
buried in specimen submitted columns

"""
import os
import pandas as pd
import constants_darwin as c_dar
from utils_darwin_etl import save_appended_df
from utils_pathology import parse_specimen_info
from darwin_pathology import DarwinDiscoveryPathology
from utils_pathology import extract_specimen_submitted_column


# Console settings
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.expand_frame_repr', False)

obj_path = DarwinDiscoveryPathology(pathname=c_dar.pathname,
                             fname='table_pathology.tsv')
df_path_orig = obj_path.return_df_original()
df_path_orig_slides = df_path_orig[['ACCESSION_NUMBER', 'SUBMITTED_SLIDES']]


# Extract MSK surgical accession number with 'MSK:'
# Regex for matching accession number for surgical procedure
# regex_rule_surg_accession = r'[MSK]{3}\:[ ]*[S][\d]{2}-[\d]{3,6}'
print('Extracting Matching Accession Numbers')
regex_rule_surg_accession = r'[MSKmsk]{3}[ ]{0,1}[\#]{0,1}[\"]{0,1}[\:]{0,1}[ ]*[s|S|c|C|h|H|m|M][\d]{1,2}[\-|\/]{1}[\d]{1,6}[\/\#]{0,1}[ ]{0,1}[\d]{0,2}'
regex_rule_surg_accession_mod = r'[ ]*[A-Za-z]*[s|S|c|C|h|H|m|M][\d]{1,2}[\-|\/]{1}[\d]{1,6}[\/\#]{0,1}[ ]{0,1}[\d]{0,2}'

### Load spec sub data from all path files (DDP column)
print('Extracting Matching Accession Numbers from specimens submitted column')
pathname = c_dar.pathname
pathfilename = c_dar.fname_darwin_path_col_spec_sub
pathfilename_path = os.path.join(pathname, pathfilename)
df_sample_rpt_list1 = pd.read_csv(pathfilename_path, header=0, low_memory=False)
df_sample_rpt_list = df_sample_rpt_list1
df_sample_rpt_list['SPECIMEN_NUMBER'] = df_sample_rpt_list['SPECIMEN_NUMBER'].fillna(1)

# LAbel if submitted slides.
rule1 = 'outside'
rule2 = 'msk'
rule3 = '|'.join(['ssl', 'sbl'])
submitted_slides = df_sample_rpt_list['SPECIMEN_SUBMITTED'].str.lower().str.contains(rule3).fillna(False)
at_msk = df_sample_rpt_list['SPECIMEN_SUBMITTED'].str.lower().str.contains(rule2).fillna(False)
outside = df_sample_rpt_list['SPECIMEN_SUBMITTED'].str.lower().str.contains(rule1).fillna(False)
submitted_slides_f = submitted_slides | (outside & ~at_msk)
df_sample_rpt_list = df_sample_rpt_list.assign(NO_MSK=submitted_slides_f)
df_sample_rpt_list = df_sample_rpt_list.merge(right=df_path_orig_slides, how='left', on='ACCESSION_NUMBER')
df_sample_rpt_list['NO_MSK'] = df_sample_rpt_list['NO_MSK'] | df_sample_rpt_list['SUBMITTED_SLIDES']
df_sample_rpt_list = df_sample_rpt_list.drop(columns=['SUBMITTED_SLIDES'])

col_label_access_num = 'ACCESSION_NUMBER'
col_label_spec_num = 'SPECIMEN_NUMBER'
col_spec_sub = 'SPECIMEN_SUBMITTED'
df_access_num_source = extract_specimen_submitted_column(df_spec_listing=df_sample_rpt_list,
                                                         regex_str=regex_rule_surg_accession,
                                                         col_spec_sub=col_spec_sub,
                                                         col_label_access_num=col_label_access_num,
                                                         col_label_spec=col_label_spec_num)

df_access_num_source_mod = extract_specimen_submitted_column(df_spec_listing=df_sample_rpt_list[df_sample_rpt_list['NO_MSK'] == False],
                                                         regex_str=regex_rule_surg_accession_mod,
                                                         col_spec_sub=col_spec_sub,
                                                         col_label_access_num=col_label_access_num,
                                                         col_label_spec=col_label_spec_num)

# Replace blanks with nan and convert to float
df_access_num_source['SOURCE_SPEC_NUM_0'] = df_access_num_source['SOURCE_SPEC_NUM_0'].str.strip()
df_access_num_source.loc[df_access_num_source['SOURCE_SPEC_NUM_0'] == '', 'SOURCE_SPEC_NUM_0'] = pd.np.nan
df_access_num_source_mod['SOURCE_SPEC_NUM_0'] = df_access_num_source_mod['SOURCE_SPEC_NUM_0'].str.strip()
df_access_num_source_mod.loc[df_access_num_source_mod['SOURCE_SPEC_NUM_0'] == '', 'SOURCE_SPEC_NUM_0'] = pd.np.nan


# Combine accession extraction methods
log1 = df_access_num_source_mod['SOURCE_ACCESSION_NUMBER_0'].notnull() & df_access_num_source['SOURCE_ACCESSION_NUMBER_0'].isnull()
ind_rep = df_access_num_source[log1].index
df_access_num_source.loc[ind_rep, 'SOURCE_ACCESSION_NUMBER_0'] = df_access_num_source_mod.loc[log1, 'SOURCE_ACCESSION_NUMBER_0']
df_access_num_source.loc[ind_rep, 'SOURCE_SPEC_NUM_0'] = df_access_num_source_mod.loc[log1, 'SOURCE_SPEC_NUM_0']

# For accession numbers that don't have a spec number, fill with '1' if only 1 spec was submitted.
df_sample_count = df_sample_rpt_list.groupby(['ACCESSION_NUMBER'])['SPECIMEN_NUMBER'].count().reset_index()
log2 = (df_access_num_source['SOURCE_ACCESSION_NUMBER_0'].notnull()) & (df_access_num_source['SOURCE_SPEC_NUM_0'].isnull())
ids_change1 = df_access_num_source.loc[log2, 'SOURCE_ACCESSION_NUMBER_0']
ids_change2 = df_sample_count.loc[df_sample_count['SPECIMEN_NUMBER'] == 1, 'ACCESSION_NUMBER']
ids_change_f = list(set.intersection(set(ids_change1), set(ids_change2)))

# Replace SOURCE_SPEC_NUM_0 with blanks with 1 if that accession has only 1 sample submitted
df_access_num_source.loc[df_access_num_source['SOURCE_ACCESSION_NUMBER_0'].isin(ids_change_f), 'SOURCE_SPEC_NUM_0'] = 1.0


#
# ### Load spec sub data from molecular spec sub sections in reports
# print('Extracting Matching Accession Numbers from DMP Notes')
# pathname = c_dar.pathname
# fname = c_dar.fname_darwin_path_molecular_note_spec_sub
# pathfilename = os.path.join(pathname, fname)
# df_sample_rpt_list2 = pd.read_csv(pathfilename, header=0, low_memory=False)
# df_sample_rpt_list = df_sample_rpt_list2
#
# df_access_num_source_dmp_spec = extract_specimen_submitted_column(df_spec_listing=df_sample_rpt_list,
#                                                          regex_str=regex_rule_surg_accession,
#                                                          col_spec_sub=col_spec_sub,
#                                                          col_label_access_num=col_label_access_num,
#                                                          col_label_spec=col_label_spec_num)
#
# ### Load spec sub data from surgical spec sub sections in reports
# print('Extracting Matching Accession Numbers from Surgical Notes')
# pathname = c_dar.pathname
# fname = c_dar.fname_darwin_path_surgical_note_spec_sub
# pathfilename = os.path.join(pathname, fname)
# df_sample_rpt_list3 = pd.read_csv(pathfilename, header=0, low_memory=False)
# df_sample_rpt_list = df_sample_rpt_list3
#
# df_access_num_source_surg_spec = extract_specimen_submitted_column(df_spec_listing=df_sample_rpt_list,
#                                                          regex_str=regex_rule_surg_accession,
#                                                          col_spec_sub=col_spec_sub,
#                                                          col_label_access_num=col_label_access_num,
#                                                          col_label_spec=col_label_spec_num)
#
# ### Load spec sub data from surgical specimen headers in diagnosis portion of reports
# pathname = c_dar.pathname
# fname = 'table_pathology_surgical_samples_parsed_specimen_long.csv'
# pathfilename = os.path.join(pathname, fname)
# df_sample_rpt_list4 = pd.read_csv(pathfilename, header=0, low_memory=False)
# df_sample_rpt_list = df_sample_rpt_list4
# col_label_spec_num = 'PATH_DX_SPEC_NUM'
# col_spec_sub = 'PATH_DX_SPEC_TITLE'
#
# df_access_num_source_surg_note = extract_specimen_submitted_column(df_spec_listing=df_sample_rpt_list,
#                                                          regex_str=regex_rule_surg_accession,
#                                                          col_spec_sub=col_spec_sub,
#                                                          col_label_access_num=col_label_access_num,
#                                                          col_label_spec=col_label_spec_num)

#### Combine frames, use spec sub column (DDP) as reference
log1 = df_access_num_source['SOURCE_ACCESSION_NUMBER_0'].notnull()
df_access_num_source_complete = df_access_num_source[log1]

# Find source of the first level of sources
df_access_num_source_complete['SOURCE_SPEC_NUM_0'] = df_access_num_source_complete['SOURCE_SPEC_NUM_0'].astype(float)
df_access_num_source_complete1 = df_access_num_source_complete[df_access_num_source_complete['SOURCE_SPEC_NUM_0'].notnull()]
df_access_num_source_complete1['SPECIMEN_NUMBER'] = df_access_num_source_complete1['SPECIMEN_NUMBER'].astype(int)
df_access_num_source_complete1['SOURCE_SPEC_NUM_0'] = df_access_num_source_complete1['SOURCE_SPEC_NUM_0'].astype(int).astype(object)
df_access_num_source_complete2 = df_access_num_source_complete1[['ACCESSION_NUMBER', 'SPECIMEN_NUMBER', 'SOURCE_ACCESSION_NUMBER_0', 'SOURCE_SPEC_NUM_0']].copy()
df_access_num_source_complete2 = df_access_num_source_complete2.rename(columns={'SOURCE_ACCESSION_NUMBER_0': 'SOURCE_ACCESSION_NUMBER_0b',
                                               'SOURCE_SPEC_NUM_0': 'SOURCE_SPEC_NUM_0b',
                                               'ACCESSION_NUMBER': 'SOURCE_ACCESSION_NUMBER_0',
                                               'SPECIMEN_NUMBER':'SOURCE_SPEC_NUM_0'})

df_access_num_source_complete3 = df_access_num_source_complete.merge(right=df_access_num_source_complete2, how='left',
                                            on=['SOURCE_ACCESSION_NUMBER_0', 'SOURCE_SPEC_NUM_0'])

### Clean /remove accessions labelled as source reports, but are not from the same patient
dmp_test = df_sample_rpt_list[['DARWIN_PATIENT_ID', 'ACCESSION_NUMBER']].drop_duplicates()
# Compute patient IDs for original accession
df_access_num_source_complete3['SOURCE_ACCESSION_NUMBER_0'] = df_access_num_source_complete3['SOURCE_ACCESSION_NUMBER_0'].str.strip()
df_dmp_test = df_access_num_source_complete3.merge(right=dmp_test, how='left', on='ACCESSION_NUMBER')
# Compute patient IDs for sourced accession
df_dmp_test1 = df_dmp_test.merge(right=dmp_test, how='left', right_on='ACCESSION_NUMBER', left_on='SOURCE_ACCESSION_NUMBER_0')
# Remove accessions that do not match original patient ID, even if outside
log_remove1 = df_dmp_test1['DARWIN_PATIENT_ID_y'].isnull()
log_remove2 = df_dmp_test1['DARWIN_PATIENT_ID_x'] != df_dmp_test1['DARWIN_PATIENT_ID_y']

# Compute patient IDs for secondary sourced accessions (source of source)
df_dmp_test2 = df_dmp_test1.merge(right=dmp_test, how='left', right_on='ACCESSION_NUMBER', left_on='SOURCE_ACCESSION_NUMBER_0b')
# Remove accessions that do not match original patient ID, even if outside
log_remove1b = df_dmp_test2['DARWIN_PATIENT_ID'].isnull()
log_remove2b = df_dmp_test2['DARWIN_PATIENT_ID_x'] != df_dmp_test2['DARWIN_PATIENT_ID']

log_remove_source0 = log_remove1 | log_remove2
cols_clean = ['SOURCE_ACCESSION_NUMBER_0', 'SOURCE_SPEC_NUM_0']
log_remove_source0b = log_remove1b | log_remove2b
cols_clean_0 = ['SOURCE_ACCESSION_NUMBER_0b', 'SOURCE_SPEC_NUM_0b']

# Clean sources
df_access_num_source_complete3.loc[log_remove_source0, cols_clean[0]] = pd.np.NaN
df_access_num_source_complete3.loc[log_remove_source0, cols_clean[1]] = pd.np.NaN
df_access_num_source_complete3.loc[log_remove_source0, cols_clean_0[0]] = pd.np.NaN
df_access_num_source_complete3.loc[log_remove_source0, cols_clean_0[1]] = pd.np.NaN

# Clean secondary sources
df_access_num_source_complete3.loc[log_remove_source0, cols_clean_0[0]] = pd.np.NaN
df_access_num_source_complete3.loc[log_remove_source0, cols_clean_0[1]] = pd.np.NaN

### Save data
df_access_num_source_complete3.to_csv('path_accessions.csv', index=False)


tmp = 0
# Add columns
# df = df.assign(ACCESSION_NUM_PATH_REPORT_0=df_access_num)
# df = df.assign(PATH_REPORT_SPEC_NUM_0=df_specimen_num)