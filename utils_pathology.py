""""
utils_pathology.py

By Chris Fong - MSKCC 2019


"""
import os
import re
import pandas as pd
import numpy as np


def parse_specimen_info(df, col_name):
    # Add number of specimens by finding sample number in text with \n and :, then find largest number

    # Add function to remove new line that are within brackets
    index_condition = df[col_name].notnull()
    df_spec_list = df.loc[index_condition, col_name]

    # Clean section, take bracketed segments and remove new line carriage return
    regex_brackets = '\([^)]*\)'
    regex = re.compile(regex_brackets)
    fn_rmv_cr = lambda x: regex.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
    df_spec_list = df_spec_list.apply(fn_rmv_cr)

    # regex_new line removal that are around dates
    regex_nl2 = '([:]{1}[ ]*[\\r\\n]+[ ]*[\d]{1,2}[/]{1}[\d]{1,2}[/]{1}[\d]{1,4}[ ]*)'
    regex_3 = re.compile(regex_nl2)
    fn_rmv_ln2 = lambda x: regex_3.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
    df_spec_list = df_spec_list.apply(fn_rmv_ln2)

    # Remove new line that happens to be next to a clock description (ex \r\n11:00)
    regex_nl3 = '([\\r\\n]+[\d]{1,2}:{1}[\d]{2})'
    regex_3 = re.compile(regex_nl3)
    fn_rmv_ln3 = lambda x: regex_3.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
    df_spec_list = df_spec_list.apply(fn_rmv_ln3)

    # Apply new line removal changes to original df
    df[col_name] = df_spec_list

    # Compute number of specimens
    # regex_rule_spec_num = '[\s]{2}[ ]*[\d]{1,2}:'
    regex_rule_spec_num = '(\\r\\n|^)([ ]*[\d]{1,2}:)'
    regex_spec_titles = re.compile(regex_rule_spec_num)

    # Split specimen section by text list TODO: There is still ~800 where counts cant be computed
    list1 = df_spec_list.apply(lambda str: regex_spec_titles.split(str))
    # Remove first column of the split asthey are all 'Specimens Submitted:'
    # TODO (cjf) Make decision on handling this part as 'Specimens Submitted:' doesn't exist for datasets
    list1 = list1.apply(lambda x: x[1:])
    # Take every 3rd entry -- (Space), (Spec num), (Spec description)
    list1 = list1.apply(lambda x: x[2::3])
    # Strip blank spaces at ends of list elements
    list1 = list1.apply(lambda x: list(map(lambda y: y.strip(), x)))
    # Remove new line from all entries since specimens are parsed
    list1 = list1.apply(lambda x: list(map(lambda y: y.replace('\r\n', ''), x)))

    # Save into a series
    dictOfWords = lambda x: {i+1: x[i] for i in range(0, len(x))}
    dictOfWords2 = lambda x: {1: x}
    list2 = list1.apply(dictOfWords)

    # Fill in the rest of blanks with single specimen
    ind_missing = list2[list2.values == {}].index
    str_single_spec = '[sSpPeEcCiImMeEnNsS sSuUbBmMiItTtTeEdD]{19}:[\s]+'
    spec_fill = df_spec_list[ind_missing].str.replace(str_single_spec, '').str.strip()
    dict_fill_in = spec_fill.apply(dictOfWords2)
    list2[ind_missing] = dict_fill_in

    # Split lists within series into a dataframe where each column is a specimen
    df_list = pd.DataFrame(list2.values.tolist(), index=list2.index)

    # Add column for dictionary of
    df = df.assign(SPEC_SUB_DICT=list2)

    return df, df_list


def extract_specimen_submitted_column(df_spec_listing, regex_str, col_spec_sub, col_label_access_num, col_label_spec):
    # This function is used within _extract_specimen_info
    # Create for loop for each specimen column, extract accession number and split again if needed, remove 'MSK:'
    # concatenate all columns
    # convert each row into list of access numbers
    # only take unique accession numbers
    # Expand series of lists into a dataframe of unique accession numbers
    # Extract the specimen number from list that corresponds to IMPACT sample
    # Merge with corresponding dates for surg path reports

    # Find the marker where the header of the substring ends
    marker = '[ ]*'
    sub_regex = regex_str[:(regex_str.find(marker) + len(marker))]
    # Extract string using regex input
    regex_series = df_spec_listing[col_spec_sub].apply(lambda x: re.findall(regex_str, str(x)))

    # Split the series if there are multiple accession numbers in the text find
    series_expand_all = pd.DataFrame(regex_series.values.tolist(), index=regex_series.index)

    # Init
    df_access_with_spec_num_all = None

    # Remove 'MSK:' from columns
    for j in range(series_expand_all.shape[1]):
        series_expand_all[j] = series_expand_all[j].apply(lambda x: re.sub(sub_regex, '', x) if type(x) is str else x)

        # Find cases where string was split twice due to incorrect entry of accession number
        # Split specimen number for IMPACT that is after the accession number
        df_access_with_spec_num = series_expand_all[j].str.split('/|#|,', expand=True)

        # Convert None to NA
        df_access_with_spec_num.fillna(value=np.NaN, inplace=True)

        if (df_access_with_spec_num.shape[1] > 2):
            # Get index of rows that have entries in 3rd column
            ind_double_split = df_access_with_spec_num[df_access_with_spec_num[2].notnull()].index

            # Combine first and second column of double split entries
            a_num = df_access_with_spec_num.loc[ind_double_split, 0] + '-' + df_access_with_spec_num.loc[ind_double_split, 1]
            df_access_with_spec_num.loc[ind_double_split, 0] = a_num

            # Move third column entries to second
            df_access_with_spec_num.loc[ind_double_split, 1] = df_access_with_spec_num.loc[ind_double_split, 2]

            # Delete third column
            df_access_with_spec_num = df_access_with_spec_num.drop(columns=[2])

        # Convert specimen number to numeric
        # df_access_with_spec_num[1] = df_access_with_spec_num[1].astype(float)

        # Relabel columns according to split
        col_accession = 'SOURCE_ACCESSION_NUMBER_' + str(j)
        col_accession_spec_num = 'SOURCE_SPEC_NUM_' + str(j)
        col_dict = {0: col_accession, 1: col_accession_spec_num}
        df_access_with_spec_num = df_access_with_spec_num.rename(columns=col_dict)

        # Place in larger dataframe
        if j == 0:
            df_access_with_spec_num_all = df_access_with_spec_num
        else:
            df_access_with_spec_num_all = pd.concat([df_access_with_spec_num_all, df_access_with_spec_num],
                                                    axis=1, sort=False)

    # Add original accesion number to the frame
    df = df_spec_listing[[col_label_access_num, col_label_spec]]
    if df_access_with_spec_num_all is not None:
        df_f = pd.concat([df, df_access_with_spec_num_all], axis=1, sort=False)
    else:
        df_f = df
    #
    #     # Combine accession and spec number again
    #     series_expand_all[j] = df_access_with_spec_num[0] + '|' + df_access_with_spec_num[1]
    #
    # # Put all rows of accession numbers into a list -- to ignore NaNs, use ffill across columns
    # regex_series = series_expand_all.ffill(axis=1).apply(lambda x: x.unique(), axis=1)
    #
    # # Split data if there are multiple unique accession numbers for each sample
    # series_expand = pd.DataFrame(regex_series.values.tolist(), index=regex_series.index)
    #
    # # Change column names
    # cols = series_expand.columns
    # cols_new = [col_label + '_' + str(x) for x in cols]
    # series_expand.columns = cols_new
    #
    # # Save the extracted strings in two forms: Just the first column, likely contains the primary sample and
    # # all columns combined in a list of strings
    # # Convert df into a series where rows are list of strings
    # series_concat_to_list = pd.Series(data=series_expand.ffill(axis=1).values.tolist(),
    #                                   index=series_expand.index,
    #                                   name=col_label + '_COMBINED')
    # series_concat_to_list = series_concat_to_list.apply(lambda x: list(set(x)))
    #
    # # Create df for specimen number attached to accession number
    # if col_label_spec is not None:
    #     specimen_num_df = pd.DataFrame(columns=range(len(series_expand.columns)), index=series_expand.index)
    #
    #     # Extract specimen number from df
    #     for i in range(series_expand.shape[1]):
    #         # Split accession number from specimen number
    #         regex_series = series_expand.iloc[:, i].str.split('|', expand=True)
    #         # Place accession number only back into df
    #         series_expand.iloc[:, i] = regex_series[0]
    #         # Place specimen number into a new dataframe
    #         specimen_num_df.iloc[:, i] = regex_series[1]
    #
    #     cols = specimen_num_df.columns
    #     cols_new = [col_label_spec + '_' + str(x) for x in cols]
    #     specimen_num_df.columns = cols_new
    #
    #     # Just take first column.. TODO: find way to keep all accession number and specimens without many columns
    #     specimen_num_df_first_col = specimen_num_df.iloc[:, 0]
    # else:
    #     specimen_num_df_first_col = None
    #
    # # Just take first column.. TODO: find way to keep all accession number and specimens without many columns
    # series_expand_first_col = series_expand.iloc[:, 0]
    #
    # return series_expand_first_col, series_concat_to_list, specimen_num_df_first_col
    return df_f

def clean_date_column(df, col_date):
    # Convert columns for datetime
    # Clean the one entry that was entered incorrectly
    df[col_date] = df[col_date].str.replace('2099', '2009')
    df[col_date] = df[col_date].str.replace('204', '2014')
    df[col_date] = df[col_date].str.replace('215', '2015')
    df[col_date] = df[col_date].str.replace('0216', '2016')
    df[col_date] = df[col_date].str.replace('206', '2016')
    df[col_date] = df[col_date].str.replace('/016', '/2016')
    df[col_date] = df[col_date].str.replace('217', '2017')
    df[col_date] = df[col_date].str.replace('/017', '/2017')
    df[col_date] = df[col_date].str.replace('/018', '/2018')
    df[col_date] = df[col_date].str.replace('/2108', '/2018')
    procedure_dates = pd.to_datetime(df[col_date], errors='coerce')
    df[col_date] = procedure_dates

    return df

def extract_path_note_header_dates(df):
    # Obtain dates of collection, receipt, and/or report
    # Date of collections
    df_note_only = df['path_prpt_p1'].copy()
    # Take only the header of the note - this will reduce the size of the note drastically
    df_note_only = df_note_only.apply(lambda x: x[:500])

    regex_rule_date_collect = r'(Date of Collection/Procedure/Outside Report:[ ]*)([\d]{1,2}[/]{1}[\d]{1,2}[/]{1}[\d]{2,4})([ ]*[\r\n]*)'
    regex_series_collect = df_note_only.apply(lambda x: re.findall(regex_rule_date_collect, x))
    # Find rows where len of list is > 0
    logic_collect = regex_series_collect.apply(lambda x: len(x)) > 0
    date_collect = regex_series_collect[logic_collect].apply(lambda x: x[0][1])

    # Date of receipt
    regex_rule_date_receipt = r'(Date of Receipt:[ ]*)([\d]{1,2}[/]{1}[\d]{1,2}[/]{1}[\d]{2,4})([ ]*[\r\n]*)'
    regex_series_receipt = df_note_only.apply(lambda x: re.findall(regex_rule_date_receipt, x))
    # Find rows where len of list is > 0
    logic_receipt = regex_series_receipt.apply(lambda x: len(x)) > 0
    date_receipt = regex_series_receipt[logic_receipt].apply(lambda x: x[0][1])

    # Date of report
    regex_rule_date_report = r'(Date of Report:[ ]*)([\d]{1,2}[/]{1}[\d]{1,2}[/]{1}[\d]{2,4})([ ]*[\r\n]*)'
    regex_series_report = df_note_only.apply(lambda x: re.findall(regex_rule_date_report, x))
    # Find rows where len of list is > 0
    logic_report = regex_series_report.apply(lambda x: len(x)) > 0
    date_report = regex_series_report[logic_report].apply(lambda x: x[0][1])

    # Create data frame with the dates
    df_date_collect = pd.concat([date_collect, date_receipt, date_report], axis=1)
    col_names = ['DATE_OF_COLLECTION_OUTSIDE_RPT', 'DATE_OF_RECEIPT', 'DATE_OF_REPORT']
    df_date_collect.columns = col_names

    # Normalize dates to birthdate so only age is shown
    df_date_collect['DATE_OF_COLLECTION_OUTSIDE_RPT'] = pd.to_datetime(df_date_collect['DATE_OF_COLLECTION_OUTSIDE_RPT'])
    df_date_collect['DATE_OF_RECEIPT'] = pd.to_datetime(df_date_collect['DATE_OF_RECEIPT'])
    df_date_collect['DATE_OF_REPORT'] = pd.to_datetime(df_date_collect['DATE_OF_REPORT'])

    # Concatenate the dates to the data frame and return
    df = pd.concat([df[['Accession Number']], df_date_collect], axis=1)

    return df



def extract_dates_from_path_headers(df_path, df_append):
    # Obtain dates of collection, receipt, and/or report from surgical path report associated with the impact sample
    print('Extracting Dates from Header')
    # Perform this for dmp reports
    df_dmp_note = df_path.loc[df_path['Accession Number'].isin(df_append['ACCESSION_NUM_DMP_REPORT'])]
    df_dmp_reporting_dates = extract_path_note_header_dates(df=df_dmp_note)
    # TODO Rename columns

    # Perform this for surgical path reports
    df_surg_note = df_path.loc[df_path['Accession Number'].isin(df_append['ACCESSION_NUM_PATH_REPORT_0'])]
    df_surg_reporting_dates = extract_path_note_header_dates(df=df_surg_note)

    # Merge date of surg path reports for the multiple columns
    df_sample_rpt_merged = df_append.merge(right=df_dmp_reporting_dates,
                                                               how='inner',
                                                               left_on=['Accession Number'],
                                                               right_on=['SURG_PATH_ACCESSION_NUM'])

    # TODO merge surgical report date headers

    return df_sample_rpt_merged

def get_associated_reports_as_table(df):
    if 'Associated Reports' in df.columns:
        # Get associated report numbers from these samples
        # Get associated report IDs that MAY have an impact sample

        # Remove brackets from associated reports
        df['Associated Reports'] = df['Associated Reports'].str.strip('[]')

        # Parse the report ids and rename the columns for the new dataframe
        assoc_rpt_id_df = df['Associated Reports'].str.split(', ', n=-1, expand=True)
        cols = assoc_rpt_id_df.columns
        cols_new = ['Report.' + str(x) for x in cols]
        assoc_rpt_id_df.columns = cols_new

        # Concatenate with the sample rpt id and sample id
        id_vars_old = ['PRPT_PATH_RPT_ID']
        id_vars = ['ORIGINAL_PATH_RPT_ID']
        col_dict = dict(zip(id_vars_old, id_vars))
        df_impact_id = df[id_vars_old]
        df_impact_id = df_impact_id.rename(columns=col_dict)
        assoc_rpt_id_df = pd.concat([df_impact_id, assoc_rpt_id_df], axis=1, sort=False)

        # Melt the associated report IDs into a single column
        assoc_rpt_ids_melt = pd.melt(frame=assoc_rpt_id_df, id_vars=id_vars,
                                     value_vars=cols_new, var_name='RPT_COUNT',
                                     value_name='ASSOCIATED_SAMPLE_REPORTS')
        assoc_rpt_ids_melt = assoc_rpt_ids_melt.replace({'ASSOCIATED_SAMPLE_REPORTS': {'': np.NaN, None: np.NaN}})
        # Drop the ones that are null, change the data type
        assoc_rpt_ids_melt = assoc_rpt_ids_melt[assoc_rpt_ids_melt['ASSOCIATED_SAMPLE_REPORTS'].notnull()]
        assoc_rpt_ids_melt['ASSOCIATED_SAMPLE_REPORTS'] = assoc_rpt_ids_melt['ASSOCIATED_SAMPLE_REPORTS'].astype(int)
        assoc_rpt_ids_melt = assoc_rpt_ids_melt.reset_index(drop=True)
        # Drop report count column
        assoc_rpt_ids_melt = assoc_rpt_ids_melt.drop('RPT_COUNT', axis=1)

    return assoc_rpt_ids_melt

def convert_associated_reports_to_accession(df_path, df_accession):
    # Convert associated reports column to accession numbers
    Associated_Accession_Reports = df_accession['ASSOCIATED_PATH_REPORT_ID'].apply(
        lambda x: [df_path.loc[df_path['PRPT_PATH_RPT_ID'] == v, 'ACCESSION_NUMBER'].iloc[0] for v in eval(x) if
                   df_path.loc[df_path['PRPT_PATH_RPT_ID'] == v, 'ACCESSION_NUMBER'].shape[0]])
    df_accession = df_accession.assign(Associated_Accession_Reports=Associated_Accession_Reports)

    return df_accession


## Function to compute indices of headers
def get_path_headers_main_indices(df, col_path_note, cols_index_list, headers_path_notes, path_header_ind_col_names):
    # This function will parse the primary sections within the surgical pathology reports
    # This primarily consists of
    # - Clinical diagnosis and history
    # - Specimens submitted
    # - Diagnosis
    # - Addendum
    # headers_path_notes: Headers/sections to build indices against.
    #   Add to this list to parse more sections. Will be a list of list
    cols_keep = cols_index_list + [col_path_note]
    df_path_indices = df[cols_keep].copy()

    col_names_index = ['IND_' + x for x in path_header_ind_col_names]

    # Find the clinical diagnosis and history section
    # Find the Specimens Submitted section
    # Find the DIAGONSIS section
    for i, header_name in enumerate(headers_path_notes):
        df = get_header_index(df=df, col_path_notes=col_path_note,
                                    header_name=header_name,
                                    col_name=col_names_index[i])

    # Create table of indices
    cols_index = list(df.columns[df.columns.str.contains('|'.join(col_names_index))])
    df_path_indices = pd.concat([df_path_indices, df[cols_index]], axis=1, sort=False)
    #
    # # Drop columns from surgical path frame
    # df = df.drop(columns=cols_index)

    # Get index of last character of path note
    note_length = df[col_path_note].apply(lambda x: len(x))
    df_path_indices = df_path_indices.assign(IND_END_NOTE=note_length)
    df_path_indices['IND_END_NOTE'] = df_path_indices['IND_END_NOTE'].fillna(-1).astype(int)

    # Drop notes columns
    df_path_indices = df_path_indices.drop(columns=[col_path_note])

    return df_path_indices

def get_header_index(df, col_path_notes, header_name, col_name):
    path_notes = df[col_path_notes]

    for i, key in enumerate(header_name):
        regex_rule_header3 = key + '[ ]*[\r\n]*'

        path_dx_sub_header_loc = path_notes.apply(lambda x: re.search(regex_rule_header3, str(x)))
        index_tuple = path_dx_sub_header_loc[path_dx_sub_header_loc.notnull()].apply(lambda x: x.span())

        # Find start and end points: _0, _1
        cols = [col_name + '_0', col_name + '_1']
        df_index_split = pd.DataFrame(index_tuple.loc[index_tuple.notnull()].tolist(),
                                      index=index_tuple.loc[index_tuple.notnull()].index,
                                      columns=cols)

        if i == 0:
            # kwargs = {col_name: index_tuple}
            # df = df.assign(**kwargs)
            df = pd.concat([df, df_index_split], axis=1)
        else:
            df[cols[0]] = df[cols[0]].fillna(df_index_split[cols[0]])
            df[cols[1]] = df[cols[1]].fillna(df_index_split[cols[1]])

    return df