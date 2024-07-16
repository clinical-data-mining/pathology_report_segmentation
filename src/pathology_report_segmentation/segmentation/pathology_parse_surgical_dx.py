import re

import pandas as pd

from msk_cdm.minio import MinioAPI


def parse_path_dx_section(df, col_name):
    # Add number of specimens by finding sample number in text with \n and :, then find largest number
    # Remove white space at ends of path dx note

    df[col_name] = df.loc[df[col_name].notnull(), col_name].apply(lambda x: x.strip())

    # Clean path DX section
    # Take bracketed segments and remove new line carriage return
    regex_brackets = '\([^)]*\)'
    regex = re.compile(regex_brackets)
    fn_rmv_cr = lambda x: regex.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
    df[col_name] = df.loc[df[col_name].notnull(), col_name].apply(fn_rmv_cr)

    # regex_new line removal - TODO: Redo this just for the title
    regex_nl = '([a-z](\\r\\n){1}[a-z])'
    regex_2 = re.compile(regex_nl)
    fn_rmv_ln = lambda x: regex_2.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
    df[col_name] = df.loc[df[col_name].notnull(), col_name].apply(fn_rmv_ln)

    # regex_new line removal around measurements
    regex_nl = '((\\r\\n){1}[\d]{1}.[\d]{1})'
    regex_2 = re.compile(regex_nl)
    fn_rmv_ln = lambda x: regex_2.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
    df[col_name] = df.loc[df[col_name].notnull(), col_name].apply(fn_rmv_ln)

    # regex_new line removal that are around dates
    regex_nl2 = '([:]{1}[ ]*[\\r\\n]+[ ]*[\d]{1,2}[/]{1}[\d]{1,2}[/]{1}[\d]{1,4}[ ]*[:]{1})'
    regex_3 = re.compile(regex_nl2)
    fn_rmv_ln2 = lambda x: regex_3.sub(lambda m: m.group().replace('\r\n', " ", 1), x)
    df[col_name] = df.loc[df[col_name].notnull(), col_name].apply(fn_rmv_ln2)


    # Add function to remove new line that are within brackets
    index_condition = df[col_name].notnull()

    # Find segments of specimens and their descriptions, typically like 1. (Description) : \r\n2. (etc)
    regex_rule_spec_num = '(^[\d]{1}[.)]{1}[ A-Za-z]+[\da-zA-Z.,\'";:#/() \-\&]+[:.]{0,1}[ ]*[\\r\\n]+|[\\r\\n]+[ ]*[\d]{1,2}[.)]{1}[ A-Za-z]+[\da-zA-Z.,\'";:#/() \-\&]+[:.]{0,1}[ ]*[\\r\\n]+)'
    regex_spec_titles = re.compile(regex_rule_spec_num)

    df_path_dx_list = df.loc[index_condition, col_name]

    # Find specimen description in list
    list_spec_header_list = df_path_dx_list.apply(lambda str: regex_spec_titles.findall(str))
    # Clean list_spec_header_list by removing white space and new lines
    regex_1 = '^[\d]{1,2}[.)]{1}'
    regex_num_header = re.compile(regex_1)
    list_spec_header_list_clean1 = list_spec_header_list.apply(lambda x: [regex_num_header.sub('', y.strip()).strip() for y in x])
    # Remove the colon at the end of the headers
    regex_2 = ':$'
    regex_end_colon = re.compile(regex_2)
    list_spec_header_list_clean = list_spec_header_list_clean1.apply(lambda x: [regex_end_colon.sub('', y) for y in x])

    # Compute number of samples according to parsed samples in path dx
    list_spec_length = list_spec_header_list_clean.apply(lambda x: len(x))

    # Get specimen number used and make into a list
    regex_rule_spec_num2 = '^[\d]{1,2}'
    r = re.compile(regex_rule_spec_num2)
    list_spec_number_listing_str = list_spec_header_list.apply(lambda y: list(map(lambda x: r.findall(x.strip())[0], y)))
    list_spec_number_listing = list_spec_number_listing_str.apply(lambda y: list(map(lambda x: int(x), y)))
    # Compute the MAX sample number in the list -- this will help determine if there is header out of format
    list_spec_number_max = list_spec_number_listing[list_spec_length != 0].apply(lambda x: max(x) if len(x) > 0 else 0)

    # Add the spec count attributes to df
    df = df.assign(PATH_DX_SPEC_LENGTH=list_spec_length)
    df = df.assign(PATH_DX_SPEC_MAX=list_spec_number_max)

    # Create dictionaries for specimen label number and descriptions
    s = pd.Series(index=list_spec_number_listing.index, dtype=object)
    for ind in list_spec_number_listing.index:
        if len(list_spec_number_listing.loc[ind]) > 0:
            d = dict(zip(list_spec_number_listing_str.loc[ind], list_spec_header_list_clean.loc[ind]))
            s[ind] = d
    df = df.assign(PATH_DX_SPEC_TITLES=s)

    # Extract the specimen notes from the individual specimens by
    # Splitting the path dx text
    # Locate the titles of the specimens in the split
    # Use list comprehension to extract the note
    # Create a dictionary of the header to the text below it

    # Split path dx note by title
    split_list_path_dx = df_path_dx_list.apply(lambda str: regex_spec_titles.split(str))
    # Remove first item in list of the split since it's always blank
    split_list_path_dx = split_list_path_dx.apply(lambda x: x[1:])
    # Use list comprehension to extract only the "even" (index= 1,3,5...) items in list, which is the specimen notes
    path_dx_notes = split_list_path_dx.apply(lambda y: [y[x].strip() for x in range(1, len(y)) if x % 2 == 1])

    # For notes that could not be parsed due to no specimen number label, take entire text
    index_1_spec_note = df.loc[(df['PATH_DX_SPEC_LENGTH'] == 0) & (df['SPECIMEN_COUNT_SURG_RPT'] == 1) & (df['PATH_SPEC_SUBMITTED_LENGTH'] == 1)].index
    list_spec_number_listing.loc[index_1_spec_note] = list_spec_number_listing.loc[index_1_spec_note].apply(lambda x: [1])
    path_dx_notes.loc[index_1_spec_note] = df_path_dx_list.loc[index_1_spec_note]

    # Create dictionaries for specimen label number and descriptions
    s = pd.Series(index=list_spec_number_listing.index, dtype=object)
    for ind in list_spec_number_listing.index:
        if len(list_spec_number_listing.loc[ind]) > 0:
            d = dict(zip(list_spec_number_listing_str.loc[ind], path_dx_notes.loc[ind]))
            s[ind] = d
    df = df.assign(PATH_DX_SPEC_NOTES=s)

    return df