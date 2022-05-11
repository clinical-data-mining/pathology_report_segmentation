from key_value_pair_extraction import PathologyKeyValuePairExtraction

# Pathnames
fname_path_dx_synoptic = '/mind_data/minio_data/cdm-data/pathology/table_pathology_surgical_samples_parsed_specimen_synoptic.csv'
fname_path_dx_key_value = '/mind_data/minio_data/cdm-data/pathology/path_synoptic_dict_all.csv'
fname_path_dx_key_value_stage = '/mind_data/minio_data/cdm-data/pathology/path_dx_stage_key_values.csv'

# Variables
list_col_index = ['ACCESSION_NUMBER', 'PATH_DX_SPEC_NUM']
list_key_terms = ['STAG', 'AJCC']
col_dx_spec_desc = 'PATH_DX_SPEC_DESC'
col_out_dict = 'PATH_DX_SPEC_DESC_DICT'
key_label = 'STAGE'

obj_key_value = PathologyKeyValuePairExtraction(fname=fname_path_dx_synoptic, 
                                                col_dx_spec_desc=col_dx_spec_desc, 
                                                col_out_dict=col_out_dict, 
                                                list_col_index=list_col_index, 
                                                fname_out=fname_path_dx_key_value)
df_kv = obj_key_value.return_df()

df_stage = obj_key_value.extract_term(list_key_terms=list_key_terms, 
                                      key_label=key_label, 
                                      fname_save=fname_path_dx_key_value_stage)
