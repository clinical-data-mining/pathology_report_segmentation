""""
create_darwin_impact_table.py

By Chris Fong - MSKCC 2018

This script will create the darwin diagnosis and treatment tables, merged with impact sample data
"""
import os
import pandas as pd
# from pathology_extraction_method import PathologyImpactSpecimenExtraction
from darwin_pathology import DarwinDiscoveryPathology
from pathology_parse_surgical import ParseSurgicalPathology
from pathology_parse_molecular import ParseMolecularPathology
from pathology_parse_specimen_submitted import PathologyParseSpecSubmitted
import constants_darwin as c_dar
from utils_darwin_etl import set_debug_console

# Console settings
set_debug_console()



# TODO: put all pathology parsing into a separate wrapper, since this is too long
fname_samples = '/Users/fongc2/Documents/github/MSK/DARWIN_ETL/data/mskimpact_clinical_data.tsv'
# # Recreate cleaned pathology data
# print('Running DarwinDiscoveryPathology...')
# obj_path = DarwinDiscoveryPathology(pathname=c_dar.pathname,
#                                     fname='table_pathology.tsv',
#                                     fname_out=c_dar.fname_darwin_path_clean)

# # Using the cleaned pathology table, parse the pathology note
# # surgical_pathology_parsing.py
# print('Running ParseSurgicalPathology...')
# obj_path_parse = ParseSurgicalPathology(pathname=c_dar.pathname,
#                                         fname_path_clean=c_dar.fname_darwin_path_clean,
#                                         fname_save=c_dar.fname_darwin_path_surgical)
#
# print('Running ParseMolecularPathology...')
# obj_dmp = ParseMolecularPathology(pathname=c_dar.pathname,
#                                    fname_path_clean=c_dar.fname_darwin_path_clean,
#                                    fname_save=c_dar.fname_darwin_path_molecular)

print('Running PathologyParseSpecSubmitted...')
obj_mol = PathologyParseSpecSubmitted(pathname=c_dar.pathname,
                                      fname_path_parsed=c_dar.fname_darwin_path_molecular,
                                      col_spec_sub='DMP_NOTE_SPEC_SUB',
                                      list_cols_id=['DARWIN_PATIENT_ID', 'ACCESSION_NUMBER'],
                                      fname_save=c_dar.fname_darwin_path_molecular_note_spec_sub)

print('Running PathologyParseSpecSubmitted...')
obj_surg = PathologyParseSpecSubmitted(pathname=c_dar.pathname,
                                       fname_path_parsed=c_dar.fname_darwin_path_surgical,
                                       col_spec_sub='PATH_NOTE_SPEC_SUB',
                                       list_cols_id=['DARWIN_PATIENT_ID', 'ACCESSION_NUMBER'],
                                       fname_save=c_dar.fname_darwin_path_surgical_note_spec_sub)

print('Merging...')
df_m = obj_mol.return_df()
df_surg = obj_surg.return_df()
df = pd.concat([df_surg, df_m], axis=0, sort=False)

pathfilename = os.path.join(c_dar.pathname, c_dar.fname_darwin_path_col_spec_sub)
df.to_csv(pathfilename, index=False)

# pathname = c_dar.pathname
# fname_out_pathology_specimens_parsed = c_dar.fname_darwin_path_clean_parsed_specimen_long
# pathfilename_path = os.path.join(pathname, fname_out_pathology_specimens_parsed)
# df_pathology = pd.read_csv(pathfilename_path, header=0, low_memory=False)

tmp = 0
# # Annotate pathology impact summary table with DOP
# TODO: Place molecular_pathology_parsing.py here and perform DOP anno after
# obj_path_anno = PathologyImpactDOPAnno(pathname=c_dar.pathname,
#                                        fname_path_summary=c_dar.fname_darwin_path_impact_summary,
#                                        fname_surgery=c_dar.fname_surgery_discovery,
#                                        fname_ir=c_dar.fname_investigational_radiology_discovery)


#
# # From the main header parsed pathology reports, parse pathology diagnosis section further by specimen
# # From pathology_parsing_surgical_specimens.py
# obj_path_parse_spec = ParseSurgicalPathologySpecimens(pathname=c_dar.pathname,
#                                                       fname_darwin_pathology_parsed=c_dar.fname_darwin_path_clean_parsed,
#                                                       fname_out_pathology_specimens_parsed=c_dar.fname_darwin_path_clean_parsed_specimen)

# # Create darwin only pathology frame --------
# # TODO: (DELETE) refactor this to obtain a dataframe only containing impact samples
# obj_impact_path = CreateIMPACTPathologyFrame(pathname=c_dar.pathname,
#                                              fname_path_parsed=c_dar.fname_darwin_path_clean_parsed_specimen,
#                                              fname_save=c_dar.fname_darwin_path_clean_parsed_specimen_impact_only)

# Extract data from pathology report ---------
# File from # From pathology_extraction_method.py
# f = c_dar.fname_darwin_path_clean_parsed_specimen_impact_only
# f2 = c_dar.fname_darwin_path_impact_summary_annotated2
# f3a = 'master_path_has_met.csv'
# f3 = 'search_chosen_concepts_for_metastasis_from_path_reports.csv'
# obj_path_spec_annotations = PathologyImpactSpecimenExtraction(pathname=c_dar.pathname,
#                                                               fname_path_summary=f2,
#                                                               fname_path_parsed=f)

# # Extract metastatic info from pathology reports ---------
# # TODO Fix this script to compute regional mets within pathology reports
# obj_path_met_anno = PathologyHasMetAnno(pathname=c_dar.pathname,
#                                         fname_path_summary=f2,
#                                         fname_path_has_met=f3,
#                                         fname_path_has_met2=f3a)