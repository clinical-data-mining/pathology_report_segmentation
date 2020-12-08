""""
create_pathology_summary_table.py

By Chris Fong - MSKCC 2020

This script will create the darwin diagnosis and treatment tables, merged with impact sample data
# TODO: Transform this into a Jupyter notebook
"""
from darwin_pathology import DarwinDiscoveryPathology
from pathology_parse_surgical import ParseSurgicalPathology
from pathology_parse_molecular import ParseMolecularPathology
from pathology_parse_specimen_submitted import PathologyParseSpecSubmitted
from pathology_parsing_surgical_specimens import ParseSurgicalPathologySpecimens
from pathology_extract_accession import PathologyExtractAccession
from pathology_extract_dop import PathologyExtractDOP
from pathology_extract_dop_impact_wrapper import CombineAccessionDOPImpact
from pathology_impact_summary_dop_annotator import PathologyImpactDOPAnno
import constants_darwin_pathology as c_dar
from utils_darwin_etl import set_debug_console

# Console settings
set_debug_console()

run_path_clean = False
run_parse_surg = False
run_parse_dmp = False
run_parse_spec_sub = False
run_parse_path_dx = False
run_spec_sub_path_dx_combiner = False

# TODO: Move the rest to
run_parse_accession = False
run_parse_dop = False
annotation_steps = True

# Recreate cleaned pathology data
if run_path_clean:
    print('Running DarwinDiscoveryPathology...')
    obj_path = DarwinDiscoveryPathology(pathname=c_dar.pathname,
                                         fname='Darwin_Discovery_Pathology_Reports_20201207_f.tsv',
                                         fname_out=c_dar.fname_darwin_path_clean)

# Using the cleaned pathology table, parse the main sections of the surgical pathology note
# surgical_pathology_parsing.py
if run_parse_surg:
    print('Running ParseSurgicalPathology...')
    obj_path_parse = ParseSurgicalPathology(pathname=c_dar.pathname,
                                            fname_path_clean=c_dar.fname_darwin_path_clean,
                                            fname_save=c_dar.fname_darwin_path_surgical)

# Using the cleaned pathology table, parse the main sections of the molecular pathology note
if run_parse_dmp:
    print('Running ParseMolecularPathology...')
    obj_dmp = ParseMolecularPathology(pathname=c_dar.pathname,
                                       fname_path_clean=c_dar.fname_darwin_path_clean,
                                       fname_save=c_dar.fname_darwin_path_molecular)

if run_parse_spec_sub:
    # Parse the individual part descriptions from the specimen submitted column of parsed reports
    # Segment specimen submissions for molecular path
    print('Running PathologyParseSpecSubmitted...')
    obj_mol = PathologyParseSpecSubmitted(pathname=c_dar.pathname,
                                          fname_path_parsed=c_dar.fname_darwin_path_clean,
                                          col_spec_sub='SPECIMEN_SUBMISSION_LIST',
                                          list_cols_id=['DMP_ID', 'ACCESSION_NUMBER'],
                                          fname_save=c_dar.fname_darwin_path_col_spec_sub)

    df_m = obj_mol.return_df()

if run_parse_path_dx:
    print('Running ParseSurgicalPathologySpecimens...')
    pathname = c_dar.pathname
    fname_out_pathology_specimens_parsed = c_dar.fname_darwin_path_clean_parsed_specimen
    fname_darwin_pathology_parsed = c_dar.fname_darwin_path_surgical
    obj_parse = ParseSurgicalPathologySpecimens(pathname=pathname,
                                                fname_darwin_pathology_parsed=fname_darwin_pathology_parsed,
                                                fname_out_pathology_specimens_parsed=fname_out_pathology_specimens_parsed)

    df_surg_path_parsed_spec = obj_parse.return_df_parsed_spec()

if run_spec_sub_path_dx_combiner:
    print('Running Combiner...')
    run = None

print('Complete!')



if annotation_steps:
    # TODO Segment pathologic diagnosis section of surgical pathology reports at the part level
    # Call pathology_parsing_surgical_specimens.py

    # Create annotation of source accession number for all pathology reports/specimen part, if indicated
    ## Constants
    col_label_access_num = 'ACCESSION_NUMBER'
    col_label_spec_num = 'SPECIMEN_NUMBER'
    col_spec_sub = 'SPECIMEN_SUBMITTED'

    fname_accessions = 'path_accessions.csv'
    fname_spec_part_dop = 'pathology_spec_part_dop.csv'
    fname_combine_dop_accession = 'pathology_dop_impact_summary.csv'
    fname_dop_anno = 'table_pathology_impact_sample_summary_dop_anno.csv'

    print('Running PathologyExtractAccession...')
    obj_p = PathologyExtractAccession(pathname=c_dar.pathname,
                                fname=c_dar.fname_darwin_path_col_spec_sub,
                                col_label_access_num=col_label_access_num,
                                col_label_spec_num=col_label_spec_num,
                                col_spec_sub=col_spec_sub,
                                fname_out=fname_accessions)

    # Create annotation for date of procedure (DOP) for all pathology reports/specimen part, if indicated
    print('Running PathologyExtractDOP...')
    obj_p = PathologyExtractDOP(pathname=c_dar.pathname,
                                fname=c_dar.fname_darwin_path_col_spec_sub,
                                col_label_access_num=col_label_access_num,
                                col_label_spec_num=col_label_spec_num,
                                col_spec_sub=col_spec_sub,
                                list_accession=None,
                                fname_out=fname_spec_part_dop)

    # TODO Create table of M accessions of IMPACT samples, source accession number, dates of reports and procedures
    #Call pathology_extract_dop_impact_wrapper.py
    obj_p = CombineAccessionDOPImpact(pathname=c_dar.pathname,
                                      fname_accession=fname_accessions,
                                      fname_dop=fname_spec_part_dop,
                                      fname_path=c_dar.fname_darwin_path_clean,
                                      fname_out=fname_combine_dop_accession)

    # TODO Add annoations for surgical reports that on the same day as the surgery/IR
    # Call pathology_impact_summary_dop_annotator.py
    objd = PathologyImpactDOPAnno(pathname=c_dar.pathname,
                                  fname_path_summary=fname_combine_dop_accession,
                                  fname_surgery='table_surgery.tsv',
                                  fname_ir='table_investigational_radiology.tsv',
                                  fname_save=fname_dop_anno)

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