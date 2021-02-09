"""
constants_darwin_pathology.py

Defined darwin data filename constants to use across the project
"""
# PATH LOCATION OF DATA FILES
pathname = '/mind_data/cdm_data/pathology'

# Pathology Segmentation tasks
## Raw files
### Pathology data from DDP
fname_path_ddp = 'Darwin_Discovery_Pathology_Reports_20201207_f.tsv'

### Pathology table from copath
fname_path_synoptic = 'synoptic_data_impact_patients.csv'

### Clean path table
fname_darwin_path_clean = 'table_pathology_clean.csv'          # Table of impact sample processed pathology

## Parsed Pathology data
### Main Pathology sections
fname_darwin_path_molecular = 'table_pathology_molecular_notes_parsed.csv'
fname_darwin_path_surgical = 'table_pathology_surgical_notes_parsed.csv'

### Specimen submitted
fname_darwin_path_col_spec_sub = 'table_pathology_col_spec_sub.csv'
fname_darwin_path_molecular_note_spec_sub = 'table_pathology_molecular_note_spec_sub.csv'
fname_darwin_path_surgical_note_spec_sub = 'table_pathology_surgical_note_spec_sub.csv'

fname_darwin_path_dmp = 'table_pathology_impact_dmp_extraction.csv'
fname_darwin_path_clean_parsed = 'table_pathology_surgical_samples_parsed.csv'            # Filename of processed pathology table (variable: fname_darwin_path)
fname_darwin_path_clean_parsed_specimen = 'table_pathology_surgical_samples_parsed_specimen.csv'            # Filename of parsed specimen pathology
fname_darwin_path_clean_parsed_specimen_impact_only = 'table_pathology_impact_only_parsed_specimen.csv'


## Annotations
### Table of MSK-IMPACT to source (Surgical/Cytology) accessions
fname_accessions = 'path_accessions.csv'
### Table of MSK-IMPACT to date of procedure
fname_spec_part_dop = 'pathology_spec_part_dop.csv'
### Table combining MSK-IMPACT report accession, source accession, date of procedure
fname_combine_dop_accession = 'pathology_dop_impact_summary.csv'
### File "fname_combine_dop_accession" with annotation for source of extraction (IMPACT report, surgical, IR)
fname_dop_anno = 'table_pathology_impact_sample_summary_dop_anno.csv'