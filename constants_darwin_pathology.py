"""
constants_darwin_pathology.py

Defined darwin data filename constants to use across the project
"""
# PATH LOCATION OF DATA FILES
minio_env = '/mind_data/fongc2/cdm-utilities/minio_env.txt'

# Pathology Segmentation tasks
## Raw files
### Pathology data from DDP
fname_path_ddp = 'pathology/ddp_pathology_reports.tsv'
### Surgery data
fname_darwin_surgery = 'surgery/ddp_surgery.tsv'
### IR data
fname_darwin_ir = 'interventional-radiology/ddp_ir.tsv'

### Pathology table from copath
fname_path_synoptic_labels = 'pathology/table_pathology_surgical_synoptic_labels.csv'
fname_path_synoptic = 'pathology/path_synoptic_predictions.tsv'

### Clean path table
fname_darwin_path_clean = 'pathology/table_pathology_clean.tsv'          # Table of impact sample processed pathology

## Parsed Pathology data
### Main Pathology sections
fname_darwin_path_molecular = 'pathology/table_pathology_molecular_notes_parsed.tsv'
fname_darwin_path_surgical = 'pathology/table_pathology_surgical_notes_parsed.tsv'
fname_darwin_path_cyto = 'pathology/table_pathology_cyto_notes_parsed.tsv'
fname_darwin_path_heme = 'pathology/table_pathology_heme_notes_parsed.tsv'

### Specimen submitted
fname_darwin_path_col_spec_sub = 'pathology/table_pathology_col_spec_sub.tsv'
fname_darwin_path_molecular_note_spec_sub = 'pathology/table_pathology_molecular_note_spec_sub.tsv'
fname_darwin_path_surgical_note_spec_sub = 'pathology/table_pathology_surgical_note_spec_sub.tsv'

fname_darwin_path_dmp = 'pathology/table_pathology_impact_dmp_extraction.tsv'
fname_darwin_path_clean_parsed = 'pathology/table_pathology_surgical_samples_parsed.tsv'            # Filename of processed pathology table (variable: fname_darwin_path)
fname_darwin_path_clean_parsed_specimen = 'pathology/table_pathology_surgical_samples_parsed_specimen.tsv'            # Filename of parsed specimen pathology
fname_darwin_path_clean_parsed_specimen_impact_only = 'pathology/table_pathology_impact_only_parsed_specimen.tsv'


## Annotations
### Table of MSK-IMPACT to source (Surgical/Cytology) accessions
fname_accessions = 'pathology/path_accessions.tsv'
### Table of MSK-IMPACT to date of procedure
fname_spec_part_dop = 'pathology/pathology_spec_part_dop.tsv'
### Table combining MSK-IMPACT report accession, source accession, date of procedure
fname_combine_dop_accession = 'pathology/pathology_dop_impact_summary.tsv'
### File "fname_combine_dop_accession" with annotation for source of extraction (IMPACT report, surgical, IR)
fname_dop_anno = 'pathology/table_pathology_impact_sample_summary_dop_anno.tsv'
fname_darwin_path_heme_parse_bm_biopsy = 'pathology/pathology_heme_bm_biopsy.tsv'
fname_darwin_path_heme_parse_periph_blood = 'pathology/pathology_heme_periph_blood.tsv'
