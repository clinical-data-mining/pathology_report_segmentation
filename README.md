# pathology_report_segmentation

## Pathology Information
### Details
Running these scripts will provide the user with pathology data related to MSK-IMPACT samples. 

This includes:
- Molecular pathology report ID and date of report
- Source surgical pathology report ID(s) and date of report
- Date of procedure (DOP) of biopsy or resection
- Source of DOP data (Pathology report, surgery, interventional radiology (IR))

### Scripts to Run
####Segmentation 
*General purpose: segment the pathology reports into elements that can be further broken down by attributes.*

These modules are executed in `create_pathology_summary_table.py`
- **DarwinDiscoveryPathology**
    - Purpose: Cleans raw data queried from DDP and saves into a digestible format
    - Source file: darwin_pathology.py
    - Input: Darwin_Discovery_Pathology_Reports_20201111.tsv (Current)
    - Output: table_pathology_clean.csv
- **ParseSurgicalPathology**
    - Purpose: Segment the surgical pathology reports into its main sections
    - Source file: pathology_parse_surgical.py
    - Input: table_pathology_clean.csv
    - Output: table_pathology_surgical_notes_parsed.csv
- **ParseMolecularPathology**
    - Purpose: Segment the molecular pathology reports into its main sections
    - Source file: pathology_parse_molecular.py
    - Input: table_pathology_clean.csv
    - Output: table_pathology_molecular_notes_parsed.csv
- **PathologyParseSpecSubmitted**
    - Purpose: Segment the specimen submitted column into individually described parts
    - Source file: pathology_parse_specimen_submitted.py
    - Input: table_pathology_clean.csv
    - Output: table_pathology_col_spec_sub.csv
- **ParseSurgicalPathologySpecimens**
    - Purpose: Segment the pathology diagnosis sections into individual specimen parts, separating also the specimen submitted header and specimen note/synoptic report
    - Source file: pathology_parsing_surgical_specimens.py
    - Input: table_pathology_surgical_notes_parsed.csv
    - Output: table_pathology_surgical_samples_parsed_specimen.csv


#### Annotations
*General purpose: With the segmented data, extract specific attributes buried in free text. These attributes will be saved in a structured format.*

- **PathologyExtractDOP**
    - Purpose: Extraction of DOP of the specimen part from specimen submitted sections
    - Source file: pathology_extract_dop.py
    - Input: table_pathology_col_spec_sub.csv
    - Output: pathology_spec_part_dop.csv
- **PathologyExtractAccession**
    - Purpose: Extraction of source pathology report that is referenced in the molecular pathology report, or specimen submitted section
    - Source file: path_accessions.py
    - Input: table_pathology_col_spec_sub.csv
    - Output: path_accessions.csv
- **CombineAccessionDOPImpact**
    - Purpose: To generate a summary table of source accession and DOP for a given  pathology report.    
    - Source file: pathology_extract_dop_impact_wrapper.py
    - Input: 
        - fname_accession=path_accessions.csv
        - fname_dop=pathology_spec_part_dop.csv 
        - fname_path=table_pathology_clean.csv
    - Output: pathology_dop_impact_summary.csv
- **PathologyImpactDOPAnno**
    - Purpose: To fill in missing DOPs by comparing dates of source surgical pathology reports with surgical and IR dates and labelling positive comparisons as the DOP. 
    - Source file: pathology_impact_summary_dop_annotator
    - Input:  
        - fname_path_summary=pathology_dop_impact_summary.csv
        - fname_surgery=table_surgery.tsv
        - fname_ir=table_investigational_radiology.tsv
    - Output: table_pathology_impact_sample_summary_dop_anno.csv