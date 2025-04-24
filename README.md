# Pathology Report Segmentation

## Overview

This repo provides a suite of tools for segmenting and extract clinical annotations from pathology data. 

## Installation
```
conda env create -f environment.yml
```


to create an environment called `radiology-report-segmentation` that you can activate via

```
conda activate radiology-report-segmentation
```


## Function Descriptions

### Segmentation Modules

| Module                                                                                         | Description                                        |
|------------------------------------------------------------------------------------------------|----------------------------------------------------|
| `segmentation.darwin_pathology.InitCleanPathology`                                           | Contains general pathology processing utilities.   |
| `segmentation.pathology_parsing_surgical_specimens.ParseSurgicalPathologySpecimens`          | Handles the parsing of surgical specimens.         |
| `segmentation.pathology_parse_specimen_submitted.PathologyParseSpecSubmitted`                | Deals with submitted specimen data.                |
| `segmentation.pathology_parse_surgical.ParseSurgicalPathology`                               | Processes surgical specimens.                      |
| `segmentation.pathology_parse_molecular.ParseMolecularPathology`                              | Parses molecular pathology data.                   |
| `segmentation.pathology_parse_surgical_dx.parse_path_dx_section`                              | Parses surgical diagnosis data.                    |

### Data Processing Modules

| Module                        | Description                                             |
|-------------------------------|---------------------------------------------------------|
| `data_processing.utils_pathology` | Provides utility functions for pathology data processing. |

### Annotations Modules

| Module                                                                                         | Description                                              |
|------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| `annotations.pathology_extract_dop.PathologyExtractDOP`                                      | Extracts date of procedure (DOP) of MSK-IMPACT specimens |
| `annotations.pathology_extract_pni.PerineuralInvasionAnnotation`                            | Extracts perineural invasion (PNI) data.                 |
| `annotations.pathology_synoptic_logistic_model.SynopticLogisticModel`                       | Contains a logistic model for synoptic pathology data.   |
| `annotations.pathology_extract_accession.PathologyExtractAccession`                         | Extracts pathology report accession numbers.             |
| `annotations.pathology_extract_mmr.extract_mmr`                                               | Extracts mismatch repair (MMR) data.                     |
| `annotations.pathology_extract_dop_impact_wrapper.CombineAccessionDOPImpact`                 | Wrapper for DOP impact extraction.                       |
| `annotations.pathology_impact_summary_dop_annotator.PathologyImpactDOPAnno`                 | Annotates DOP impact summaries.                          |
| `annotations.pathology_extract_gleason.extract_gleason_scores`                              | Extracts Gleason scores.                                 |
| `annotations.pathology_id_mapping.create_id_mapping_pathology`                              | Maps MRNs to MSK-IMPACT patient and sample IDs           |
| `annotations.pathology_extract_surgical_type.extract_surgical_type_data`                    | Extracts surgical type data.                             |
| `annotations.pathology_extract_pdl1.PathologyExtractPDL1`                                    | Extracts PD-L1 data.                                     |

## Example Usage
Here's an example of how to instantiate and use the package:

```python
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.segmentation import ParseSurgicalPathology

obj_s = ParseSurgicalPathology(
  fname_minio_env=c_dar.minio_env,
  fname_path_clean=c_dar.fname_path_clean,
  fname_save=c_dar.fname_darwin_path_surgical
)
df = obj_s.return_df()


```

- `fname_minio_env` is the file that contains credentials to access MinIO object storage to access relevant data files. 
- `fname_path_clean` is the path name of the "cleaned" pathology dataframe created from `InitCleanPathology`
- `fname_save` is the filename saved on MinIO
