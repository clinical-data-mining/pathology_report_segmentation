# Pathology Report Segmentation & Annotation Pipeline

## Project Overview

This pipeline provides automated clinical annotation extraction from pathology reports using natural language processing techniques. It supports processing data from Epic EHR systems and institutional databases to generate biomarker annotations, patient timelines, and cBioPortal-compatible datasets.

**Key capabilities:**
- Biomarker detection and extraction from pathology reports
- cBioPortal timeline generation for clinical events
- Dual-source processing (Epic EHR + legacy institutional databases)

## Features

### Clinical Annotations
- **Gleason score extraction** - Automated detection of prostate cancer grading
- **MMR (Mismatch Repair) status detection** - Identification of DNA repair deficiency markers
- **PD-L1 expression analysis** - Immunotherapy biomarker extraction
- **Date of procedure (DOP) extraction** - Surgical and biopsy date identification
- **Specimen accession number parsing** - Sample tracking and identification

### Data Processing
- **Institutional data integration** - Support for Epic electronic health records and Darwin/IDB databases
- **Automated cleaning and normalization** - Standardized data preprocessing pipelines

### Outputs
- **cBioPortal timeline files** - Compatible formats for cancer genomics visualization
- **Patient/sample-level summaries** - Aggregated clinical annotations by patient or specimen

## Installation

### Prerequisites
- Python 3.8+
- Conda package manager
- Access to MinIO object storage
- Databricks environment

### Setup
```bash
# Clone the repository
git clone https://github.com/clinical-data-mining/pathology_report_segmentation.git
cd pathology_report_segmentation

# Create and activate conda environment
conda env create -f environment.yml
conda activate pathology-report-segmentation

# Install the package
pip install -e .
```

## Quick Start (Examples)

### Standard Pipeline (Institutional Data)
```bash
# Extract Gleason scores
python pipeline/transformations/pipeline_gleason_extraction.py \
  --minio_env /path/to/minio_env.txt

# Extract MMR status
python pipeline/transformations/pipeline_mmr_extraction.py \
  --minio_env /path/to/minio_env.txt

# Generate cBioPortal timeline
python pipeline/cbioportal/cbio_timeline_gleason.py \
  --minio_env /path/to/minio_env.txt
```

### Epic EHR Pipeline
```bash
# Extract from Epic pathology reports
python pipeline/transformations_epic/pipeline_gleason_extraction_epic.py \
  --minio_env /path/to/minio_env.txt \
  --databricks_env /path/to/databricks_env.txt

# Combine Epic + institutional data
python pipeline/transformations_epic/pipeline_gleason_extraction_idb_epic_combined.py \
  --fname_minio /path/to/minio_env.txt
```

## Pipeline Structure

The pipeline is organized into three main categories:

### Core Scripts (`pipeline/transformations/`)
Standard annotation extraction for institutional databases:
- `pipeline_clean_pathology.py` - Data preprocessing and cleaning
- `pipeline_gleason_extraction.py` - Gleason score extraction
- `pipeline_mmr_extraction.py` - MMR status detection
- `pipeline_pdl1_extraction.py` - PD-L1 expression analysis
- `pipeline_dop_extraction.py` - Date of procedure extraction
- `pipeline_*_wrapper.py` - Data combination and mapping scripts

### Epic Scripts (`pipeline/transformations_epic/`)
Epic EHR-specific processing with Databricks integration:
- `pipeline_*_extraction_epic.py` - Direct Epic data extraction
- `pipeline_*_idb_epic_combined.py` - Epic + institutional data combination
- Enhanced argument parsing with `--databricks_env` support

### cBioPortal Integration (`pipeline/cbioportal/`)
Timeline and summary generation:
- `cbio_timeline_*.py` - Generate timeline files for clinical events
- `cbio_*_summary.py` - Create patient/sample-level summaries
- `cbio_timeline_specimen.py` - Surgery and specimen collection timelines

## Configuration

### Required Environment Files

**MinIO Credentials** (`minio_env.txt`):
```
MINIO_URL=https://your-minio-server.com
MINIO_ACCESS_KEY=your-access-key
MINIO_SECRET_KEY=your-secret-key
MINIO_BUCKET=your-bucket-name
```

**Databricks Environment** (`databricks_env.txt`, for Epic processing):
```
DATABRICKS_SERVER_HOSTNAME=your-databricks-host.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=your-access-token
```

### File Path Conventions

- **Epic data**: Uses `epic_ddp_concat/pathology/...` path structure
- **Institutional data**: Uses `pathology/...` path structure
- **Combined outputs**: Epic datasets include `_epic_idb_combined` suffix

### Command-Line Arguments

All scripts now support standardized argument parsing:
- `--minio_env`: Required for all scripts, path to MinIO credentials
- `--databricks_env`: Required for Epic scripts, path to Databricks credentials


## Data Flow

```
Raw Pathology Reports (Epic EHR / Institutional DB)
                    ↓
            Data Cleaning & Preprocessing
                    ↓
        Clinical Annotation Extraction
           (Gleason, MMR, PD-L1, DOP)
                    ↓
         Timeline Generation & Summarization
                    ↓
    cBioPortal Files + Patient/Sample Summaries
```

### Processing Stages:
1. **Clean**: Remove artifacts, standardize formatting
2. **Extract**: Apply NLP models to identify clinical annotations
3. **Annotate**: Link annotations to patient/sample identifiers
4. **Combine**: Merge Epic and institutional data sources
5. **Export**: Generate cBioPortal timelines and summary tables

## Output Files

### Annotation Extracts
- `pathology_gleason_calls*.tsv` - Gleason score annotations
- `pathology_mmr_calls*.tsv` - MMR status calls
- `pathology_pdl1_calls*.tsv` - PD-L1 expression data

### Timeline Files
- `table_timeline_gleason_scores.tsv` - Gleason score timeline
- `table_timeline_mmr_calls.tsv` - MMR testing timeline
- `table_timeline_specimen_surgery.tsv` - Surgery dates
- `table_timeline_sequencing.tsv` - Sequencing dates

### Summary Tables
- `table_summary_*_patient.tsv` - Patient-level aggregations
- `table_summary_*_sample.tsv` - Sample-level annotations

### File Naming Conventions
- **Epic datasets**: Include `_epic` or `_epic_idb_combined` suffixes
- **Standard datasets**: No special suffix
- **Combined outputs**: `_idb_epic_combined` for unified datasets
