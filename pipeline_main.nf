nextflow.enable.dsl=2

include {
    DEID_COLLECTION as DEID_RADIOLOGY;
    DEID_COLLECTION as DEID_INIT_CONSULTS;
    INIT_NLTK_CACHE } from './nextflow_modules/deid'

include { COPY_FILES_TO_MINIO } from './nextflow_modules/minio'

params.cache_dir = "${projectDir}/NLTK_CACHE"

params.radiology_reports_file = "radiology/radiology_report_segmentation/impact/ddp_radiology_reports.tsv"

params.init_consults_file = "clindoc/ddp_merged_clindoc_initial_and_fu_notes_genie_bpc.tsv"

params.n_philter_workers = 1000
params.radiology_reports_text_col = "RRPT_REPORT_TXT"
params.init_consults_text_col = "MERGED_CDO_SECTION_VALUE_TEXT"

params.local_deid_output_dir = "${projectDir}/DEID_DOCS"
params.minio_deid_output_dir = "experimental/deid_reports/"

workflow {
    cache_dir_ch = INIT_NLTK_CACHE(params.cache_dir)

    // Each DEID returns a dummy output that allows us to synchronize on termination:
    dummy_1 = DEID_RADIOLOGY(
        params.radiology_reports_file,
        params.radiology_reports_text_col,
        cache_dir_ch,
        params.n_philter_workers,
        params.local_deid_output_dir)
    dummy_2 = DEID_INIT_CONSULTS(
        params.init_consults_file,
        params.init_consults_text_col,
        cache_dir_ch,
        params.n_philter_workers,
        params.local_deid_output_dir)

    // Join on dummy variables to create barrier so this blocks on the DEID_*
    // subworkflows terminating.
    COPY_FILES_TO_MINIO(dummy_1.join(dummy_2),
                        params.local_deid_output_dir,
                        params.minio_deid_output_dir)
}