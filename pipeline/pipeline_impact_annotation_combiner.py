from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations import CombineAccessionDOPImpact


def main():
    # Extract source accession number
    obj_p = CombineAccessionDOPImpact(
        fname_minio_env=c_dar.minio_env,
        fname_accession=c_dar.fname_path_accessions,
        fname_dop=c_dar.fname_spec_part_dop,
        fname_path=c_dar.fname_path_clean,
        fname_save=c_dar.fname_combine_dop_accession
    )

    df = obj_p.return_df()

    tmp = 0

if __name__ == '__main__':
    main()