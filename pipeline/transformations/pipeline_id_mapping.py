from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations import create_id_mapping_pathology
def main():

    # Extract DOP
    df_mapping = create_id_mapping_pathology(
        fname_minio_env=c_dar.minio_env,
        fname_path=c_dar.fname_path_clean,
        fname_out_mapping=c_dar.fname_pid
    )

if __name__ == '__main__':
    main()