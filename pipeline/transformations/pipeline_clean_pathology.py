from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.segmentation import InitCleanPathology

def main():

    obj_path = InitCleanPathology(
        fname_minio_env=c_dar.minio_env,
        fname=c_dar.fname_pathology,
        fname_save=c_dar.fname_path_clean
    )

    df = obj_path.return_df()
    tmp = 0

if __name__ == '__main__':
    main()