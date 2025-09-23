from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.segmentation import ParseSurgicalPathology

def main():

    obj_s = ParseSurgicalPathology(
        fname_minio_env=c_dar.minio_env,
        fname_path_clean=c_dar.fname_path_clean,
        fname_save=c_dar.fname_darwin_path_surgical
    )
    df = obj_s.return_df()

    tmp = 0

if __name__ == '__main__':
    main()