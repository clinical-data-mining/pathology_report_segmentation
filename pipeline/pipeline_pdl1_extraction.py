from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.annotations import PathologyExtractPDL1


def main():
    ## Constants
    col_text = 'PATH_REPORT_NOTE'
    fname_save = c_dar.fname_path_pdl1
    fname_path = c_dar.fname_path_clean
    fname_minio_env = c_dar.minio_env

    # Extract PD-L1
    obj_p = PathologyExtractPDL1(
        minio_env=fname_minio_env,
        fname=fname_path,
        col_text=col_text,
        fname_save=fname_save
    )

    df = obj_p.return_extraction()
    print(df.sample())

    tmp = 0

if __name__ == '__main__':
    main()