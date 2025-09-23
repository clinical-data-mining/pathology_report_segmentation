from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar
from pathology_report_segmentation.segmentation import ParseMolecularPathology

def main():

    obj_m = ParseMolecularPathology(
        fname_minio_env=c_dar.minio_env,
        fname_path_clean=c_dar.fname_path_clean,
        fname_save=c_dar.fname_darwin_path_molecular
    )

    df = obj_m.return_df()

if __name__ == '__main__':
    main()