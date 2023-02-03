import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api')))
import pandas as pd
from minio_api import MinioAPI
from utils import mrn_zero_pad


def create_id_mapping_pathology(fname_minio_env, fname_path, fname_out_mapping=None):
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
    obj = obj_minio.load_obj(fname_path)
    df_ = pd.read_csv(obj, sep='\t', low_memory=False)
    
    df_ = mrn_zero_pad(df=df_, col_mrn='MRN')
    
    df_path_mapping = df_[['MRN', 'DMP_ID', 'SAMPLE_ID']].dropna().drop_duplicates()
    
    if fname_out_mapping is not None:
        obj_minio.save_obj(df=df_path_mapping, path_object=fname_out_mapping, sep='\t')
    
    return df_path_mapping

def main():
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'cdm-utilities')))
    from data_classes_cdm import CDMProcessingVariables as c_dar

    # Extract DOP
    df_mapping = create_id_mapping_pathology(fname_minio_env=c_dar.minio_env,
                                             fname_path=c_dar.fname_path_clean,
                                             fname_out_mapping=c_dar.fname_pid)

if __name__ == '__main__':
    main()