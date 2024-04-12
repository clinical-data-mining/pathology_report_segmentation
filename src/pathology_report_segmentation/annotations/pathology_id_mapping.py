import pandas as pd
from msk_cdm.minio import MinioAPI
from msk_cdm.data_processing import mrn_zero_pad


def create_id_mapping_pathology(fname_minio_env, fname_path, fname_out_mapping=None):
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
    obj = obj_minio.load_obj(fname_path)
    df_ = pd.read_csv(obj, sep='\t', low_memory=False)
    
    df_ = mrn_zero_pad(df=df_, col_mrn='MRN')
    
    df_path_mapping = df_[['MRN', 'DMP_ID', 'SAMPLE_ID']].dropna().drop_duplicates()
    
    if fname_out_mapping is not None:
        obj_minio.save_obj(df=df_path_mapping, path_object=fname_out_mapping, sep='\t')
    
    return df_path_mapping

