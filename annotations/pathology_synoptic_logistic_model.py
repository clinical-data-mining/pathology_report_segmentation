""""
 pathology_synoptic_logistic_model.py

 By Chris Fong - MSKCC 2019, updated 2021


"""
import os
import sys 
sys.path.insert(0, '/mind_data/fongc2/cdm-utilities/')
sys.path.insert(0, '/mind_data/fongc2/cdm-utilities/minio_api')
import pandas as pd
import numpy as np
from minio_api import MinioAPI
from utils import read_minio_api_config
from sklearn.linear_model import LogisticRegression


class SynopticReportClassifier(object):
    def __init__(self, fname_minio_env, fname_parsed_spec, fname_synoptic_labels, fname_save):
        self._fname_minio_env = fname_minio_env
        self._fname_parsed_spec = fname_parsed_spec
        self._fname_synoptic_labels = fname_synoptic_labels
        self._fname_save = fname_save
        self._model = None
        self._df_label_synoptic = None
        self._obj_minio = None
        self._bucket = None
        
        self._col_syn = 'IS_SYNOPTIC'
        self._col_spec_desc = 'PATH_DX_SPEC_DESC'
        self._col_id = 'ACCESSION_NUMBER'
        self._col_id_num = 'PATH_DX_SPEC_NUM'
        self._col_label_prediction = 'IS_PREDICTION'
        self._cols_feat = ['FEATURE1', 'FEATURE2', 'FEATURE3']
        self._col_keep_output = [self._col_id, self._col_id_num, self._col_syn, self._col_label_prediction]
        
        self._process_data()
        
    def _save_results(self, fname_save):
        if self._df_label_synoptic is not None:
            print('Saving %s' % fname_save)
            self._obj_minio.save_obj(df=self._df_label_synoptic, bucket_name=self._bucket, path_object=fname_save, sep='\t')
        else:
            print('No data to save')
        
    def return_synoptic(self):
        return self._df_label_synoptic
    
    def _init_minio(self):
        # Setup Minio configuration
        minio_config = read_minio_api_config(fname_env=self._fname_minio_env)
        ACCESS_KEY = minio_config['ACCESS_KEY']
        SECRET_KEY = minio_config['SECRET_KEY']
        CA_CERTS = minio_config['CA_CERTS']
        URL_PORT = minio_config['URL_PORT']
        BUCKET = minio_config['BUCKET']
        self._bucket = BUCKET

        self._obj_minio = MinioAPI(ACCESS_KEY=ACCESS_KEY, 
                                     SECRET_KEY=SECRET_KEY, 
                                     ca_certs=CA_CERTS, 
                                     url_port=URL_PORT)
        return None
        
    def _load_text(self):
        print('Loading %s' % self._fname_parsed_spec)
        obj = self._obj_minio.load_obj(bucket_name=self._bucket, path_object=self._fname_parsed_spec)
        df = pd.read_csv(obj, header=0, low_memory=False, sep='\t')

        return df
    
    def _load_labels(self):
        print('Loading %s' % self._fname_synoptic_labels)
        obj = self._obj_minio.load_obj(bucket_name=self._bucket, path_object=self._fname_synoptic_labels)
        df_path_labels = pd.read_csv(obj, header=0, low_memory=False, sep=',')
        df_path_labels = df_path_labels[df_path_labels[self._col_syn].notnull()]
        df_path_labels[self._col_syn] = df_path_labels[self._col_syn].astype(int)
        df_path_labels.drop(columns=[self._col_spec_desc], inplace=True)
        
        return df_path_labels
    
    def _process_data(self):
        self._init_minio()
        df_path_long = self._load_text()
        df_path_labels = self._load_labels()
        
        df = self._create_features(df=df_path_long, df_labels=df_path_labels)
        df_training, df_validation = self._create_training_and_validation(df=df)
        
        self._build_model(df_training=df_training)
        
        df_validation = self.predict_synoptic(df_validation=df_validation, list_col_feat=self._cols_feat)
        
        df_label_synoptic = self._build_results(df_training=df_training, df_validation=df_validation)
        
        # Set member variable
        self._df_label_synoptic = df_label_synoptic
        
        if self._fname_save is not None:
            self._save_results(fname_save=self._fname_save)
        
    def _create_features(self, df, df_labels):
        feature1 = df[self._col_spec_desc].str.count('- ')
        feature2 = df[self._col_spec_desc].str.count(':')
        feature3 = df[self._col_spec_desc].str.len()
        df = df.assign(FEATURE1=feature1)
        df = df.assign(FEATURE2=feature2)
        df = df.assign(FEATURE3=feature3)
        df = df.merge(right=df_labels, how='left', on=[self._col_id, self._col_id_num])

        df = df[[self._col_id, self._col_id_num, self._col_syn] + self._cols_feat]
        
        return df
    
    def _create_training_and_validation(self, df):
        logic_keep = df[self._cols_feat].notnull().sum(axis=1) == 3
        logic_labeled = df[self._col_syn].notnull()
        logic_not_labeled = df[self._col_syn].isnull()
        df_training = df[logic_keep & logic_labeled].reset_index(drop=True)
        kwargs = {self._col_label_prediction : lambda x: False}
        df_training = df_training.assign(**kwargs)
        
        df_validation = df.loc[logic_not_labeled & logic_keep].reset_index(drop=True).copy()
        df_validation_features = df_validation[self._cols_feat]
        
        return df_training, df_validation
        
    def _build_model(self, df_training):
        kwargs = {self._col_label_prediction : False}
        df_training = df_training.assign(**kwargs)

        df_training_features = df_training[self._cols_feat]
        data_norm = (df_training_features - df_training_features.mean(axis=0))/df_training_features.std(axis=0)
        df_training_labels = df_training[self._col_syn]
    
        # all parameters not specified are set to their defaults
        logisticRegr = LogisticRegression(solver='lbfgs')
        logisticRegr.fit(df_training_features, df_training_labels)
        
        self._model = logisticRegr
        
    def predict_synoptic(self, df_validation, list_col_feat):
        x_validation = df_validation[list_col_feat]
        predicted = self._model.predict(x_validation)
        
        kwargs = {self._col_syn : lambda x: predicted}
        df_validation = df_validation.assign(**kwargs)
#         df_validation = df_validation.assign(IS_SYNOPTIC=predicted)
        
        kwargs = {self._col_label_prediction : lambda x: True}
        df_validation = df_validation.assign(**kwargs)
        
        return df_validation
        
    def _build_results(self, df_training, df_validation):
        df_label_synoptic = pd.concat([df_training, df_validation], axis=0, sort=False)[self._col_keep_output].reset_index(drop=True)
        
        df_label_synoptic[self._col_syn] = df_label_synoptic[self._col_syn].astype(int)
        
        return df_label_synoptic

def main():
    import sys
    sys.path.insert(0, '/mind_data/fongc2/pathology_report_segmentation/')
    import constants_darwin_pathology as c_dar
    

    fname_save = c_dar.fname_path_synoptic
    fname = c_dar.fname_darwin_path_clean_parsed_specimen
    fname_labels = c_dar.fname_path_synoptic_labels

    obj_syn = SynopticReportClassifier(fname_minio_env=c_dar.minio_env,
                                       fname_parsed_spec=fname,   
                                       fname_synoptic_labels=fname_labels,
                                       fname_save=fname_save)
    df_results = obj_syn.return_synoptic()

if __name__ == '__main__':
    main()
