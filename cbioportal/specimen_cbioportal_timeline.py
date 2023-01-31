""""
specimen_cbioportal_timeline.py

By Chris Fong - MSKCC 2022


"""
import os
import sys
sys.path.insert(0, '../../cdm-utilities/')
sys.path.insert(0, '../../cdm-utilities/minio_api')
import pandas as pd
import numpy as np
from minio_api import MinioAPI
from utils import drop_cols, mrn_zero_pad, convert_to_int, read_minio_api_config, save_appended_df


class cBioPortalSpecimenInfo(object):
    def __init__(self, fname_minio_config, fname_impact_sid, fname_impact_summary, fname_demo, fname_save_seq, fname_save_spec):
        self._fname_summary = fname_impact_summary
        self._fname_sid = fname_impact_sid
        self._fname_demo = fname_demo
        self._fname_save_timeline_spec = fname_save_spec
        self._fname_save_timeline_seq = fname_save_seq
        self._minio_env = fname_minio_config

        # Objects
        self._obj_minio = None

        # Data frames
        ## Input dataframes
        self._df_demo = None
        self._df_summary = None
        self._df_sid = None
        
        ## Timeline dfs
        self._df_seq = None
        self._df_spec = None
        
        # Hardcode column names for cBioPortal formatting (TODO)        

        self._init_process()
        
    def _init_process(self):
        # Init Minio
        obj_minio = MinioAPI(fname_minio_env=self._minio_env)
        self._obj_minio = obj_minio
        
        # Load data files
        self._load_data()
        
        # Transform data
        df_timeline_seq = self._create_timeline_seq()
        df_timeline_spec = self._create_timeline_spec()
        
        self._df_seq = df_timeline_seq
        self._df_spec = df_timeline_spec
        
        if self._fname_save_timeline_seq is not None:
            save_appended_df(df_timeline_seq, filename=self._fname_save_timeline_seq, sep='\t')
            save_appended_df(df_timeline_spec, filename=self._fname_save_timeline_spec, sep='\t')
            

    def _load_data(self):
        # Load files
        self._load_demographics()
        self._load_summary()
        self._load_sid()
        
        return None
    
    def _load_demographics(self):
        print('Loading %s' % self._fname_demo)
        obj = self._obj_minio.load_obj(path_object=self._fname_demo)
        usecols = ['MRN', 'PT_BIRTH_DTE']
        df_demo = pd.read_csv(obj, header=0, low_memory=False, sep='\t',usecols=usecols)  
        df_demo = mrn_zero_pad(df=df_demo, col_mrn='MRN')
        df_demo['PT_BIRTH_DTE'] = pd.to_datetime(df_demo['PT_BIRTH_DTE'])
        
        self._df_demo = df_demo
        
        return None
    
    def _load_summary(self):
        ### Load Dx timeline data\
        col_use = ['MRN', 'DMP_ID', 'DATE_SEQUENCING_REPORT', 'SAMPLE_ID', 'DATE_OF_PROCEDURE_SURGICAL_EST']
        fname = self._fname_summary
        print('Loading %s' % fname)
        obj = self._obj_minio.load_obj(path_object=fname)
        df_samples_seq = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=col_use)
        df_samples_seq['DATE_SEQUENCING_REPORT'] = pd.to_datetime(df_samples_seq['DATE_SEQUENCING_REPORT'])
        df_samples_seq['DATE_OF_PROCEDURE_SURGICAL_EST'] = pd.to_datetime(df_samples_seq['DATE_OF_PROCEDURE_SURGICAL_EST'])
        df_samples_seq = convert_to_int(df=df_samples_seq, list_cols=['MRN'])
        df_samples_seq = mrn_zero_pad(df=df_samples_seq, col_mrn='MRN')
        
        self._df_summary = df_samples_seq
        
        return None
    
    def _load_sid(self):
        ## Surgery
        fname = self._fname_sid
        print('Loading %s' % fname)
        obj = self._obj_minio.load_obj(path_object=fname)
        df_samples_current = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=['sampleId', 'CANCER_TYPE', 'SAMPLE_TYPE', 'CANCER_TYPE_DETAILED'])
        df_samples_current = df_samples_current.rename(columns={'sampleId': 'SAMPLE_ID'})
        
        self._df_sid = df_samples_current
        
    def _create_timeline_seq(self):
        df_samples_seq = self._df_summary
        df_demo = self._df_demo
        df_samples_current = self._df_sid
        
        col_order = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'SAMPLE_ID', 'CANCER_TYPE', 'CANCER_TYPE_DETAILED', 'SAMPLE_TYPE']

        df_samples_seq_f = df_samples_current.merge(right=df_samples_seq, how='left', on='SAMPLE_ID')
        df_samples_seq_f = df_samples_seq_f.merge(right=df_demo, how='left', on='MRN')
        START_DATE = (df_samples_seq_f['DATE_SEQUENCING_REPORT'] - df_samples_seq_f['PT_BIRTH_DTE']).dt.days
        df_samples_seq_f = df_samples_seq_f.assign(START_DATE=START_DATE)
        df_samples_seq_f = convert_to_int(df=df_samples_seq_f, list_cols=['START_DATE'])
        df_samples_seq_f = df_samples_seq_f.assign(STOP_DATE=np.NaN)
        df_samples_seq_f = df_samples_seq_f.assign(EVENT_TYPE='Sequencing')

        # Rename columns
        df_samples_seq_f = df_samples_seq_f.rename(columns={'DMP_ID': 'PATIENT_ID'}) 
        df_samples_seq_f = df_samples_seq_f[col_order]

        # Drop samples without sequencing date
        df_samples_seq_f = df_samples_seq_f[df_samples_seq_f['START_DATE'].notnull()]
        df_samples_seq_f = convert_to_int(df=df_samples_seq_f, list_cols=['START_DATE'])

        # df_samples_seq_f.head()
        
        return df_samples_seq_f
    
    def _create_timeline_spec(self):
        df_samples_seq = self._df_summary
        df_demo = self._df_demo
        df_samples_current = self._df_sid

        col_order = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'SAMPLE_ID', 'CANCER_TYPE', 'CANCER_TYPE_DETAILED', 'SAMPLE_TYPE', 'SURGICAL_METHOD']

        df_samples_surg_f = df_samples_current.merge(right=df_samples_seq, how='left', on='SAMPLE_ID')
        df_samples_surg_f = df_samples_surg_f.merge(right=df_demo, how='left', on='MRN')
        START_DATE = (df_samples_surg_f['DATE_OF_PROCEDURE_SURGICAL_EST'] - df_samples_surg_f['PT_BIRTH_DTE']).dt.days
        df_samples_surg_f = df_samples_surg_f.assign(START_DATE=START_DATE)
        df_samples_surg_f = convert_to_int(df=df_samples_surg_f, list_cols=['START_DATE'])
        df_samples_surg_f = df_samples_surg_f.assign(STOP_DATE=np.NaN)
        df_samples_surg_f = df_samples_surg_f.assign(EVENT_TYPE='Sample acquisition')
        df_samples_surg_f = df_samples_surg_f.assign(SURGICAL_METHOD='Under construction')

        # Rename columns
        df_samples_surg_f = df_samples_surg_f.rename(columns={'DMP_ID': 'PATIENT_ID'}) 
        df_samples_surg_f = df_samples_surg_f[col_order]

        # Drop samples without sequencing date        
        df_samples_surg_f = df_samples_surg_f[df_samples_surg_f['START_DATE'].notnull()]
        df_samples_surg_f = convert_to_int(df=df_samples_surg_f, list_cols=['START_DATE'])

        # df_samples_surg_f.head()
        
        return df_samples_surg_f
    
    
 

def main():
    
    import sys
    sys.path.insert(0, '/mind_data/fongc2/cdm-utilities/')
    from data_classes_cdm import CDMProcessingVariables as config_cdm
    
    fname_sid = config_cdm.fname_cbio_sid
    fname_summary = config_cdm.fname_impact_summary_sample
    fname_demo = fname_demo = config_cdm.fname_demo
    fname_save_timeline_seq = config_cdm.fname_save_spec_timeline
    fname_save_timeline_spec = config_cdm.fname_save_spec_surg_timeline

    obj_dx_timeline = cBioPortalSpecimenInfo(fname_minio_config=config_cdm.minio_env,
                                             fname_impact_sid=fname_sid, 
                                             fname_impact_summary=fname_summary, 
                                             fname_demo=fname_demo, 
                                             fname_save_seq=fname_save_timeline_seq, 
                                             fname_save_spec=fname_save_timeline_spec
                                            )


if __name__ == '__main__':
    main()
