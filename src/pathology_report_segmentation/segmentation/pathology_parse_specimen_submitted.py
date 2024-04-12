""""
pathology_parse_specimen_submitted.py

Parses specimen submitted column into individual parts

"""
import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))

import pandas as pd
from src.pathology_report_segmentation.data_processing.utils_pathology import parse_specimen_info
from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_dar



class PathologyParseSpecSubmitted(object):
    def __init__(self, fname_minio_env, fname_path_parsed, col_spec_sub, list_cols_id, fname_save=None):
        self._fname_path_parsed = fname_path_parsed
        self._fname_save = fname_save
        self._col_spec_sub = col_spec_sub
        self._list_cols_id = list_cols_id
        self._obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

        self._df_input = None
        self._df_parsed_spec_sub = None

        self._process_data()

    def _process_data(self):
        # Use different loading process if clean path data set is accessible
        #Load data_conv
        df_sample_rpt = self._load_data()
        print('Parsing Specimen List')

        # Remove Amended Diagnosis
        df_sample_rpt = self._remove_amended_diagnosis(df=df_sample_rpt)

        # Parse by number
        df, df_spec_parse_list_note = parse_specimen_info(df=df_sample_rpt, col_name=self._col_spec_sub)

        # Reorganize to place into a long format
        cols_id = self._list_cols_id
        df_spec_list = pd.concat([df[cols_id], df_spec_parse_list_note], axis=1, sort=False)
        df_spec_list_melt = pd.melt(frame=df_spec_list,
                                    value_vars=df_spec_parse_list_note.columns,
                                    id_vars=cols_id,
                                    value_name='SPECIMEN_SUBMITTED',
                                    var_name='SPECIMEN_NUMBER')
        # Remove rows with NA in SPECIMEN_SUBMITTED column
        df_spec_list_melt1 = df_spec_list_melt[df_spec_list_melt['SPECIMEN_SUBMITTED'].notnull()]
        # Merge with original acceesion number list to find gaps in parsing
        df_spec_list_f = df[cols_id].merge(right=df_spec_list_melt1, how='left', on=cols_id)

        self._df_parsed_spec_sub = df_spec_list_f

        # Save data
        if self._fname_save is not None:
            print('Saving %s' % self._fname_save)
            self._obj_minio.save_obj(
                df=df_spec_list_f,
                path_object=self._fname_save,
                sep='\t'
            )

    def _remove_amended_diagnosis(self, df):
        key = 'amended diagnosis'
        log_nonull = df[self._col_spec_sub].notnull()
        ind_contains = df[self._col_spec_sub].str.lower().str.contains(key)

        if ind_contains.sum() > 0:
            t = df[log_nonull & ind_contains]
            ind = t[self._col_spec_sub].str.lower().str.find(key)
            t = t.assign(INDEX = ind)
            p = t.apply(lambda x: x[self._col_spec_sub][:x['INDEX']], axis=1)

            df.loc[p.index, self._col_spec_sub] = p

        return df

    def return_df(self):
        return self._df_parsed_spec_sub

    def _load_data(self):
        print('Loading %s' % self._fname_path_parsed)
        obj = self._obj_minio.load_obj(path_object=self._fname_path_parsed)
        df = pd.read_csv(obj, header=0, low_memory=False, sep='\t')

        return df

def main():

    obj_mol = PathologyParseSpecSubmitted(
        fname_minio_env=c_dar.minio_env,
        fname_path_parsed=c_dar.fname_path_clean,
        col_spec_sub='SPECIMEN_SUBMISSION_LIST',
        list_cols_id=['MRN', 'ACCESSION_NUMBER'],
        fname_save=c_dar.fname_darwin_path_col_spec_sub
    )

    df_m = obj_mol.return_df()

    tmp = 0

if __name__ == '__main__':
    main()