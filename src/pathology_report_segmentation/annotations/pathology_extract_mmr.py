COLS_SAVE = ['MRN','Accession Number','Path Procedure Date','MMR_ABSENT']


'''
     MLH1:     staining absent in tumor
     PMS2:     staining absent in tumor
     MSH2:     staining present in tumor
     MSH6:     staining present in tumor'''


def _extractMMR_from_str(s):
    factors = ['MLH1','PMS2','MSH2','MSH6']
    for f in factors:
        if f in s:
            tokens = s.split(f)
            for i, token in enumerate(tokens[1:]):
                statement = '--'+token[:50] +'***'+ tokens[i][-50:]+'--'
                absentLoc = max(statement.lower().find('absent'),
                                statement.lower().find('loss'),
                               statement.lower().find('lost'))
                presentLoc= statement.lower().find('present')
                if absentLoc>=0 and presentLoc>=0:
                    if absentLoc<presentLoc:
                        return True
                if absentLoc>=0 and presentLoc<0:
                        return True
    return False




def extractMMR(df):
    filter_mmr = df['PATH_REPORT_NOTE'].fillna('').str.contains('MLH1|PMS2|MSH2|MSH6',regex=True,case=False)
    filter_mnumber = ~df['Accession Number'].str.contains('M')
    df_mmr = df[filter_mmr & filter_mnumber].copy()
    df_mmr['MMR_ABSENT'] = df_mmr['PATH_REPORT_NOTE'].apply(_extractMMR_from_str)
    df_save = df_mmr[COLS_SAVE]

    return df_save


# df_mmr[['MRN','Accession Number','Path Procedure Date','MMR_ABSENT']].to_csv('MMR.csv',index=False)

# bytedata=df_mmr[COLS_SAVE].to_csv(index=False).encode('utf-8')
# bufferdata=BytesIO(bytedata)
# client.put_object('cdm-data', 'pathology/pathology_mmr_calls.csv',
#                   data=bufferdata,
#                   length=len(bytedata),content_type='application/csv')
