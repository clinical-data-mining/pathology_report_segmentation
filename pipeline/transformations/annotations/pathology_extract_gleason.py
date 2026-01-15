# TODO: Migrate code from pipeline/pipeline_gleason_extraction.py into this script for cleaner code
import numpy as np
import re


def extractGleason(s):
    splitstr = 'Gleason'
    if 'Gleason score' in s:
        splitstr = 'Gleason score'
    tokens = re.split(splitstr,s,flags=re.IGNORECASE)
    if len(tokens)>1:
        maxGleason = []
        for t in tokens[1:]:
            ppg = parsePostGleasonStr(t[:min(len(t),20)])
            if ppg==ppg:
                maxGleason+=[ppg]
        if len(maxGleason)>0:
            return max(maxGleason)
    return np.nan

def parsePostGleasonStr(s):
    if '+' in s:
        s = ''.join(s.split())
        tokens = s.split('+')
        if len(tokens)>1 and len(tokens[0])>0 and len(tokens[1])>0 and tokens[0][-1].isnumeric() and tokens[1][0].isnumeric():
            return int(tokens[0][-1])+int(tokens[1][0])
        #else:
        #    print(tokens[0])
        #    print(tokens[1])
    elif ':' in s:
        s = ''.join(s.split())
        tokens = s.split(':')
        if len(tokens)>0 and len(tokens[1])>0:
            if tokens[1][0].isnumeric():
                return int(tokens[1][0])
    return np.nan

