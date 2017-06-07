#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 11:55:17 2017

@author: nicolas
"""
import pandas as pd
import numpy as np
from collections import Counter


#%% Load data

All_news = pd.read_csv('/home/nicolas/Code/SpecVsGlob/Export/All_news_sample.csv')
TRNA_News = pd.read_csv('/home/nicolas/Code/SpecVsGlob/Export/TRNA_news_sample.csv')

FF_factors = pd.read_sas('/home/nicolas/MEGAsync/crsp_nico_ff.sas7bdat')



#%% Remove useless beginning and ending of text body

#==============================================================================
# foo = TRNA_News.ix[(TRNA_News['sentimentNegative']>0.5)]['ID1 index news']
# a = [val for val in foo]
# b = [int(val[1:-1]) for val in a if len(val[1:-1]) > 1]
# 
# text_bodies = All_news[['body', 'headline']]
# text_bodies = text_bodies.iloc[b]
# 
# intros = []
# 
# def isNaN(num):
#     return num != num
# 
# for i in text_bodies['body']:
#     if isNaN(i):
#         print("is nan")
#     else:
#         intros.append(i[0:450])
# 
# Counter(intros)
#==============================================================================

#%% Indicate when a stock is value and when not

findstock = FF_factors[['COMNAM', 'TICKER', 'CUSIP', 'BM', 'DATE', 'retadj', 'PERMNO']]
findstock['COMNAM'] = findstock['COMNAM'].str.decode('utf-8')
findstock['TICKER'] = findstock['TICKER'].str.decode('utf-8')
findstock['CUSIP'] = findstock['CUSIP'].str.decode('utf-8')
findstock['BM'] = findstock['BM'].str.decode('utf-8')



row = 0
for i in findstock['COMNAM']:
    if 'BEST BUY' in i:
        print(findstock['BM'][row])
        print(findstock['DATE'][row])
    row += 1

#%% Convert sas time

import datetime
epoch = datetime.datetime(1960, 1, 1)
findstock['DATE'] = findstock['DATE'].apply(
                        lambda s: epoch + datetime.timedelta(days=int(s))
                        ) 