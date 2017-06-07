E#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  9 10:11:14 2017

@author: nicolas
"""

#Analyse database
import datetime
import time
import math
import pandas as pd
import numpy as np
start = time.time()
import json
RTRS = 0
TRNA = 0
import pymongo
from pymongo import MongoClient
client = MongoClient()
db = client.News2005ter
y_start = 2005
y_end = 2006
m_start = 1
m_end = 13

kk = 0

#%%
a = db.AllNews.find({"$where":"this.takes.length > 8"})
b = []
for i in a:
    b.append(i)
    
#%%
y = 2005
m = "01"
df = pd.read_json("/mnt/42446FBE446FB379/Reuters/News/"+str(y)+"/News.RTRS."+str(y)+str(m)+".0210.txt/data")

#%%
df[(['body', 'mimeType', 'firstCreated', 'language', 
     'altId', 'headline', 'takeSequence', 'pubStatus', 
     'subjects', 'audiences', 'versionCreated', 'provider', 
     'instancesOf', 'id', 'urgency'])] = df.Items.apply(
    lambda x: pd.Series(x['data'], index=x['data'].keys())
    )

#%%
for i in range(0,len(b)):
    a = df.loc[df['firstCreated'] == b[i]['firstCreated']]
    print(b[i]['firstCreated'])
    if len(a)>0:
        break
    
#%%
