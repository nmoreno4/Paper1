#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  3 11:45:30 2017

@author: nicolas
"""

#%%
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
db = client.TRNews
y_start = 2003
y_end = 2004
m_start = 1
m_end = 13

kk = 0


#%%
stats = []
allCompanies = db.AllNews.distinct("takes.analytics.assetName")
for company in allCompanies:
    stats.append(db.AllNews.find({"takes.analytics.assetName":company}).count())
    
#%% Plot

import numpy as np
import random
from matplotlib import pyplot as plt

data = np.array(stats)

# fixed bin size
bins = np.arange(min(data), max(data), 10) # fixed bin size

plt.xlim([min(data),300])


plt.hist(data, bins=bins, alpha=0.5)
plt.title('Number of news for a company over a year. Bin size = 10 news')
plt.xlabel('Number of news over the year')
plt.ylabel('Number of companies concerned')

plt.show()
    