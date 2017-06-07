#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  8 14:50:23 2017

@author: nicolas
"""

#Stock market data to mongodb
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
db = client.TRNewsPenta
y_start = 2003
y_end = 2004
m_start = 1
m_end = 13

kk = 0

#%% Docs in collection
Stock_Data = pd.read_sas('/home/nicolas/MEGAsync/crsp_nico_ff.sas7bdat')
