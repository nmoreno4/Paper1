#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 31 10:04:08 2017

@author: nicolas
"""

# Add companies
import datetime
import time
import math
import pandas as pd
import numpy as np
start = time.time()
import json
import re
RTRS = 0
TRNA = 0
from pymongo import MongoClient
client = MongoClient()
db = client.News20032

collection = db.Companies
a = pd.read_csv("/mnt/HDD/Reuters/Companies/TRNA_Companies.csv")

for ix, row in a.iterrows():
    collection.insert_one(
                              {
                                 "ticker" : row['ticker'],
                                 "COMNAM" : row['companyName'],
                                 "country" : row['countryOfDomicile'],
                                 "sector" : row['TRBCEconomicSector'],
                                 "status" : row['status'],
                                 "RIC" : row['RIC'],
                                 "marketMIC" : row['marketMIC'],
                                 "permID" : row['PermID']
                               })