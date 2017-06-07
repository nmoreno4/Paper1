#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 22 19:33:43 2017

@author: nicolas
"""

import datetime
import time
start = time.time()
import json
import pprint
RTRS = 0
from pymongo import MongoClient
client = MongoClient()
db = client.NewsDB


#%%
collection = db.Archives
for y in range(2005,2016):
    for m in ["%.2d" % i for i in range(1,13)]:
        del RTRS
        print("Month: "+m)
        end = time.time()
        print("This month has taken: " + str(end-start))
        start = time.time()
        with open("/mnt/42446FBE446FB379/Reuters/News/"+str(y)+"/News.RTRS."+str(y)+str(m)+".0210.txt/data") as data_file:
            RTRS = json.load(data_file)
            insert_id = collection.insert_many(RTRS["Items"])
     
#%%
collection = db.TRNA
for y in range(2005,2016):
    del RTRS
    end = time.time()
    print("This month has taken: " + str(end-start))
    start = time.time()
    with open("/mnt/42446FBE446FB379/Reuters/Analytics/JSON/Historical/TRNA.TR.News.CMPNY_AMER.EN."+str(y)+".40020076.JSON.txt/data") as data_file:
        RTRS = json.load(data_file)
        insert_id = collection.insert_many(RTRS["Items"])
