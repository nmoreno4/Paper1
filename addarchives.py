#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  8 13:42:45 2017

@author: nicolas
"""

# Add archives
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
db = client.News20032
y_start = 2003
y_end = 2004
m_start = 1
m_end = 13

kk = 0

#%% Docs in collection

for y in range(y_start,y_end):
    for m in ["%.2d" % i for i in range(m_start,m_end)]:
        del RTRS
        print("Month: "+m)
        end = time.time()
        print("This month has taken: " + str(end-start))
        start = time.time()
        with open("/mnt/42446FBE446FB379/Reuters/News/"+str(y)+"/News.RTRS."+str(y)+str(m)+".0210.txt/data") as data_file:
            RTRS = json.load(data_file)
            
            advance_counter = 1
            tot_len = len(RTRS["Items"])
            print("Number of rows: " + str(tot_len))
            previous_print = 0
            
            for item in RTRS['Items']:
                
                advance_counter += 1
                crt_adv = math.floor((advance_counter/tot_len)*100)
                if crt_adv not in range(previous_print-1, previous_print+4):
                    print("Current advancement is of: "+ str(crt_adv) + "%")
                    previous_print = crt_adv
                    
                altId = item['data']['altId']
                firstCreated = item['data']['firstCreated']
                guId = item['guid']
                if guId != item['data']['id']:
                    print("IDs do nt match!!")
                body = item['data']['body']
                headline = item['data']['headline']
                provider = item['data']['provider']
                pubStatus = item['data']['pubStatus']
                takeSequence = item['data']['takeSequence']
                urgency = item['data']['urgency']
                versionCreated = item['data']['versionCreated']
                takeSequence = item['data']['takeSequence']
            
#                if guId == "20050103-130420000-nWEN1920-1-2":
#                    break
                
                if len(body)>0:
#                    guId = guId[:-1]+str(1)
#                    a = db.AllNews.find_one({"takes.guId" : guId}, {"takes.headline" : 1})
                    db.AllNews.update_one({
#                                            "altId" : altId, 
#                                            "firstCreated" : firstCreated,
                                            
                                            "takes.guId" : guId, 
    #                                        "takes.urgency":3, 
                                            "takes.body" : {"$ne" : body}, "takes.archiveHeadline" : {"$ne" : headline}, 
                                            "takes.archiveVersionCreated" : {"$ne" : versionCreated},
                                            "takes.pubStatus" : {"$ne" : pubStatus}, "takes.archiveUrgency" : {"$ne" : urgency}
                                          },
                                           {
                                            "$set": {
                                               "takes.$.body" : body,
                                               "takes.$.archiveHeadline": headline,
                                               "takes.$.archiveVersionCreated" : versionCreated,
                                               "takes.$.pubStatus" : pubStatus,
                                               "takes.$.archiveUrgency" : urgency,
                                               "takes.$.archiveProvider" : provider,
                                               "takes.$.archiveTakeSequence" : takeSequence,
                                               "takes.$.archiveId" : guId
                                             }
                                           },
                                            upsert=False
                                           )



