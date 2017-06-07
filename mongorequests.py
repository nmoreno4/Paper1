#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 13:04:54 2017

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
import re
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

#docs2004 = db.AllNews.find({})
#a =[doc for doc in docs2004]
#db = client.TRNews
#for i in a:
#    db.AllNews.insert_one(i, { "ordered": False })



#%%
collection = db.AllNews
for y in range(y_start,y_end):
    del TRNA
    end = time.time()
    print("This year has taken: " + str(end-start))
    start = time.time()
    with open("/mnt/HDD/Reuters/Analytics/JSON/Historical/TRNA.TR.News.CMPNY_AMER.EN."+str(y)+".40020076.JSON.txt/data") as data_file:
        TRNA = json.load(data_file)
        row = 0
#        X = np.array([['altId', 'firstCreated', 'guId', 'assetId', 'firstMentionSentence', 'ticker', 'assetName', 'linkedIds', 'noveltyCounts', 'relevance', 'sentimentClass', 'sentimentNegative', 'sentimentNeutral', 'sentimentPositive', 'sentimentWordCount', 'volumeCounts', 'bodySize', 'companyCount', 'exchangeAction', 'headlineTag', 'marketCommentary', 'sentenceCount', 'wordCount', 'headline', 'language', 'urgency', 'subjects', 'provider', 'sourceTimestamp', 'audiences', 'feedTimestamp', 'takeSequence']])
        X1 = [['altId', 'firstCreated', 'guId', 'assetId', 'firstMentionSentence', 'ticker', 'assetName', 'linkedIds', 'noveltyCounts', 'relevance', 'sentimentClass', 'sentimentNegative', 'sentimentNeutral', 'sentimentPositive', 'sentimentWordCount', 'volumeCounts', 'bodySize', 'companyCount', 'exchangeAction', 'headlineTag', 'marketCommentary', 'sentenceCount', 'wordCount', 'headline', 'language', 'urgency', 'subjects', 'provider', 'sourceTimestamp', 'audiences', 'feedTimestamp', 'takeSequence']]
        df = pd.DataFrame(columns = ['altId', 'firstCreated', 'guId', 'assetId', 'firstMentionSentence', 'ticker', 'assetName', 'linkedIds', 'noveltyCounts', 'relevance', 'sentimentClass', 'sentimentNegative', 'sentimentNeutral', 'sentimentPositive', 'sentimentWordCount', 'volumeCounts', 'bodySize', 'companyCount', 'exchangeAction', 'headlineTag', 'marketCommentary', 'sentenceCount', 'wordCount', 'headline', 'language', 'urgency', 'subjects', 'provider', 'sourceTimestamp', 'audiences', 'feedTimestamp', 'takeSequence'])

        #%%
        end = time.time()
        start = time.time()
        advance_counter = 1
        tot_len = len(TRNA["Items"])
        print("Number of rows: " + str(tot_len))
        previous_print = 0
        for item in TRNA['Items']:
            advance_counter += 1
            crt_adv = math.floor((advance_counter/tot_len)*100)
            if crt_adv not in range(previous_print-1, previous_print+4):
                end = time.time()
                print(end-start)
                start = time.time()
                print("Current advancement is of: "+ str(crt_adv) + "%")
                previous_print = crt_adv
                
            altId = item["newsItem"]["metadata"]["altId"]
            firstCreated = item["newsItem"]["metadata"]["firstCreated"]
            guId = item["newsItem"]["sourceId"]
            assetId =  item["analytics"]["analyticsScores"][0]["assetId"]
            firstMentionSentence =  item["analytics"]["analyticsScores"][0]["firstMentionSentence"]
            ticker =  item["analytics"]["analyticsScores"][0]["assetCodes"][1].partition(':')[-1].rpartition('.')[0]
            assetName =  item["analytics"]["analyticsScores"][0]["assetName"].lower()
            linkedIds =  item["analytics"]["analyticsScores"][0]["linkedIds"]
            noveltyCounts =  item["analytics"]["analyticsScores"][0]["noveltyCounts"]
            relevance =  item["analytics"]["analyticsScores"][0]["relevance"]
            sentimentClass =  item["analytics"]["analyticsScores"][0]["sentimentClass"]
            sentimentNegative =  item["analytics"]["analyticsScores"][0]["sentimentNegative"]
            sentimentNeutral =  item["analytics"]["analyticsScores"][0]["sentimentNeutral"]
            sentimentPositive =  item["analytics"]["analyticsScores"][0]["sentimentPositive"]
            sentimentWordCount =  item["analytics"]["analyticsScores"][0]["sentimentWordCount"]
            volumeCounts =  item["analytics"]["analyticsScores"][0]["volumeCounts"]
            bodySize =  item["analytics"]["newsItem"]["bodySize"]
            companyCount =  item["analytics"]["newsItem"]["companyCount"]
            exchangeAction =  item["analytics"]["newsItem"]["exchangeAction"]
            headlineTag =  item["analytics"]["newsItem"]["headlineTag"]
            marketCommentary =  item["analytics"]["newsItem"]["marketCommentary"]
            sentenceCount =  item["analytics"]["newsItem"]["sentenceCount"]
            wordCount =  item["analytics"]["newsItem"]["wordCount"]
            headline = item["newsItem"]["headline"]
            language = item["newsItem"]["language"]
            urgency = item["newsItem"]["urgency"]
            subjects = item["newsItem"]["subjects"]
            provider = item["newsItem"]["provider"]
            sourceTimestamp = item["newsItem"]["sourceTimestamp"]
            audiences = item["newsItem"]["metadata"]["audiences"]
            feedTimestamp = item["newsItem"]["metadata"]["feedTimestamp"]
            takeSequence = item["newsItem"]["metadata"]["takeSequence"]
            
#            df.loc[row] = pd.Series({'altId':altId, 'firstCreated':firstCreated, 'guId':guId, 'assetId':assetId, 'firstMentionSentence':firstMentionSentence, 'ticker':ticker, 'assetName':assetName, 'linkedIds':linkedIds, 'noveltyCounts':noveltyCounts, 'relevance':relevance, 'sentimentClass':sentimentClass, 'sentimentNegative':sentimentNegative, 'sentimentNeutral':sentimentNeutral, 'sentimentPositive':sentimentPositive, 'sentimentWordCount':sentimentWordCount, 'volumeCounts':volumeCounts, 'bodySize':bodySize, 'companyCount':companyCount, 'exchangeAction':exchangeAction, 'headlineTag':headlineTag, 'marketCommentary':marketCommentary, 'sentenceCount':sentenceCount, 'wordCount':wordCount, 'headline':headline, 'language':language, 'urgency':urgency, 'subjects':subjects, 'provider':provider, 'sourceTimestamp':sourceTimestamp, 'audiences':audiences, 'feedTimestamp':feedTimestamp, 'takeSequence':takeSequence})
            row+=1
#            if row > 4:
#                break
            A = [altId, firstCreated, guId, assetId, firstMentionSentence, ticker, assetName, linkedIds, noveltyCounts, relevance, sentimentClass, sentimentNegative, sentimentNeutral, sentimentPositive, sentimentWordCount, volumeCounts, bodySize, companyCount, exchangeAction, headlineTag, marketCommentary, sentenceCount, wordCount, headline, language, urgency, subjects, provider, sourceTimestamp, audiences, feedTimestamp, takeSequence]
#            X = np.vstack((X, A)) # Here is the bottleneck
            X1.append(A)
            
                         #%%
#            if row == 30000:
#                break
#        XX = np.asarray(X1)
        AA = pd.DataFrame(data=X1[1:], columns=X1[0])
#        AA = pd.DataFrame(data=X[1:,:], columns=X[0,:])
        BB = list(AA['altId'] + AA['firstCreated'])
        print('Finished')
        
        #%%
        seen, dup_idx = set(), []
        for idx, item in enumerate(BB):
            if item not in seen:
                seen.add(item)          # First time seeing the element
            else:
                dup_idx.append(idx)
                
        collection = []
        print('prepare')
        end = time.time()
        start = time.time()
        advance_counter = 1
        tot_len = len(BB)
        print("Number of rows: " + str(tot_len))
        previous_print = 0
        for i in range(0,len(BB)):
            advance_counter += 1
            crt_adv = math.floor((advance_counter/tot_len)*100)
            if crt_adv not in range(previous_print-1, previous_print+4):
                end = time.time()
                print(end-start)
                start = time.time()
                print("Current advancement is of: "+ str(crt_adv) + "%")
                previous_print = crt_adv
            if i not in dup_idx:
                collection.append({
                                        'altId' : AA['altId'][i], 
                                        'firstCreated' : AA['firstCreated'][i],
                                   'takes':[]
                                   })
        print('ready to go')
        
    #%%
        doc_idx = 0  
        cuid = []
        docCount = 0
        previous_print = 0
        start = time.time()
        for doc in collection:
#            print("new doc")
            cuid.append(collection[doc_idx])
            crt_adv = math.floor((docCount/len(collection))*100)
            if crt_adv not in range(previous_print-1, previous_print+1):
                end = time.time()
                print(end-start)
                start = time.time()
                print("Current advancement is of: "+ str(crt_adv) + "%")
                previous_print = crt_adv
            crtDf = AA[(AA['altId'] == doc['altId']) & (AA['firstCreated']==doc['firstCreated'])]
#            print(len(crtDf))
            if len(crtDf)==1:
                if doc['takes']==[]:
#                    print("New news")
                    doc['takes'].append({
                                        'guId':crtDf['guId'].values[0],
                                        'analytics':[
                                                    {'assetId':crtDf['assetId'].values[0], 'firstMentionSentence':crtDf['firstMentionSentence'].values[0].item(), 'ticker':crtDf['ticker'].values[0], 'assetName':crtDf['assetName'].values[0], 'linkedIds':crtDf['linkedIds'].values[0], 'noveltyCounts':crtDf['noveltyCounts'].values[0], 'relevance':crtDf['relevance'].values[0].item(), 'sentimentClass':crtDf['sentimentClass'].values[0].item(), 'sentimentNegative':crtDf['sentimentNegative'].values[0].item(), 'sentimentNeutral':crtDf['sentimentNeutral'].values[0].item(), 'sentimentPositive':crtDf['sentimentPositive'].values[0].item(), 'sentimentWordCount':crtDf['sentimentWordCount'].values[0].item(), 'volumeCounts':crtDf['volumeCounts'].values[0]}
                                                    ],
                                        'bodySize':crtDf['bodySize'].values[0].item(), 'companyCount':crtDf['companyCount'].values[0].item(), 'exchangeAction':crtDf['exchangeAction'].values[0], 'headlineTag':crtDf['headlineTag'].values[0], 'marketCommentary':crtDf['marketCommentary'].values[0].item(), 'sentenceCount':crtDf['sentenceCount'].values[0].item(), 'wordCount':crtDf['wordCount'].values[0].item(), 'headline':crtDf['headline'].values[0], 'language':crtDf['language'].values[0], 'urgency':crtDf['urgency'].values[0].item(), 'subjects':crtDf['subjects'].values[0], 'provider':crtDf['provider'].values[0], 'sourceTimestamp':crtDf['sourceTimestamp'].values[0], 'audiences':crtDf['audiences'].values[0], 'feedTimestamp':crtDf['feedTimestamp'].values[0], 'takeSequence':crtDf['takeSequence'].values[0].item()
                                        })
            elif len(crtDf)>1:
                counter = -1
                for ix, row in crtDf.iterrows():
                    counter+=1
                    if counter == 0:
                        if doc['takes']==[]:
                            doc['takes'].append({
                                                'guId':row['guId'],
                                                'analytics':[
                                                            {'assetId':row['assetId'], 'firstMentionSentence':row['firstMentionSentence'], 'ticker':row['ticker'], 'assetName':row['assetName'], 'linkedIds':row['linkedIds'], 'noveltyCounts':row['noveltyCounts'], 'relevance':row['relevance'], 'sentimentClass':row['sentimentClass'], 'sentimentNegative':row['sentimentNegative'], 'sentimentNeutral':row['sentimentNeutral'], 'sentimentPositive':row['sentimentPositive'], 'sentimentWordCount':row['sentimentWordCount'], 'volumeCounts':row['volumeCounts']}
                                                            ],
                                                'bodySize':row['bodySize'], 'companyCount':row['companyCount'], 'exchangeAction':row['exchangeAction'], 'headlineTag':row['headlineTag'], 'marketCommentary':row['marketCommentary'], 'sentenceCount':row['sentenceCount'], 'wordCount':row['wordCount'], 'headline':row['headline'], 'language':row['language'], 'urgency':row['urgency'], 'subjects':row['subjects'], 'provider':row['provider'], 'sourceTimestamp':row['sourceTimestamp'], 'audiences':row['audiences'], 'feedTimestamp':row['feedTimestamp'], 'takeSequence':row['takeSequence']
                                                })
                        else:
                            print("Hey there is a problem, no?")
                    else:
                        if doc['takes']==[]:
                            print("There is a problem")
                        else:
#                            print(len(doc['takes']))
                            wasadded = False
                            uidadd = 0
                            for take in doc['takes']:
                                if take['guId'] == row['guId']:
#                                    print("Add company ")
                                    uidadd += 1
                                    take['analytics'].append({
                                                              'assetId':row['assetId'], 'firstMentionSentence':row['firstMentionSentence'], 'ticker':row['ticker'], 'assetName':row['assetName'], 'linkedIds':row['linkedIds'], 'noveltyCounts':row['noveltyCounts'], 'relevance':row['relevance'], 'sentimentClass':row['sentimentClass'], 'sentimentNegative':row['sentimentNegative'], 'sentimentNeutral':row['sentimentNeutral'], 'sentimentPositive':row['sentimentPositive'], 'sentimentWordCount':row['sentimentWordCount'], 'volumeCounts':row['volumeCounts']
                                                              })
                                    wasadded = True
                            if not wasadded:
#                                print("Add Take")
                                doc['takes'].append({
                                            'guId':row['guId'],
                                            'analytics':[
                                                        {'assetId':row['assetId'], 'firstMentionSentence':row['firstMentionSentence'], 'ticker':row['ticker'], 'assetName':row['assetName'], 'linkedIds':row['linkedIds'], 'noveltyCounts':row['noveltyCounts'], 'relevance':row['relevance'], 'sentimentClass':row['sentimentClass'], 'sentimentNegative':row['sentimentNegative'], 'sentimentNeutral':row['sentimentNeutral'], 'sentimentPositive':row['sentimentPositive'], 'sentimentWordCount':row['sentimentWordCount'], 'volumeCounts':row['volumeCounts']}
                                                        ],
                                            'bodySize':row['bodySize'], 'companyCount':row['companyCount'], 'exchangeAction':row['exchangeAction'], 'headlineTag':row['headlineTag'], 'marketCommentary':row['marketCommentary'], 'sentenceCount':row['sentenceCount'], 'wordCount':row['wordCount'], 'headline':row['headline'], 'language':row['language'], 'urgency':row['urgency'], 'subjects':row['subjects'], 'provider':row['provider'], 'sourceTimestamp':row['sourceTimestamp'], 'audiences':row['audiences'], 'feedTimestamp':row['feedTimestamp'], 'takeSequence':row['takeSequence']
                                            })
                                wasadded = True
#                print(counter)
                        
                    
            docCount += 1
                
            
import pickle
def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
save_obj(collection)      

# db.AllNews.insert_many(collection)
        
        #%%
            
#            collection.update_one({'$and':[ 
#                                      {'_id.altId' : firstCreated},
#                                      {'_id.firstCreated' : altID}
#                                      ]},
#                                  {'$set':{
#                                      'Takes': [ {'guId': guId, 
#                                                  'Analytics': [{'assetId' : assetId,
#                                                                 'firstMentionSentence' : firstMentionSentence,
#                                                                 'ticker' : ticker,
#                                                                 'assetName' : assetName,
#                                                                 'linkedIds' : linkedIds,
#                                                                 'noveltyCounts' : noveltyCounts,
#                                                                 'relevance' : relevance,
#                                                                 'sentimentClass' : sentimentClass,
#                                                                 'sentimentNegative' : sentimentNegative,
#                                                                 'sentimentNeutral' : sentimentNeutral,
#                                                                 'sentimentPositive' : sentimentPositive,
#                                                                 'sentimentWordCount' : sentimentWordCount,
#                                                                 'volumeCounts' : volumeCounts
#                                                                 }],
#                                                  'bodySize' : bodySize,
#                                                  'companyCount' : companyCount,
#                                                  'exchangeAction' : exchangeAction,
#                                                  'headlineTag' : headlineTag,
#                                                  'marketCommentary' : marketCommentary,
#                                                  'sentenceCount' : sentenceCount,
#                                                  'wordCount' : wordCount,
#                                                  'headline' : headline,
#                                                  'languageTRNA' : language,
#                                                  'audiences' : audiences,
#                                                  'feedTimestamp' : feedTimestamp,
#                                                  'takeSequence' : takeSequence,
#                                                  'provider' : provider,
#                                                  'sourceTimestamp' : sourceTimestamp,
#                                                  'subjects' : subjects,
#                                                  'urgency' : urgency
#                                                  }]
#                                      }}, 
#                              upsert=True)
#        
#        
#            collection.update_one({'$and':[ 
#                                      {'_id.altId' : firstCreated},
#                                      {'_id.firstCreated' : altID}
#                                      ]},
#                                  {'$addToSet':{
#                                      'Takes': [ {'guId': guId, 
#                                                  'Analytics': [{'assetId' : assetId,
#                                                                 'firstMentionSentence' : firstMentionSentence,
#                                                                 'ticker' : ticker,
#                                                                 'assetName' : assetName,
#                                                                 'linkedIds' : linkedIds,
#                                                                 'noveltyCounts' : noveltyCounts,
#                                                                 'relevance' : relevance,
#                                                                 'sentimentClass' : sentimentClass,
#                                                                 'sentimentNegative' : sentimentNegative,
#                                                                 'sentimentNeutral' : sentimentNeutral,
#                                                                 'sentimentPositive' : sentimentPositive,
#                                                                 'sentimentWordCount' : sentimentWordCount,
#                                                                 'volumeCounts' : volumeCounts
#                                                                 }],
#                                                  'bodySize' : bodySize,
#                                                  'companyCount' : companyCount,
#                                                  'exchangeAction' : exchangeAction,
#                                                  'headlineTag' : headlineTag,
#                                                  'marketCommentary' : marketCommentary,
#                                                  'sentenceCount' : sentenceCount,
#                                                  'wordCount' : wordCount,
#                                                  'headline' : headline,
#                                                  'languageTRNA' : language,
#                                                  'audiences' : audiences,
#                                                  'feedTimestamp' : feedTimestamp,
#                                                  'takeSequence' : takeSequence,
#                                                  'provider' : provider,
#                                                  'sourceTimestamp' : sourceTimestamp,
#                                                  'subjects' : subjects,
#                                                  'urgency' : urgency
#                                                  }]
#                                      }},
#                            upsert=True)
#                        
