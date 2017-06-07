#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 13:28:36 2017

@author: nicolas
"""

import pymysql.cursors
import json
import os
import datetime
import pandas as pd
import warnings
import math
import time
RTRS = 0
start = time.time()
warnings.filterwarnings("ignore")


#%% Populate Subjects Table
Codes = pd.read_excel("/home/nicolas/MEGAsync/Reuters example data/HEC News TRNA Sample Feb 14 2017/NewsCodes_20170207.xls")
# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='NewsDB',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # Create a new record
        sql = "INSERT INTO `Subjects` (`idSubjects`, `Description`, `Definition`, `TRCS_Concept_type`) VALUES (%s, %s, %s, %s)"
        sql1 = "INSERT INTO `Subjects` (`idSubjects`, `Definition`, `TRCS_Concept_type`) VALUES (%s, %s, %s)"
        sql2 = "INSERT INTO `Subjects` (`idSubjects`, `Description`, `TRCS_Concept_type`) VALUES (%s, %s, %s)"
        for key, row in Codes.iterrows():
            if str(row[1])=='nan':
                keys = (row[0], row[2], row[3])
                cursor.execute(sql1, keys)
            elif str(row[2])=='nan':
                keys = (row[0], row[1], row[3])
                cursor.execute(sql2, keys)
            elif str(row[0])!='nan':
                keys = (row[0], row[1], row[2], row[3])
                cursor.execute(sql, keys)

    # connection is not autocommit by default. So you must commit to save
    # your changes.
    connection.commit()

#            with connection.cursor() as cursor:
#                # Read a single record
#                sql = "SELECT * FROM `Stories`"
#                cursor.execute(sql)
#                result = cursor.fetchall()
#                print(type(result))
finally:
    connection.close()

#%% Load News Archives
for y in range(2004,2005):
    for m in ["%.2d" % i for i in range(1,13)]:
        print("Month: "+m)
        del RTRS
        end = time.time()
        print("This month has taken: " + str(end-start))
        start = time.time()
        with open("/mnt/42446FBE446FB379/Reuters/News/"+str(y)+"/News.RTRS."+str(y)+str(m)+".0210.txt/data") as data_file:
            RTRS = json.load(data_file)
    
#db_filename = '/home/nicolas/MEGAsync/Code/Project1/DB/News_DB.mwb'


        #%Connect to the database
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='NewsDB',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        
        #% Populate Stories Table
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = """INSERT INTO `Stories` (`altID`, `Date`) VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE `altID`=%s, `Date` = %s"""
                        
                advance_counter = 1
                tot_len = len(RTRS["Items"])
                print("Number of rows: " + str(tot_len))
                previous_print = 0       
                
                for item in RTRS['Items']:
                    
                    advance_counter += 1
                    crt_adv = math.floor((advance_counter/tot_len)*100)
                    if crt_adv not in range(previous_print-1, previous_print+9):
                        print("Current advancement is of: "+ str(crt_adv) + "%")
                        previous_print = crt_adv
                        
                    a = datetime.datetime.strptime(item['data']['firstCreated'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    keys = (item['data']['altId'], 
                            a.strftime('%Y-%m-%d %H:%M:%S'))
                    keys = keys+keys
#                   keys = ("n22324627", '2005-12-31 23:56:19')
                    cursor.execute(sql, keys)
        
            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        
#            with connection.cursor() as cursor:
#                # Read a single record
#                sql = "SELECT * FROM `Stories`"
#                cursor.execute(sql)
#                result = cursor.fetchall()
#                print(type(result))
        finally:
            connection.close()
            print("Stories populated")
            
            
#%% Populate Stories with TRNA data
#% Load News Archives
for y in range(2004,2005):
    end = time.time()
    print("This month has taken: " + str(end-start))
    start = time.time()
    with open("/mnt/42446FBE446FB379/Reuters/Analytics/JSON/Historical/TRNA.TR.News.CMPNY_AMER.EN."+str(y)+".40020076.JSON.txt/data") as data_file:
        RTRS = json.load(data_file)
        
        #% Populate Stories Table Part II
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='NewsDB',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = """INSERT INTO `Stories` (`altID`, `Date`) VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE `altID`=%s, `Date` = %s"""
                        
                advance_counter = 1
                tot_len = len(RTRS["Items"])
                print("Number of rows: " + str(tot_len))
                previous_print = 0               
                
                for item in RTRS['Items']:
                    
                    advance_counter += 1
                    crt_adv = math.floor((advance_counter/tot_len)*100)
                    if crt_adv not in range(previous_print-1, previous_print+2):
                        print("Current advancement is of: "+ str(crt_adv) + "%")
                        previous_print = crt_adv
                    
                    a = datetime.datetime.strptime(item['newsItem']['metadata']['firstCreated'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    keys = (item['newsItem']['metadata']['altId'], 
                            a.strftime('%Y-%m-%d %H:%M:%S'))
                    keys = keys+keys
#                   keys = ("n22324627", '2005-12-31 23:56:19')
                    cursor.execute(sql, keys)
        
            # connection is not autocommit by default. So you must commit to save
            # your changes.
                connection.commit()
        
#            with connection.cursor() as cursor:
#                # Read a single record
#                sql = "SELECT * FROM `Stories`"
#                cursor.execute(sql)
#                result = cursor.fetchall()
#                print(type(result))
        finally:
            connection.close()
            print("Stories populated")
            
            
            
            
#%% Populate Takes with archives
        
for y in range(2004,2005):
    for m in ["%.2d" % i for i in range(1,13)]:
        print("Month: "+m)
        del RTRS
        end = time.time()
        print("This month has taken: " + str(end-start))
        start = time.time()
        with open("/mnt/42446FBE446FB379/Reuters/News/"+str(y)+"/News.RTRS."+str(y)+str(m)+".0210.txt/data") as data_file:
            RTRS = json.load(data_file)
        
            connection = pymysql.connect(host='localhost',
                                         user='root',
                                         password='',
                                         db='NewsDB',
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)
            #% Populate Takes Table Part I
            try:
                with connection.cursor() as cursor:
                    # Insert from archives to Takes
                    sql = """INSERT INTO `Takes` (`Stories_altID`, `Stories_Date`, `guID`, `Body`, 
                                                         `Headline`, `Language`, `Provider`, `PubStatus`, 
                                                         `TakeSequence`, `Urgency`, `VersionCreated`)  
                                                         VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    
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
                        
                        a = datetime.datetime.strptime(item['data']['firstCreated'], "%Y-%m-%dT%H:%M:%S.%fZ")
                        keys = (item['data']['altId'], a.strftime('%Y-%m-%d %H:%M:%S'), item['guid'], 
                                item['data']['body'], item['data']['headline'], item['data']['language'], 
                                item['data']['provider'], item['data']['pubStatus'], item['data']['takeSequence'],
                                item['data']['urgency'], item['data']['versionCreated'])
                        cursor.execute(sql, keys)
                    
                    # Commit changes    
                    connection.commit()
            
            finally:
                connection.close()
            
            
#%% Populate Takes with TRNA data
for y in range(2004,2005):
    end = time.time()
    print("This month has taken: " + str(end-start))
    start = time.time()
    with open("/mnt/42446FBE446FB379/Reuters/Analytics/JSON/Historical/TRNA.TR.News.CMPNY_AMER.EN."+str(y)+".40020076.JSON.txt/data") as data_file:
        RTRS = json.load(data_file)
        
        # Create a new record
        
                
        advance_counter = 1
        tot_len = len(RTRS["Items"])
        print("Number of rows: " + str(tot_len))
        previous_print = 0
        
        for item in RTRS['Items']:
            
            advance_counter += 1
            crt_adv = math.ceil((advance_counter/tot_len)*1000)
            if crt_adv not in range(previous_print-1, previous_print+1):
                end = time.time()
                print("This month has taken: " + str(end-start))
                start = time.time()
                print("Current advancement is of: "+ str(crt_adv) + "%")
                previous_print = crt_adv
                
            a = datetime.datetime.strptime(item['newsItem']['sourceTimestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
            b = datetime.datetime.strptime(item['newsItem']['metadata']['firstCreated'], "%Y-%m-%dT%H:%M:%S.%fZ")
            c = datetime.datetime.strptime(item['newsItem']['metadata']['feedTimestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
            if item['newsItem']['metadata']['isArchive']==True:
                d = 1
            else:
                d = 0
            keys = (item['newsItem']['sourceId'],
                    d,
                    
                    item['analytics']['analyticsScores'][0]['firstMentionSentence'],
                    item['analytics']['analyticsScores'][0]['noveltyCounts'][0]['itemCount'],
                    item['analytics']['analyticsScores'][0]['noveltyCounts'][1]['itemCount'],
                    item['analytics']['analyticsScores'][0]['noveltyCounts'][2]['itemCount'],
                    item['analytics']['analyticsScores'][0]['noveltyCounts'][3]['itemCount'],
                    item['analytics']['analyticsScores'][0]['noveltyCounts'][4]['itemCount'],
                        
                    item['analytics']['analyticsScores'][0]['relevance'],
                    item['analytics']['analyticsScores'][0]['sentimentClass'],
                    item['analytics']['analyticsScores'][0]['sentimentNegative'],
                    item['analytics']['analyticsScores'][0]['sentimentNeutral'],
                    item['analytics']['analyticsScores'][0]['sentimentPositive'],
                    item['analytics']['analyticsScores'][0]['sentimentWordCount'],
                    item['analytics']['analyticsScores'][0]['volumeCounts'][0]['itemCount'],
                    item['analytics']['analyticsScores'][0]['volumeCounts'][1]['itemCount'],
                    item['analytics']['analyticsScores'][0]['volumeCounts'][2]['itemCount'],
                    item['analytics']['analyticsScores'][0]['volumeCounts'][3]['itemCount'],
                    item['analytics']['analyticsScores'][0]['volumeCounts'][4]['itemCount'],

                    item['analytics']['newsItem']['companyCount'],
                    item['analytics']['newsItem']['exchangeAction'],
                        
                    item['analytics']['newsItem']['wordCount'],
                    item['newsItem']['metadata']['altId'], 
                    b.strftime('%Y-%m-%d %H:%M:%S'))
  
            keys = keys+keys
#            print(keys)
#                   keys = ("n22324627", '2005-12-31 23:56:19')

            sql = """INSERT INTO `Takes`  (`guID`,`IsArchive`,`FirstMentionSentence`,`12Hnovelty`,`24Hnovelty`,`3Dnovelty`,`5Dnovelty`,`7Dnovelty`,
                      `Relevance`,`SentimentClass`,`SentimentNegative`,`SentimentNeutral`,`SentimentPositive`,`SentimentWordCount`,`12Hvolume`,`24Hvolume`,`3Dvolume`,`5Dvolume`,`7Dvolume`,`CompanyCount`,`SentenceCount`,`WordCount`,`Stories_altID`,`Stories_Date`)
                      VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                      ON DUPLICATE KEY UPDATE `guID`=%s,`IsArchive`=%s,
                      
                      `FirstMentionSentence`=%s,`12Hnovelty`=%s,`24Hnovelty`=%s,`3Dnovelty`=%s,
                      `5Dnovelty`=%s,`7Dnovelty`=%s,`Relevance`=%s,
                      `SentimentClass`=%s,`SentimentNegative`=%s,`SentimentNeutral`=%s,
                      `SentimentPositive`=%s,`SentimentWordCount`=%s,`12Hvolume`=%s,`24Hvolume`=%s,
                      `3Dvolume`=%s,`5Dvolume`=%s,`7Dvolume`=%s,`CompanyCount`=%s,
                      `SentenceCount`=%s,
                      `WordCount`=%s,`Stories_altID`=%s,`Stories_Date`=%s"""
            
            connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='NewsDB',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
            
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql, keys)
                    connection.commit()
            finally:
                connection.close()
                
            
#                    sql = """INSERT OR IGNORE INTO `Takes_has_Subjects` (`Takes_guID`, `Subjects_idSubjects`)
#                            VALUES(%s,%s)""" 
#                    try:
#                        with connection.cursor() as cursor:
#                            cursor.execute(sql, keys)
#                            connection.commit()
#                    finally:
#                        connection.close()
        

        
        
#        #%% Test
#    connection = pymysql.connect(host='localhost',
#                                     user='root',
#                                     password='',
#                                     db='NewsDB',
#                                     charset='utf8mb4',
#                                     cursorclass=pymysql.cursors.DictCursor)        
#        
#    try:
#        with connection.cursor() as cursor:
#            # Insert from archives to Takes
#            sql = """INSERT INTO Takes (`guID`,`Stories_altID`, `Stories_Date` ) 
#                                VALUES (%s,%s,%s)
#                     ON DUPLICATE KEY UPDATE `SentimentNeutral`=%s, `Stories_altID`=%s, `Stories_Date`=%s;)
#                            """
#            advance_counter = 1
#            tot_len = len(RTRS["Items"])
#            print(tot_len)
#            previous_print = 0
#            for item in RTRS['Items']:
#                advance_counter += 1
#                crt_adv = math.ceil((advance_counter/tot_len)*100)
#                if crt_adv not in range(previous_print-1, previous_print+1):
#                    print("Current advancement is of: "+ str(crt_adv) + "%")
#                    previous_print = crt_adv
#                        
#                a = datetime.datetime.strptime(item['newsItem']['sourceTimestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
#                b = datetime.datetime.strptime(item['newsItem']['metadata']['firstCreated'], "%Y-%m-%dT%H:%M:%S.%fZ")
#                c = datetime.datetime.strptime(item['newsItem']['metadata']['feedTimestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
#                if item['newsItem']['metadata']['isArchive']==True:
#                    d = 1
#                else:
#                    d = 0
#                keys = (a.strftime('%Y-%m-%d %H:%M:%S'),
#                        item['newsItem']['dataType'],
#                        item['newsItem']['feedFamilyCode'],
#                        b.strftime('%Y-%m-%d %H:%M:%S'),
#                        c.strftime('%Y-%m-%d %H:%M:%S'),
#                        d,
#                        item['analytics']['analyticsScores'][0]['brokerAction'],
#                        item['analytics']['analyticsScores'][0]['firstMentionSentence'],
#                        item['analytics']['analyticsScores'][0]['linkedIds'],
#                        item['analytics']['analyticsScores'][0]['noveltyCounts'][0]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['noveltyCounts'][1]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['noveltyCounts'][2]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['noveltyCounts'][3]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['noveltyCounts'][4]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['priceTargetIndicator'],
#                        item['analytics']['analyticsScores'][0]['relevance'],
#                        item['analytics']['analyticsScores'][0]['sentimentClass'],
#                        item['analytics']['analyticsScores'][0]['sentimentNegative'],
#                        item['analytics']['analyticsScores'][0]['sentimentNeutral'],
#                        item['analytics']['analyticsScores'][0]['sentimentPositive'],
#                        item['analytics']['analyticsScores'][0]['sentimentWordCount'],
#                        item['analytics']['analyticsScores'][0]['volumeCounts'][0]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['volumeCounts'][1]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['volumeCounts'][2]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['volumeCounts'][3]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['volumeCounts'][4]['itemCount'],
#                        item['analytics']['newsItem']['bodySize'],
#                        item['analytics']['newsItem']['companyCount'],
#                        item['analytics']['newsItem']['exchangeAction'],
#                        item['analytics']['newsItem']['headlineTag'],
#                        item['analytics']['newsItem']['marketCommentary'],
#                        item['analytics']['newsItem']['sentenceCount'],
#                        item['analytics']['newsItem']['wordCount'],
#                        item['newsItem']['metadata']['altId'], 
#                        b.strftime('%Y-%m-%d %H:%M:%S'))
#                print("a")
#                keys = (item['id'][3:-11],) + keys
#                keys = keys + keys
#                cursor.execute(sql, (item['id'][3:-11], item['newsItem']['metadata']['altId'], b.strftime('%Y-%m-%d %H:%M:%S'), 0.35, item['newsItem']['metadata']['altId'],  b.strftime('%Y-%m-%d %H:%M:%S')) )#, keys)
#            
#        # Commit changes    
#                connection.commit()
#    
#    finally:
#        connection.close()
#    
#    
#        #%%For real
#        
#    connection = pymysql.connect(host='localhost',
#                                     user='root',
#                                     password='',
#                                     db='NewsDB',
#                                     charset='utf8mb4',
#                                     cursorclass=pymysql.cursors.DictCursor)
#        
#    try:
#        with connection.cursor() as cursor:
#            # Insert from archives to Takes
#            sql = """INSERT INTO Takes (`guID`, `SourceTimestamp`, `DataType`, `FeedFamilyCode`,
#                            `FirstCreated`, `FeedTimestamp`,`IsArchive`, `BrokerAction`,
#                            `FirstMentionSentence`, `LinkedIDs`, `12Hnovelty`, `24Hnovelty`,
#                            `3Dnovelty`, `5Dnovelty`, `7Dnovelty`, `PriceTargetIndicator`,
#                            `Relevance`, `SentimentClass`, `SentimentNegative`, `SentimentNeutral`,
#                            `SentimentPositive`, `SentimentWordCount`, `12Hvolume`, `24Hvolume`,
#                            `3Dvolume`, `5Dvolume`, `7Dvolume`, `BodySize`, `CompanyCount`,
#                            `ExchangeAction`, `HeadlineTag`, `MarketCommentary`, `SentenceCount`,
#                            `WordCount`,`Stories_altID`, `Stories_Date`)
#                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
#                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
#                                    %s, %s, %s, %s, %s, %s)
#                        ON DUPLICATE KEY UPDATE
#                            `SourceTimestamp` = %s,
#                            `DataType` = %s,
#                            `FeedFamilyCode` = %s,
#                            `FirstCreated` = %s,
#                            `FeedTimestamp` = %s,
#                            `IsArchive` = %s,
#                            `BrokerAction` = %s,
#                            `FirstMentionSentence` = %s,
#                            `LinkedIDs` = %s,
#                            `12Hnovelty` = %s,
#                            `24Hnovelty` = %s,
#                            `3Dnovelty` = %s,
#                            `5Dnovelty` = %s,
#                            `7Dnovelty` = %s,
#                            `PriceTargetIndicator` = %s,
#                            `Relevance` = %s,
#                            `SentimentClass` = %s,
#                            `SentimentNegative` = %s,
#                            `SentimentNeutral` = %s,
#                            `SentimentPositive` = %s,
#                            `SentimentWordCount` = %s,
#                            `12Hvolume` = %s,
#                            `24Hvolume` = %s,
#                            `3Dvolume` = %s,
#                            `5Dvolume` = %s,
#                            `7Dvolume` = %s,
#                            `BodySize` = %s,
#                            `CompanyCount` = %s,
#                            `ExchangeAction` = %s,
#                            `HeadlineTag` = %s,
#                            `MarketCommentary` = %s,
#                            `SentenceCount` = %s,
#                            `WordCount` = %s,
#                            `Stories_altID` = %s, 
#                            `Stories_Date` =%s
#                            """
#            
#            for item in RTRS['Items']:
#                a = datetime.datetime.strptime(item['newsItem']['sourceTimestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
#                b = datetime.datetime.strptime(item['newsItem']['metadata']['firstCreated'], "%Y-%m-%dT%H:%M:%S.%fZ")
#                c = datetime.datetime.strptime(item['newsItem']['metadata']['feedTimestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
#                if item['newsItem']['metadata']['isArchive']==True:
#                    d = 1
#                else:
#                    d = 0
#                keys = (a.strftime('%Y-%m-%d %H:%M:%S'),
#                        item['newsItem']['dataType'],
#                        item['newsItem']['feedFamilyCode'],
#                        b.strftime('%Y-%m-%d %H:%M:%S'),
#                        c.strftime('%Y-%m-%d %H:%M:%S'),
#                        d,
#                        item['analytics']['analyticsScores'][0]['brokerAction'],
#                        item['analytics']['analyticsScores'][0]['firstMentionSentence'],
#                        item['analytics']['analyticsScores'][0]['linkedIds'],
#                        item['analytics']['analyticsScores'][0]['noveltyCounts'][0]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['noveltyCounts'][1]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['noveltyCounts'][2]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['noveltyCounts'][3]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['noveltyCounts'][4]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['priceTargetIndicator'],
#                        item['analytics']['analyticsScores'][0]['relevance'],
#                        item['analytics']['analyticsScores'][0]['sentimentClass'],
#                        item['analytics']['analyticsScores'][0]['sentimentNegative'],
#                        item['analytics']['analyticsScores'][0]['sentimentNeutral'],
#                        item['analytics']['analyticsScores'][0]['sentimentPositive'],
#                        item['analytics']['analyticsScores'][0]['sentimentWordCount'],
#                        item['analytics']['analyticsScores'][0]['volumeCounts'][0]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['volumeCounts'][1]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['volumeCounts'][2]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['volumeCounts'][3]['itemCount'],
#                        item['analytics']['analyticsScores'][0]['volumeCounts'][4]['itemCount'],
#                        item['analytics']['newsItem']['bodySize'],
#                        item['analytics']['newsItem']['companyCount'],
#                        item['analytics']['newsItem']['exchangeAction'],
#                        item['analytics']['newsItem']['headlineTag'],
#                        item['analytics']['newsItem']['marketCommentary'],
#                        item['analytics']['newsItem']['sentenceCount'],
#                        item['analytics']['newsItem']['wordCount'],
#                        item['newsItem']['metadata']['altId'], 
#                        b.strftime('%Y-%m-%d %H:%M:%S'))
#                keys = keys + keys # Have all variables 2 times
#                keys = (item['id'][3:-11],) + keys
#                cursor.execute(sql, keys)
#            
#        # Commit changes    
#        connection.commit()
#    
#    finally:
#        connection.close()


#%% Populate Takes_has_Subjects

for y in range(2003,2004):
    end = time.time()
    print("This year has taken: " + str(end-start))
    start = time.time()
    with open("/mnt/42446FBE446FB379/Reuters/Analytics/JSON/Historical/TRNA.TR.News.CMPNY_AMER.EN."+str(y)+".40020076.JSON.txt/data") as data_file:
        RTRS = json.load(data_file)
        
        advance_counter = 1
        tot_len = len(RTRS["Items"])
        print("Number of rows: " + str(tot_len))
        previous_print = 0
        
        for item in RTRS['Items']:
            
            advance_counter += 1
            crt_adv = math.ceil((advance_counter/tot_len)*100)
            if crt_adv not in range(previous_print-1, previous_print+1):
                end = time.time()
                print("This month has taken: " + str(end-start))
                start = time.time()
                print("Current advancement is of: "+ str(crt_adv) + "%")
                previous_print = crt_adv
            
            for subject in set(item['newsItem']['subjects']):
                keys = (item['newsItem']['sourceId'], subject.replace("N2:", ""))
                
                connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='NewsDB_v2',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
                sql = """INSERT IGNORE INTO `Takes_has_Subjects` (`Takes_guID`, `Subjects_idSubjects`) VALUES (%s, %s)"""
                disable_foreign_keys = """SET FOREIGN_KEY_CHECKS = 0;"""

                try:
                    with connection.cursor() as cursor:
                        cursor.execute(disable_foreign_keys)
                        cursor.execute(sql, keys)
                        connection.commit()
                finally:
                    connection.close()