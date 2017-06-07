#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 21:50:58 2017

@author: nicolas
"""
import datetime
import sqlite3
import json
import os
import math
import pandas as pd
import array
RTRS = 0

db_filename = '/mnt/42446FBE446FB379/DB/NewsDB.sqlite3'
db_is_new = not os.path.exists(db_filename)

#%% Create the DB schema
with sqlite3.connect(db_filename) as conn:
    if db_is_new:
        schema = '''
                    CREATE TABLE Copy (
                        altID  TEXT  NOT NULL,
                        DateID   TEXT  NOT NULL,
                        PRIMARY KEY(`altID`, `DateID`)
                    );
                    
                    CREATE TABLE IF NOT EXISTS `Takes` (
                      `guID` TEXT NOT NULL,
                      `Body` TEXT NULL,
                      `SourceTimestamp` TEXT NULL,
                      `Headline` TEXT NULL,
                      `Langue` TEXT NULL,
                      `Provider` TEXT NULL,
                      `PubStatus` TEXT NULL,
                      `TakeSequence` INTEGER NULL,
                      `Urgency` INTEGER NULL,
                      `DataType` TEXT NULL,
                      `FeedFamilyCode` TEXT NULL,
                      `FirstCreated` TEXT NULL,
                      `FeedTimestamp` TEXT NULL,
                      `IsArchive` INTEGER NULL,
                      `BrokerAction` TEXT NULL,
                      `FirstMentionSentence` INT NULL,
                      `LinkedIDs` BLOB NULL,
                      `12Hnovelty` INTEGER NULL,
                      `24Hnovelty` INTEGER NULL,
                      `3Dnovelty` INTEGER NULL,
                      `5Dnovelty` INTEGER NULL,
                      `7Dnovelty` INTEGER NULL,
                      `PriceTargetIndicator` TEXT NULL,
                      `Relevance` REAL NULL,
                      `SentimentClass` REAL NULL,
                      `SentimentNegative` REAL NULL,
                      `SentimentNeutral` REAL NULL,
                      `SentimentPositive` REAL NULL,
                      `SentimentWordCount` INTEGER NULL,
                      `12Hvolume` INTEGER NULL,
                      `24Hvolume` INTEGER NULL,
                      `3Dvolume` INTEGER NULL,
                      `5Dvolume` INTEGER NULL,
                      `7Dvolume` INTEGER NULL,
                      `BodySize` INTEGER NULL,
                      `CompanyCount` INTEGER NULL,
                      `ExchangeAction` TEXT NULL,
                      `HeadlineTag` TEXT NULL,
                      `MarketCommentary` TEXT NULL,
                      `SentenceCount` INTEGER NULL,
                      `WordCount` INTEGER NULL,
                      `Stories_altID` TEXT NOT NULL,
                      `Stories_DateID` TEXT NOT NULL,
                      `VersionCreated` TEXT NULL,
                      PRIMARY KEY (`guID`),
                      FOREIGN KEY (`Stories_altID` , `Stories_DateID`)
                      REFERENCES `Stories` (`altID` , `DateID`)
                      ON DELETE NO ACTION
                      ON UPDATE NO ACTION);
                    	
                    	
                    CREATE TABLE IF NOT EXISTS `Companies` (
                      `PermID` INTEGER NOT NULL,
                      `CompanyName` TEXT NULL,
                      `Country` TEXT NULL,
                      `Sector` TEXT NULL,
                      `Status` TEXT NULL,
                      `RIC` TEXT NULL,
                      `Ticker` TEXT NULL,
                      `MarketMIC` TEXT NULL,
                      PRIMARY KEY (`PermID`));
                      
                     CREATE TABLE IF NOT EXISTS `Subjects` (
                      `idSubjects` TEXT NOT NULL,
                      `Description` TEXT NULL,
                      `Definition` TEXT NULL,
                      `TRCS_Concept_type` TEXT NULL,
                      PRIMARY KEY (`idSubjects`));
                      
                    CREATE TABLE IF NOT EXISTS `Audiences` (
                      `idAudiences` TEXT NOT NULL,
                      `CodeName` TEXT NULL,
                      PRIMARY KEY (`idAudiences`));
                      
                      
                    CREATE TABLE IF NOT EXISTS `Takes_has_Subjects` (
                      `Takes_guID` TEXT NOT NULL,
                      `Subjects_idSubjects` TEXT NOT NULL,
                      PRIMARY KEY (`Takes_guID`, `Subjects_idSubjects`)
                      );  
                    
                    CREATE TABLE IF NOT EXISTS `Takes_has_Audiences` (
                      `Takes_guID` TEXT NOT NULL,
                      `Audiences_idAudiences` TEXT NOT NULL,
                      PRIMARY KEY (`Audiences_idAudiences`, `Takes_guID`),
                        FOREIGN KEY (`Takes_guID`)
                        REFERENCES `Takes` (`guID`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION,
                        FOREIGN KEY (`Audiences_idAudiences`)
                        REFERENCES `Audiences` (`idAudiences`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION);
                    	
                    CREATE TABLE IF NOT EXISTS `Takes_has_Companies` (
                      `Takes_guID` TEXT NOT NULL,
                      `Companies_PermID` INTEGER NOT NULL,
                      PRIMARY KEY (`Takes_guID`, `Companies_PermID`),
                        FOREIGN KEY (`Takes_guID`)
                        REFERENCES `Takes` (`guID`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION,
                        FOREIGN KEY (`Companies_PermID`)
                        REFERENCES `Companies` (`PermID`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION);
                    	
                    CREATE TABLE IF NOT EXISTS `Dates` (
                      `DateID` TEXT NOT NULL,
                      PRIMARY KEY (`DateID`));
                      
                    CREATE TABLE IF NOT EXISTS `Companies_has_Dates` (
                      `Companies_PermID` INTEGER NOT NULL,
                      `Dates_DateID` TEXT NOT NULL,
                      `AdjReturn` REAL NULL,
                      `WeightPortfolio` REAL NULL,
                      `WeightPortfolio3M` REAL NULL,
                      `WeightPortfolio6M` REAL NULL,
                      `WeightPortfolio1M` REAL NULL,
                      `MarketCap` TEXT NULL,
                      `BM` TEXT NULL,
                      `ROE` TEXT NULL,
                      `INV` TEXT NULL,
                      `CUSIP` TEXT NULL,
                      PRIMARY KEY (`Companies_PermID`, `Dates_DateID`),
                        FOREIGN KEY (`Companies_PermID`)
                        REFERENCES `Companies` (`PermID`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION,
                        FOREIGN KEY (`Dates_DateID`)
                        REFERENCES `Dates` (`DateID`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION);  
                '''
                # + add analytics to Takes
        conn.executescript(schema)
        print("Database created")
    
    else:
        print('Database exists already')


#%% Populate Subjects Table
Codes = pd.read_excel("/home/nicolas/MEGAsync/Reuters example data/HEC News TRNA Sample Feb 14 2017/NewsCodes_20170207.xls")

with sqlite3.connect(db_filename) as conn:
    # Create a new record
    conn.execute("PRAGMA foreign_keys=1;")
    sql = "INSERT INTO `Subjects` (`idSubjects`, `Description`, `Definition`, `TRCS_Concept_type`) VALUES (?,?,?,?)"
    sql1 = "INSERT INTO `Subjects` (`idSubjects`, `Definition`, `TRCS_Concept_type`) VALUES (?,?,?)"
    sql2 = "INSERT INTO `Subjects` (`idSubjects`, `Description`, `TRCS_Concept_type`) VALUES (?,?,?)"
    for key, row in Codes.iterrows():
        if str(row[1])=='nan':
            keys = (row[0], row[2], row[3])
            conn.execute(sql1, keys)
        elif str(row[2])=='nan':
            keys = (row[0], row[1], row[3])
            conn.execute(sql2, keys)
        elif str(row[0])!='nan':
            keys = (row[0], row[1], row[2], row[3])
            conn.execute(sql, keys)
del Codes           

#%% Populate the Companies Table
companies = pd.read_csv('/mnt/42446FBE446FB379/Reuters/Companies/TRNA_Companies.csv', sep=",")

with sqlite3.connect(db_filename) as conn:
    populate_Companies = '''
                     INSERT INTO Companies VALUES(?,?,?,?,?,?,?,?) 
                     '''
                       
    for index, row in companies.iterrows():
        keys = (row['PermID'], row['companyName'], row['countryOfDomicile'], 
                row['TRBCEconomicSector'], row['status'], row['RIC'], 
                row['ticker'], row['marketMIC'])
        conn.execute("PRAGMA foreign_keys=1;")
        conn.execute(populate_Companies, keys)
del companies

#%% Load News Archives
RTRS = 0
for y in range(2006,2007):
    print("Populate Stories from Archives, current year: " + str(y))
    for m in ["%.2d" % i for i in range(12,13)]:
        print("Month: "+m)
        del RTRS
        with open("/mnt/42446FBE446FB379/Reuters/News/"+str(y)+"/News.RTRS."+str(y)+str(m)+".0210.txt/data") as data_file:
            RTRS = json.load(data_file)
            
            #% Populate the Stories Tables
            with sqlite3.connect(db_filename) as conn:
                sql = '''
                        INSERT OR REPLACE INTO `Stories` (`altID`, `DateID`) VALUES (?, ?)
                      '''
#=========================Advancement======================================
                advance_counter = 1
                tot_len = len(RTRS["Items"])
                print("Number of rows: " + str(tot_len))
                previous_print = 0                 
#==============================================================================
                for item in RTRS['Items']:
#============================Advancement=====================================
                    advance_counter += 1
                    crt_adv = math.floor((advance_counter/tot_len)*100)
                    if crt_adv not in range(previous_print-1, previous_print+9):
                        print("Current advancement is of: "+ str(crt_adv) + "%")
                        previous_print = crt_adv
#==============================================================================
                    a = datetime.datetime.strptime(item['data']['firstCreated'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    keys = (item['data']['altId'], a.strftime('%Y-%m-%d %H:%M:%S'))
                    conn.execute("PRAGMA foreign_keys=1;")
                    conn.execute(sql, keys)
                    
#%% Populate Stories with TRNA data
#% Load News Archives
for y in range(2004,2006):
    del RTRS
    print("Populate Stories from TRNA, current year: " + str(y))
    with open("/mnt/42446FBE446FB379/Reuters/Analytics/JSON/Historical/TRNA.TR.News.CMPNY_AMER.EN."+str(y)+".40020076.JSON.txt/data") as data_file:
        RTRS = json.load(data_file)
        
        #% Populate Stories Table Part II
        with sqlite3.connect(db_filename) as conn:
        
            # Create a new record
            sql = """INSERT OR IGNORE INTO `Stories` (`altID`, `DateID`) VALUES (?, ?)"""
            
#=========================Advancement======================================
            advance_counter = 1
            tot_len = len(RTRS["Items"])
            print("Number of rows :" + str(tot_len))
            previous_print = 0                 
#==============================================================================

            for item in RTRS['Items']:
                
#============================Advancement=====================================
                advance_counter += 1
                crt_adv = math.floor((advance_counter/tot_len)*100)
                if crt_adv not in range(previous_print-1, previous_print+9):
                    print("Current advancement is of: "+ str(crt_adv) + "%")
                    previous_print = crt_adv
#==============================================================================
                
                a = datetime.datetime.strptime(item['newsItem']['metadata']['firstCreated'], "%Y-%m-%dT%H:%M:%S.%fZ")
                keys = (item['newsItem']['metadata']['altId'], 
                        a.strftime('%Y-%m-%d %H:%M:%S'))
                conn.execute("PRAGMA foreign_keys=1;")
                conn.execute(sql, keys)               
                 
                
#%% Populate the Takes Table Part I
# I still need to populate 1997-2002 incl and after 2004
for y in range(1997,2003):
    print("Populate takes from archives, current year: " + str(y))
    for m in ["%.2d" % i for i in range(1,13)]:
        print("Month: "+m)
        del RTRS
        with open("/mnt/42446FBE446FB379/Reuters/News/"+str(y)+"/News.RTRS."+str(y)+str(m)+".0210.txt/data") as data_file:
            RTRS = json.load(data_file)
        
            with sqlite3.connect(db_filename) as conn:
                sql = '''
                        INSERT INTO Takes  (`guID`,`Stories_altID`,`Body`,`VersionCreated`, `Headline`,
                                            `Langue`,`Provider`,`PubStatus`,`TakeSequence`,`Urgency`, 
                                            `Stories_DateID`) 
                                            VALUES(?,?,?,?,?,?,?,?,?,?,?) 
                      '''
                                 
#=========================Advancement======================================
                advance_counter = 1
                tot_len = len(RTRS["Items"])
                print("Number of rows: " + str(tot_len))
                previous_print = 0                 
#==============================================================================
                                   
                for item in RTRS['Items']:
#============================Advancement=====================================
                    advance_counter += 1
                    crt_adv = math.floor((advance_counter/tot_len)*100)
                    if crt_adv not in range(previous_print-1, previous_print+9):
                        print("Current advancement is of: "+ str(crt_adv) + "%")
                        previous_print = crt_adv
#============================================================================== 
                    a = datetime.datetime.strptime(item['data']['versionCreated'], "%Y-%m-%dT%H:%M:%S.%fZ") 
                    b = datetime.datetime.strptime(item['data']['firstCreated'], "%Y-%m-%dT%H:%M:%S.%fZ") 
                    keys = (item['guid'], item['data']['altId'], item['data']['body'], a.strftime('%Y-%m-%d %H:%M:%S'),
                            item['data']['headline'], item['data']['language'], item['data']['provider'],
                            item['data']['pubStatus'], item['data']['takeSequence'], item['data']['urgency'],
                            b.strftime('%Y-%m-%d %H:%M:%S'))
                    conn.execute("PRAGMA foreign_keys=1;")
                    conn.execute(sql, keys)      
                    
                    
                    
                    
                    
#==============================================================================
   
  #%% Populate Takes with TRNA data
  #% Load News Archives
for y in range(2003,2005):
  del RTRS
  print("current year: " + str(y))
  with open("/mnt/42446FBE446FB379/Reuters/Analytics/JSON/Historical/TRNA.TR.News.CMPNY_AMER.EN."+str(y)+".40020076.JSON.txt/data") as data_file:
      RTRS = json.load(data_file)
      
               
      # Add record to Takes
      sql1 = """INSERT OR REPLACE INTO `Takes` (`guID`,`IsArchive`, `SourceTimestamp`,`Headline`,
              `Langue`,`Provider`,`TakeSequence`,`Urgency`,`DataType`,`FeedFamilyCode`,
              `FirstCreated`,`FeedTimestamp`,`FirstMentionSentence`,
              `12Hnovelty`,`24Hnovelty`,`3Dnovelty`,`5Dnovelty`,`7Dnovelty`,`PriceTargetIndicator`,
              `Relevance`,`SentimentClass`,`SentimentNegative`,`SentimentNeutral`,`SentimentPositive`,
              `SentimentWordCount`,`12Hvolume`,`24Hvolume`,`3Dvolume`,`5Dvolume`,`7Dvolume`,`BodySize`,
              `CompanyCount`,`ExchangeAction`,`HeadlineTag`,`MarketCommentary`,`SentenceCount`,`WordCount`,
              `Stories_altID`,`Stories_DateID`) 
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                  ?, ?, ?, ?, ?, COALESCE((SELECT `Stories_altID` FROM `Takes` WHERE `guID` = ?), ?), 
                  COALESCE((SELECT `Stories_DateID` FROM `Takes` WHERE `guID` = ?), ?))"""
  #            sql1 = """INSERT OR REPLACE INTO `Takes` (`guID`,`Body`,`PubStatus`,`SourceTimestamp`,`Headline`,
  #            `Langue`,`Provider`,`TakeSequence`,`Urgency`,`DataType`,`FeedFamilyCode`,
  #            `FirstCreated`,`FeedTimestamp`,`IsArchive`,`BrokerAction`,`FirstMentionSentence`,`LinkedIDs`,
  #            `12Hnovelty`,`24Hnovelty`,`3Dnovelty`,`5Dnovelty`,`7Dnovelty`,`PriceTargetIndicator`,
  #            `Relevance`,`SentimentClass`,`SentimentNegative`,`SentimentNeutral`,`SentimentPositive`,
  #            `SentimentWordCount`,`12Hvolume`,`24Hvolume`,`3Dvolume`,`5Dvolume`,`7Dvolume`,`BodySize`,
  #            `CompanyCount`,`ExchangeAction`,`HeadlineTag`,`MarketCommentary`,`SentenceCount,`WordCount`,
  #            `Stories_altID`,`Stories_DateID`) 
  #                VALUES (?, (SELECT `Body` FROM `Takes` WHERE `guID` = ?), 
  #                        (SELECT `PubStatus` FROM `Takes` WHERE `guID` = ?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
  #                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
  #                        ?, ?, ?, ?, ?, ?, ?, ?, (SELECT `Stories_altID` FROM `Takes` WHERE `guID` = ?),  
  #                        (SELECT `Stories_DateID` FROM `Takes` WHERE `guID` = ?))"""
          
      # Add record to `Takes_has_Subjects`
      sql2 = """INSERT OR IGNORE INTO `Takes_has_Subjects` (`Takes_guID`, `Subjects_idSubjects`)
                  VALUES(?,?)""" # I need to select the PermID from table companies that matches with the ticker.
      
  #=========================Advancement======================================
      advance_counter = 1
      tot_len = len(RTRS["Items"])
      print("Number of rows: " + str(tot_len))
      previous_print = 0  
      print("bordel")
      listcounter = ['kel', 'kechose']
  #==============================================================================
  
      for item in RTRS['Items']:
          
  #============================Advancement=====================================
          advance_counter += 1
          crt_adv = math.floor((advance_counter/tot_len)*10000)
          if crt_adv not in range(previous_print-1, previous_print+1):
              print("Current advancement is of: "+ str(crt_adv) + "%")
              previous_print = crt_adv
  #==============================================================================
          
          a = datetime.datetime.strptime(item['newsItem']['sourceTimestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
          b = datetime.datetime.strptime(item['newsItem']['metadata']['firstCreated'], "%Y-%m-%dT%H:%M:%S.%fZ")
          c = datetime.datetime.strptime(item['newsItem']['metadata']['feedTimestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
          if item['newsItem']['metadata']['isArchive']==True:
              d = 1
          else:
              d = 0
          keys = (item['id'][3:-11],
                  d,
                  a.strftime('%Y-%m-%d %H:%M:%S'),
                  item['newsItem']['headline'],
                  item['newsItem']['language'],
                  item['newsItem']['provider'],
                  item['newsItem']['metadata']['takeSequence'],
                  item['newsItem']['urgency'],
                  item['newsItem']['dataType'],
                  item['newsItem']['feedFamilyCode'],
                  b.strftime('%Y-%m-%d %H:%M:%S'),
                  c.strftime('%Y-%m-%d %H:%M:%S'),
                  item['analytics']['analyticsScores'][0]['firstMentionSentence'],
                  item['analytics']['analyticsScores'][0]['noveltyCounts'][0]['itemCount'],
                  item['analytics']['analyticsScores'][0]['noveltyCounts'][1]['itemCount'],
                  item['analytics']['analyticsScores'][0]['noveltyCounts'][2]['itemCount'],
                  item['analytics']['analyticsScores'][0]['noveltyCounts'][3]['itemCount'],
                  item['analytics']['analyticsScores'][0]['noveltyCounts'][4]['itemCount'],
                  item['analytics']['analyticsScores'][0]['priceTargetIndicator'],
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
                  item['analytics']['newsItem']['bodySize'],
                  item['analytics']['newsItem']['companyCount'],
                  item['analytics']['newsItem']['exchangeAction'],
                  item['analytics']['newsItem']['headlineTag'],
                  item['analytics']['newsItem']['marketCommentary'],
                  item['analytics']['newsItem']['sentenceCount'],
                  item['analytics']['newsItem']['wordCount'],
                  item['id'][3:-11], item['newsItem']['metadata']['altId'], 
                  item['id'][3:-11], b.strftime('%Y-%m-%d %H:%M:%S'))
          
              
             
          
#          for subject in item['newsItem']['subjects']:
#              with sqlite3.connect(db_filename) as conn:
#                  conn.execute("PRAGMA foreign_keys=1;")
#                  conn.execute(sql2, (item['id'][3:-11], subject.replace("N2:","")))
                 
          if item['id'][3:-11] in listcounter:
              print("ID already used!!")
          listcounter.append(item['id'][3:])
          #% Populate Stories Table Part II
          with sqlite3.connect(db_filename) as conn:
              conn.execute("PRAGMA foreign_keys=1;")
              conn.execute(sql1, keys)
#==============================================================================
                
            
                
                                
                

                   