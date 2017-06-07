#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 18:23:30 2017

@author: nicolas

Create an SQLite database of all news stories (body text + analytics + other metadata)
"""

import sqlite3
import json
import os
import pandas as pd

with open("/mnt/42446FBE446FB379/Reuters/News/2004/News.RTRS.200401.0210.txt/data") as data_file:
    RTRS = json.load(data_file)

db_filename = '/home/nicolas/MEGAsync/Code/Project1/DB/Reuters_News_DB.sqlite'
db_is_new = not os.path.exists(db_filename)

#%% Create the DB schema
with sqlite3.connect(db_filename) as conn:
    if db_is_new:
        schema = '''
                    CREATE TABLE Stories (
                        altID           TEXT       PRIMARY KEY,
                        Date            TEXT
                    );
                    
                    CREATE TABLE Takes (
                        guID            TEXT        PRIMARY KEY,
                        StoryID         TEXT,
                        Body            TEXT,
                        Date            TEXT,
                        Headline        TEXT,
                        Language        TEXT,
                        Provider        TEXT,
                        PubStatus       TEXT,
                        TakeSequence    INTEGER,
                        Urgency         INTEGER,
                        FOREIGN KEY (StoryID) REFERENCES Stories (altID)               
                    );
        
                    CREATE TABLE Companies (
                        PermID          INTEGER     PRIMARY KEY,
                        CompanyName     TEXT,
                        Country         TEXT,
                        Sector          TEXT,
                        Status          TEXT,
                        RIC             TEXT,
                        Ticker          TEXT,
                        MarketMIC       TEXT
                    );
                    
                    CREATE TABLE Subjects (
                        sID             TEXT        PRIMARY KEY,
                        CodeName        TEXT
                    );
                    
                    CREATE TABLE Audiences (
                        auID            TEXT        PRIMARY KEY,
                        CodeName        TEXT
                    );
                    
                    CREATE TABLE Audience_Appearences (
                        AudienceID      TEXT,
                        TakeID          TEXT,
                        FOREIGN KEY (AudienceID) REFERENCES Audiences (auID),
                        FOREIGN KEY (TakeID) REFERENCES Takes (guiD)
                    );
                    
                    CREATE TABLE Subject_Appearences (
                        SubjectID         TEXT,
                        TakeID          TEXT,
                        FOREIGN KEY (SubjectID) REFERENCES Subjects (sID),
                        FOREIGN KEY (TakeID) REFERENCES Takes (guiD)
                    );
                    
                    CREATE TABLE Company_Mentions (
                        companyID       INTEGER,
                        TakeID          TEXT,
                        FOREIGN KEY (companyID) REFERENCES Companies (PermID),
                        FOREIGN KEY (TakeID) REFERENCES Takes (guiD)
                    );
                '''
                # + add analytics to Takes
        conn.executescript(schema)
    
    else:
        print('Database exists already')
        
        
#%% Populate the Stories Tables
with sqlite3.connect(db_filename) as conn:
    populate_Stories = '''
                           INSERT OR IGNORE INTO Stories VALUES(?,?) 
                       '''
                       
    for item in RTRS['Items']:
        keys = (item['data']['altId'], item['data']['firstCreated'])
        conn.execute(populate_Stories, keys)


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
        conn.execute(populate_Companies, keys)


#%% Populate the Takes Table
with sqlite3.connect(db_filename) as conn:
    populate_Takes = '''
                     INSERT INTO Takes VALUES(?,?,?,?,?,?,?,?,?,?) 
                     '''
                       
    for item in RTRS['Items']:
        keys = (item['guid'], item['data']['altId'], item['data']['body'], item['data']['versionCreated'],
                item['data']['headline'], item['data']['language'], item['data']['provider'],
                item['data']['pubStatus'], item['data']['takeSequence'], item['data']['urgency'])
        conn.execute(populate_Takes, keys)


#%% Populate Company_Mentions table


