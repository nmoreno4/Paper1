#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 15 10:27:17 2017

@author: nicolas
"""

# Queries

from pymongo import MongoClient
import datetime
client = MongoClient()
db = client.News20032

#%%
cursor = db.Stockdata.find({},
                            {
#                            "COMNAM":1
#                            , "cusip":1, "PERMNO":1, 
                            "ticker":1
                            })

#companies = []
#for entry in cursor:
#    companies.append(entry['COMNAM'])
#companies = set(companies)
#    
#PERMNO = []
#for entry in cursor:
#    PERMNO.append(entry['PERMNO'])
#PERMNO = set(PERMNO)
#    
#cusip = []
#for entry in cursor:
#    cusip.append(entry['cusip'])
#cusip = set(cusip)
#    
ticker = []
for entry in cursor:
    ticker.append(entry['ticker'])
ticker = set(ticker)


#%%
import sys
import requests
def getpermid():
    if len(sys.argv) < 2:
        print ('Please enter unique access key as 1st command line parameter')
        sys.exit(1)
    url = 'https://api.thomsonreuters.com/permid/search?q=ticker:IBM'
    access_token = sys.argv[1]
    headers = {'X-AG-Access-Token' : access_token}
    try:
        print ('connecting to %s' % url)
        response = requests.get(url, headers=headers)
    except Exception  as e:
        print ('Error in connect ' , e)
        return
    print ('Status code: %s' % response.status_code)
    if response.status_code == 200:
        print ('Results received: %s' % response.text)


for stock in ticker:
   getpermid(stock) 








#%% Plot how many takes per company
collected_news = []
valueNews = []
found_stories = []
for stock in ticker:
    cursor = db.AllNews.find({"takes.analytics.ticker":stock})
    tot_takes_stock = 0
    for story in cursor:
        for take in story.get("takes"):
            tot_takes_stock += 1
            
    valueNews.append(tot_takes_stock)

import numpy as np
from matplotlib import pyplot as plt
import pandas as pd
#Store data in dataframe for excel export
#labels = ['Nb takes per company']
#df_takes = pd.DataFrame.from_items(valueNews, columns =labels)


bins = np.arange(0, 100, 5) # fixed bin size
plt.xlim([min(valueNews), 100])
plt.hist(valueNews, bins=bins, alpha=1)
plt.title('# of takes for value companies: jan-jun 2003')
plt.xlabel('# of takes for a company (bin size = 5)')
plt.ylabel('count')
plt.show()


#%% Plot how many stories per company
collected_news = []
valueNews = []
for stock in ticker:
    cursor = db.AllNews.find({"takes.analytics.ticker":stock, "takes.body":{"$exists": 1}},
                             {"altId":1, "firstCreated":1})
    stock_i_news = 0
    found_stories = []
    for story in cursor:
        stock_i_news+=1
    valueNews.append(stock_i_news)

import numpy as np
from matplotlib import pyplot as plt
import pandas as pd
#Store data in dataframe for excel export
labels = ['Nb stories per company']
df_stories = pd.DataFrame.from_records(valueNews, columns =labels)

bins = np.arange(0, 100, 5) # fixed bin size
plt.xlim([min(valueNews), 100])
plt.hist(valueNews, bins=bins, alpha=1)
plt.title('# of stories for value companies: jan-jun 2003')
plt.xlabel('# of stories for a company (bin size = 5)')
plt.ylabel('count')
plt.show()

#%% Plot how many companies per story
collected_news = []
valueNews = []
stories_compcount = []
for stock in ticker:
    cursor = db.AllNews.find({"takes.analytics.ticker":stock, "takes.body":{"$exists": 1}},
                             {"altId":1, "firstCreated":1, "takes.analytics.ticker":1})
    stock_i_news = 0
    found_stocks = []
    found_stories = []
    crt = 1
    for take in cursor:
        if take.get("altId")+take.get("firstCreated") not in found_stories and crt == 1:
            stock_i_news+=1
            found_stories.append(take.get("altId")+take.get("firstCreated"))
            for company in take.get("takes")[0].get("analytics"):
                found_stocks.append(company.get("ticker"))
            crt = 0
        else:
            for company in take.get("takes")[0].get("analytics"):
                found_stocks.append(company.get("ticker"))
            crt = 1
    valueNews.append(stock_i_news)
    stories_compcount.append(found_stocks)


a = [set(i) for i in stories_compcount]
b = [len(i) for i in a]
b.sort()
b = b[525:1000]
import numpy as np
from matplotlib import pyplot as plt
fig = plt.figure()
ax = fig.add_subplot(111)
#n, bins, patches = ax.hist(b, bins=30, normed=1, cumulative=0)
#ax.set_xlabel('Bins', size=20)
#ax.set_ylabel('Frequency', size=20)
ax.hist(b, bins=np.array([1,2,5,10,15,20,25,30]), weights=np.zeros_like(b) + 1. / len(b))
ax.legend

#plt.show()


#%% Get info about location and sector of chosen companies


collected_news = []
statuses = []
countries = []
sectors = []
marketMICs = []
for stock in ticker:
    cursor = db.Companies.find({"ticker":stock},
                             {"sector":1, "status":1, "country":1, "marketMIC":1, "ticker":1, "RIC":1, "permID":1})
    for i in cursor:
        statuses.append(i.get("status"))
        sectors.append(i.get("sector"))
        countries.append(i.get("country"))
        marketMICs.append(i.get("marketMIC"))
        if stock == "RIV":
            print("aa")
            print(i)
            print(stock)
        
#%%
import pandas as pd
from collections import Counter
country_counts = Counter(countries)
df = pd.DataFrame.from_dict(country_counts, orient='index')
df = df.sort_values(by=0, ascending=False)
df.plot(kind='bar')


#sector_counts = Counter(sectors)
#df = pd.DataFrame.from_dict(sector_counts, orient='index')
#df = df.sort_values(by=0, ascending=False)
#df.plot(kind='bar')
#
#marketMICs_counts = Counter(marketMICs)
#df = pd.DataFrame.from_dict(marketMICs_counts, orient='index')
#df = df.sort_values(by=0, ascending=False)
#df.plot(kind='bar')



#%% Nb of stories per country

statuses = []
countries = []
sectors = []
marketMICs = []
for i in stories_compcount:
    for j in i:
        cursor = db.Companies.find({"ticker":j},
                                   {"sector":1, "status":1, "country":1, "marketMIC":1})
        for l in cursor:
            statuses.append(l.get("status"))
            sectors.append(l.get("sector"))
            countries.append(l.get("country"))
            marketMICs.append(l.get("marketMIC"))
            
#%%
import pandas as pd
from collections import Counter
country_counts = Counter(countries)
df = pd.DataFrame.from_dict(country_counts, orient='index')
df = df.sort_values(by=0, ascending=False)
df.plot(kind='bar')


sector_counts = Counter(sectors)
df = pd.DataFrame.from_dict(sector_counts, orient='index')
df = df.sort_values(by=0, ascending=False)
df.plot(kind='bar')

marketMIC_counts = Counter(marketMICs)
df = pd.DataFrame.from_dict(marketMIC_counts, orient='index')
df = df.sort_values(by=0, ascending=False)
df.plot(kind='bar')


#%% Stories by topic codes
collected_news = []
valueNews = []
stories_compcount = []
topics_compcount = []
tot_stories = 0
for stock in ticker:
    cursor = db.AllNews.find({"takes.analytics.ticker":stock, "takes.body":{"$exists": 1}},
                             {"altId":1, "firstCreated":1, "takes.analytics.ticker":1, "takes.subjects":1})
    stock_i_news = 0
    found_stocks = []
    found_stories = []
    found_topics = []
    crt = 1
    for story in cursor:
        tot_stories += 1
        if story.get("altId")+story.get("firstCreated") not in found_stories and crt == 1:
            stock_i_news+=1
            found_stories.append(story.get("altId")+story.get("firstCreated"))
            for topics in story.get("takes"):
                for topic in topics.get("subjects"):
                    found_topics.append(topic)
            for company in story.get("takes")[0].get("analytics"):
                found_stocks.append(company.get("ticker"))
            crt = 0
        else:
            for topics in story.get("takes"):
                for topic in topics.get("subjects"):
                    found_topics.append(topic)
            for company in story.get("takes")[0].get("analytics"):
                found_stocks.append(company.get("ticker"))
            crt = 1
    valueNews.append(stock_i_news)
    stories_compcount.append(found_stocks)
    topics_compcount.append(list(set(found_topics)))

topics = [item for sublist in topics_compcount for item in sublist]
import pandas as pd
from collections import Counter
topic_counts = Counter(topics)
#%
df = pd.DataFrame.from_dict(topic_counts, orient='index')
df = df.sort_values(by=0, ascending=False)
percenter = lambda x: (x/tot_stories)*100
df = df.apply(percenter)
#df.plot(kind='bar')




#%%Export results in latex
import numpy as np

from pylatex import Document, Section, Subsection, Tabular, Math, TikZ, Axis, Plot, Figure, Matrix, Alignat
from pylatex.utils import italic
import os

if __name__ == '__main__':
    geometry_options = {"tmargin": "2cm", "lmargin": "2cm"}
    doc = Document(geometry_options=geometry_options)

    with doc.create(Section('The data sample')):
        doc.append('''All the below proposed statistics are based on the data sample which was extracted as follows:\n
        First I select on a yearly basis all the stocks froming part of the Value (Growth) portfolios.
        ''')
        doc.append(italic('italic text. '))
        doc.append('\nAlso some crazy characters: $&#{}')
        with doc.create(Subsection('Math that is incorrect')):
            doc.append(Math(data=['2*3', '=', 9]))
            
        with doc.create(Subsection('Table of something')):
            with doc.create(Tabular('rc|cl')) as table:
                table.add_hline()
                table.add_row((1, 2, 3, 4))
                table.add_hline(1, 2)
                table.add_empty_row()
                table.add_row((4, 5, 6, 7))
                
                
    doc.generate_pdf(filepath="/home/nicolas/Documents/exports/test", clean_tex=False)
