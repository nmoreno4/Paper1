#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 17 14:48:01 2017

@author: nicolas
"""
import pandas as pd

#==============================================================================
# Load data and keep desired variables
#==============================================================================
All_news = pd.read_csv('/home/nicolas/Code/SpecVsGlob/Export/All_news_sample.csv')
TRNA_News = pd.read_csv('/home/nicolas/Code/SpecVsGlob/Export/TRNA_news_sample.csv')

FF_factors = pd.read_sas('/home/nicolas/MEGAsync/crsp_nico_ff.sas7bdat')

FF_factors = FF_factors[['COMNAM', 'TICKER', 'BM', 'DATE', 'retadj']]
FF_factors['COMNAM'] = FF_factors['COMNAM'].str.decode('utf-8')
FF_factors['TICKER'] = FF_factors['TICKER'].str.decode('utf-8')
FF_factors['BM'] = FF_factors['BM'].str.decode('utf-8')

remaining_FF = FF_factors.ix[FF_factors['BM']=='H']

#%% 
#==============================================================================
# Convert to dates and sort
#==============================================================================

import datetime
epoch = datetime.datetime(1960, 1, 1)
FF_factors['DATE'] = FF_factors['DATE'].apply(
                        lambda s: epoch + datetime.timedelta(days=int(s))
                        ) 
monthly_dates = FF_factors['DATE'].unique()
monthly_dates.sort()

#%% 
#==============================================================================
# Assign random dates to news
#==============================================================================

from random import randrange
from datetime import timedelta
from datetime import datetime


def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


#==============================================================================
# BE careful to make TRNA and archives dates match!!!
#==============================================================================
All_news['timestamp'] = All_news['timestamp'].apply(
                        lambda s: random_date(datetime.strptime('16Sep1955', '%d%b%Y'), 
                                              datetime.strptime('16Sep2015', '%d%b%Y'))
                        ) 

# Create first a set of random dates
TRNA_News['timestamp'] = TRNA_News['timestamp'].apply(
                        lambda s: random_date(datetime.strptime('16Sep1955', '%d%b%Y'), 
                                              datetime.strptime('16Sep2015', '%d%b%Y'))
                        ) 
                        
# Make the match with dates already existing in All_news
for s_index, s_row in All_news.iterrows():
    a_indexes = TRNA_News[TRNA_News['sourceID'] == s_row['ID']].index.tolist()
    for i in a_indexes:
        TRNA_News.set_value( i, 'timestamp', s_row['timestamp'])

#%% For each day get all the news articles as well as the associated analytics and prices
for date in monthly_dates:
    
    # Get price series for desired period with index classification
    current_period_FF = FF_factors.ix[FF_factors['DATE']<=date] #Rename it to value_matrix later on
    # All the remaining data after the period
    remaining_FF = remaining_FF.ix[remaining_FF['DATE']>date]
    
    print(date)
    print(len(FF_factors['DATE']))
    print(len(current_period_FF))
    
    # Tickers of companies concerned on this period
    tickers = set(current_period_FF['TICKER'])
    
    # Get all archives for desired period
    current_news = All_news.ix[All_news['timestamp']<=date]
    All_news = All_news.ix[All_news['timestamp']>date]
    
    # Get all analytics for desired period
    current_TRNA = TRNA_News.ix[TRNA_News['timestamp']<=date]
    TRNA_News = TRNA_News.ix[TRNA_News['timestamp']>date]
    
    
    ## Match price, analytics, archive and index classification
    
    
    
    
    
    
    
    
    
#%%







