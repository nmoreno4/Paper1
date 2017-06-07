import pandas as pd
import numpy as np
from itertools import chain

#%% Load all data

# Load Business wire
BSW_raw = open('Data/BSW').read()
import json
BSW_json = json.loads(BSW_raw)

# Load Market wire
MKW_raw = open('Data/MKW').read()
import json
MKW_json = json.loads(MKW_raw)

# Load 
PRN_raw = open('Data/PRN').read()
import json
PRN_json = json.loads(PRN_raw)

# Load TRNA
TRNA_raw = open('Data/TRNA').read()
import json
TRNA_json = json.loads(TRNA_raw)

# Load RTRS_CMP
RTRS_CMP_raw = open('Data/RTRS_CMP').read()
import json
RTRS_CMP_json = json.loads(RTRS_CMP_raw)



#%% Link each news to TRNA

a = BSW_json['Items'][1]['data']
b = TRNA_json['Items'][1]['data']

    
#%% flatten BSW JSON
columns = ['body','headline', 'altID', 'subjects', 'ID', 
           'urgency', 'audience', 'provider', 'timestamp']
BSW_News = pd.DataFrame(index=(range(0,(len(BSW_json['Items'])))), columns=columns)

i = 0
for item in BSW_json['Items']:
    df =pd.DataFrame.from_dict(item['data'],orient='index')
    BSW_News.set_value(i,'body', df[0][0])
    BSW_News.set_value(i,'headline',df[0][5])
    BSW_News.set_value(i,'altID', df[0][4])
    BSW_News.set_value(i,'subjects', df[0][8])
    BSW_News.set_value(i,'ID', df[0][13])
    BSW_News.set_value(i,'urgency', df[0][14])
    BSW_News.set_value(i,'audience', df[0][9])
    BSW_News.set_value(i,'provider', df[0][11])
    BSW_News.set_value(i,'timestamp', df[0][2])
    i+=1

#%% flatten TRNA JSON
columns = ['ID1', 'headline', 'altID', 'provider', 'timestamp', 'subjects', 'urgency', 'feedFamilyCode',
           'sourceID', 'audiences', 'isArchive', 'takeSequence', 'datatype', 'bodySize', 
           'companyCount', 'exchangeAction', 'marketCommentary', 'sentenceCount', 'wordCount',
           'assetClass', 'assetCodes', 'assetID', 'assetName', 'firstMentionSentence', 'brokerAction',
           'linkedIDs', 'priceTarget', 'relevance', 'sentimentClass', 'sentimentNegative', 'sentimentNeutral',
           'sentimentPositive', 'sentimentWordCount', '12Hnovelty', '24Hnovelty', '3Dnovelty', 
           '5Dnovelty', '7Dnovelty', '12Hvolume', '24Hvolume', '3Dvolume', '5Dvolume', '7Dvolume']

TRNA_News = pd.DataFrame(index=(range(0,(len(TRNA_json['Items'])))), columns=columns)

i = 0
for item in TRNA_json['Items']:
    TRNA_News.set_value(i,'ID1', item['data']['id'])
    TRNA_News.set_value(i,'headline',item['data']['newsItem']['headline'])
    TRNA_News.set_value(i,'altID', item['data']['newsItem']['metadata']['altId'])
    TRNA_News.set_value(i,'provider', item['data']['newsItem']['provider'])
    TRNA_News.set_value(i,'feedFamilyCode', item['data']['newsItem']['feedFamilyCode'])
    TRNA_News.set_value(i,'timestamp', item['data']['newsItem']['sourceTimestamp'])
    TRNA_News.set_value(i,'subjects', item['data']['newsItem']['subjects'])
    TRNA_News.set_value(i,'urgency', item['data']['newsItem']['urgency'])
    TRNA_News.set_value(i,'sourceID', item['data']['newsItem']['sourceId'])
    TRNA_News.set_value(i,'audiences', item['data']['newsItem']['metadata']['audiences'])
    TRNA_News.set_value(i,'isArchive', item['data']['newsItem']['metadata']['isArchive'])
    TRNA_News.set_value(i,'takeSequence', item['data']['newsItem']['metadata']['takeSequence'])
#    TRNA_News.set_value(i,'datatype', df[0][2])
    TRNA_News.set_value(i,'bodySize', item['data']['analytics']['newsItem']['bodySize'])
    TRNA_News.set_value(i,'companyCount', item['data']['analytics']['newsItem']['companyCount'])
    TRNA_News.set_value(i,'exchangeAction', item['data']['analytics']['newsItem']['exchangeAction'])
    TRNA_News.set_value(i,'marketCommentary', item['data']['analytics']['newsItem']['marketCommentary'])
    TRNA_News.set_value(i,'sentenceCount', item['data']['analytics']['newsItem']['sentenceCount'])
    TRNA_News.set_value(i,'wordCount', item['data']['analytics']['newsItem']['wordCount'])
    TRNA_News.set_value(i,'assetClass', item['data']['analytics']['analyticsScores'][0]['assetClass'])
    TRNA_News.set_value(i,'assetCodes', item['data']['analytics']['analyticsScores'][0]['assetCodes'])
    TRNA_News.set_value(i,'assetID', item['data']['analytics']['analyticsScores'][0]['assetId'])
    TRNA_News.set_value(i,'assetName', item['data']['analytics']['analyticsScores'][0]['assetName'])
    TRNA_News.set_value(i,'firstMentionSentence', item['data']['analytics']['analyticsScores'][0]['firstMentionSentence'])
    TRNA_News.set_value(i,'brokerAction', item['data']['analytics']['analyticsScores'][0]['brokerAction'])
    TRNA_News.set_value(i,'linkedIDs', item['data']['analytics']['analyticsScores'][0]['linkedIds'])
    TRNA_News.set_value(i,'priceTarget', item['data']['analytics']['analyticsScores'][0]['priceTargetIndicator'])
    TRNA_News.set_value(i,'relevance', item['data']['analytics']['analyticsScores'][0]['relevance'])
    TRNA_News.set_value(i,'sentimentClass', item['data']['analytics']['analyticsScores'][0]['sentimentClass'])
    TRNA_News.set_value(i,'sentimentNegative', item['data']['analytics']['analyticsScores'][0]['sentimentNegative'])
    TRNA_News.set_value(i,'sentimentNeutral', item['data']['analytics']['analyticsScores'][0]['sentimentNeutral'])
    TRNA_News.set_value(i,'sentimentPositive', item['data']['analytics']['analyticsScores'][0]['sentimentPositive'])
    TRNA_News.set_value(i,'sentimentWordCount', item['data']['analytics']['analyticsScores'][0]['sentimentWordCount'])
    TRNA_News.set_value(i,'12Hnovelty', item['data']['analytics']['analyticsScores'][0]['noveltyCounts'][0]['itemCount'])
    TRNA_News.set_value(i,'24Hnovelty', item['data']['analytics']['analyticsScores'][0]['noveltyCounts'][1]['itemCount'])
    TRNA_News.set_value(i,'3Dnovelty', item['data']['analytics']['analyticsScores'][0]['noveltyCounts'][2]['itemCount'])
    TRNA_News.set_value(i,'5Dnovelty', item['data']['analytics']['analyticsScores'][0]['noveltyCounts'][3]['itemCount'])
    TRNA_News.set_value(i,'7Dnovelty', item['data']['analytics']['analyticsScores'][0]['noveltyCounts'][4]['itemCount'])
    TRNA_News.set_value(i,'12Hvolume', item['data']['analytics']['analyticsScores'][0]['volumeCounts'][0]['itemCount'])
    TRNA_News.set_value(i,'24Hvolume', item['data']['analytics']['analyticsScores'][0]['volumeCounts'][1]['itemCount'])
    TRNA_News.set_value(i,'3Dvolume', item['data']['analytics']['analyticsScores'][0]['volumeCounts'][2]['itemCount'])
    TRNA_News.set_value(i,'5Dvolume', item['data']['analytics']['analyticsScores'][0]['volumeCounts'][3]['itemCount'])
    TRNA_News.set_value(i,'7Dvolume', item['data']['analytics']['analyticsScores'][0]['volumeCounts'][4]['itemCount'])
    i+=1
    
#%% flatten MKW JSON
columns = ['body','headline', 'altID', 'subjects', 'ID', 
           'urgency', 'audience', 'provider', 'timestamp']
MKW_News = pd.DataFrame(index=(range(0,(len(MKW_json['Items'])))), columns=columns)

i = 0
for item in MKW_json['Items']:
    df =pd.DataFrame.from_dict(item['data'],orient='index')
    MKW_News.set_value(i,'body', df[0][0])
    MKW_News.set_value(i,'headline',df[0][5])
    MKW_News.set_value(i,'altID', df[0][4])
    MKW_News.set_value(i,'subjects', df[0][8])
    MKW_News.set_value(i,'ID', df[0][13])
    MKW_News.set_value(i,'urgency', df[0][14])
    MKW_News.set_value(i,'audience', df[0][9])
    MKW_News.set_value(i,'provider', df[0][11])
    MKW_News.set_value(i,'timestamp', df[0][2])
    i+=1
    
    
#%% flatten PRN JSON
columns = ['body','headline', 'altID', 'subjects', 'ID', 
           'urgency', 'audience', 'provider', 'timestamp']
PRN_News = pd.DataFrame(index=(range(0,(len(PRN_json['Items'])))), columns=columns)

i = 0
for item in PRN_json['Items']:
    df =pd.DataFrame.from_dict(item['data'],orient='index')
    PRN_News.set_value(i,'body', df[0][0])
    PRN_News.set_value(i,'headline',df[0][5])
    PRN_News.set_value(i,'altID', df[0][4])
    PRN_News.set_value(i,'subjects', df[0][8])
    PRN_News.set_value(i,'ID', df[0][13])
    PRN_News.set_value(i,'urgency', df[0][14])
    PRN_News.set_value(i,'audience', df[0][9])
    PRN_News.set_value(i,'provider', df[0][11])
    PRN_News.set_value(i,'timestamp', df[0][2])
    i+=1
    
    
#%% flatten RTRS_CMP JSON
columns = ['body','headline', 'altID', 'subjects', 'ID', 
           'urgency', 'audience', 'provider', 'timestamp']
RTRS_CMP_News = pd.DataFrame(index=(range(0,(len(RTRS_CMP_json['Items'])))), columns=columns)

i = 0
for item in RTRS_CMP_json['Items']:
    df =pd.DataFrame.from_dict(item['data'],orient='index')
    RTRS_CMP_News.set_value(i,'body', df[0][0])
    RTRS_CMP_News.set_value(i,'headline',df[0][5])
    RTRS_CMP_News.set_value(i,'altID', df[0][4])
    RTRS_CMP_News.set_value(i,'subjects', df[0][8])
    RTRS_CMP_News.set_value(i,'ID', df[0][13])
    RTRS_CMP_News.set_value(i,'urgency', df[0][14])
    RTRS_CMP_News.set_value(i,'audience', df[0][9])
    RTRS_CMP_News.set_value(i,'provider', df[0][11])
    RTRS_CMP_News.set_value(i,'timestamp', df[0][2])
    i+=1
    
#%% Join all news and link in TRNA
All_news = pd.concat([MKW_News, RTRS_CMP_News, BSW_News, PRN_News])
TRNA_News['altID index news'] = None
TRNA_News['ID1 index news'] = None
TRNA_News['linked news index'] = None
row = 0
copy_altID = All_news['altID'].tolist()
for n in TRNA_News['altID']:
    a = [i for i, j in enumerate(copy_altID) if j == n]
    TRNA_News.set_value(row, 'altID index news', a)
    row += 1
    
#%% Join all news and link in TRNA
 
row = 0    
copy_ID = All_news['ID'].tolist()
for n in TRNA_News['sourceID']:
    a = [i for i, j in enumerate(copy_ID) if j == n]
    TRNA_News.set_value(row, 'ID1 index news', a)
    row += 1
    
    
#%% Get linked news index

row = 0    
copy_ID = All_news['ID'].tolist()
for n in TRNA_News['linkedIDs']:
    a = []
    for linked in n:
        a.append([i for i, j in enumerate(copy_ID) if j == linked['linkedId'][3:]])
    TRNA_News.set_value(row, 'linked news index', list(chain.from_iterable(a)))
    row += 1


#%% Save Data
All_news.to_csv('/home/nicolas/Code/SpecVsGlob/Export/All_news_sample.csv', encoding='utf-8')
TRNA_News.to_csv('/home/nicolas/Code/SpecVsGlob/Export/TRNA_news_sample.csv', encoding='utf-8')