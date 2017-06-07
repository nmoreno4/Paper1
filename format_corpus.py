#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 09:38:19 2017

@author: nicolas
"""

import pandas as pd
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
from langdetect import detect
from nltk.corpus import stopwords
cachedStopWords = stopwords.words("english")
cachedStopWords.extend(['.', ',', ';',':','#','_','-','?','!','%', '@','&','/', ')','(', '[', ']', '``', '`', '\'', '\'\'','"', '""', '--'])
from collections import defaultdict
frequency = defaultdict(int)
from pprint import pprint
from nltk.tokenize import sent_tokenize
import nltk
from gensim import corpora

#%% Load data
All_news = pd.read_csv('/home/nicolas/Code/SpecVsGlob/Export/All_news_sample.csv')
corpus = All_news['body'].tolist()


#%% Remove nan or no characters
corpus = [x for x in corpus if str(x) != 'nan']
corpus = [x for x in corpus if len(x) > 5]

#%% Keep only english language
corpus = [x for x in corpus if detect(str(x)) == 'en']
#for x in corpus:
#    if detect(str(x)) != 'en':
#        print(detect(x))

#%% Tokenize, stem and remove stop words

#==============================================================================
# Tokenize words
#==============================================================================
tokenized = [nltk.word_tokenize(document) for document in corpus]

#==============================================================================
# Stem / normalize words
#==============================================================================
stemmer = nltk.stem.SnowballStemmer('english')
stemmed = [[stemmer.stem(token) for token in document]
            for document in tokenized]


#==============================================================================
# Remove stop words
#==============================================================================
no_stopwords = [[word for word in document if word not in cachedStopWords]
                for document in stemmed]


#==============================================================================
# Remove figures
#==============================================================================
def isnot_number(s):
    try:
        float(s)
        return False
    except ValueError:
        return True
    
no_numbers = [[word for word in document if isnot_number(str.replace(word, ",", ""))]
                for document in no_stopwords]


#==============================================================================
# Remove tokens occuring too few times
#==============================================================================
for text in no_numbers:
    for token in text:
        frequency[token] += 1
             
rare_filtered = [[token for token in text if frequency[token] > 10] 
                    for text in no_numbers]


#%% Create Dictionnary and Corpus
class MyCorpus(object):
    def __init__(self, txt_collection):
        self.corpus = txt_collection
    def __iter__(self):
        for text in self.corpus:
            yield text
            
#==============================================================================
# Create dictionnary
#==============================================================================
dictionnary_memory_friendly = MyCorpus(rare_filtered)
dictionary = corpora.Dictionary(dictionnary_memory_friendly)
dictionary.save('/tmp/toytest.dict')
print(dictionary.token2id)
print(dictionary)


#%%
class MyCorpus(object):
    def __init__(self, txt_collection):
        self.corpus = txt_collection
    def __iter__(self):
        for text in self.corpus:
            yield dictionary.doc2bow(text)
            
#==============================================================================
# Create corpus
#==============================================================================
corpus_memory_friendly = MyCorpus(rare_filtered)  # doesn't load the corpus into memory!
corpora.MmCorpus.serialize('/tmp/toytest.mm', corpus_memory_friendly)  # store to disk, for later use
