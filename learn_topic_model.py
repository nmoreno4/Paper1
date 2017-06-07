#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 15:39:26 2017

@author: nicolas
"""

#Model can be updated with lda.update(other_corpus), don't forget to tf-idf other coprus for consistency
#In future version propose script to automatically take the initial model and update with all desired corpuses

from gensim import corpora, models
from datetime import datetime

#%% Hyperparameters

model = 'LDA' # Set model to 'LDA' or 'HDP'nb_topics = 50

nb_topics = 10
dict_path = '/tmp/toytest.dict'
corpus_path = '/tmp/toytest.mm'
output_path = '/tmp/toytest' #path + name bare extension

#%% Load data
dictionary = corpora.Dictionary.load(dict_path)
corpus = corpora.MmCorpus(corpus_path)

#%% Train model

#==============================================================================
# Tf-idf
#==============================================================================
tfidf = models.TfidfModel(corpus) 
corpus_tfidf = tfidf[corpus]

#==============================================================================
# LDA
#==============================================================================
if model == 'LDA':
    lda = models.LdaModel(corpus, id2word=dictionary, num_topics=nb_topics)
    corpus_lda = lda[corpus_tfidf]
    lda.save(output_path+'.lda')
elif model == 'HDP':
    hdp = models.HdpModel(corpus, id2word=dictionary)
    corpus_hdp = hdp[corpus_tfidf]
    lda.save(output_path+'.hdp')


