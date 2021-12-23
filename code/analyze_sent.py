# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import string

def main():
    tweet_fpath = '/home/carsonsow/pj/data/tweets'
    sent_fpath = '/home/carsonsow/pj/data/sents'

    keys = ['neg', 'neu', 'pos', 'compound']
    analyzer = SentimentIntensityAnalyzer()
    colnames = ['Date', 'Hour', 'neg', 'neu', 'pos', 'compound', 'tweet']

    tobe_analyzed_fnames = list(set(os.listdir(tweet_fpath)).difference(os.listdir(sent_fpath)))
    print('[TO BE analyzed] ({}): {}'.format(len(tobe_analyzed_fnames), tobe_analyzed_fnames))
    for f in tobe_analyzed_fnames:
        df = pd.read_csv(os.path.join(tweet_fpath, f), index_col=0)
        df['tweet'] = df['tweet'].astype(str)
        for k in keys:
            df[k] = df['tweet'].apply(lambda x : analyzer.polarity_scores(x)[k])
        df = df[colnames]
        df.to_csv(os.path.join(sent_fpath, f))

if __name__ == '__main__':
    main()
