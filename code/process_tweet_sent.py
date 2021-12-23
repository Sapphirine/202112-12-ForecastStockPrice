# -*- coding: utf-8 -*-
import pandas as pd
import os
from pytz import timezone
import datetime
import re

def findall_index(tweet):
    index_re = r"\$[A-Z]+"
    index_lst = re.findall(index_re, tweet)
    return [x[1:].upper() for x in index_lst]

def main():
    tweets_sent_fpath = '/home/carsonsow/pj/data/sents'

    # 获取今天的tweet_sent数据
    # nowdate = datetime.datetime.now(timezone('US/Eastern'))
    # now = nowdate.strftime("%Y%m%d")
    # fname = '{}.csv'.format(now)
    # now_fpath = os.path.join(tweets_sent_fpath, fname)

    # 获取所有未处理的tweet_sent数据
    dst_path = '/home/carsonsow/pj/data/processed_tweets'
    tobe_analyzed_fnames = list(set(os.listdir(tweets_sent_fpath)).difference(os.listdir(dst_path)))
    print('[TO BE analyzed] ({}): {}'.format(len(tobe_analyzed_fnames), tobe_analyzed_fnames))

    for fname in tobe_analyzed_fnames:
        fdate = fname[:-4]
        fpath = os.path.join(tweets_sent_fpath, fname)

        if not os.path.exists(fpath):
            return 0

        df = pd.read_csv(fpath, index_col=0, dtype=str).reset_index(drop=True)

        # 读取NASDAQ100的股票代码
        index_fpth = '/home/carsonsow/pj/data/NASDAQ100.txt'
        with open(index_fpth, 'r') as f:
            NASDAQ100 = [x.strip('\n') for x in f.readlines()]
            
        data_df = []
        data_colnames = ['stock', 'Date', 'Hour', 'compound']
        for _, row in df.iterrows():
            try:
                index_lst = findall_index(row['tweet'])
                row_data = row[['Date', 'Hour', 'compound']].values
                
                # No stock index
                if len(index_lst) == 0:
                    continue
                
                for stock_index in index_lst:
                    if stock_index in NASDAQ100:
                        new_row = list(row_data).copy()
                        new_row.insert(0, stock_index)
                        data_df.append(new_row)
            except:
                print('Current tweet on {} goes wrong!\n{}'.format(row['Date'], row['tweet']))
                
        data_df = pd.DataFrame(data_df, columns=data_colnames)

        # 保存
        dst_fpath = '{}/{}.csv'.format(dst_path, fdate)
        data_df.to_csv(dst_fpath)

if __name__ == '__main__':
    main()