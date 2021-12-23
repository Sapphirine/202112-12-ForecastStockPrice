# -*- coding: utf-8 -*-

import tweepy
print('tweepy', tweepy.__version__)

from tweepy import Stream
from datetime import datetime
from pytz import timezone
import json
import pandas as pd
import os
import time

ACCESS_TOKEN = '898953167454457857-hS9q5w45XwnSR4TLVWgy5WObFy5bHc5'     # your access token
ACCESS_SECRET = 'cOITj90vdFOsDutvt49Ct8D3yYAN1TUg11Mo2ka7DeqCB'    # your access token secret
CONSUMER_KEY = 'JDHHKwJmQbCdzeT9Tz9WgHYoh'     # your API key
CONSUMER_SECRET = '1EnKriNtN02xjXLVp8fzA5wZdexBAS7QnK139f8ZyD2yjQNyEM'  # your API secret key

class tweetStreamer(Stream):
    def __init__(self, max_entries, **args):
        super(tweetStreamer,self).__init__(**args)
        self.start = datetime.now(timezone('US/Eastern'))
        self.cnt = 0
        self.max_entries = max_entries
        self.res = []
        
    def on_data(self, data):        
        self.cnt += 1
        running_time = datetime.now(timezone('US/Eastern'))-self.start
        
        if self.cnt > self.max_entries or running_time.seconds/60>10:
            self.disconnect()
        else:
            msg = json.loads( data )
            self.res.append(msg['text'])
    
    def on_limit(self, status):
        print ("Rate Limit Exceeded, Sleep for 3 Mins")
        time.sleep(3 * 60)
        return True

def main():
  # 读取NASDAQ100的股票代码
  with open('/home/carsonsow/pj/data/NASDAQ100.txt', 'r') as f:
      NASDAQ100 = [x.strip('\n') for x in f.readlines()]

  tags = ['${}'.format(x) for x in NASDAQ100]

  # 获取实时tweets
  mystream = tweetStreamer(max_entries=300, \
                          consumer_key = CONSUMER_KEY, \
                          consumer_secret = CONSUMER_SECRET,\
                          access_token = ACCESS_TOKEN, \
                          access_token_secret = ACCESS_SECRET)
  mystream.filter(track=tags, languages=['en'])
  mystream.sample()

  # 保存结果
  res_tweets = mystream.res
  now = datetime.now(timezone('US/Eastern')).strftime("%Y%m%d%H")
  df = pd.DataFrame.from_dict({'tweet': res_tweets})
  df['Date'] = now[:8]
  df['Hour'] = now[8:]

  dst_path = '/home/carsonsow/pj/data/tweets'
  today_date = now[:8]
  save_fpath = '{}/{}.csv'.format(dst_path, today_date)
  if os.path.exists(save_fpath):
    prev_df = pd.read_csv(save_fpath, index_col=0)
    prev_df['Hour'] = prev_df['Hour'].astype(str)
    prev_df['Date'] = prev_df['Date'].astype(str)
    df = pd.concat([prev_df, df]).reset_index(drop=True)
    df.sort_values(by='Hour', ascending=True)

  print('[{}.csv]: {}'.format(today_date, df.shape))
  df.to_csv(save_fpath)

if __name__ == '__main__':
  main()