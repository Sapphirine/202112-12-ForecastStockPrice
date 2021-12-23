# -*- coding: utf-8 -*-

import os
import pandas as pd
import yfinance as yf
from pytz import timezone
import datetime

def get_stock_data(index):
    data = yf.Ticker(index.upper())
    today_data = data.history(period='16d').reset_index()
    today_data['index'] = index.upper()
    today_data['Date'] = today_data['Date'].apply(lambda x: x.strftime('%Y%m%d'))
    today_data = today_data.sort_values(by='Date')

    nrow = today_data.shape[0]

    today_data['prev_label'] = None        
    today_data.loc[1:, 'prev_label'] = today_data.loc[1:, 'Close'].values>=today_data.loc[0:nrow-2, 'Close'].values

    today_data['prev_rate'] = None
    today_data.loc[1:, 'prev_rate'] = (today_data.loc[1:, 'Close'].values-today_data.loc[0:nrow-2, 'Close'].values)/today_data.loc[0:nrow-2, 'Close'].values

    today_data = today_data.loc[1:, :].reset_index(drop=True)
    return today_data

now = datetime.datetime.now(timezone('US/Eastern')).strftime("%Y%m%d")

# 保存股票数据的路径
dst_path = '/home/carsonsow/pj/data/stock'

# 读取NASDAQ100的股票代码
index_fpth = '/home/carsonsow/pj/data/NASDAQ100.txt'
with open(index_fpth, 'r') as f:
    NASDAQ100 = [x.strip('\n') for x in f.readlines()]

# 获取历史股票数据
data_lst = []
for index in NASDAQ100:
    today_data = get_stock_data(index)
    data_lst.append(today_data)

# 分日期保存
prev_df = pd.concat(data_lst).sort_values(by='Date')
print(prev_df.shape)
print(prev_df.head(5))
for date, group in prev_df.groupby('Date'):
    cur_prev_df = group.reset_index(drop=True)
    cur_prev_df.to_csv('{}/{}.csv'.format(dst_path, date))