# -*- coding: utf-8 -*-

import os
import pandas as pd
import yfinance as yf
from pytz import timezone
import datetime

def get_stock_data(index):
    data = yf.Ticker(index.upper())
    today_data = data.history(period='2d').reset_index()

    if today_data.shape[0] != 2 or len(set(today_data['Date'])) != 2:
        return None

    today_data['index'] = index.upper()
    today_data['Date'] = today_data['Date'].apply(lambda x: x.strftime('%Y%m%d'))
    today_data = today_data.sort_values(by='Date')

    today_data['prev_label'] = None
    today_data.loc[1, 'prev_label'] = today_data.loc[1, 'Close']>=today_data.loc[0, 'Close']

    today_data['prev_rate'] = None
    today_data.loc[1, 'prev_rate'] = (today_data.loc[1, 'Close']-today_data.loc[0, 'Close'])/today_data.loc[0, 'Close']

    today_data = today_data.loc[[1], :]
    return today_data

def main():
    nowdate = datetime.datetime.now(timezone('US/Eastern'))
    now = nowdate.strftime("%Y%m%d")
    if nowdate.weekday() > 4:
        return 0

    # 读取NASDAQ100的股票代码
    index_fpth = '/home/carsonsow/pj/data/NASDAQ100.txt'
    with open(index_fpth, 'r') as f:
        NASDAQ100 = [x.strip('\n') for x in f.readlines()]

    # 获取历史股票数据
    data_lst = []
    for index in NASDAQ100:
        today_data = get_stock_data(index)
        if today_data is not None:
            data_lst.append(today_data)

    # 保存
    dst_path = '/home/carsonsow/pj/data/stock'
    today_df = pd.concat(data_lst).reset_index(drop=True)
    today_df.to_csv('{}/{}.csv'.format(dst_path, now))

if __name__ == '__main__':
    main()