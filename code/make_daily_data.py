import pandas as pd
import os
from pytz import timezone
import datetime
import numpy as np

def main():
    # 获取今日的日期
    # nowdate = datetime.datetime.now(timezone('US/Eastern'))
    # now = nowdate.strftime("%Y%m%d")
    # fname = '{}.csv'.format(now)

    # 获取所有未处理的日期
    stock_fpath = '/home/carsonsow/pj/data/stock'
    processed_fpath = '/home/carsonsow/pj/data/processed_tweets'
    save_fpath = '/home/carsonsow/pj/data/daily'
    existing_fnames = list(set(os.listdir(stock_fpath)).intersection(set(os.listdir(processed_fpath))))

    tobe_analyzed_fnames = list(set(existing_fnames).difference(os.listdir(save_fpath)))
    print('[TO BE analyzed] ({}): {}'.format(len(tobe_analyzed_fnames), tobe_analyzed_fnames))

    for fname in tobe_analyzed_fnames:
        nowdate = datetime.datetime.strptime(fname[:-4], '%Y%m%d')
        now = nowdate.strftime("%Y%m%d")
        if nowdate.weekday() > 4:
            return 0
            
        # 获取今天的stock数据
        stock_df = pd.read_csv(os.path.join(stock_fpath, fname), index_col=0).reset_index(drop=True)

        # 获取今天处理好的tweet情感数据
        processed_df = pd.read_csv(os.path.join(processed_fpath, fname), index_col=0).reset_index(drop=True)
        processed_df['Hour'] = processed_df['Hour'].astype(str)

        # 制作今天的数据集
        X_df = {'stock':[], 'Date':[], 'avg': []}
        for i in range(24):
            X_df[str(i)] = []

        for stock, group in processed_df.groupby('stock'):
            X_df['stock'].append(stock)
            X_df['Date'].append(now)
            
            for i in range(24):
                if str(i) in group['Hour'].tolist():
                    sents = group.loc[group['Hour']==str(i), 'compound'].values
                    X_df[str(i)].append(np.mean(sents))
                else:
                    X_df[str(i)].append(0)
            
            X_df['avg'].append(np.mean(group['compound'].values))
            
        X_df = pd.DataFrame.from_dict(X_df)

        # 和今日的股价数据join，获取prev_label
        X_df[['stock', 'Date']] = X_df[['stock', 'Date']].astype(str)
        stock_df[['index', 'Date']] = stock_df[['index', 'Date']].astype(str)

        X_df = X_df.merge(stock_df[['index', 'Date', 'prev_label', 'prev_rate']], left_on=['stock', 'Date'], right_on=['index', 'Date'], suffixes=(None, '_y'))\
                .filter(regex='^(?!.*_y)')\
                .reset_index(drop=True)
        
        # 保存
        X_df.to_csv('{}/{}.csv'.format(save_fpath, now))

if __name__ == '__main__':
    main()