import pandas as pd
import numpy as np
import pickle as pkl
import datetime
from pytz import timezone
import os

def get_next_business_day_from_str(date_str):
    nowdate = datetime.datetime.strptime(date_str, '%Y%m%d')
    next_business_date = nowdate + datetime.timedelta(days= 7-nowdate.weekday() if nowdate.weekday()>3 else 1)
    next_business_date_str = next_business_date.strftime("%Y%m%d")
    return next_business_date_str

def main():
    # 导入今日数据
    # nowdate = datetime.datetime.now(timezone('US/Eastern'))
    # now = nowdate.strftime("%Y%m%d")

    # 获取所有未处理的数据
    daily_fpath = '/home/carsonsow/pj/data/daily'
    save_fpath = '/home/carsonsow/pj/data/daily_pred'

    tobe_analyzed_fnames = os.listdir(daily_fpath)
    print('[TO BE analyzed] ({}): {}'.format(len(tobe_analyzed_fnames), tobe_analyzed_fnames))

    for fname in tobe_analyzed_fnames:
        nowdate = datetime.datetime.strptime(fname[:-4], '%Y%m%d')
        now = nowdate.strftime("%Y%m%d")
        if nowdate.weekday() > 4:
            return 0

        # 检查是否已存在pred
        next_business_date = nowdate + datetime.timedelta(days= 7-nowdate.weekday() if nowdate.weekday()>3 else 1)
        next_business_date_str = next_business_date.strftime("%Y%m%d")
        pred_fpath = '{}/{}.csv'.format(save_fpath, next_business_date_str)
        if os.path.exists(pred_fpath):
            continue
        
        print('Processing {}...'.format(now))
        data_fpath = '{}/{}.csv'.format(daily_fpath, now)
        X_df = pd.read_csv(data_fpath, index_col=0).reset_index(drop=True)

        # 获取今日数据的特征
        feat_colnames = ['avg', 'prev_rate']
        X = X_df[feat_colnames].values

        # 导入模型
        model_fpath = '/home/carsonsow/pj/model/avg_svm.sav'
        loaded_model = pkl.load(open(model_fpath, 'rb'))
        result = loaded_model.predict_proba(X)[:,1]

        # 生成结果
        res_df = X_df.copy()[['stock']]
        res_df['pred_date'] = next_business_date_str

        res_df['pred_tomorrow'] = result
        res_df = res_df.sort_values(by='pred_tomorrow', ascending=False).reset_index(drop=True)

        res_df.to_csv('{}/{}.csv'.format(save_fpath, next_business_date_str))

if __name__ == '__main__':
    main()