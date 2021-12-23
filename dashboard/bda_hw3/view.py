from django.http import HttpResponse
from django.shortcuts import render
import pandas_gbq
from google.oauth2 import service_account
import pandas as pd
import os

# Make sure you have installed pandas-gbq at first;
# You can use the other way to query BigQuery.
# please have a look at
# https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-nodejs
# To get your credential

credentials = service_account.Credentials.from_service_account_file(
    '/Users/tianchunhuang/Desktop/Columbia/EECS 6893 Big Data/hw3/big-data-6893-326220-735b9dd3db47.json')


def hello(request):
    context = {}
    context['content1'] = 'Hello World!'
    return render(request, 'helloworld.html', context)


def dashboard(request):
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = "big-data-6893-326220"

    SQL = "select substr(cast(time as string), 12, 5) as time, \
                           ai, \
                           data, \
                           good, \
                           movie, \
                           spark \
                  from `hw3.result` \
                  order by time \
                  limit 8"
    df = pandas_gbq.read_gbq(SQL)

    data_path = '/Users/tianchunhuang/Desktop/Columbia/EECS 6893 Big Data/bda_pj/dashboard/data'
    top = pd.read_csv(os.path.join(data_path, 'top.csv'))
    bottom = pd.read_csv(os.path.join(data_path, 'bottom.csv'))

    data = {'top5': [], 'bottom5': []}

    for _, row in top.iterrows():
        data['top5'].append(
            {   'stock': row['stock'],
                'sentiment score': row['score'],
                'probability': row['prob'],
                'yesterday': str(row['yesterday'])+'%'
            }
        )
    
    for _, row in bottom.iterrows():
        data['bottom5'].append(
            {   'stock': row['stock'],
                'sentiment score': row['score'],
                'probability': row['prob'],
                'yesterday': str(row['yesterday'])+'%'
            }
        )

    return render(request, 'dashboard.html', data)