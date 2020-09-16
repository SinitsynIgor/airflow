from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def read_table_send_message():
    import pandas as pd
    import requests
    import json
    from urllib.parse import urlencode

    # read url
    url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR-ti6Su94955DZ4Tky8EbwifpgZf_dTjpBdiVH0Ukhsq94jZdqoHuUytZsFZKfwpXEUCKRFteJRc9P/pub?gid=889004448&single=true&output=csv'
    df = pd.read_csv(url)

    # grouping
    data = df.groupby(['date', 'event', 'ad_cost'], as_index=False) \
        .agg({'time': 'count'}) \
        .pivot(index=['date', 'ad_cost'], columns='event', values='time') \
        .reset_index() \
        .round(2)

    # count CTR, cost per day
    data = round(data.assign(ctr=data.click / data.view * 100), 2)
    data = round(data.assign(cost_day=data.ad_cost / 1000 * data.view), 2)

    # selection columns
    full_data = data[['click', 'view', 'ctr', 'cost_day']]

    # difference calculation
    diff = round((full_data.loc[1] - full_data.loc[0]) / full_data.loc[0] * 100)

    # Series to Dataframe
    data_diff = pd.DataFrame({'metrics': diff.index,
                              'difference': diff.values})

    # writing to file
    with open('metrics.txt', 'w+') as f:
        f.write(
            f'''Отчет по объявлению 121288 за 2 апреля
    Траты: {full_data['cost_day'][1]} рублей ({data_diff['difference'][3]}%)
    Показы: {full_data['view'][1]} ({data_diff['difference'][1]}%)
    Клики: {full_data['click'][1]} ({data_diff['difference'][0]}%)
    CTR: {full_data['ctr'][1]} ({data_diff['difference'][2]}%)
    '''
        )

    # вконтакте не использую, поэтому отправляю в телеграм
    token = '1292472120:AAFFDL2FSecU7Y39fpyTN3hxGSKqrgjbpM'
    chat_id = 34681220

    chat_id = chat_id

    params = {'chat_id': chat_id, 'caption': 'Here is your analytics file'}

    base_url = f'https://api.telegram.org/bot{token}/'
    url = base_url + 'sendDocument?' + urlencode(params)
    filepath = 'metrics.txt'
    files = {'document': open(filepath, 'rb')}
    # sending file to telegram
    resp = requests.get(url, files=files)

# default arguments
default_args = {
    'owner': 'isinitsyn',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'retries': 0
}

# dag parameters
dag = DAG('pycharm_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval='00 12   * * 1')

# task
t1 = PythonOperator(
    task_id='write_report',
    python_callable=read_table_send_message,
    dag=dag)