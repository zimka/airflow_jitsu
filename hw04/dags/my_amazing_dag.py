import datetime
import os
import logging

import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

BASE_URL = 'https://yastat.net/s3/milab/2020/covid19-stat/data/deep_history_struct_1.json'
STATIC_ROOT = os.environ.get("AIRFLOW_JITSU_STATIC_ROOT", '/var/cache/airflow_jitsu/files')

FILENAME_LOG = f'coronadata_{datetime.datetime.now().date()}.csv'
FILENAME_CURRENT = 'coronadata.csv'


def parse_to_df(data):
    """
    Парсит dict из api в pd.DataFrame нужного формата 
    """
    rudata = data['russia_stat_struct']
    dates = pd.Series(rudata['dates'], name='date').to_frame()

    raw = rudata['data']
    region_df_list = []
    for region_id, region_data in raw.items():
        region_name = region_data['info']['name']
        population = region_data['info']['population']
        if population > 25 * 10**6:
            # skip country-level stats
            continue
        infected = pd.DataFrame.from_dict(region_data['cases'])['v'].rename('infected')
        recovered = pd.DataFrame.from_dict(region_data['cured'])['v'].rename('recovered')
        if region_data['deaths']:
            dead = pd.DataFrame.from_dict(region_data['deaths'])['v'].rename('dead')
        else:
            # some regions don't have deaths (yet)
            dead = pd.Series(index=infected.index, data=0).rename('dead')
        region_df = dates.join(infected).join(recovered).join(dead)
        region_df['region'] = region_name
        region_df_list.append(region_df)
    total = pd.concat(region_df_list).set_index('date').sort_index()
    return total


def dump_corona_data():
    """
    Скачивает из апи данные и сохраняет дамп на диск
    """
    response = requests.get(BASE_URL)
    if not response.ok:
        raise RuntimeError(f"Can't load data {response} from {BASE_URL}")

    data = response.json()
    try:
        df = parse_to_df(data)
    except KeyError as exc:
        raise RuntimeError(f"Can't parse data: {exc}") from None

    df.to_csv(os.path.join(STATIC_ROOT, FILENAME_CURRENT))
    df.to_csv(os.path.join(STATIC_ROOT, FILENAME_LOG))


# TODO: different modules

default_args = {
    'owner': 'NOT-airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 6, 1, 9, 0, 0),
    'catchup_by_default': False # no backfill
}

dag = DAG(
    'corona_dag',
    default_args=default_args,
    description='Collects and dumps corona data.',
    schedule_interval=datetime.timedelta(days=1),
)

t1 = PythonOperator(
    task_id='collect_n_dump',
    depend_on_past=False,
    python_callable=dump_corona_data,
    dag=dag
)

