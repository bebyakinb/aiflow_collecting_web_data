import logging
import requests
import os
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

SCHEDULE_INTERVAL = '10 9 * * *'
DEFAULT_ARGS = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 16),
}

TOKEN = Variable.get('YA_TOKEN')
URL = f"https://api-metrika.yandex.net/management/v1/counter/{Variable.get('YA_COUNTER')}"
AUTHORIZATION = {'Authorization': f'OAuth {TOKEN}'}

DB_TABLE_NAME = 'Visits'
FIELDS = (
    'ym:s:visitID',
    'ym:s:dateTime',
    'ym:s:isNewUser',
    'ym:s:visitDuration',
    'ym:s:ipAddress',
    'ym:s:regionCountry',
    'ym:s:regionCity',
    'ym:s:clientID',
    'ym:s:lastTrafficSource',
    'ym:s:referer',
)
PARAMS = {
    'date1': (datetime.strptime('2022-02-23', '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d'),
    'date2': (datetime.strptime('2022-02-23', '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d'),
    'fields': ",".join(FIELDS),
    'source': 'visits',
}


def get_request(url, headers=None, params=None):
    return requests.get(url=url, headers=headers, params=params).json()


def post_request(url, headers=None, params=None):
    return requests.post(url=url, headers=headers, params=params).json()


def table_get_request(url, headers=None, params=None):
    r = requests.get(url=url, headers=headers, params=params)
    return pd.read_csv(StringIO(r.text), sep='\t')


def log_status(log_request_id):
    log_status_response = get_request(url=URL + f'/logrequest/{log_request_id}',
                                      headers=AUTHORIZATION)
    log.info(log_status_response)
    is_log_ready = log_status_response['log_request']['status'] == 'processed'
    if is_log_ready:
        return {'is_ready': is_log_ready,
                'parts_num': len(log_status_response['log_request']['parts'])}
    return {'is_ready': is_log_ready,
            'parts_num': 0}


def get_log(log_request_id, parts_num):
    result_df = pd.DataFrame()
    for part_num in range(parts_num):
        log.info(f'part number: {part_num} is ready to be request')
        response_df = table_get_request(url=URL + f'/logrequest/{log_request_id}/part/{part_num}/download',
                                        headers=AUTHORIZATION)
        result_df = pd.concat([result_df, response_df])
    return result_df


@dag(default_args=DEFAULT_ARGS,
     schedule_interval=SCHEDULE_INTERVAL,
     catchup=False)
def ya_metrica_dag():

    @task
    def log_creation_check():
        context_date = get_current_context()['ds']
        parameters = {
            'date1': (datetime.strptime(context_date, '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d'),
            'date2': (datetime.strptime(context_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d'),
            'fields': ",".join(FIELDS),
            'source': 'visits',
        }

        log_creation_check_response = get_request(url=URL + f'/logrequests/evaluate',
                                                  headers=AUTHORIZATION,
                                                  params=parameters)
        if log_creation_check_response['log_request_evaluation']['possible']:
            return parameters
        raise ValueError("Impossible to create log with this set of parameters.")

    @task()
    def log_create_request(parameters):
        log_creation_response = post_request(url=URL + f'/logrequests',
                                             headers=AUTHORIZATION,
                                             params=parameters)
        return log_creation_response['log_request']['request_id']



    @task()
    def check_and_get_log(log_request_id, delay_minutes=10, retries=10):
        for i in range(retries):
            log.info(f'Trying to get data for {i+1} time')
            status = log_status(log_request_id=log_request_id)
            if status['is_ready']:
                return get_log(log_request_id=log_request_id,
                               parts_num=status['parts_num'])
            time.sleep(60 * delay_minutes)  # Delay for 'delay_minutes' minute (60 seconds).
        raise ValueError("TimeOut. log status didn't changed to \'processed\'")

    @task()
    def delete_log(log_request_id):
        log.info(post_request(url=URL + f'/logrequest/{log_request_id}/clean',
                              headers=AUTHORIZATION))

    create_table_sqlite = SqliteOperator(
        task_id="create_table_sqlite",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME} (
                {''.join([x[5:] + ' TEXT, ' for x in FIELDS])[:-2]}
            );
            """,
    )

    @task()
    def transform_log_for_sqlite(df):
        df.rename(columns=dict(zip(FIELDS, [x[5:] for x in FIELDS])),
                  inplace=True)
        df['visitID'] = df['visitID'].astype(str)
        log.info(df.columns)
        return(df)

    @task()
    def load_logs_to_sqlite(df):
        conn = SqliteHook().get_conn()
        query = f"""select * from {DB_TABLE_NAME} order by dateTime asc limit 5"""
        current_table_state = pd.read_sql(query, conn)

        log.info("BEFORE ADD NEW ROWS: " + current_table_state.to_string())
        df.to_sql(DB_TABLE_NAME,
                  conn,
                  if_exists='append',
                  index=False
                  )

        query = f"""select * from {DB_TABLE_NAME} order by dateTime asc limit 5"""
        current_table_state = pd.read_sql(query, conn)
        log.info("AFTER ADD NEW ROWS" + current_table_state.to_string())

        conn.commit()
        conn.close()

    parameters = log_creation_check()
    log_request_id = log_create_request(parameters)
    ya_metrics_log = check_and_get_log(log_request_id=log_request_id,
                                       delay_minutes=10,
                                       retries=10)
    transformed_ya_metrics_logs = transform_log_for_sqlite(ya_metrics_log)
    create_table_sqlite >> load_logs_to_sqlite(transformed_ya_metrics_logs)\
                        >> (delete_log(log_request_id=log_request_id))

ya_metrica_dag = ya_metrica_dag()
