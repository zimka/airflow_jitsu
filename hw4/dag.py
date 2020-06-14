import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator, BranchPythonOperator

from hw2.utils import read_orders, read_transactions, read_customers_n_goods, compile_dataset
from hw2.dag import save_dataset, read_pg_engine, write_pg_engine
from hw4.utils import publish_tg_message, is_engine_available, is_saved_data_sane

TG_CHAT_ID = Variable.get('tg_chat_id')
TG_TOKEN = Variable.get('tg_token')


default_args = {
    'owner': 'Boris Zimka',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 6, 14, 9, 0, 0),
    'catchup_by_default': False # no backfill
}


def check_data_func(**kwargs):
    """
    Если данные в норме - возвращает task_id шага
    сохранения датасета, иначе пушит в xcom текст ошибки
    и возвращает task_id шага публикации tg-уведомления
    """
    sane = is_saved_data_sane()
    if sane:
        return 'save_dataset_step'
    error_message = f"""
    Failed to save dataset, data is broken.
    Dag({kwargs['dag']}), Task({kwargs['task'].task_id})
    """
    kwargs['ti'].xcom_push(key='error_message', value=error_message)
    return 'alert_telegram_step'


def alert_telegram_func(**kwargs):
    """
    Посылает в чат сообщение из xcom
    """
    error_message = kwargs['ti'].xcom_pull(key='error_message')
    publish_tg_message(
        TG_TOKEN,
        TG_CHAT_ID,
        error_message
    )


with DAG(
        'DAG_ORDERS_DATASET_WITH_BELLS_N_WHISTLES_HW4',
        default_args=default_args,
        description='Collects and dumps orders data from different sources.'\
        ' Sends msg to telegram if something goes wrong.',
        schedule_interval=datetime.timedelta(hours=4),
        ) as dag:

    check_db = lambda: is_engine_available(read_pg_engine) and is_engine_available(write_pg_engine)
    check_db_step = ShortCircuitOperator(
        task_id='check_db_step',
        python_callable=check_db,
        dag=dag
    )

    order_step = PythonOperator(
        task_id='order_step',
        python_callable=read_orders,
        dag=dag
    )

    trans_step = PythonOperator(
        task_id='trans_step',
        python_callable=read_transactions,
        dag=dag
    )

    customers_n_goods_step = PythonOperator(
        task_id='customers_n_goods_step',
        python_callable=lambda: read_customers_n_goods(engine=read_pg_engine),
        dag=dag
    )

    check_data_step = BranchPythonOperator(
        task_id='check_data_step',
        python_callable=check_data_func,
        dag=dag
    )

    save_dataset_step = PythonOperator(
        task_id='save_dataset_step',
        python_callable=save_dataset,
        dag=dag
    )

    alert_telegram_step = PythonOperator(
        task_id='alert_telegram_step',
        python_callable=alert_telegram_func,
        dag=dag
    )

    check_db_step >> order_step >> trans_step >> customers_n_goods_step
    customers_n_goods_step >> check_data_step >> [save_dataset_step, alert_telegram_step]

