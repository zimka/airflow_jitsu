import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from hw2.utils import read_orders, read_transactions, read_customers_n_goods, compile_dataset
from hw2.model import OrderDataset


READ_POSTGRES_HOOK_ID = 'read_pg_hook_id'
WRITE_POSTGRES_HOOK_ID = 'write_pg_hook_id'


default_args = {
    'owner': 'airbender',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 6, 7, 9, 0, 0),
    'catchup_by_default': False # no backfill
}

with DAG(
        'DAG_ORDERS_DATASET_HW2',
        default_args=default_args,
        description='Collects and dumps orders data from different sources.',
        schedule_interval=datetime.timedelta(hours=4),
        ) as dag:

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

    read_pg_engine = PostgresHook(conn_id=READ_POSTGRES_HOOK_ID).get_sqlalchemy_engine()
    customers_n_goods_step = PythonOperator(
        task_id='customers_n_goods_step',
        python_callable=lambda: read_customers_n_goods(engine=read_pg_engine),
        dag=dag
    )

    write_pg_engine = PostgresHook(conn_id=WRITE_POSTGRES_HOOK_ID).get_sqlalchemy_engine()

    def save_dataset():
        dataset = compile_dataset()
        OrderDataset.dump_dataset(dataset=dataset, engine=write_pg_engine)

    save_dataset_step = PythonOperator(
        task_id='save_dataset',
        python_callable=save_dataset,

    )

    order_step >> trans_step >> customers_n_goods_step >> save_dataset_step

