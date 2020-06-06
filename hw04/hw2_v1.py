import datetime
from enum import Enum
from functools import wraps
import logging
import os

import pandas as pd
import requests
from sqlalchemy import create_engine

LOG_PATH = '/home/ubuntu/tasks.log'
log = logging.getLogger('airflow_task_logger')
log.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_PATH)
fh.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
)
log.addHandler(fh)


def log_execution(func):
    @wraps(func)
    def verbose_func(*args, **kwargs):
        try:
            val = func(*args, **kwargs)
            log.info(f"Finished '{func.__name__}'")
            return val
        except Exception as exc:
            log.error(str(exc))
            raise
    return verbose_func


class BicycleCache:

    class Key(Enum):
        GOODS = 'goods.csv'
        ORDERS = 'orders.csv'
        CUSTOMERS = 'customers.csv'
        TRANS = 'transactions.csv'

    def __init__(self, root_path: str = '/tmp/'):
        self.root_path = root_path

    def set(self, key: Key, value: pd.DataFrame) -> bool:
        value.to_csv(os.path.join(self.root_path, str(key)))

    def get(self, key: Key) -> pd.DataFrame:
        return pd.read_csv(os.path.join(self.root_path, str(key)), index_col=0)


@log_execution
def read_orders(save=True):
    """
    Читает из csv заказы. Опционально сохраняет в кэш.
    `id заказа` считается PK - дубликаты выбрасываются с
     приоритетом первой записи.
    """
    url = 'https://airflow101.python-jitsu.club/orders.csv'
    expected_columns = ['id заказа', 'uuid заказа', 'название товара', 'дата заказа',
       'количество', 'ФИО', 'email']
    orders = pd.read_csv(url)

    assert orders.columns.tolist() == expected_columns
    orders.columns = ['id', 'uuid', 'good_title', 'date', 'amount', 'username', 'email']
    orders = orders.drop_duplicates(subset='id', keep='first')
    if save:
        BicycleCache().set(BicycleCache.Key.ORDERS, orders)
    return orders


@log_execution
def read_transactions(save=True):
    """
    Читает из csv заказы. Опционально сохраняет в кэш.
    index(uuid) считается PK - дубликаты выбрасываются с
    приоритетом первой записи (но вроде их нет здесь).
    """
    url = 'https://api.jsonbin.io/b/5ed7391379382f568bd22822'
    response = requests.get(url)
    assert response.ok
    trans = pd.read_json(response.text, orient='index')
    trans.index.name = 'uuid'
    trans = trans.reset_index().drop_duplicates(subset='uuid', keep='first')
    if save:
        BicycleCache().set(BicycleCache.Key.TRANS, trans)
    return trans


@log_execution
def read_customers_n_goods(save=True):
    """
    Читает из базы юзеров и товары. Опционально сохраняет в кэш.
    customers.id - PK, не проверяем дубликаты.
    Для goods.id - не PK, проверяются дубликаты. :(
    """
    sqa_string = "postgresql+psycopg2://shop:1ec4fae2cb7a90b6b25736d0fa5ff9590e11406@109.234.36.184:5432/postgres"
    engine = create_engine(sqa_string)
    customers = pd.read_sql_table('customers', engine)
    goods = pd.read_sql_table('goods', engine).drop_duplicates(subset='id', keep='first')

    if save:
        cache = BicycleCache()
        cache.set(BicycleCache.Key.CUSTOMERS, customers)
        cache.set(BicycleCache.Key.GOODS, goods)
    return customers, goods


@log_execution
def compile_dataset(orders=None, transactions=None, customers=None, goods=None, save=True):
    """
    Сохраняет объекдиненный датасет в базу.
    Читает данные из кэша, если он не переданы в аргументах.
    Данные джойнятся так, чтоб оставались только чистые записи
    (все отсутствующие ключи выбрасываются)
    """
    sqa_string = "postgresql+psycopg2://toy:toy:127.0.0.1:5432/sandbox"

    cache = BicycleCache()
    if orders is None:
        orders = cache.get(BicycleCache.Key.ORDERS)
    if transactions is None:
        transactions = cache.get(BicycleCache.Key.TRANS)
    if customers is None:
        customers = cache.get(BicycleCache.Key.CUSTOMERS)
    if goods is None:
        goods = cache.get(BicycleCache.Key.GOODS)

    # Join всего, везде INNER - оставляем только чистые данные
    total = orders.join(
        transactions.set_index('uuid'), on='uuid', how='inner'
    ).join(
        customers.set_index('email'), on='email', how='inner', rsuffix='_customer',
    ).join(
        goods.set_index('name'), on='good_title', how='inner', rsuffix='_good'
    )
    total = total[['username', 'good_title', 'birth_date', 'success', 'amount', 'price', 'date']]
    total['name'] = total.pop('username')
    now = datetime.datetime.now()
    birth = pd.to_datetime(total.pop('birth_date'))
    total['age'] = (
        -(birth - now).dt.days / 365.25
    ).astype(int)
    total['payment_status'] = total.pop('success')
    total['total_price'] = total.pop('price') * total['amount']
    total = total[['name', 'age', 'good_title', 'date', 'payment_status', 'total_price', 'amount']]
    return total



