import datetime
from enum import Enum
from functools import wraps
import logging
import os

import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine

log = logging.getLogger('hw2_logger')
log.setLevel(logging.INFO)
fh = logging.FileHandler('/home/ubuntu/tasks.log')
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
        value.to_csv(os.path.join(self.root_path, str(key.value)))

    def get(self, key: Key) -> pd.DataFrame:
        data_path = os.path.join(self.root_path, str(key.value))
        if not os.path.exists(data_path):
            return None
        return pd.read_csv(data_path, index_col=0)


@log_execution
def read_orders(url=None, save=True):
    """
    Читает из csv заказы. Опционально сохраняет в кэш.
    `id заказа` считается PK - дубликаты выбрасываются с
     приоритетом первой записи.
    """
    if url is None:
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
def read_transactions(url=None, save=True):
    """
    Читает из csv заказы. Опционально сохраняет в кэш.
    index(uuid) считается PK - дубликаты выбрасываются с
    приоритетом первой записи (но вроде их нет здесь).
    """
    if url is None:
        url = 'https://api.jsonbin.io/b/5ed7391379382f568bd22822'
    response = requests.get(url, timeout=3)
    assert response.ok
    trans = pd.read_json(response.text, orient='index')
    trans.index.name = 'uuid'
    trans = trans.reset_index().drop_duplicates(subset='uuid', keep='first')
    if save:
        BicycleCache().set(BicycleCache.Key.TRANS, trans)
    return trans


@log_execution
def read_customers_n_goods(engine=None, save=True):
    """
    Читает из базы юзеров и товары. Опционально сохраняет в кэш.
    customers.id - PK, не проверяем дубликаты.
    Для goods.id - не PK, проверяются дубликаты. :(
    """
    if engine is None:
        engine = get_debug_read_engine()
    customers = pd.read_sql_table('customers', engine)
    goods = pd.read_sql_table('goods', engine).drop_duplicates(subset='id', keep='first')

    if save:
        cache = BicycleCache()
        cache.set(BicycleCache.Key.CUSTOMERS, customers)
        cache.set(BicycleCache.Key.GOODS, goods)
    return customers, goods


@log_execution
def compile_dataset(orders=None, transactions=None, customers=None, goods=None):
    """
    Сохраняет объекдиненный датасет в базу.
    Читает данные из кэша, если он не переданы в аргументах.
    Данные джойнятся так, чтоб оставались только чистые записи
    (все отсутствующие ключи выбрасываются)
    """
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
    dataset = orders.join(
        transactions.set_index('uuid'), on='uuid', how='inner'
    ).join(
        customers.set_index('email'), on='email', how='inner', rsuffix='_customer',
    ).join(
        goods.set_index('name'), on='good_title', how='inner', rsuffix='_good'
    ).set_index('uuid')
    dataset = dataset[['username', 'good_title', 'birth_date', 'success', 'amount', 'price', 'date']]
    dataset['name'] = dataset.pop('username')
    now = datetime.datetime.now()
    birth = pd.to_datetime(dataset.pop('birth_date'))
    dataset['age'] = (
        -(birth - now).dt.days / 365.25
    ).astype(int)
    dataset['payment_status'] = dataset.pop('success')
    dataset['total_price'] = np.round(dataset.pop('price') * dataset['amount'], 2)
    dataset = dataset[['name', 'age', 'good_title', 'date', 'payment_status', 'total_price', 'amount']]
    return dataset


def get_debug_read_engine():
    """
    Получение connection не через hook, а через connection_string из env
    """
    sqa_string = os.environ.get("AIRFLOW_DEBUG_READ_ENGINE_CONNECTION_STRING")
    engine = create_engine(sqa_string)
    return engine
