import requests
from typing import List
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.base import Engine
from hw2.utils import log_execution, BicycleCache


@log_execution
def publish_tg_message(token: str, chat_id: str, message: str):
    """
    Публикует сообщение в телеграм-бота
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    response = requests.post(
        url, json={"chat_id": chat_id, "text": message}, timeout=3
    )
    assert response.ok


@log_execution
def is_engine_available(engine: Engine) -> bool:
    """
    Проверяет, что соединение с sqlalcyemy-egine открывается.
    """
    try:
        conn = engine.connect()
        conn.detach()
    except SQLAlchemyError as exc:
        return False
    return True


def is_saved_data_sane() -> bool:
    """
    Проверяет что данные ок:
    - что они вообще существуют (из кэша пришел не None)
    - что имеют ненулевую длину
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

    dataframes = [orders, customers, goods, transactions]
    if any([df is None for df in dataframes]):
        return False

    if any([len(df) == 0 for df in dataframes]):
        return False

    # Здесь могла бы быть проверка, что все индексы уникальны, но
    # пайплайн так построен, что сначала данные сохраняются по-отдельности грязные,
    # а потом совместо чистятся, т.е. неуникальные индексы здесь - это ок.

    # if not dataframe.index.nunique() == len(dataframe):
    #    return False
    return True