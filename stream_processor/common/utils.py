from pymemcache.client.base import Client

from stream_processor.common.config import MEMCACHED_CONN_STR


def get_memcached_client() -> Client:
    return Client(MEMCACHED_CONN_STR)


def update_cache(key: str, value: int) -> None:
    client = get_memcached_client()
    client.set(key, value)


def get_corona_case_count_from_cache(key: str) -> int:
    client = get_memcached_client()
    return client.get(key).decode("utf-8")
