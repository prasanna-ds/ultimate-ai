import pytest

from stream_processor.common.utils import Cache


@pytest.fixture(scope="session")
def cache():
    return Cache()


def test_cache_singleton(cache):
    another_cache = Cache()
    assert id(cache) == id(another_cache)


def test_key_set_get(cache):
    cache.set("total_case_count", 100)
    assert cache.get("total_case_count") == 100


def test_key_not_found_error(cache):
    with pytest.raises(Cache.KeyNotFoundError):
        cache.get("total_case_count_new")


def test_key_update(cache):
    cache.set("total_case_count", 1000)
    assert cache.get("total_case_count") == 1000
