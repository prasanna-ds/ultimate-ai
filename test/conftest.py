import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import Row


@pytest.fixture(scope="session")
def spark_session():
    spark_session = SparkSession.builder.getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="session")
def twitter_raw_dataframe(spark_session):
    twitter_raw_dataframe = spark_session.createDataFrame(
        [
            Row(
                value="#Gregor then turned to look out the window at the dull weather.\n"
            ),
            Row(value="RT: It wasn't a dream.\n"),
            Row(value='"What\'s happened to me?" he thought. http://www.ultimate.ai\n'),
        ]
    )
    return twitter_raw_dataframe


class MockMemcacheClient(object):
    class KeyNoneError(Exception):
        pass

    def __init__(self):
        self.cache = {}

    def set(self, key, value):
        self.cache[key] = value

    def key_exists(self, key):
        if self.cache[key]:
            return True
        return False

    def get(self, key):
        if not self.key_exists(key):
            raise MockMemcacheClient.KeyNoneError
        return self.cache[key]
