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
    df = spark_session.createDataFrame(
        [
            Row(
                value="#Gregor then turned to look out the window at the dull weather.\n"
            ),
            Row(value="RT: It wasn't a dream.\n"),
            Row(value='"What\'s happened to me?" he thought. http://www.ultimate.ai\n'),
        ]
    )
    return df
