from unittest.mock import patch

import pytest
import requests

from stream_processor.common.udfs import get_total_case_count_udf, process_tweet_udf
from test.conftest import MockMemcacheClient


def test_total_case_count_udf(twitter_raw_dataframe):
    mock_memcache = patch('stream_processor.common.utils.get_memcached_client')
    mock_memcache.return_value = MockMemcacheClient()
    df = twitter_raw_dataframe.withColumn(
        "total_case_count", get_total_case_count_udf()
    )
    rows = df.collect()

    assert len(rows) == 3

    for row in rows:
        assert row["total_case_count"] is not None
        assert isinstance(row["total_case_count"], int)


def test_total_case_count_udf_on_exception(twitter_raw_dataframe):
    mock_memcache = patch('stream_processor.common.utils.Client')
    mock_memcache.return_value = MockMemcacheClient()

    mock_total_case_count = patch("stream_processor.common.udfs.get_total_case_count_udf")
    mock_total_case_count.side_effect = requests.exceptions.ConnectionError

    df = twitter_raw_dataframe.withColumn(
        "total_case_count", get_total_case_count_udf()
    )
    rows = df.collect()

    assert len(rows) == 3

    for row in rows:
        assert row["total_case_count"] is not None
        assert isinstance(row["total_case_count"], int)


def test_process_tweet_udf(twitter_raw_dataframe):
    df = twitter_raw_dataframe.withColumn("content", process_tweet_udf("value"))
    rows = df.collect()

    assert len(rows) == 3
    assert (
            rows[0]["content"]
            == "Gregor then turned to look out the window at the dull weather."
    )
    assert rows[1]["content"] == "It wasn't a dream."
    assert rows[2]["content"] == '"What\'s happened to me?" he thought.'
