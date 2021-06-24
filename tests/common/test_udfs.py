from unittest.mock import patch

from requests import Timeout
from stream_processor.common.udfs import get_total_case_count_udf, process_tweet_udf


def test_total_case_count_udf(twitter_raw_dataframe):
    df = twitter_raw_dataframe.withColumn(
        "total_case_count", get_total_case_count_udf()
    )
    rows = df.collect()

    assert len(rows) == 3

    for row in rows:
        assert row["total_case_count"] is not None
        assert isinstance(row["total_case_count"], int)


@patch("stream_processor.common.udfs.requests.get", side_effect=Timeout())
def test_total_case_count_udf_on_exception(mock_request, twitter_raw_dataframe):

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
