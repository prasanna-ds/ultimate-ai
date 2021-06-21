from unittest.mock import patch

from stream_processor.sources.twitter_source import TwitterSocketSource


def test_process(spark_session, twitter_raw_dataframe):
    test_twitter_stream = TwitterSocketSource(application_name="test-ultimate-ai")

    with patch.object(test_twitter_stream, 'spark', spark_session):
        with patch.object(test_twitter_stream, 'stream', twitter_raw_dataframe):
            df = test_twitter_stream.process()

    rows = df.collect()

    assert len(rows) == 3

    for row in rows:
        assert row["content"] and row["content"] is not None
