from unittest.mock import patch

from stream_processor.sources.twitter_stream_processor import TwitterStreamProcessor


def test_process(spark_session, twitter_raw_dataframe):
    test_twitter_stream = TwitterStreamProcessor(application_name="test-ultimate-ai")

    with patch.object(test_twitter_stream, "spark", spark_session):
        with patch.object(test_twitter_stream, "stream", twitter_raw_dataframe):
            df = test_twitter_stream.process()

    rows = df.collect()

    assert len(rows) == 3

    for row in rows:
        assert row["content"] and row["content"] is not None


def test_group_contents_in_a_batch(spark_session, twitter_raw_dataframe):
    test_twitter_stream = TwitterStreamProcessor(application_name="test-ultimate-ai")

    with patch.object(test_twitter_stream, "spark", spark_session):
        with patch.object(test_twitter_stream, "stream", twitter_raw_dataframe):
            processed_df = test_twitter_stream.process()
            df = test_twitter_stream.process_micro_batch(processed_df)

    rows = df.collect()

    assert len(rows) == 1

    assert rows[0]["content"] and isinstance(rows[0]["content"], list)
    assert rows[0]["timestamp"]
    assert rows[0]["total_case_count"]
