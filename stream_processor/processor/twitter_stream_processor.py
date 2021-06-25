import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list, current_timestamp, lit

from stream_processor.common.udfs import get_total_case_count_udf, process_tweet_udf
from stream_processor.processor.abstract_stream_processor import StreamProcessor


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TwitterStreamProcessor(StreamProcessor):
    def __init__(self, application_name: str, source: str):
        super().__init__(application_name=application_name)
        self.stream: DataFrame = (
            self.spark.readStream.format(source)
            .option("host", "localhost")
            .option("port", 5555)
            .load()
        )

    def process(self) -> DataFrame:
        """pre-processing events to remove #, RT: and URLs from the tweets."""
        try:
            self.stream = (
                self.stream.withColumn("processed_tweet", process_tweet_udf("value"))
                .withColumnRenamed(new="content", existing="processed_tweet")
                .drop("value")
            )
            return self.stream
        except Exception as e:
            logger.error(e)
            logger.error("Exception occurred.Stopping the application..")
            self._shutdown()
            raise

    def process_micro_batch(self, batch: DataFrame) -> DataFrame:
        """Assigning a static fixed key to get the list of tweets in a batch.
        This is also possible by using collect(), but it is better to avoid
        as it brings the data to driver.
        """
        return (
            batch.withColumn("key", lit(1))
            .groupby("key")
            .agg(collect_list("content").alias("content"))
            .withColumn("timestamp", current_timestamp())
            .withColumn("total_case_count", get_total_case_count_udf())
            .drop("key")
        )
