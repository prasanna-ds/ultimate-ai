import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list, current_timestamp, lit
from pyspark.sql.streaming import StreamingQuery
from stream_processor.common.config import MONGODB_COLLECTION, MONGODB_DATABASE
from stream_processor.common.udfs import get_total_case_count_udf, process_tweet_udf
from stream_processor.sources.abstract_stream_processor import StreamProcessor


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TwitterStreamProcessor(StreamProcessor):
    def __init__(self, application_name: str):
        super().__init__(application_name=application_name)
        self.stream: DataFrame = (
            self.spark.readStream.format("socket")
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

    def group_contents_in_a_batch(self, batch: DataFrame) -> DataFrame:
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

    def write(self, stream: DataFrame, batch_id: int) -> None:
        logger.info("Writing a batch to Mongodb...")
        updated_stream = self.group_contents_in_a_batch(stream)
        (
            updated_stream.write.format("mongo")
            .mode("append")
            .option("database", MONGODB_DATABASE)
            .option("collection", MONGODB_COLLECTION)
            .save()
        )
        pass

    def write_stream(self) -> None:
        """Write the streaming data every n seconds as micro batches."""
        try:
            self.streaming_query: StreamingQuery = (
                self.stream.writeStream.trigger(processingTime="20 seconds")
                .foreachBatch(self.write)
                .start()
            )
            self.streaming_query.awaitTermination()
        except Exception as e:
            logger.error(e)
            logger.error(
                "Exception occurred while writing the data to Mongodb.Stopping the application.."
            )
            self._shutdown()
            raise
