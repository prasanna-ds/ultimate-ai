import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

from stream_processor.common.udfs import get_total_case_count, process_tweet
from stream_processor.sources.abstract_source import Source


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TwitterSocketSource(Source):
    def __init__(self, application_name: str):
        super().__init__(application_name=application_name)
        self.stream = (
            self.spark.readStream.format("socket")
            .option("host", "localhost")
            .option("port", 5555)
            .load()
        )

    def process(self) -> DataFrame:
        try:
            self.stream = (
                self.stream.withColumn("processed_tweet", process_tweet("value"))
                .withColumn("timestamp", current_timestamp())
                .withColumn("total_case_count", get_total_case_count())
                .drop("value")
                .withColumnRenamed("content", "processed_tweet")
            )
            return self.stream
        except Exception as e:
            logger.error(e)
            logger.error("Exception occurred.Stopping the application..")
            self.shutdown()
