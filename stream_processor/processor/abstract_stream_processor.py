from __future__ import annotations

import logging
import signal
import time

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from stream_processor.common.config import (
    BATCH_DURATION,
    MONGODB_COLLECTION,
    MONGODB_CONNECTION_STR,
    MONGODB_DATABASE,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StreamProcessor(ABC):
    def __init__(self, application_name: str):
        self.spark = (
            SparkSession.builder.appName(application_name)
            .config("spark.mongodb.input.uri", MONGODB_CONNECTION_STR)
            .config("spark.mongodb.output.uri", MONGODB_CONNECTION_STR)
            .getOrCreate()
        )
        self.stream: DataFrame = self.spark.createDataFrame([], StructType([]))
        self.streaming_query: StreamingQuery = None  # type: ignore

        # Catch Signals for graceful shutdown.
        signal.signal(signal.SIGTERM, lambda x, y: self._shutdown())
        signal.signal(signal.SIGINT, lambda x, y: self._shutdown())

    @abstractmethod
    def process(self) -> DataFrame:
        raise NotImplementedError

    def process_micro_batch(self, stream: DataFrame) -> DataFrame:
        return stream

    def write(self, stream: DataFrame, batch_id: int) -> None:
        logger.info("Writing a batch to Mongodb...")
        (
            self.process_micro_batch(stream)
            .write.format("mongo")
            .mode("append")
            .option("database", MONGODB_DATABASE)
            .option("collection", MONGODB_COLLECTION)
            .save()
        )
        pass

    def write_stream(self) -> None:
        """Write the streaming data every n seconds as micro batches."""
        try:
            self.streaming_query = (
                self.stream.writeStream.trigger(processingTime=BATCH_DURATION)
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

    def _shutdown(self) -> None:
        """Shutdown handler to safely shutdown the running streaming query by
        checking query status and in-flight messages.
        """
        logging.info("Received shutdown signal, stopping stream processing...")
        while self.streaming_query.isActive:
            message: str = self.streaming_query.status["message"]
            data_available: bool = self.streaming_query.status["isDataAvailable"]
            trigger_active: bool = self.streaming_query.status["isTriggerActive"]
            if (
                not data_available
                and not trigger_active
                and message != "Initializing sources"
            ):
                self.streaming_query.stop()
            time.sleep(10)

        self.streaming_query.awaitTermination(10)
